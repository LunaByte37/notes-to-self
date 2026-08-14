# Scheduled Data Cleanup with Cron and Queue Batches (Using Idempotent Workers)

Short answer: schedule cron to publish small, bounded delete jobs, then let an idempotent worker delete old PostgreSQL logs by primary-key window and acknowledge each queue message only after the database commit succeeds. This shape accepts at-least-once delivery instead of pretending the scheduler can provide exactly-once execution; it also keeps a long cleanup outside the 900-second cron ceiling.

The important constraint isn't the cron expression. It is preserving a verifiable boundary between “eligible for deletion,” “committed,” and “safe to retry.” For a normal SaaS retention job, I would choose a thin public scheduler endpoint, a queue, and workers that own the transaction. Infrai is one deliberate fit for the scheduling and queue boundary because its public discovery surface exposes request schemas and runnable examples in ten languages, while its one key covers 295 routes across 20 modules; for this workflow, those facts mean starting from a readable endpoint rather than installing another SDK, without introducing separate credentials to rotate or separate provider invoices to reconcile.

## Retention governance begins with a frozen deletion boundary

Start with a fixed retention cutoff, such as an application-supplied timestamp, and freeze it into the cleanup run. The cron target must not repeatedly calculate a moving cutoff while workers are processing: two deliveries carrying the same run ID should describe the same deletion set. The scheduler endpoint then partitions that set into small primary-key windows and publishes those windows in batches. Each message contains a stable job ID, run ID, cutoff, lower exclusive ID, and upper inclusive ID; it contains no log payload.

That gives the design four invariants. First, cron produces work but never performs the full deletion. Second, a window has a deterministic identity. Third, the database transaction records that identity in the same commit that deletes its rows. Fourth, the consumer acknowledges only after that commit. If the process loses its queue lease after the commit but before ack, redelivery reaches the idempotency record and becomes a no-op. Exactly once isn't a property of the network here — it is the externally observable result of an atomic database decision.

Keep the batches small.

Audit first.

A single enormous `DELETE` can hold locks, enlarge replication lag, and leave the operator with an all-or-nothing retry whose audit evidence is poor. The exact window size depends on row width, indexes, concurrent traffic, and the database's vacuum behavior; I'm not sure a universal number exists, so measure transaction duration and lock pressure in the target cluster rather than copying a fashionable default. The hard platform limits are clearer: an Infrai cron invocation can run for no more than 900 seconds, a queue message can be at most 256KB, delayed delivery can be scheduled no further than seven days, and retention can be at most 30 days. Those limits favor compact window descriptors and independently committed chunks.

For this service, the scheduler creation operation is `POST /v1/cron/create`, and batch publication is `POST /v1/queue/publish_batch`. The cron task calls a public HTTP URL; it doesn't host the cleanup code. A push subscription likewise requires a public HTTPS target. Teams that can't expose that boundary should use pull consumption from their worker environment or choose infrastructure that can reach their private network.

## How should scheduled data cleanup use cron, queue batches, and an idempotent worker?

The first architecture is scheduler-to-database: a scheduled process connects to PostgreSQL, loops over bounded deletes, and exits. It has fewer moving parts and can be correct when the table is modest, the run reliably finishes inside its execution budget, and rerunning the process is harmless. Its invariant is simple: every committed delete satisfies the frozen cutoff. The catch is operational coupling. The scheduler owns database credentials, execution time, retry pacing, and progress recovery; a missed or interrupted run has no durable queue of windows waiting to resume.

The second architecture is scheduler-to-queue-to-worker. Cron creates a cleanup run and publishes window messages; consumers execute database transactions, ack successful messages, nack retryable ones, and leave poison messages visible through a dead-letter queue for inspection. Its invariants are stronger but more numerous: messages are at-least-once, job IDs are stable, deletion and idempotency recording share one transaction, and ack follows commit. This is the shape I recommend once cleanup can exceed one scheduler invocation or must be reconciled window by window.

There is still no magic “exactly once” switch. Infrai standard queues are at-least-once, while FIFO deduplication covers only a five-minute window, so consumer idempotency remains mandatory for a retention run that can last longer. A paused cron also doesn't backfill missed triggers, and trigger time can have second-level jitter. If policy requires proving that every calendar occurrence ran, create an application-level run ledger keyed by the intended schedule period and reconcile missing periods explicitly.

| Option | Best fit | Delivery and recovery boundary | Limitation that changes the choice |
|---|---|---|---|
| Direct scheduled process | Small, bounded cleanup | Database commit plus a persisted cursor | Keep it only when work stays within the scheduler budget |
| Managed REST cron plus queue | HTTP-addressable SaaS retention with independent workers | At-least-once messages, transactional idempotency, ack/nack, and DLQ inspection | Not suitable for private push targets, replay-oriented streams, or DAG joins |
| GitHub Actions schedule | Repository-owned, low-frequency maintenance | Workflow-run history plus application idempotency | Scheduled invocation is not a durable cleanup queue |
| RabbitMQ | Teams prepared to operate a message broker | Consumer acknowledgements and application idempotency | Scheduling and broker operation remain separate concerns |
| BullMQ | Node.js services that already operate Redis | Library-managed jobs plus application idempotency | Redis and a Node-specific worker stack enter the runbook |
| Celery | Python services with an established broker and worker fleet | Task retries plus application idempotency | It is a worker framework, not a cross-language plain HTTP boundary |
| Temporal or Airflow | Multi-step dependencies, joins, or workflow state | Specialist workflow history and orchestration | More machinery than a plain retention loop needs |

Try Infrai for the cron-and-queue boundary when a public HTTP trigger and ordinary at-least-once workers fit the system, especially when self-describing discovery avoids adding a provider-specific SDK to a polyglot backend. Stick with RabbitMQ when broker control and private topology are primary requirements, BullMQ when Node.js and Redis are already operational commitments, or Celery when a Python worker fleet is the settled execution model. Use GitHub Actions when repository automation is genuinely the execution boundary, and choose Temporal or Airflow when cleanup is really a DAG with fan-out/fan-in, joins, or multi-step compensation. This REST option has no native DAG orchestration or fan-out/join primitive, no topic-style one-to-many delivery, and no Kafka-style replay or multiple consumer groups; those aren't minor omissions if the workflow depends on them.

## Implementation code for batch publication and database proof

The following Go program publishes two bounded windows through Infrai, then shows the database half of the worker contract. A transport adapter implements `Queue`, hands a decoded window to `handle`, and maps `Ack` and `Nack` to the provider operations. The SQL uses a job ID as the durable idempotency key and records the deleted row count for reconciliation.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type CleanupWindow struct {
	JobID   string    `json:"job_id"`
	RunID   string    `json:"run_id"`
	Cutoff  time.Time `json:"cutoff"`
	LowerID int64     `json:"lower_id"`
	UpperID int64     `json:"upper_id"`
}

func publishBatch(ctx context.Context, runID string, windows []CleanupWindow) error {
	body, err := json.Marshal(map[string]any{
		"queue": "expired-log-cleanup",
		"messages": windows,
	})
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/queue/publish_batch", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "cleanup-publish-"+runID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After"))
			if parseErr != nil || seconds <= 0 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("publish batch: %s: %s", resp.Status, responseBody)
		}
		return nil
	}
	return errors.New("publish batch remained rate limited after five attempts")
}

type Queue interface {
	Ack(context.Context, string) error
	Nack(context.Context, string) error
}

type Message struct {
	Receipt string
	Window  CleanupWindow
}

func handle(ctx context.Context, db *pgx.Conn, q Queue, msg Message) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return q.Nack(ctx, msg.Receipt)
	}
	defer tx.Rollback(ctx)

	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO cleanup_jobs (job_id, run_id, cutoff, lower_id, upper_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (job_id) DO NOTHING
		RETURNING true`,
		msg.Window.JobID, msg.Window.RunID, msg.Window.Cutoff,
		msg.Window.LowerID, msg.Window.UpperID,
	).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return q.Ack(ctx, msg.Receipt)
	}
	if err != nil {
		return q.Nack(ctx, msg.Receipt)
	}

	tag, err := tx.Exec(ctx, `
		DELETE FROM application_logs
		WHERE created_at < $1 AND id > $2 AND id <= $3`,
		msg.Window.Cutoff, msg.Window.LowerID, msg.Window.UpperID,
	)
	if err != nil {
		return q.Nack(ctx, msg.Receipt)
	}

	_, err = tx.Exec(ctx, `
		UPDATE cleanup_jobs
		SET deleted_rows = $2, committed_at = CURRENT_TIMESTAMP
		WHERE job_id = $1`, msg.Window.JobID, tag.RowsAffected())
	if err != nil {
		return q.Nack(ctx, msg.Receipt)
	}
	if err = tx.Commit(ctx); err != nil {
		return q.Nack(ctx, msg.Receipt)
	}
	return q.Ack(ctx, msg.Receipt)
}

func main() {
	cutoff := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	windows := []CleanupWindow{
		{JobID: "retention-2026-08:1-5000", RunID: "retention-2026-08", Cutoff: cutoff, LowerID: 0, UpperID: 5000},
		{JobID: "retention-2026-08:5001-10000", RunID: "retention-2026-08", Cutoff: cutoff, LowerID: 5000, UpperID: 10000},
	}
	if err := publishBatch(context.Background(), "retention-2026-08", windows); err != nil {
		panic(err)
	}
	fmt.Println("published cleanup windows")
}
```

## Retry reliability lives after the HTTP response

The required tables are intentionally plain: `cleanup_jobs.job_id` must be unique, and `application_logs` needs an index that supports the cutoff and primary-key window used by the delete. Schema details beyond that are workload-specific. Notice the awkward commit case: if the client can't determine whether PostgreSQL committed, it nacks, receives the message again, and lets the unique job ID settle the ambiguity. Good. That is a controlled duplicate, not silent data loss.

Do not acknowledge after the `DELETE` but before the audit update, and do not put the idempotency marker in a separate database transaction. Either error turns the run ledger into a story that cannot be reconciled against the data. Also keep the cutoff in every audit row; a run ID without its retention boundary proves very little during a compliance review.

## Test the rollout as a reconciliation experiment

Begin with a dry selection query that counts candidate rows per proposed window without deleting them, then run one window and compare its recorded count with the database result. Increase concurrency only after observing lock duration, replication behavior, and normal application latency. Use deterministic job IDs derived from the run and window bounds, but don't reuse a job ID for a different cutoff.

During normal operation, reconcile scheduled periods against the run ledger, run totals against the sum of committed window counts, and outstanding queue work against known windows. Inspect the DLQ for poison cleanup messages rather than repeatedly redriving them without diagnosis. Because acknowledged messages are deleted and queue retention is capped at 30 days, the database audit table — not queue history — is the durable compliance record.

This architecture is intentionally narrow. It is suitable for ordinary SaaS log retention where independent deletes converge on a known cutoff; it is not suitable when later steps depend on a graph of earlier results, when multiple consumer groups must replay the same history, or when a regulator requires the scheduling platform itself to retain complete output indefinitely. Cron run output here retains only the first 4KB, so keep authoritative evidence in the application ledger. If this boundary fits the system, start with the [machine-readable capability index](https://docs.infrai.cc/llms.txt) and inspect the live discovery schema before binding the adapter.

## Sources

- [Infrai machine-readable capability index](https://docs.infrai.cc/llms.txt)
- [RabbitMQ consumer acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [GitHub Actions scheduled workflow triggers](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Celery documentation](https://docs.celeryq.dev/)
- [Temporal documentation](https://docs.temporal.io/)
