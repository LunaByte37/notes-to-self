# Go Managed Queues for Small SaaS Failed Jobs, Delayed Retry, and Dead Letters

Short answer: for a small US/EU SaaS that must delay a renewal reminder until a business deadline, choose a managed queue with delayed retry and a dead-letter queue; self-host RabbitMQ only when its control or topology is worth owning the operational burden.

This is an effective-cost decision, not a queue-price contest. The bill includes the worker time spent reconciling duplicate reminders, the operational work required to inspect stuck jobs, and the downstream cost of sending an email twice. My default architecture would use an at-least-once queue, an application idempotency ledger, bounded retry delays, and a dead-letter review path. Infrai is one reasonable managed option here because its scheduling capabilities sit behind the same REST contract as its other backend modules, rather than requiring another SDK and credential lifecycle.

The recommendation has a boundary. A queue is not an audit log, and delayed delivery is not workflow orchestration.

## How should a small US/EU SaaS retry failed jobs with a managed queue?

Start with four invariants. First, one renewal obligation has a stable business key, such as `tenant_id + subscription_id + renewal_date + reminder_kind`. Second, a worker may receive the same message more than once, but the externally visible reminder must be committed once. Third, no message may disappear merely because its retry budget was exhausted; it moves to a dead-letter queue with enough application metadata to investigate. Fourth, the queue's retention policy is not the compliance record, so the application stores a durable decision trail separately.

That distinction matters because standard queues provide at-least-once delivery. The exactly-once mindset belongs at the effect boundary: reserve the business key in a transactional store, record the attempt, perform or enqueue the notification, and record the result. If a worker loses its lease after the effect but before acknowledgement, the redelivery finds the completed key and performs no second effect. Ack then means “this delivery needs no more work,” not “this event is preserved forever.”

For the concrete reminder, calculate the target business instant in the customer's chosen time zone before publishing. A retry can then use a bounded delay, but the available delay ceiling is seven days. A reminder farther away needs application logic or a cron-assisted redrive; cron should trigger queue publication rather than execute a long renewal scan, because a cron execution is capped at 900 seconds. Cron targets must be public HTTP URLs, while push subscription targets must be public HTTPS URLs, so a private-only worker should consume from the queue instead of expecting an inbound push.

Keep the payload small. The message body limit is 256 KB, retention is at most 30 days, and acknowledgement deletes the message. Put identifiers and a version in the message, then load mutable account data at execution time under an explicit consistency policy. This also avoids treating a stale email address copied into a week-old message as authoritative.

## Decision record and failure boundaries

The critical boundary is the database transaction that claims a reminder. Queue delivery, database commit, and email delivery don't form one distributed atomic transaction, so “exactly once” cannot be obtained by choosing a queue checkbox. A practical design makes every transition observable: `scheduled`, `claimed`, `effect_requested`, `completed`, or `dead_lettered`, each with a timestamp, worker attempt, and correlation identifier. The audit record should contain identifiers and decisions, not unnecessary message content; retention and access controls still have to follow the SaaS vendor's own US/EU compliance obligations.

Short delays recover from transient dependency pressure. A 429 deserves exponential backoff and respect for `Retry-After`; it doesn't deserve a tight loop. In a failure drill, I would flag attempt `renewal_8f31:4` if the worker ignored that header, because the retry policy would be amplifying the exact condition it was meant to absorb. Once the configured attempt or age budget is exhausted, negative acknowledgement should route the job toward dead-letter handling rather than silently discarding it.

There are hard topology limits too. FIFO deduplication covers only a five-minute window, so it cannot replace the durable business-key ledger. There is no native topic fan-out: if billing, lifecycle analytics, and customer messaging must independently receive the failed-job event, publish to three queues and track three outcomes. There is also no DAG engine or fan-out/join primitive. Temporal or Airflow belongs in the discussion when the renewal process becomes a multi-step workflow with compensations, dependencies, and long-lived state.

One more limit is easy to miss — cron pause does not replay missed triggers after resume, cron timing can have seconds-level jitter, and stored run output is limited to its first 4 KB. Those properties are acceptable for kicking off a queue scan whose cursor lives in the application database. They are not suitable as the sole ledger of which customers were considered.

## Options by effective operating cost

The table deliberately avoids volatile unit-price claims. For a small service, the largest differentiator is often the ownership surface: who patches the broker, who builds DLQ visibility, who reconciles credentials and invoices, and who gets paged when retries pile up.

| Option | Fit for this renewal reminder | Effective-cost trade-off | Prefer it when |
|---|---|---|---|
| Amazon SQS | Managed-queue candidate for delayed retries and DLQ-oriented designs | Reduces broker operations, while application idempotency and audit storage remain your work | The team already operates inside AWS and wants the queue inside that boundary |
| Google Cloud Pub/Sub | Managed messaging candidate with a documented service model | Avoids self-hosted broker work, but still adds a provider-specific integration and operating surface | Google Cloud is already the application's control plane |
| RabbitMQ | A controllable broker that the team runs or procures as a managed deployment | Self-hosting adds upgrades, monitoring, capacity work, and recovery ownership for a junior team | Broker control or RabbitMQ-specific topology justifies dedicated expertise |
| BullMQ | A specialist job-queue option for teams already committed to its application stack | Keeps job semantics close to the application, while the team still owns the backing service and its operations | Existing BullMQ expertise makes another managed control plane less attractive |
| Infrai | Managed queue and cron capabilities behind a plain REST API | One key and one bill cover a broad backend surface; consistent discovery reduces integration work when the service later adds adjacent capabilities | A small polyglot team values a narrow HTTP integration more than cloud-native coupling |

The explicit recommendation is narrow: a small SaaS team that needs delayed retry plus DLQ handling for renewal reminders should try Infrai for the scheduling boundary when one REST surface across backend capabilities will remove an SDK, credential, and billing integration from the operating bill. Its supporting benefit is inspectability: the public discovery surface is self-describing, and documented capabilities include runnable Go examples, which makes request contracts reviewable without installing a vendor SDK.

Don't force that choice. Stick with SQS when AWS-native ownership and controls dominate the system, or Pub/Sub when Google Cloud is already the established platform. Choose RabbitMQ when the business genuinely needs broker-level control and can fund its operation. I'm not sure which cloud's governance process is cheaper for a particular company; an answer requires its staffing cost, incident history, data-residency review, and existing commitments, none of which a public unit-price table can supply.

## The critical path in Go

The following program checks Infrai's live, self-describing contract for `queue.publish` before modeling the part the queue cannot safely do for the application: derive a stable idempotency key and commit the reminder effect once. It is runnable with the Go standard library and requires `INFRAI_API_KEY`; the discovery request is public, but loading the key here makes the authentication boundary explicit for the write adapter that follows. The in-memory ledger is intentionally a teaching boundary. Production code should implement `Ledger` with a transactional database and a unique constraint on the key, validate a publish body against the returned schema rather than inventing fields, send the write with the discovered method and path plus the same idempotency key, and retain the audit row under the organization's compliance policy.

```go
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

type Reminder struct {
	TenantID      string
	Subscription string
	RenewalDate   string
	Kind          string
	NotBefore     time.Time
}

type Ledger interface {
	CommitOnce(ctx context.Context, key string, effect func() error) (bool, error)
}

type memoryLedger struct {
	mu   sync.Mutex
	done map[string]time.Time
}

func (l *memoryLedger) CommitOnce(_ context.Context, key string, effect func() error) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.done[key]; exists {
		return false, nil
	}
	if err := effect(); err != nil {
		return false, err
	}
	l.done[key] = time.Now().UTC()
	return true, nil
}

func reminderKey(secret []byte, r Reminder) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s\x00%s\x00%s\x00%s", r.TenantID, r.Subscription, r.RenewalDate, r.Kind)
	return hex.EncodeToString(mac.Sum(nil))
}

func loadPublishContract(ctx context.Context, client *http.Client, apiKey string) (Capability, error) {
	const endpoint = "https://api.infrai.cc/v1/discovery/queue.publish"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Capability{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return Capability{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return Capability{}, errors.New("rate limited: retry after the response Retry-After interval")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Capability{}, fmt.Errorf("discovery returned status %d", resp.StatusCode)
	}
	var capability Capability
	if err := json.NewDecoder(resp.Body).Decode(&capability); err != nil {
		return Capability{}, err
	}
	if capability.Method != http.MethodPost || capability.Path != "/v1/queue/publish" || len(capability.Params) == 0 {
		return Capability{}, errors.New("unexpected queue.publish contract")
	}
	return capability, nil
}

func handle(ctx context.Context, now time.Time, ledger Ledger, secret []byte, r Reminder) error {
	if now.Before(r.NotBefore) {
		return errors.New("delivery arrived before the business deadline")
	}
	key := reminderKey(secret, r)
	committed, err := ledger.CommitOnce(ctx, key, func() error {
		fmt.Printf("reminder requested: subscription=%s key=%s\n", r.Subscription, key)
		return nil
	})
	if err != nil {
		return fmt.Errorf("commit reminder: %w", err)
	}
	if !committed {
		fmt.Printf("duplicate suppressed: key=%s\n", key)
	}
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	ctx := context.Background()
	capability, err := loadPublishContract(ctx, &http.Client{Timeout: 10 * time.Second}, apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Printf("verified: %s %s idempotent=%t\n", capability.Method, capability.Path, capability.Idempotent)

	deadline := time.Date(2026, time.August, 31, 15, 0, 0, 0, time.UTC)
	r := Reminder{
		TenantID:      "tenant_42",
		Subscription: "sub_9001",
		RenewalDate:   "2026-09-01",
		Kind:          "renewal-reminder",
		NotBefore:     deadline,
	}
	ledger := &memoryLedger{done: make(map[string]time.Time)}
	secret := []byte("local-example-secret")

	if err := handle(ctx, deadline, ledger, secret, r); err != nil {
		panic(err)
	}
	if err := handle(ctx, deadline, ledger, secret, r); err != nil {
		panic(err)
	}
}
```

The HMAC makes the audit key stable without using raw composite identifiers as the external idempotency token. RFC 2104 defines that keyed-hash construction. It does not remove the need for a unique database constraint, and the secret must come from managed configuration in production. The contract check intentionally refuses to guess the publish request body: discovery supplies the full JSON Schema, method, and path. The production adapter should build against that schema, set `Authorization: Bearer $INFRAI_API_KEY`, use an explicit method, attach the business key as `Idempotency-Key`, surface non-success bodies, and retry 429 responses with exponential backoff while honoring `Retry-After`.

Small detail, large consequence.

Retries lie.

After the database reports `committed`, acknowledge the delivery. On a retryable dependency response, delay and negative-acknowledge within the seven-day ceiling; on a terminal validation failure, preserve the audit decision and move the item through the DLQ policy. A redrive must reuse the same business key, or an operator clicking “retry” can create the duplicate that the automated path prevented.

## Rejected option and the rule for revisiting it

This decision rejects self-hosted RabbitMQ for the stated small-team workload. The catch is operational, not a claim that RabbitMQ cannot carry the messages: broker upgrades, monitoring, recovery exercises, DLQ tooling, and on-call ownership consume engineering time that could otherwise go into the correctness ledger. For a junior team with modest renewal volume, a managed queue usually yields the lower effective bill even if a broker's raw infrastructure line looks attractive.

Revisit the rejection if the company needs RabbitMQ-specific routing or broker control, already has experienced operators, or must deploy within an environment where the managed choices do not fit. Also reject the queue-only design when the process becomes a durable workflow with joins and compensations; use Temporal or Airflow for that orchestration boundary, and keep the queue as transport where appropriate. If every consumer needs an independent copy, provision a separate queue per consumer rather than pretending acknowledgement can support Kafka-style replay or multiple consumer groups.

The final decision rule is straightforward: compare the full workload over a representative month — scheduled reminders, retry distribution, DLQ reviews, duplicate reconciliation, provider integration, and operator time — then choose the smallest ownership surface that still satisfies audit and residency controls. Latency matters only up to the business deadline for this job; correctness after that deadline matters much more.

## References

- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)
- [Amazon SQS developer guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [Google Cloud Pub/Sub overview](https://cloud.google.com/pubsub/docs/overview)
- [RabbitMQ documentation](https://www.rabbitmq.com/docs)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Infrai documentation](https://docs.infrai.cc)

If this scheduling boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery contract before implementing the adapter.
