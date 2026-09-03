# Nightly Reconciliation Under a Per-Minute API Rate Limit: Queue Backpressure, Not Cron

Use a durable queue with worker-side admission control, and give cron exactly one responsibility: waking the producer that enqueues the cycle. For a nightly reconciliation run against a payment provider, the per-minute API rate limit is a property of the consumer, not of the schedule — a cron expression decides when a batch starts and says nothing about how fast that batch may proceed. The axis that settles this design is delivery guarantees. Cost differences between job processing backends are real, but they are a rounding error next to the cost of one duplicated ledger posting.

The system in question is a healthtech billing backend. Every night it pulls the previous day's settlement lines from the payment provider, matches each line against a patient invoice, and posts the result to a ledger: roughly 40,000 lines, against a documented ceiling of 600 requests per minute. Both of the obvious failure directions are reportable events. Bill a patient twice and you have a refund, an apology and a support ticket; drop a line silently and the discrepancy surfaces weeks later during month-end close, when nobody remembers what the run did.

So the invariant is narrow, and it is the only one worth arguing about: a retry may repeat a delivery, but it must never repeat a business effect.

Everything below follows from that sentence.

## The failure modes a nightly reconciliation actually hits

The interesting failures are not exotic. A worker crashes in the window between applying a settlement line to the ledger and acknowledging the message, so the transport redelivers work that has already changed money. A process dies at line 18,000 with no durable record of its position, and the operator restarting it has to guess whether re-running the whole file is safe. Six replicas each hold a private in-process limiter sized to the documented ceiling, so the service politely exceeds that ceiling sixfold the moment autoscaling reacts to the queue depth.

Then there is the poison line: one malformed record that fails deterministically, gets retried forever, and starves the rest of the cycle behind it.

Timezones deserve their own sentence, because they cause a category of bug that survives every retry policy you write. The provider's settlement cycle is stamped in its own timezone; your scheduler fires at local midnight; twice a year one of those two moves and the other doesn't, and the run either reprocesses yesterday or skips a day entirely. Deriving the cycle date from the provider's payload rather than from the clock at trigger time removes that failure class outright, and it is a two-line change if you do it before the first production run rather than after the first audit finding.

## How should a nightly job queue behave when the payment API rate limit is per minute?

It should hold work durably, hand each item to a worker under a lease, and let the worker — not the scheduler — decide whether this minute has room for another call. That ordering matters because admission is the only place that sees the thing being admitted. A limiter next to the outbound call can count the attempt, observe a `429 Too Many Requests`, honor `Retry-After` when the provider sends it, and put the item back without ever having touched the ledger.

The limiter has to be shared state. In a Node.js worker this bites earlier than people expect, because the natural implementation is a token bucket in module scope, which is per process, which means your effective ceiling is the documented limit multiplied by your replica count. Keep the budget in Redis or in a database row, and treat the number of concurrent in-flight calls as derived from the budget rather than configured separately.

Backpressure is the second half. When the minute is full, the correct behavior is to leave the message unacknowledged and let the lease expire, or to reschedule it with a delay — not to sleep inside the handler while holding a lease, which converts a rate limit problem into a lease expiry problem and gets the same item delivered to a second worker. Bounded exponential backoff with jitter, capped somewhere around a minute, is enough. Unbounded retry against a throttling dependency is how a rate limit becomes an outage.

## Comparing at-most-once, at-least-once, and effectively-once delivery

Exactly-once delivery over a network is not on the menu; the acknowledgment can always be lost after the effect has landed. Exactly-once *effect* is entirely achievable, and it is what a reconciliation run needs.

| Delivery model | What the transport gives you | What your code must add | Where it fails this workload |
|---|---|---|---|
| At-most-once | One attempt, never repeated | Nothing, which is the problem | A crashed worker silently drops settlement lines; the gap appears at month-end |
| At-least-once | Redelivery until acknowledged | Idempotent apply keyed on a domain operation id | Duplicate postings if the acknowledgment is treated as the commit point |
| Effectively-once | Still redelivery — the transport is unchanged | Uniqueness constraint inside the same transaction as the effect, plus an audit row per attempt | Needs a real datastore and one extra write per attempt |

Almost every managed cloud queue and every self-hosted worker library lands in the middle row. That is the useful takeaway from the comparison: the delivery guarantee you can actually buy is at-least-once, and the third row is something you build on top of it in about thirty lines. Choosing between job backends on throughput or price is arguing about the wrong column.

## The critical path in code: admit, apply, then acknowledge

The operation id is the whole design. It is derived from the provider's own identifiers — cycle date plus the provider's line reference — so the same settlement line produces the same key on every redelivery, on a rerun, and on a manual replay six months later during an audit. It is not a random UUID minted by the publisher, because a publisher retry would mint a second one and the uniqueness constraint would happily let both through. The examples are Go; the state machine is runtime-independent, and a Node.js worker needs the same three records.

```go
package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Line is one settlement row published by the payment provider for a cycle date.
type Line struct {
	CycleDate   string // provider's settlement date, taken from the payload
	ProviderRef string // stable per-line reference assigned by the provider
	InvoiceID   string
	AmountMinor int64
}

// opID is the domain identity of the work: same line, same key, however many
// times the queue redelivers it.
func opID(l Line) string { return fmt.Sprintf("settle:%s:%s", l.CycleDate, l.ProviderRef) }

// Admitter is the shared per-minute gate. Back it with Redis or a database row
// so every worker replica draws from one budget.
type Admitter interface {
	Admit(ctx context.Context, cost int) error
}

// Handle applies one settlement line and returns nil only when the transport
// message may be acknowledged.
func Handle(ctx context.Context, db *sql.DB, adm Admitter, l Line, attempt int) error {
	if err := adm.Admit(ctx, 1); err != nil {
		return err // no capacity this minute: the message stays unacknowledged
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var claimed bool
	err = tx.QueryRowContext(ctx,
		`insert into applied_operations (op_id, cycle_date, claimed_at)
		 values ($1, $2, now())
		 on conflict (op_id) do nothing
		 returning true`, opID(l), l.CycleDate).Scan(&claimed)

	if errors.Is(err, sql.ErrNoRows) {
		// Redelivery of work already in the ledger. Record it, then acknowledge.
		if _, aerr := db.ExecContext(ctx,
			`insert into ledger_audit (op_id, attempt, outcome, at)
			 values ($1, $2, 'duplicate_suppressed', now())`, opID(l), attempt); aerr != nil {
			return aerr // an unrecorded duplicate is still an audit gap
		}
		return nil
	}
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`update invoices set settled_minor = settled_minor + $1, settled_at = now()
		 where id = $2`, l.AmountMinor, l.InvoiceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`insert into ledger_audit (op_id, attempt, outcome, at)
		 values ($1, $2, 'applied', now())`, opID(l), attempt); err != nil {
		return err
	}
	return tx.Commit() // the acknowledgment happens after this returns, never before
}
```

Three records, one transaction, and an acknowledgment that is strictly downstream of the commit. The claim row answers "did this line already move money"; the audit row answers "what did each attempt decide, and when"; the ledger mutation is the effect itself. A reconciliation service that cannot answer the second question is a service that will eventually tell an auditor "the queue says it was delivered", which is not an answer about money at all.

Throttling is the other half of the critical path, and it is worth handling precisely rather than with a bare sleep:

```go
package reconcile

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// retryDelay converts a throttled response into a wait. Retry-After may be a
// number of seconds or an HTTP-date; both forms are legal.
func retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp.StatusCode == http.StatusTooManyRequests {
		if v := resp.Header.Get("Retry-After"); v != "" {
			if secs, err := strconv.Atoi(v); err == nil {
				return time.Duration(secs) * time.Second
			}
			if t, err := http.ParseTime(v); err == nil {
				return time.Until(t)
			}
		}
	}
	d := time.Duration(1<<min(attempt, 6)) * time.Second // 1s … 64s ceiling
	return d/2 + time.Duration(rand.Int63n(int64(d/2)))  // jitter, so replicas don't resynchronize
}
```

Observability for this path is three counters and one gauge, not a dashboard project: admissions granted, admissions deferred, duplicates suppressed, and the age of the oldest unacknowledged item in the cycle. That last one is the only number worth paging on. Queue depth alone tells you nothing during a nightly batch, since depth is supposed to be 40,000 at 02:00 and zero by 04:00; oldest-item age tells you whether the run is progressing or wedged. Testing follows the same shape: a test that delivers the same line twice and asserts one ledger row and two audit rows is worth more than any load test you will write for this system, and it runs in milliseconds against a real database in a container.

## Where cron still belongs, and what the queue retention window won't hold

Cron was rejected as the processor, not as a component. It remains the right trigger for the producer step — fetch the settlement file, derive the cycle date from the payload, publish one message per line — because that step is short, safe to repeat, and has no per-item pacing to enforce. Scheduled tasks are good at saying "now". They have never been good at saying "not faster than this".

Stick with a bare cron script and skip the queue entirely when the nightly run is a few hundred items, finishes inside one process well under any lease timeout, and the provider's ceiling sits far above your throughput. A queue you don't need is another system to be paged about at 03:00, and the honest version of this article admits that plenty of reconciliation jobs are exactly that small. The threshold isn't a line count I can hand you — it's whether a mid-run crash forces a human decision. When it does, you need the durable per-item record, whatever the item count.

The retention boundary is where teams in regulated environments get surprised. Managed queues measure retention in days and delete a message on acknowledgment; there is no replay log to consult later. The HIPAA Security Rule requires certain documentation to be retained for six years from creation or last effective date, and PCI DSS v4.0 requires at least twelve months of audit log history with the most recent three months immediately available. Those windows do not overlap with any queue's retention window, which is exactly why the audit table above lives in your own database rather than in the transport. Dead-letter queues have a related sharp edge: on at least one major managed queue, a message moved to a dead-letter queue keeps its original enqueue timestamp, so the retention clock does not restart when the message lands there — a poison line quarantined on day five of a fourteen-day retention window has nine days left, not fourteen.

One more boundary, since it decides more architectures than throughput does. A queue plus workers doesn't support multi-stage coordination: if the nightly close is really fetch, then match, then post, then generate a regulatory report only after every line has settled, you are describing a workflow with fan-in, and a durable workflow engine expresses that far better than a chain of queues with a completion counter bolted on. The catch is that it is another runtime to operate, another failure domain, and another thing your on-call rotation has to understand. For a single-stage nightly reconciliation against one rate-limited API, it is not suitable — the queue and the idempotency key are enough, and I would rather spend that operational budget on the audit trail.

## Further reading

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After
- https://www.rfc-editor.org/rfc/rfc6585
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header
- https://www.ecfr.gov/current/title-45/section-164.316
- https://www.pcisecuritystandards.org/document_library/
