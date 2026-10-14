# Logistics SMS Alerts Provider Controls: Shipping, Appointment Reminders, Account Activity

TL;DR: Choose the SMS integration that lets the application own a stable message key, consent and suppression decisions, template revisions, provider receipts, and a replayable audit trail. A simple REST call is useful, but it is not the system of record. For a logistics compliance notice, the lowest integration effort that remains defensible is usually one internal send contract with an outbox, a durable attempt ledger, and a thin provider adapter.

This decision rule covers shipping alerts directly, while the same boundary can carry appointment reminders and account-activity messages without pretending that those message classes have identical consent, urgency, or retention policies. It separates two questions that procurement worksheets often collapse: how quickly can an engineer make the first request, and how much work is required to explain every later outcome? The second question matters more for an auditable notice.

## How should an SMS alerts provider handle shipping and appointment reminders?

Start with invariants, because an API feature list cannot settle them. Each business event produces at most one logical notice for a recipient, purpose, channel, and template revision. Every network attempt has its own immutable record. A suppression decision is recorded before dispatch, and a later transport status is appended rather than overwriting history. Credentials, phone numbers, and message bodies do not belong in broadly searchable logs.

Exactly once is a business invariant here, not a claim about the network. A client can time out after a remote system accepted a request; a callback can be duplicated or arrive after a retry; and a worker can stop after sending but before recording the response. The application therefore needs one deterministic logical key and many attempt records. Duplicate execution may happen. Duplicate business effects must be constrained.

The failure boundaries should be explicit:

- Before the outbox commit, no notice exists. The business transaction can roll back.
- After that commit but before dispatch, the notice is pending and safely replayable.
- After dispatch with no response, the outcome is unknown, not failed. Retry only after reconciliation, unless the adapter can reuse a stable idempotency reference.
- Transport acceptance means accepted for processing. The final delivery state remains separate.
- A suppression result is a terminal policy decision for that attempt, but the underlying event remains auditable.

These states prevent a dashboard from converting uncertainty into a false failure and then sending the same compliance notice twice.

Unknown means unknown.

For security-sensitive account activity, do not stretch a general notification record into an authentication protocol. OWASP's forgot-password guidance says reset tokens or codes should be randomly generated, sufficiently long, stored securely, single use, and expiring; it also recommends consistent response messages and rate limiting. Those controls belong in the authentication boundary even if SMS is one delivery channel. A shipping update does not acquire invented OTP semantics.

## Compare the control planes, not the demo requests

Integration effort is the work needed to preserve those invariants across deploys and uncertain responses.

| Option | Initial integration | Audit ownership | Policy boundary | Appropriate use |
|---|---:|---|---|---|
| Direct calls from each service | Low first, rising per caller | Fragmented | Repeated or remotely coupled | One low-consequence event with one owner |
| Internal contract, outbox, and adapter | Moderate once | Application owns the logical record | Internal policy; rendering can vary | Governed notices shared by several event types |
| Full internal messaging platform | High | Fully internal | Internal policy and orchestration | Multiple teams and channels justify platform ownership |

The middle option is the decision for this logistics system. **It minimizes total integration effort without outsourcing the evidence model.** Product services get a small contract, transport churn stays at the edge, and the team avoids building a general communications platform before that need exists.

The trade-off is real: the team must operate the outbox, ledger, callback verifier, and reconciliation job. This option is not suitable when nobody owns those controls. In that case, use a narrower direct integration for a low-consequence message, or assign platform ownership before moving regulated notices onto it.

Templates need the same discipline. Store the immutable template revision or content digest used for the notice, plus allowed variable names and locale. Do not reconstruct historical content from whichever template is current. If rendering is delegated, retain the remote template identifier and revision evidence the integration actually receives; do not fabricate a revision field when none exists.

Suppressions are policy inputs, not cleanup after a send. Record the source, reason category, evaluation time, and policy version while limiting exposure of the recipient address. Yahoo's sender guidance concerns email rather than SMS, so it is not an SMS compliance rule; it is a useful operational analogue because it treats unsubscribe support, prompt handling of unsubscribes, complaint monitoring, and authentication as sender responsibilities. SMS consent and notice obligations require jurisdiction- and message-specific legal review. The architecture should make the resulting policy executable and auditable without claiming that one global checkbox settles it.

## The critical path in Go

The domain interface should expose business intent and evidence, not a transport's request shape. This example claims a pending notice, evaluates suppression before transport, records every attempt, and treats an unknown result as its own state. Repository locking and transaction semantics are part of the implementation contract.

```go
package notices

import (
	"context"
	"errors"
	"time"
)

type Notice struct {
	MessageKey, RecipientRef, Purpose, TemplateVersion, Locale string
}

type Result struct {
	TransportRef string
	State        string // accepted, rejected, or unknown
}

type Decision struct {
	Allowed       bool
	Reason        string
	PolicyVersion string
}

type Repository interface {
	ClaimPending(context.Context, string) (Notice, error)
	Policy(context.Context, Notice, time.Time) (Decision, error)
	RecordSuppressed(context.Context, Notice, Decision, time.Time) error
	StartAttempt(context.Context, Notice, time.Time) (string, error)
	FinishAttempt(context.Context, string, Result, time.Time) error
}

type Transport interface {
	Send(context.Context, string, Notice) (Result, error)
}

type Dispatcher struct {
	repo Repository
	send Transport
	now  func() time.Time
}

func (d Dispatcher) Dispatch(ctx context.Context, key string) error {
	n, err := d.repo.ClaimPending(ctx, key)
	if err != nil {
		return err
	}

	at := d.now().UTC()
	decision, err := d.repo.Policy(ctx, n, at)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return d.repo.RecordSuppressed(ctx, n, decision, at)
	}

	attemptID, err := d.repo.StartAttempt(ctx, n, at)
	if err != nil {
		return err
	}
	result, sendErr := d.send.Send(ctx, n.MessageKey, n)
	if sendErr != nil {
		result.State = "unknown"
	}
	if err := d.repo.FinishAttempt(ctx, attemptID, result, d.now().UTC()); err != nil {
		return err
	}
	if sendErr != nil {
		return errors.New("transport outcome unknown; reconcile before retry")
	}
	return nil
}
```

`MessageKey` identifies the logical notice, while `attemptID` identifies one execution. Using a fresh random transport idempotency key on every retry defeats the control. Reusing one attempt row and overwriting its result destroys the timeline reconciliation needs.

The persistence implementation must make `ClaimPending` exclusive for the logical key, and `StartAttempt` must commit before the external call. If a datastore cannot provide that ordering, use a transactional outbox consumer. This is where a superficially simple REST integration becomes a correctness problem.

Keep the audit payload narrow: event identifier, pseudonymous recipient reference, purpose, policy version, template revision or digest, timestamps, attempt identity, transport reference when available, and normalized state transitions. Restrict access and define retention with the responsible compliance team. Storing every message body may look convenient during an incident, but it can create a second repository of sensitive logistics and account data.

## Verification belongs in provider selection

Evaluate an adapter with contract tests, not a happy-path script. Test a timeout after request transmission, duplicate callbacks, an out-of-order terminal status, malformed destination data, suppression races, and worker restart between attempt creation and response persistence. Test template-variable rejection too, and confirm that logs omit raw phone numbers, credentials, and message bodies.

Use a 3/2/1 verification rule: 3 fixed fixtures, 2 destination regions, and 1 logical message key followed through every record. The fixtures are a shipment custody notice, an appointment reminder, and an account-activity alert; the destinations represent US and EU configurations. The point is not volume. It is proving that policy and retention can differ without forking transport code. Those two destinations can expose routing and policy configuration, but they are not evidence of legal compliance or production deliverability.

Operational metrics should preserve uncertainty: pending age, attempts by normalized outcome, unknown outcomes awaiting reconciliation, suppression decisions by reason, callback lag, and logical notices with more than one accepted attempt. Page on growing pending age or unreconciled unknowns; do not page merely because a recipient is suppressed. Suppression is often the correct result.

Deploy with shadow policy evaluation, then a limited event class and an explicit rollback path. Reconciliation must compare the internal attempt ledger with transport evidence. A daily aggregate count can reveal drift, but it cannot explain one disputed notice; retain stable references on both sides.

Procurement questions then become concrete. Can the transport accept a caller-controlled idempotency reference? Does it return a stable reconciliation reference? Are status events authenticated, documented, and replayable? Can templates be identified immutably? Can suppressions be evaluated without placing raw recipient data in general logs? What evidence remains after a timeout? Answers requiring manual portal inspection increase integration effort because they move correctness outside the deployable system.

## Rejected option and decision boundary

We rejected direct calls from shipment, appointment, and account services. They win the first-day comparison: fewer tables, no dispatcher, less code. They lose once three callers must implement the same suppression order, redact the same fields, correlate callbacks, and explain unknown outcomes. Scattered policy is harder to review.

Direct calls still have a valid use case. A single service sending a low-consequence internal notification, with no regulated content or cross-channel audit requirement, may accept that coupling. Record the boundary and the trigger for revisiting it.

We also rejected building a full internal messaging platform now. Owning routing rules, template authoring, multiple channel schedulers, and policy administration would exceed the stated job. **The selected boundary owns correctness evidence and leaves commodity transport behind an adapter.** That is enough architecture for an auditable logistics notice.

No option removes reconciliation.

Make the final selection with a short proof: implement one adapter, force ambiguous failures, export the audit record for one message key, and ask engineering and compliance reviewers to explain the same timeline independently. Choose a transport only after that record remains coherent.

## References

- OWASP, Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Yahoo, Sender Best Practices and Requirements: https://senders.yahooinc.com/best-practices/
