# Go SMS OTP Delivery: Prefer Capped to Blind Retries for Delayed Login

The bill for delayed SMS OTP delivery includes repeated submissions and the work required to explain unresolved login or signup verification to customer support. For the submission component, the accounting identity is initial attempts plus additional attempts: in an illustrative signup with one initial message and three retries, four submissions are attributable to one verification intent. Three quarters are additional attempts. This is arithmetic for a design example, not a measured failure rate or a claim about carrier charges.

TL;DR: Prefer capped retries over blind SMS retries when delivering a verification link for customer-support account signup. Delayed login OTPs and missing signup links require the same initial distinction: an accepted submission, a delivery report, and successful verification are different evidence. For US carriers, investigate sender eligibility and filtering; for EU destinations, investigate the destination country's sender and routing requirements. Another submission should follow an explicit decision about the previous attempt, rather than treating silence as proof of failure.

Count attempts first.

The change that moves the submission count is a capped retry policy attached to one verification intent; keeping more message bodies does not reduce that count or establish receipt. This note treats evidence minimization as an incident-response design problem, with delivery reliability as the deciding criterion and expenditure as a consequence.

## Why is SMS OTP delivery delayed across carriers after submission?

A messaging service's acceptance records progress through one boundary. Carrier handling and the recipient's handset remain separate boundaries, so the support account must remain unverified until the user completes verification, regardless of what a delivery callback says. The SMS documentation listed in References describes message status handling, delivery reporting, and destination-specific messaging guidance; those categories help diagnose the transport without making the transport the authority for account state.

For a US destination, check whether the sender type and its registration satisfy the applicable messaging requirements, then examine reported filtering or rejection. A valid-looking telephone number does not establish that its destination can receive the chosen traffic. For EU destinations, preserve the actual destination country in the investigation: sender identity rules and supported messaging arrangements require country-specific checks, and an undifferentiated EU label cannot establish route suitability. Consult the relevant destination guidance through the documentation in References before attributing a rejection to a regional rule.

Other possibilities belong in a different part of the investigation. Submission throughput limits or queuing can delay handoff; an unavailable handset can delay receipt; and an incorrect number or unsupported destination can prevent useful delivery. These are candidate causes to test against status evidence, not conclusions to infer from a user requesting another message. A delayed callback can also leave the application uncertain about transport progress even when the user has already received the message.

Silence proves little.

For the customer-support signup flow, receiving a verification link and successfully consuming it are different events. If delivery evidence looks satisfactory but verification completion falls, examine link expiry and token validation before creating more transport attempts. Never accept a delivery report as permission to activate the account.

## Compare capped and blind retries at the intent boundary

Blind retries interpret an unresolved transport outcome as a new obligation to send. Capped retries preserve one business intent, allow a limited number of delivery attempts under that intent, and make the remaining uncertainty visible to support. Choose the latter when delayed or repeated messages could confuse a user waiting to enter a support portal, especially when an older message might arrive after a newer one.

The boundary matters. An intent identifier represents the signup verification obligation; an attempt identifier represents one submission operation; and a provider message identifier represents the external service's record, when one exists. Collapsing these identifiers makes reconciliation harder because the question "Did this signup finish?" becomes entangled with "Did this submission reach the next system?" An exactly-once mindset belongs at the business transition: consume a valid verification token and activate the account once. It is not evidence that the external SMS transport provides exactly-once delivery.

As an illustrative policy, allow at most two authorized send attempts per intent. Moving the earlier four-submission example to this cap permits at most two submissions for that intent, but it does not establish an improvement in delivery success. The cost is a deliberate limit on recovery through the same channel. A user who exhausts the allowance needs an explicit recovery path, such as a separately verified channel or a support process with its own identity checks. Changing the channel must not weaken the verification requirement.

Link semantics need a decision too. Reusing the same unexpired verification challenge can avoid invalidating a message still in transit, while rotating a challenge can support revocation requirements but makes late messages more confusing. Document which challenge remains valid. Expiry, single use, and account binding should be checked by the verification service independently of delivery status; none of these checks can be delegated to a carrier receipt.

A capped policy limits ambiguous work while preserving evidence. It does not eliminate sender registration, destination checks, or transport diagnostics.

The trade-off is reduced automatic recovery. A fixed cap is not suitable as the sole recovery mechanism when signup must continue through a prolonged carrier interruption; use a separately verified recovery channel instead. Its limitation is explicit: the cap limits attempts, not transport uncertainty.

## Reserve a send without confusing it with delivery

This Go example implements a small part of the policy: reserving an attempt in an in-memory intent. Repeating an authorized attempt identifier does not reserve another send, and a new identifier is rejected after the cap. Two attempts is a policy choice, not a transport guarantee.

```go
package verification

import (
    "errors"
    "sync"
)

var ErrAttemptLimit = errors.New("verification attempt limit reached")
var ErrEmptyAttempt = errors.New("attempt identifier is required")

type Intent struct {
    mu       sync.Mutex
    attempts map[string]struct{}
}

func (i *Intent) Reserve(attemptID string) (bool, error) {
    if attemptID == "" {
        return false, ErrEmptyAttempt
    }
    i.mu.Lock()
    defer i.mu.Unlock()

    if _, exists := i.attempts[attemptID]; exists {
        return false, nil
    }
    if len(i.attempts) >= 2 {
        return false, ErrAttemptLimit
    }
    if i.attempts == nil {
        i.attempts = make(map[string]struct{})
    }
    i.attempts[attemptID] = struct{}{}
    return true, nil
}
```

A true result authorizes the caller to proceed with that reserved attempt. A false result with no error means the identifier was already reserved; it does not mean the previous send succeeded. The distinction is consequential: a process can fail after reservation and before submission, or lose the response after submission. Automatically issuing a fresh attempt identifier in either case defeats the policy and can create duplicates.

For production, persist the intent and attempt reservation with a uniqueness constraint on their identifiers, and commit pending dispatch work with the reservation. A dispatcher must still reconcile ambiguous external outcomes before resubmitting. The mutex above protects a single object within one process; it supplies neither durable audit trails nor coordination across replicas. Use any external idempotency mechanism only within its documented scope. If no mechanism resolves a lost response, preserve the unknown outcome instead of relabeling it as failed.

Uncertainty deserves a state.

## Make the evidence useful during an incident

An audit trail should connect intent creation, attempt reservation, submission outcome, subsequent delivery evidence, and verification completion. Store event occurrence time separately from ingestion time so a late callback does not appear to be a newly initiated send. Record the external status as evidence with its source, and avoid letting an older event overwrite a later business transition. A verified account must remain verified when a delayed transport failure event arrives.

For diagnosis, group unresolved attempts by destination country, sender configuration, and reported error category. Include destination network information only when available and appropriately sourced; a phone-number prefix is insufficient evidence of the current carrier. Separate the interval before submission acceptance from the interval between acceptance and delivery evidence. Neither interval measures when a person read the message. Track completed verifications alongside transport statuses so the operational view remains connected to the signup job.

Test the awkward cases before rollout: duplicate retry requests, a response lost after submission, callbacks arriving out of order, process termination after reservation, and an expired link arriving late. These tests should assert the number of authorized attempts and the final account state, not merely that a send function returned successfully. During deployment, review unresolved intents and verification completion against the previous policy before expanding the change; a lower submission count is insufficient evidence that signup is healthier.

Completion remains the outcome.

Support needs a concise explanation of the current state and the next permitted action. It should be possible to report an unresolved attempt without exposing the full recipient number or copying a live verification link into a ticket. Structured identifiers reduce the temptation to use message bodies as the incident investigation interface.

## Keep an audit trail with a deliberate stopping point

Retention is a security and compliance decision as well as an operational one. Define separate lifetimes for verification secrets, recipient details, transport evidence, and aggregated metrics; choose those lifetimes through the applicable legal and security review rather than treating one retention period as valid across US and EU recipients. This note establishes no statutory retention period and does not make an SMS receipt proof of identity or regulatory compliance.

The unsubscribe mechanism in RFC 8058 concerns one-click unsubscribe for email. It is not a remedy for delayed SMS verification and supplies no justification for retaining authentication secrets. Operational messaging controls must be evaluated for the channel and purpose they actually govern.

Prefer capped retries for this signup flow, retain enough structured evidence to reconcile each authorized attempt, and make unresolved delivery an explicit support state. Deliberately stop keeping raw verification links and OTP values in the audit log; after the approved diagnostic window, delete recipient-level transport detail that no longer has a justified purpose. The cost appears during a later dispute: support may establish that an attempt was authorized and the account was verified, yet no longer reconstruct the exact destination or message content. Accept that investigative limit explicitly, and do not compensate by quietly retaining secrets elsewhere.

## References

- Twilio SMS documentation: message status, delivery reporting, and country-specific messaging guidance. https://www.twilio.com/docs/sms
- RFC 8058, One-Click Unsubscribe: email unsubscribe scope. https://datatracker.ietf.org/doc/html/rfc8058

## Further reading

- https://www.twilio.com/docs/sms
- https://datatracker.ietf.org/doc/html/rfc8058
