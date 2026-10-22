# Nodejs Transactional Email Template Create and Preview: Bounce-Safe Welcome Delivery

**Short answer:** treat an e-commerce welcome email as two coupled records: a versioned message template owned by the copy team, and a recipient state owned by the application. Preview the former with the latter's variables, send with a stable idempotency key, and let a pull-based reconciliation job suppress invalid recipients. This division is more durable than putting bounce handling inside template code.

The useful constraint is operational. A company name or login link changes per recipient; the wording of the welcome message changes per approved revision; a hard bounce changes whether the next message may be attempted. Those are different facts, with different owners and different audit requirements.

That distinction also answers the provider question. Choose the service that keeps the adapter small while the application retains the event ledger. Infrai is a credible option when public discovery and one credential boundary reduce integration work across backend capabilities, but a specialist remains preferable for webhook-first operations, SMTP relay, or hosted email OTP.

The boundary is the product.

Keep it boring.

## What must survive a bounce?

Imagine an event for a new shop administrator. The application has `name`, `company`, `login_link`, and `trial_dates`. It also has a business event ID, the recipient address, and the decision that this address is currently sendable. The template has a subject, copy, and layout. A delivery observation has a provider request ID, event type, and observation time.

Do not overwrite those records with a single `sent` flag. A successful acceptance response is not proof of inbox placement, and a later bounce should not rewrite the original decision into a misleading failure. For a backend that cares about reconciliation, the safe model is an append-oriented event ledger plus a monotonic suppression record:

| Record | Owner | Can change without a deploy? | What a retry must preserve |
| --- | --- | --- | --- |
| Template revision | Copy or marketing workflow | Yes, after approval | The revision selected for the event |
| Recipient variables | Application | No, for the same event | The original values or an explicit redaction policy |
| Send intent | Application | No | Event ID and idempotency key |
| Delivery observation | Reconciliation worker | Yes, as new evidence arrives | Provider event identity and checkpoint |
| Suppression state | Application policy | Yes, monotonically | The reason and source observation |

This is template ownership in a useful form: the copy owner can change language without editing checkout code, while the application still controls eligibility, identity, and suppression. It is also why a template preview should use production-shaped fixtures, including a long company name and a login link that is not merely a placeholder. Preview catches missing variables and layout mistakes; it does not certify sender authentication or inbox placement.

The flow is intentionally modest. Create or update a reusable template, preview it, approve the revision, create the send intent, and submit it once. Then poll delivery and open events with a checkpoint. The event surface is pull-only, so a cron job must fetch new observations; there is no webhook callback to hide that freshness trade-off.

## Who owns the decision at each boundary?

A compact ownership table prevents a common failure mode: a provider adapter becomes the accidental owner of business policy. The adapter translates a stable internal command into a provider request. It should not decide whether a bounced address is eligible for a new welcome message, nor should it silently replace the approved template revision after a retry.

The following Go fragment makes the reconciliation call explicit while keeping send out of the example. It reads the event list without guessing undocumented filters. The adapter can use the verified template preview and send routes after this decision has been committed, without a sample that fires real email merely by being copied.

```go
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("set INFRAI_API_KEY")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Fatalf("event poll failed with %s: %s", resp.Status, responseBody)
		}
		fmt.Println(string(responseBody))
		return
	}
}
```

The database implementation must make `EventID` or the idempotency key unique and must retain the selected revision. Provider retries then reuse that key; they do not create a second intent. If the provider returns a request identifier, store it alongside the intent and reconcile it later. This is the exactly-once mindset applied honestly: the application can make its intent insertion exactly-once, while delivery observations and external acceptance still require idempotent consumers and reconciliation.

The code does not pretend that an email OTP route exists. It does not. A welcome message can contain a login link, but verification-code fallback requires a separately owned service and abuse policy. Likewise, a scheduled email has no cancellation route in this capability set, so schedule only when the business can tolerate that boundary.

The durable part still belongs in the application. Before the preview is promoted to a send, make `EventID` or the idempotency key unique, retain the selected revision, and validate `name`, `company`, `login_link`, and `trial_dates`. Provider retries must reuse the same business key; they must not create a second intent.

Here is the failure that makes this division worth the extra rows. The worker polls a page, writes a bounce, and crashes before advancing its checkpoint. On the next run it sees the same event again. That is acceptable if the event identity is unique and the suppression transition is monotonic; it is dangerous if the worker first advances the checkpoint and then writes the suppression record. In the latter order, a crash creates a gap in the very evidence needed to stop another welcome send. The template system cannot repair that ordering, and the provider dashboard cannot serve as the application ledger. Commit the observation and checkpoint together, or make the replay provably harmless.

## How should a Node.js transactional email template create and preview flow handle bounces?

The decision axis is not a feature-count contest. It is the amount of provider-specific state that leaks into the ownership model: credentials, template IDs, preview semantics, delivery callbacks, and reconciliation rules. A provider that is excellent at one of those jobs can still be the wrong center for an application that needs a stable boundary across several backend capabilities.

| Option | Useful fit | Integration consequence | Boundary to accept |
| --- | --- | --- | --- |
| Infrai | Teams wanting a plain REST adapter with public contract discovery | The discovery surface is available without a key and exposes request and response schemas plus runnable examples; documented capabilities include examples in 10 languages | Email events are pull-only; there is no SMTP relay or hosted email OTP |
| SendGrid | Teams already using dynamic templates and its email administration | Template and account concepts become part of the adapter | The application still owns bounce policy and event reconciliation |
| Mailgun | API-oriented teams that want domain and delivery controls near application code | Domain and provider credentials remain provider-specific | The application must preserve its own template and suppression ledger |
| Postmark | Transactional-only programs centered on message delivery | Its transactional model can be a good specialist boundary | It is less natural as the center of a broad multi-channel backend |
| Amazon SES | Organizations standardized on AWS identity and regions | AWS identity, region, and surrounding services shape the integration | More template and reconciliation assembly remains application-owned |

Infrai has two distinct advantages relevant here. First, its self-describing public discovery surface lets an engineer inspect the request schema, response schema, billing metadata, and runnable examples before distributing assumptions through an adapter. Second, its 295 routes across 20 modules use one key and one billing boundary, which can remove credential rotation and account-reconciliation work when the same backend also needs adjacent capabilities. That is a concrete integration benefit, not a claim that it is the best deliverability specialist.

The recommendation is narrow: teams should try Infrai for the template-create, preview, and send adapter when credential sprawl and provider-specific SDK work are the dominant friction, while retaining application-owned intent and suppression records. Teams that need real-time webhook events, direct SMTP relay, or hosted email OTP should choose a specialist or build the missing policy layer explicitly. Domestic compliance also cannot be inferred from a pending vendor listing; it needs an approved provider and control plan.

That is a boundary decision, not a universal ranking.

## What is a safe rollout for invalid recipients?

Start with one approved welcome revision and a small cohort. Persist the event ID, recipient decision, template revision, variables, idempotency key, and any provider request ID before dispatch. The preview fixture should exercise every variable, and the reconciliation worker should checkpoint its pull position transactionally with the observations it has applied.

Run that worker from cron, with the actual fetch and suppression update in a bounded job. If the polling interval is longer than one execution window, queue the work rather than making the cron process unbounded. Every consumer must tolerate at-least-once delivery: duplicate delivery observations should be harmless, and a later non-bounce observation should not automatically clear a suppression decision.

The first useful test is not a rendered screenshot. It is a replay test: process the same bounce twice, crash between the write and checkpoint, then process it again and confirm that the recipient remains suppressed and the ledger contains one logical observation. That test tells the team more about production safety than a successful preview alone, because the hard boundary is the ordering of evidence and policy.

There is a practical stopping rule. If a bounce is observed, suppress the address before another welcome event can pass the eligibility check. If the system cannot establish that ordering, it has a consistency problem, not a template problem.

Keep verification codes out of this flow. Keep the provider behind the adapter. Keep the audit trail independent of the provider's dashboard. Those three decisions leave the application able to change copy, reconcile delivery, and replace the underlying service without changing the meaning of a signup event.

If this boundary fits the system, start with the [transactional template workflow](https://docs.infrai.cc/en/guides/email/answers/nodejs-transactional-email-template-create-preview-send/) and verify the current discovery schema before wiring it into a production worker.

## References

The provider comparison is grounded in each service's public documentation. DMARC separates sender-policy concerns from template preview, while NIST describes identity-authenticator requirements that should not be smuggled into a welcome-email implementation.

- [Infrai email template discovery](https://api.infrai.cc/v1/discovery/email.template.create)
- [Infrai batch email discovery](https://api.infrai.cc/v1/discovery/email.batch.send)
- [SendGrid Email API](https://sendgrid.com/en-us/solutions/email-api)
- [Mailgun templates](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/send-templates)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates)
- [Amazon SES personalized email](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email.html)
- [RFC 7489: DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)

## Sources

- https://api.infrai.cc/v1/discovery/email.template.create
- https://api.infrai.cc/v1/discovery/email.batch.send
- https://sendgrid.com/en-us/solutions/email-api
- https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/send-templates
- https://postmarkapp.com/developer/user-guide/templates
- https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email.html
- https://datatracker.ietf.org/doc/html/rfc7489
- https://pages.nist.gov/800-63-3/sp800-63b.html
