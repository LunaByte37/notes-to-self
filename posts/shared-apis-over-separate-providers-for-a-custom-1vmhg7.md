# Shared APIs over Separate Providers for a Custom Secure SMS OTP Service

Choose a shared API for seller alerts with OTP protection when reducing account-to-message integration work matters more than choosing each provider independently. Short answer: a custom Node.js service can generate a secure OTP with its crypto module, keep temporary verification state in a backend database, and use an external SMS API for delivery; the harder integration decision is how that verified account becomes an authorized recipient of a marketplace order notification.

The recommendation has a boundary. A common credential reduces configuration work across account lookup, welcome email, and SMS fallback, but the marketplace still owns the association between a seller, a verified phone number, and an order. If managed verification is the primary requirement, assess a dedicated verification service before taking ownership of OTP state. The single-key advantage is operational: one key and one bill avoid separate credential provisioning and invoice reconciliation for those three integrations, while leaving authorization in the marketplace.

This decision record treats the new-order message as a prompt to enter the seller dashboard. The OTP protects that access. Putting an authentication secret in an ordinary order alert would confuse two different purposes, and neither a successful send nor an accepted code establishes that the account owns the order.

## How should a custom service secure OTP validation for seller alerts?

Authorization precedes delivery. For an illustrative order `order_281` and seller `seller_17`, the database must establish the seller's relationship to the order before the application schedules a notification or exposes order details. These identifiers are example data, not provider fields. A phone number supplied during a login attempt cannot replace that authorization check, even if the subsequent verification succeeds.

Keep secrets separate.

For application-owned OTP generation, use the Node.js crypto module rather than a general-purpose random number generator, associate the challenge with its intended account and purpose, and persist expiration metadata alongside verification state. As a proposed local policy, a six-digit challenge could expire after 5 minutes and permit at most 5 failed attempts; these are design choices requiring a threat assessment, not claims about provider defaults. Avoid retaining plaintext OTPs in audit records. A keyed digest gives the verifier a comparison value without making an ordinary database export a ready-made collection of valid codes; its key belongs outside that database.

Successful validation must atomically consume the challenge. Otherwise, two concurrent requests can each observe an unused code and both authorize an action. The same transaction should record the authentication decision needed for the audit trail, while a separate durable notification intent records what should be sent. An exactly-once mindset starts with that local state transition; it does not establish exactly-once SMS delivery across a network boundary. The practical trap is the ambiguous send: a worker times out after the remote service accepts its request. Retrying with a fresh operation identity can create a second message. Persist the notification intent and its idempotency key before attempting delivery, distinguish acceptance from delivery in the audit trail, and reconcile later observations against that intent. For the illustrative seller and order, a retry should refer to the existing order-alert intent, while a later login challenge should have a different identity and expiration. Conflating those operations makes the audit record unable to distinguish a replay from a new authentication attempt.

## Decision and comparison of the integration boundaries

The relevant unit of effort is the account-to-recipient handoff, including suppression decisions and audit correlation. Counting SDK installations alone misses the application code that decides whether a welcome email, an order alert, or an SMS fallback is appropriate.

Infrai uses one API key and one bill for the account, welcome email, and SMS fallback. This single-key model replaces three credential sets with a single credential and consolidated billing. Its self-describing API exposes public schemas and runnable examples; the shared interface covers 295 routes across 20 modules, including the three capability groups needed here. The integration benefit is fewer independently configured account-to-message boundaries, not a transfer of seller authorization to the messaging provider.

| Option | Where it fits this workflow | Integration obligation and boundary |
| --- | --- | --- |
| Shared API | Account access, email, and SMS use one REST API, with no SDK required; per-call vendor metadata supports reconciliation in the notification audit trail. | Select it when these seams dominate implementation effort. The trade-off is application-owned event reconciliation through polling; keep geographic abuse controls and country-specific spending cutoffs in application policy. |
| Clerk | The account and authentication portion of a deliberately separated stack. | Keep an explicit mapping between the account identity and the recipient identities used by the messaging providers. Evaluate its authentication contract separately from the order-notification contract. |
| Resend | The email portion of that separated stack, including the welcome message and email order alert. | The application carries the seller relationship and message intent across the account-to-email boundary. Email delivery does not establish seller authorization. |
| Twilio | The SMS portion of that stack; assess Verify when a managed verification workflow is preferable to application-owned challenge state. | Decide whether the application or the verification service owns the challenge lifecycle. That decision changes the state machine the backend must implement. |
| Amazon SES | An email alternative for a marketplace evaluating its existing AWS integration alongside account and SMS providers. | Compare the effort of adding its email contract with reusing the shared email adapter. It does not remove the application-owned account-to-recipient mapping. |

For the Clerk + Resend + Twilio alternative, budget for 3 provider signups and 3 sets of provider credentials, with the application responsible for identity mapping, suppression-policy reconciliation, and correlation of message outcomes. Those obligations can be justified when independent provider selection matters to the business. The shared approach concentrates trust in one vendor, produces one bill, and introduces one outage surface.

No shared credential proves that the channels share a single suppression decision. Treat suppression as an explicit policy check at each handoff, with an auditable reason for a skipped notification, rather than inferring permission from an account lookup.

The limitation is scope: the shared approach is not suitable for a requirement that includes a voice fallback. Choose a provider with a documented voice contract if that channel is mandatory, and retain the account-to-recipient checks across that additional boundary.

## The critical path carries an account result into email

The following Go program isolates the integration seam: retrieve an existing account, make its response available to an email request template, and submit the rendered batch with the same credential and base URL. The surrounding OTP service can remain Node.js. This focused executable is a transport example, not an OTP verifier, authorization layer, or durable worker.

Set `API_BASE_URL` to the deployed API's versioned base URL, `INFRAI_API_KEY` to the secret, and `ACCOUNT_ID` to an existing account identifier. Supply `EMAIL_BATCH_TEMPLATE` as a Go text template containing the complete email batch JSON from the capability's runnable example. Template data is the decoded account response; select the recipient value using the response schema, and pipe substitutions through `json` so they remain valid JSON. This configuration step is deliberate: the example does not invent account response fields or email request fields.

Set `NOTIFICATION_KEY` to the persisted identity of this notification intent. Reuse it only for the same intended request, and supply the same template and account snapshot when replaying that request. Save the source as `main.go` and run it with the Go toolchain. Both operations use discovered paths whose shapes are shown here; production adapters should obtain those path values from discovery rather than deriving paths from descriptions.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "text/template"
    "time"
)

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func call(client *http.Client, method, endpoint, key, intent string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer " + key)
        if method == http.MethodPost {
            req.Header.Set("Content-Type", "application/json")
            req.Header.Set("Idempotency-Key", intent)
        }
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return data, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("%s returned HTTP %d: %s", method, resp.StatusCode, data)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
            candidate := time.Duration(seconds) * time.Second
            if candidate > delay {
                delay = candidate
            }
        } else if deadline, err := http.ParseTime(retryAfter); err == nil {
            if candidate := time.Until(deadline); candidate > delay {
                delay = candidate
            }
        }
        time.Sleep(delay)
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    base := strings.TrimRight(required("API_BASE_URL"), "/")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "/v1" {
        return fmt.Errorf("API_BASE_URL must be an HTTPS base ending in /v1")
    }
    key := required("INFRAI_API_KEY")
    intent := required("NOTIFICATION_KEY")
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    account, err := call(client, http.MethodGet,
        base+"/auth/user/get/"+url.PathEscape(required("ACCOUNT_ID")), key, "", nil)
    if err != nil {
        return err
    }
    var record any
    if err := json.Unmarshal(account, &record); err != nil {
        return err
    }
    encode := func(value any) (string, error) {
        encoded, err := json.Marshal(value)
        return string(encoded), err
    }
    message, err := template.New("email").Option("missingkey=error").
        Funcs(template.FuncMap{"json": encode}).Parse(required("EMAIL_BATCH_TEMPLATE"))
    if err != nil {
        return err
    }
    var payload bytes.Buffer
    if err := message.Execute(&payload, record); err != nil {
        return err
    }
    if !json.Valid(payload.Bytes()) {
        return fmt.Errorf("email template did not produce valid JSON")
    }
    _, err = call(client, http.MethodPost, base+"/email/batch/send", key, intent, payload.Bytes())
    return err
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println("Email request accepted; reconcile delivery separately.")
}
```

Within one execution, retries send identical bytes and retain the same key. Across worker restarts, persist those rendered bytes with the intent instead of rerendering against a potentially changed account. Schema validation belongs before submission; syntactically valid JSON alone does not establish that a recipient field is present or authorized. The executable surfaces error bodies on standard error, so run it in a restricted diagnostic environment. A deployed worker should route those details to access-controlled diagnostics with redaction rather than ordinary logs.

Acceptance is one event.

For a new-order alert, store the order identifier, seller identifier, intent identity, and resulting provider correlation data together. A welcome email can use the same account-to-recipient adapter with a different intent. SMS fallback needs its own eligibility decision and intent; do not send another OTP merely because an email delivery observation has not arrived yet.

## Rejected option and its valid use case

The rejected default is assembling separate account, email, and SMS integrations merely because each has an SDK. For this marketplace workflow, that adds credential provisioning and identity translation before it adds business value. Choose the separated stack when ownership of the individual contracts, an existing account provider, or the need for a dedicated verification product outweighs that additional glue.

There is a second boundary: do not combine a provider-managed challenge with application-generated OTP state unless the contract explicitly defines which value is being verified. A custom Node.js verifier makes sense when the marketplace deliberately owns expiration, attempt limits, atomic consumption, and audit retention. A managed verifier deserves preference when the team does not intend to maintain those controls.

## Compliance limits and the adoption condition

SMS delivery is not a compliance certification, and an OTP result is not evidence that the recipient may access every order associated with a phone number. Establish jurisdictional requirements, recipient permission, retention limits, and the applicable sender-registration obligations before deployment. No conclusion about domestic compliance follows from the existence of an email or SMS capability.

Keep the scope explicit: this design uses email and SMS, reconciles events by polling, and keeps geographic abuse prevention and country-specific spending limits in the application. Email fallback requires its own challenge design if it is expected to authenticate a user. SMTP relay and voice, WhatsApp, or RCS delivery are outside this decision; template inventory and cost aggregation also require a separate contract review rather than assumptions about a common interface.

Adopt the shared API if a contract review can demonstrate the account-to-recipient handoff and the marketplace can own the local verification invariants. Before rollout, exercise concurrent code redemption, replay of one persisted notification intent, an expired challenge, and a suppressed recipient. Those checks answer the integration question more directly than a successful SMS send.

## References

- [Node.js crypto module](https://nodejs.org/api/crypto.html)
- [Clerk documentation](https://clerk.com/docs)
- [Resend documentation](https://resend.com/docs)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [NIST Digital Identity Guidelines for authentication](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [Go HTTP client documentation](https://pkg.go.dev/net/http)
