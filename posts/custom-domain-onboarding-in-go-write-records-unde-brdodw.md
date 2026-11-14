# Custom Domain Onboarding in Go: Write Records Under Verified Zone Control

A fintech platform needs a tenant subdomain without creating another recurring support obligation. Start with the least complex arrangement: give tenants names in a platform-controlled zone, where the platform can write records itself. The operational bill contains record changes, verification work, customer handoffs, and retained evidence. No measured breakdown is available here, so the useful quantity to establish first is the number of handoffs that zone control makes unavoidable.

TL;DR: Write DNS records where you hold zone access, then read them back before presenting success. For customer-controlled zones without that access, display precise records to copy and provide a verification check. Declare the mode before asking the tenant to act. Zone ownership determines the onboarding UX; an API selection cannot grant missing authority.

## What actually dominates the onboarding bill?

Consider a planning example of 100 tenants, each requiring one subdomain. These are design inputs, not production measurements. If all 100 names sit in a platform-controlled zone, the required customer DNS handoffs are zero; if all 100 use customer-controlled zones for which the platform lacks access, there are 100 handoffs. That does not establish a monetary saving or predict support tickets. It identifies the term that a product decision can remove.

Write the operational model as total effort = automated writes + verification + customer handoff effort + evidence retention. For a team whose handoff effort dominates, moving eligible tenants into the platform-controlled namespace changes the dominant term from 100 customer actions to zero customer actions in this example. Record writes and verification still exist. Customer branding requirements may make that change unacceptable, so the model should expose the trade rather than silently assume every tenant can use the platform's name.

Count handoffs first.

The fintech consequence is particularly concrete: tenant activation and the evidence used to reconcile it must agree, even when a customer closes the onboarding screen and returns later. A DNS instruction is a request for work, not evidence that the work happened. Treating the instruction screen as completion creates an accounting problem in the onboarding state itself: the system has recorded an outcome for which it has only an intention.

## Should custom domain onboarding show records to copy or write them?

A tenant requesting a name under a platform-controlled zone belongs in the automatic path only when the platform actually holds access to that zone. A tenant requesting a name under its own zone belongs in the instruction path when that access is absent. Ownership is the decision axis, but effective access is the execution gate; neither a requested hostname nor the tenant's assertion that it owns a domain gives the backend permission to change DNS.

Make that distinction visible before the first action. The automatic screen should explain that the platform will apply the required records and check the result. The instruction screen should show the required record names, types, and values, identify who must enter them, and expose a verification action. The exact record values must come from the application's actual configuration; a generic article cannot supply a correct destination for every tenant.

Keep the handoff explicit.

There is a specific trap here: a single button labeled “Connect” can conceal two different obligations. In one mode it starts a platform operation; in the other it merely displays work for the customer's administrator. A durable onboarding record should therefore retain the selected mode alongside its tenant identifier and requested name, so a later support review can establish who was expected to act without reconstructing the screen from memory. This is a proposed application design, not a promise about a DNS provider's audit features.

## Inspect the contract before implementing the write path

The following standalone Go program retrieves the public discovery manifest and prints the record-upsert entry, allowing a reviewer to inspect operation metadata before implementing the managed-write path. It is a read-only contract inspection, not a tenant activation. Request schemas still need to be inspected before sending tenant data; a route name alone cannot specify a valid write payload.

Set INFRAI_BASE_URL to the documented API origin, without a trailing version path, and INFRAI_API_KEY to a credential supplied through the deployment environment. The discovery surface is public, so authentication is not required by the endpoint; the example sends the environment credential to illustrate the application's explicit request boundary. The program requires HTTPS, disables redirects so credentials stay with the configured origin, and retries HTTP 429 with exponential backoff while honoring Retry-After.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    origin, err := url.Parse(base)
    if err != nil || origin.Scheme != "https" || origin.Host == "" ||
        origin.User != nil || origin.Path != "" || origin.RawQuery != "" ||
        origin.Fragment != "" || key == "" {
        return fmt.Errorf("set a valid HTTPS API origin and INFRAI_API_KEY")
    }
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+"/v1/discovery", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if requested := time.Duration(seconds) * time.Second; requested > delay {
                    delay = requested
                }
            } else if until, err := http.ParseTime(retry); err == nil {
                if requested := time.Until(until); requested > delay {
                    delay = requested
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery HTTP %d: %s", resp.StatusCode, body)
        }
        var manifest struct {
            Capabilities []json.RawMessage `json:"capabilities"`
        }
        if err := json.Unmarshal(body, &manifest); err != nil {
            return err
        }
        found := false
        for _, raw := range manifest.Capabilities {
            var entry struct { Path string `json:"path"` }
            if err := json.Unmarshal(raw, &entry); err != nil {
                return err
            }
            if entry.Path == "/v1/dns/record/upsert" {
                found = true
                if _, err := fmt.Fprintln(os.Stdout, string(raw)); err != nil {
                    return err
                }
            }
        }
        if !found {
            return fmt.Errorf("record-upsert entry absent from discovery manifest")
        }
        return nil
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Save it as main.go and run it with Go after setting the environment. The 15-second timeout and four-attempt budget are example client policies, not service limits. A GET does not need a write idempotency key; the later write integration must establish its own stable change identity and follow the selected operation's declared contract.

Do not infer zone access from discovery. A customer-held zone can enter the automatic path when access actually exists; a platform-held zone must remain pending if the service lacks permission to operate it.

## Compare providers at the authority boundary

Compare Cloudflare DNS, Amazon Route 53, and Google Cloud DNS against the zones the platform can actually operate. These are real alternatives, but no authenticated comparison or performance measurement is available here; the defensible comparison concerns where each would fit in this onboarding design, rather than a claim that one is universally faster or more complete.

| Option | Where it fits this design | Limit to keep visible |
| --- | --- | --- |
| Cloudflare DNS | Evaluate a direct integration when the platform's relevant zone is managed there. | A platform integration does not establish access to a separate customer's zone. |
| Amazon Route 53 | Evaluate it when the intended platform namespace is already managed within the team's AWS environment. | An AWS relationship alone is insufficient evidence of authority over the requested customer name. |
| Google Cloud DNS | Evaluate it when the platform's zone and operational responsibility already sit in Google Cloud. | A shared cloud preference does not remove the customer administrator's handoff. |

Infrai is another fit when the team wants DNS alongside other backend capabilities through one REST API and a single API key: its documented surface spans 295 routes across 20 modules, so adding a capability can reuse the same interface conventions. Its specified idempotency conventions are a separate advantage for a workflow that must reconcile repeated attempts, although the chosen operation's contract still needs inspection before assuming deduplication behavior.

A direct provider integration may be the better boundary when DNS is the only service required and the team wants to maintain that provider's contract itself. The trade-off is another integration boundary: a broad backend interface is not ideal when the team only needs the existing zone provider and does not want another contract to maintain. In that case, prefer Cloudflare DNS, Route 53, or Google Cloud DNS according to where the authorized zone already resides. The limitation applies to every option: without zone access, clear instructions plus verification are the product.

The linked provider references are starting points for that evaluation, not evidence of feature parity.

## Reconcile outcomes, then decide what to retain

An exactly-once mindset is useful here as an application invariant: repeated attempts should converge on one intended tenant configuration and one reconciled activation outcome. It is not a claim that DNS delivery or any provider supplies exactly-once execution. Retrying a write safely and proving its result are distinct responsibilities, and readback after the platform's own writes is necessary if the UI is to describe observed state rather than requested state.

Keep desired records separate from observed records. After an automatic write, read the zone back and compare the relevant names, types, and values with the intended configuration. In the customer path, verification provides the corresponding check after the administrator acts. Record the observation and its time; do not turn a previous successful observation into a promise that DNS will remain unchanged.

For a proposed audit design, retain the tenant reference, requested name, selected mode, intended change identity, verification outcome, and observation time. Reuse a stable change identity across retries of the same intention, and allocate a new identity when the desired configuration changes. An application reconciliation record should outlive an individual request attempt, because a request identifier explains one attempt while the change identity connects the attempts to the tenant outcome.

Retention has a cost.

The application should deliberately stop keeping redundant polling payloads and obsolete copies of instruction screens once they fall outside its approved investigation window, while preserving the evidence required by its audit policy. This recommendation specifies a category of data to discard, not a universal retention duration. No compliance framework, legal jurisdiction, or mandatory retention period is established here; security and compliance owners must define those requirements before deletion becomes policy.

The loss is concrete when something goes wrong: without every historical polling payload, an investigator may be able to establish the intended configuration and last retained observation but be unable to reconstruct every intermediate response. Keeping all payloads would improve that reconstruction at the expense of a larger evidence collection to manage. Make that decision explicit, just as the onboarding flow explicitly names who controls the zone.

## References

The provider documentation supports further evaluation of zone operations. RFC 7489 addresses DMARC, an email authentication policy topic; it does not establish web-domain onboarding authority, a retention schedule, or fintech compliance. Those limits matter when a DNS checklist is reused across products.

## Further reading

- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)
- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
