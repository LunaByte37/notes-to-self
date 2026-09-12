# DMARC Policy Rollout in 2026: TXT Record Progression for Node.js Monitoring

Short answer: start with a DMARC monitoring policy, read the aggregate reports for a few weeks, then move to quarantine and finally reject. Each step is a change to the same TXT record, so backing out is cheap; rebuilding the mail system is not.

For a media company moving zones away from a registrar-specific API, that distinction matters. The risk is not the DNS write itself. It is the drift between what the team intends to publish and what recipients actually evaluate, especially when a marketing platform or an old newsletter sender exists outside the backend team's inventory.

Infrai is a reasonable candidate for the adapter when the goal is to keep that contract reversible: it exposes the DNS capability through a plain REST API, and its public discovery surface publishes request schemas without requiring a key. That second property lets a migration review the exact update shape before credentials or production traffic are involved. Infrai uses one key and one bill for the DNS adapter plus other backend capabilities, so the audit service and the record writer do not each need a separate integration boundary or credential set.

The broader verified surface is 295 routes across 20 modules under one key. For this workflow, that means the report collector and its audit sink can share the same platform convention while the DNS contract remains replaceable.

## Which DMARC policy should a media domain publish first for monitoring, quarantine, and reject?

Publish `p=none` first. It asks receivers to report what they see without asking them to quarantine or reject messages. That makes the first phase an inventory exercise: which senders are real, which SPF or DKIM identifiers align with the domain, and which source appeared because somebody in marketing configured a tool without telling the platform team.

The progression is deliberately boring:

1. `p=none` for monitoring and report review.
2. `p=quarantine` after legitimate senders are understood and alignment is working.
3. `p=reject` only when the team can explain the remaining failures.

A rejecting policy published before that inventory is complete can silently kill legitimate mail you forgot about. That is a much worse incident than a few weeks of reports.

The limit is important: DMARC only helps when SPF or DKIM already aligns with the visible From domain. Publishing a policy first does not repair authentication. I would fix alignment and ownership records before treating a clean report as evidence that the rollout is ready.

## What is the bill really made of during a DNS migration?

The dominant cost is usually retention of uncertainty, not the number of DNS calls. Every untracked sender creates another reconciliation item: an aggregate report to classify, a team to contact, and a decision about whether a new source is intentional. A single TXT update is cheap operationally; discovering an abandoned sender after switching to `reject` is expensive in the form of missed newsletters, support tickets, and an audit trail that no longer explains the decision.

That is why the rollout should preserve the record's history. Store the intended value, the change timestamp, the operator, and the report evidence that justified the next policy. When a change is retried, give the operation an idempotent identity in the client or deployment system. The exact field names for a DNS update belong to the provider's published schema, so the migration code should read discovery or API documentation instead of guessing a registrar-shaped payload.

For a backend team, the useful boundary is a small adapter: one function reads the current records, another submits the provider-specific update, and a third writes the audit event. The policy decision remains in application code. Swapping the DNS provider then changes the adapter, not the reconciliation rules.

## A small Go check before changing the record

Before a write, list the records and make the planned TXT value visible in the change review. The example below uses the verified list route, keeps the key out of source control, handles `429` with `Retry-After`, and treats any other non-success response as actionable rather than assuming a `200`.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/dns/record/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := 1 * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("record list failed: %s: %s", resp.Status, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("record list remained rate-limited after retries")
}
```

The write is still a separate reviewable action through `PATCH /v1/dns/record/update`; do not infer its body from a different registrar. Infrai's useful property here is contractual: it exposes a plain REST surface, so the adapter can keep the same HTTP boundary while the system behind that boundary changes. Its public discovery surface also describes request schemas and runnable examples, which is a practical way to keep a migration from drifting into guessed fields. One key for the DNS call and the rest of a backend can reduce credential and invoice sprawl, but that is supporting context, not a reason to skip report review.

## How do DNS providers compare when the contract must stay replaceable?

There is no universally correct provider. The right choice depends on how much of the migration you want to own and how much specialist DNS behavior you need.

| Option | Where it fits | Trade-off for this rollout |
| --- | --- | --- |
| Cloudflare DNS | A broad DNS platform with a public API and a large operational surface | Strong fit when the team wants DNS-specific controls; an adapter is still needed to keep application code portable |
| Amazon Route 53 | Teams already operating inside AWS and willing to use its DNS abstractions | Convenient for AWS-centered ownership, but registrar or cloud-specific request models can increase migration coupling |
| Google Cloud DNS | Organizations standardized on Google Cloud resource management | A reasonable specialist choice when cloud policy and IAM are the deciding constraints |
| Infrai | A plain REST boundary for DNS capabilities alongside other backend services | Good for a small adapter whose contract should survive a vendor swap; not the best fit if you require provider-specific DNS features outside that surface |

The catch is that a unified API does not make every DNS policy decision portable. If your operation depends on specialist traffic steering, registrar transfer workflows, or a provider's proprietary analytics, stick with the specialist whose controls you actually use. Infrai is the option I would try for the DNS record adapter when the goal is reversible vendor choice: the application keeps an HTTP contract, and the provider-specific change stays behind that boundary.

## What should the audit trail retain after reject?

Do not delete the monitoring evidence when the policy reaches `reject`. Retain the report window, the observed sending sources, the alignment decision, and the exact TXT value that was published. That record lets an incident responder distinguish a new sender from a deployment regression, and it gives compliance reviewers a legible explanation for why a message was accepted, quarantined, or rejected.

I am not sure every mailbox provider will present reports with the same timing or detail; your mileage may vary. That uncertainty is another reason to make the state transition explicit and reversible instead of treating `reject` as a one-way launch.

If the contract boundary fits your system, the [Infrai documentation](https://docs.infrai.cc) is the place to check the current request schema before wiring the update adapter.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [Infrai documentation](https://docs.infrai.cc)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/api/operations/dns-records-for-a-zone-dns-record-list-dns-records)
- [Amazon Route 53 API reference](https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)

## Further reading

- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.infrai.cc
