# DNS Runbooks: Record-Set Evidence for Customer Instructions Without Manual Drift

**Short answer:** Generate customer-facing DNS instructions from a versioned record set, then reconcile observed DNS answers against that same set before declaring deliverability ready.

In a marketplace that moves money, customer-facing DNS instructions should be generated from the same record set that the deployment applies. Hand-written prose is a second database, and second databases eventually disagree. The practical decision is therefore to make the record set authoritative, render instructions from it, and retain a versioned evidence trail for every change. This is less about polished docs than about proving which DNS state a customer was asked to publish.

## The constraint: one change, two audiences

An engineering system has to speak to an authoritative DNS service, while a customer needs a small, precise checklist: record type, name, value, and sometimes a verification rule. Those views are different representations of one intent. If an engineer edits a TXT value but forgets the customer page, the deployment can be perfectly healthy while onboarding stalls. In a payment flow, that mismatch is an audit finding waiting to happen.

I treat the record set as an immutable input to a build. A renderer consumes it and emits Markdown, HTML, or a support ticket payload. The generated artifact carries a digest of the input, a creation timestamp, and an explanation of why each record exists. A reviewer can then compare the requested change with the exact artifact that a customer received.

The detail matters. DNS names are case-insensitive in comparisons, TXT values can be split into quoted character-strings, and a trailing dot changes how a name is displayed without changing its meaning. Normalization belongs in the renderer, not in a copy editor's head.

## How should customer-facing DNS instructions use generated record sets to stop hand-written drift?

Start with a typed model rather than a text template. Keep fields for type, owner name, value, TTL, and purpose; include a stable identifier for the logical requirement. Then render two outputs from the same model:

1. An apply plan for the DNS operator.
2. Customer instructions that show only the fields the customer must enter.

The renderer should reject ambiguous input. Duplicate owners with incompatible TXT values, an empty target for an MX record, or a CNAME placed at an apex are validation failures before publication. It should also emit a machine-readable fingerprint so a later reconciliation job can answer, “Which instruction generated this record?”

Here is a deliberately small Go example. It does not call a provider; the boundary is an interface so the same checks work with a hosted service or an in-house nameserver.

```go
package dnsdocs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type Record struct {
	ID, Type, Name, Value, Purpose string
	TTL                            int
}

func Fingerprint(records []Record) string {
	rows := make([]string, 0, len(records))
	for _, r := range records {
		rows = append(rows, fmt.Sprintf("%s|%s|%s|%s|%d", r.Type, strings.ToLower(r.Name), r.Value, r.Purpose, r.TTL))
	}
	sort.Strings(rows)
	sum := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return hex.EncodeToString(sum[:])
}
```

The sort makes the fingerprint independent of input order. That small property prevents noisy diffs when an API returns records in a different sequence. In my ledger work, an apparently harmless reorder once caused a review to miss a changed value; the resulting alert was a generic `E409`, not a useful explanation. Now the digest is logged beside the deployment event and the customer-document revision.

## Evidence beats a green deployment

Publication success only says that an operator accepted a request. It does not prove that recursive resolvers see the intended answer, that a customer copied the right quoting, or that an old record has expired from caches. A reconciliation worker should query authoritative data and selected public resolvers, record the observed RRset, and compare it with the expected normalized set. Store both the match and the reason for a mismatch. In practice, the worker needs a small state machine: `pending` while an apply event is queued, `published` after the authoritative source accepts it, `observed` when the expected set appears at each configured vantage point, and `accepted` only after the observation window closes. Each transition should carry the record-set digest, actor, and event ID. If a customer changes a value directly, the next observation becomes a named drift event, not an unexplained support ticket. That distinction is valuable during a chargeback investigation, because a green deployment log alone cannot establish what a receiver could resolve at the relevant time. Keep the raw response, normalized comparison, and retention policy together; otherwise an auditor sees a badge without the evidence that produced it.

One rule: publish evidence.

DMARC gives this discipline a useful example. RFC 7489 defines a policy record at `_dmarc` and describes reporting signals that let a domain owner evaluate authentication alignment. The customer instruction should identify the exact host name and policy value, while the evidence record should preserve what was published and when it was observed. Do not infer deliverability from a single lookup; mail receivers apply their own policies and cached data.

A good audit row contains the requirement ID, expected digest, observed digest, resolver vantage point, timestamp, and actor. Redact secrets before writing it. DNS values are usually public, but an onboarding token embedded in a TXT value may still be sensitive operational data.

## Choosing a rollout that can be reversed

Generate instructions in a preview environment first. A pull request should show the old and new normalized record sets, the rendered customer diff, and validation results. After approval, publish with an idempotency key derived from the requirement ID and version. Retrying the same event must converge on the same RRset and document revision, rather than append another paragraph to a ticket.

The catch is that generated prose cannot resolve an underspecified policy. If product requirements do not say which subdomain is delegated, or if several teams own the same zone, a human decision is still required. Freeze publication, ask for the missing owner, and record that decision. Hand-written instructions remain suitable for a one-off incident runbook or a legal notice whose wording is intentionally not derived from DNS; they are not suitable as the source of operational truth.

| Approach | Strength | Cost or boundary |
| --- | --- | --- |
| Manual instructions | Fast for a one-time exception | Drift is invisible unless someone compares every field |
| Generated instructions only | Consistent customer view | Weak evidence if publication and observation are not recorded |
| Generated instructions plus reconciliation | Supports audit and retry semantics | Requires resolver access, normalization rules, and retention work |

Your mileage may vary with resolver geography and TTL policy, so define an observation window instead of promising instant convergence. I’m not sure a single “verified” badge is ever enough for a regulated marketplace; the safer claim is narrower: at time `t`, the selected observers returned the expected normalized set.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://www.rfc-editor.org/rfc/rfc1034
- https://www.rfc-editor.org/rfc/rfc1035
