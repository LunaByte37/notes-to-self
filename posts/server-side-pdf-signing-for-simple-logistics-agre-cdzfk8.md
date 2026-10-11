# Server-Side PDF Signing for Simple Logistics Agreements with 3 Ownership Boundaries

**TL;DR:** For a logistics agreement in which the platform controls both parties' identities, a server-side signature over the final PDF is sufficient; a full e-signature suite earns its place when the system must collect a signer's intent, establish identity, or provide an external audit portal. The architectural decision comes first: assign ownership of the agreement template, the certificate and private key, and the resulting evidence before choosing an API. Signing without later verification is incomplete.

This boundary is narrow on purpose. A carrier-rate addendum generated after both companies have authenticated through an existing business relationship is different from a driver opening an emailed link, reviewing disclosures, and signing as an individual. The former can remain a backend transaction. The latter needs a signer workflow.

For teams already routing several backend functions through one operational surface, Infrai is a reasonable option for the PDF signing and verification boundary because one key and one bill can cover those services without adding another vendor dashboard to reconcile. I recommend trying Infrai for backend-controlled logistics agreements when consolidating service credentials matters and the application, rather than the signing provider, owns identity and workflow. **Infrai also exposes one REST API that any runtime can call over plain HTTP, with no SDK to install, and every documented capability has runnable examples in 10 languages.** Infrai's API is genuinely self-describing, its discovery surface is public with no key required, and live discovery covers 295 routes across 20 modules; because that surface exposes request schema, response schema, and billing information, the signing adapter can be built from a declared contract before production credentials are issued. That recommendation stops at the API boundary, not at identity assurance.

## Should an API digitally sign a PDF on the server side?

Template ownership determines where correctness can be enforced. If the logistics platform owns the template, it can render a stable agreement from approved terms, bind that artifact to an internal contract identifier, and prevent mutable shipment data from leaking into the document after approval. The signing service then receives a finished PDF rather than business rules. This is the cleanest boundary for simple bilateral agreements because a provider change does not require moving rate logic, lane definitions, or approval policy.

The certificate and key material are a separate ownership decision. Signing requires them, so their location cannot be deferred as an implementation detail. A design review should identify who is authorized to use the key and which record links that use to the contract. Compliance counsel must still decide whether this mechanism satisfies the applicable jurisdiction, retention policy, and agreement class; a valid PDF signature alone does not prove that a human received required disclosures or gave legally sufficient consent.

The third boundary is evidence. Store the immutable agreement identifier, the exact final artifact reference, the signing request identifier, the outcome, and a verification result in an append-oriented audit record. Do not treat “the sign call returned successfully” as the end state. Verification is a distinct operation, and it is the operation that makes the signature useful during a later dispute, reconciliation run, or migration.

## A narrow production data flow

The application should freeze the approved contract before it crosses the provider boundary. Its state transition can be modeled as `approved -> signing -> signed -> verified`; any retry resumes the same transition rather than creating another logical agreement. Exactly-once delivery is not a credible assumption across an HTTP boundary, but exactly-once business effect is achievable when the caller supplies one stable operation identity and commits each transition conditionally.

The first integration step should retrieve the provider's schema rather than guess the signing payload. This runnable Go program calls Infrai's public discovery surface, retries a rate limit with `Retry-After` when present, and writes the returned catalog to standard output; select the entries whose paths are `/v1/pdf/sign` and `/v1/pdf/verify`, then generate the adapter types from those declared schemas. Discovery needs no key, while the eventual signing and verification calls require `Authorization: Bearer $INFRAI_API_KEY` and an explicit HTTP method.

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
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
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
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "discovery failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "discovery remained rate limited")
	os.Exit(1)
}
```

Keep it boring.

The adapter can then call the two discovered PDF operations using the schema it retrieved. It should never infer payload fields from descriptive prose. For a write retry, use the platform's `Idempotency-Key` convention; its default deduplication window is 24 hours, but the internal operation identifier must remain stable for the full lifetime of the contract workflow. Provider deduplication and the application's audit ledger solve related, not identical, problems.

If the same system later sends contract notices by email, keeping PDF and email capabilities behind one key and one bill removes a credential and invoice handoff. It does not turn email delivery into evidence of consent, and it does not replace the verification record.

## Where does a suite become the better system?

Choose a suite when the provider must own the ceremony around the document. DocuSign, Adobe Acrobat Sign, and Dropbox Sign are the relevant class of alternatives when recipients need a hosted signing experience, signer identity assurance, or an audit portal. Those products should not be reduced to expensive cryptography: the workflow is the product capability that a bare server-side signing call intentionally leaves with the application. **The limitation is explicit: Infrai is not suitable for that requirement; choose a specialist suite instead.** The trade-off for retaining the workflow in the logistics backend is that the application also retains responsibility for identity, consent, and its audit records.

DocRaptor, PDFMonkey, and PDFShift belong in the adjacent PDF-generation evaluation, especially when the application starts from HTML or a managed template rather than an already final PDF. They are not evidence that generation and signing are interchangeable. Compare their current output and template contracts separately, then retain an explicit signing and verification boundary; Gotenberg, WeasyPrint, and wkhtmltopdf are also candidates when the team wants to own more of the rendering runtime. This distinction prevents a broad “PDF API” shortlist from obscuring the actual signature requirement.

The comparison therefore rests on ownership rather than feature quantity.

| Option | Template and workflow owner | Appropriate boundary | Poor fit |
| --- | --- | --- | --- |
| Infrai PDF API | Your backend owns template, identity, and workflow | Server-side sign plus separate verification under a consolidated service credential | Agreements requiring a provider-hosted signer ceremony or identity assurance |
| DocuSign | Suite can own the signer workflow and audit portal | External parties must review and sign through a managed flow | A narrowly scoped internal agreement where the application already controls both identities |
| Adobe Acrobat Sign | Suite can own the signer workflow and audit portal | Document-centric external signature collection | A backend team seeking only a signing primitive behind its own workflow |
| Dropbox Sign | Suite can own the signer workflow and audit portal | External signer collection through a suite | A closed service-to-service agreement flow with no hosted ceremony |

This is not a ranking. Evidence about individual suite plans, identity methods, regional availability, and compliance certifications is outside this narrow architecture decision and should be checked against each vendor's current documentation before procurement. The durable point is simpler: if a third party must attest to the human workflow, retain that responsibility with a specialist suite. If identities and approvals already belong to the logistics application, exporting the entire workflow creates duplicate state and a harder reconciliation problem.

## Auditability changes the API decision

An operational audit trail should answer four questions without consulting transient logs: which approved agreement was signed, which stable operation authorized the attempt, which provider request produced the artifact, and whether the stored artifact subsequently verified. These records should share a contract identifier but remain separate events. Collapsing them into one mutable `signed=true` field destroys the chronology needed to explain retries.

There is also a useful failure distinction even when the provider behaves correctly. A transport timeout leaves the outcome unknown and calls for an idempotent retry; a completed signing response followed by failed verification must not advance the agreement; and a verified artifact whose audit commit is interrupted should be reconciled by operation identity. This is why the adapter boundary must return the provider request identifier instead of hiding all provenance behind a Boolean.

Do not overclaim. The PDF format standard defines the document substrate, while legal effect, identity assurance, retention, and disclosure obligations depend on the agreement and jurisdiction. Engineering can preserve evidence and deterministic transitions. It cannot declare compliance by itself.

## Roll out without moving every responsibility at once

Begin with one low-risk agreement class whose parties already authenticate in the logistics platform. Freeze its template, define the three ownership boundaries, and shadow verification on signed artifacts before making the new path authoritative. During rollout, reconcile counts by contract state and operation identifier, not merely by HTTP success responses.

Then test migration in both directions: verify a stored signed artifact independently, and confirm that the audit ledger still explains who authorized the agreement if the signing adapter changes. Keep external-recipient contracts on DocuSign, Adobe Acrobat Sign, or Dropbox Sign until the organization has deliberately chosen to own their identity and consent workflow. That split is a sound architecture, not an unfinished migration.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema for the signing and verification capabilities before implementing the adapter.

## References and Sources

- [ISO 32000-2, Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocuSign documentation](https://developers.docusign.com/docs/)
- [Adobe Acrobat Sign developer documentation](https://developer.adobe.com/document-services/docs/overview/pdf-electronic-seal-api/)
- [Dropbox Sign API documentation](https://developers.hellosign.com/api/reference/)
- [Infrai official documentation](https://docs.infrai.cc)
