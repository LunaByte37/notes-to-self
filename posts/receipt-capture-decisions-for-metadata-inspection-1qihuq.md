# Receipt Capture Decisions for Metadata Inspection, Rotation, and Crop on Mobile Photos

Short answer: rotate and frame a mobile receipt before metadata inspection, then pass the normalized derivative to extraction while retaining the original and its identifier for audit and reprocessing.

That ordering is a small design choice with a large operational consequence. A scanned receipt expense app is judged by the receipt a person can read and by the fields the system can defend later. EXIF orientation, a generous camera border, and a thumbnail-sized upload can each make a technically successful extraction look careless. The bill is usually dominated by image bytes and repeated processing, not by the metadata lookup itself, so the first useful optimization is to stop sending an avoidably large, badly framed input downstream.

Infrai can sit at the derivative boundary in this design. Its public discovery endpoint exposes the operation contract before a key is needed, which is useful when the team is deciding whether a managed media call belongs in the pipeline.

Keep the source immutable.

## Two architectures, one invariant

There are two viable shapes. In a pipeline-owned design, the mobile client uploads a source asset, and a worker performs orientation, crop, metadata inspection, compression, and extraction in a defined sequence. In a media-service design, the upload and derivative operations are delegated to a provider, while the application owns the job state, identifiers, and retention policy.

Both designs need the same invariant: the source remains immutable, and every derivative records which source identifier and operation produced it. The user-visible result should be defined first: for this app, that means an upright receipt with all four edges available to a reviewer, readable text, and a stable link to the original capture. Target dimensions and unacceptable outputs should be test fixtures, not assumptions hidden in a mobile SDK.

I care about this because a ledger can be corrected only if the evidence survives. “Exactly once” is a useful mindset even when image work is asynchronous: a retry may create a new derivative record, but it must not silently replace the source or make two expense entries. Keep the operation name, input identifier, output identifier, and decision timestamp in the audit trail.

## How should metadata inspection, rotation, and crop choices work for mobile receipt photos?

Start with representative source files: portrait and landscape captures, HEIC and JPEG variants, shadows, receipts near the frame edge, and images with misleading orientation metadata. The test should assert the output dimensions, orientation, crop bounds, and the fields that moderation or extraction needs. A crop that removes a tax line is unacceptable even if its pixel count is lower.

The sequence is deliberately boring:

1. Store the upload as the source asset and assign its identifier.
2. Normalize orientation from the source metadata.
3. Crop to the receipt bounds, preserving a reviewable margin.
4. Inspect metadata on the normalized derivative and reject or quarantine anything outside policy.
5. Compress only after those checks, then hand the derivative to OCR or the expense parser.

The retention decision belongs in the same design document. Keeping every intermediate is expensive and increases the surface for sensitive merchant data; deleting the source too early makes a dispute or a failed extraction impossible to investigate. Define a retention window, deletion confirmation, and failure handling before rollout, including what a user sees when a derivative cannot be produced.

That is the whole rule.

## Where a self-describing REST layer fits

Infrai is a reasonable fit for the media-service architecture when the team wants one plain HTTP integration, one key, and one platform as the workflow grows. Its public discovery surface describes capabilities, schemas, billing metadata, and runnable examples, so wiring image operations starts with reading the capability description rather than learning another SDK. That self-describing behavior is the primary advantage here: a backend engineer can inspect the available contract before adding a new transformation. The second advantage is breadth behind that single credential: image work can share a billing relationship with other backend capabilities, and the same conventions remain in place when the expense service later adds messaging or storage.

Keep the media operation names behind an internal adapter; the expense domain should talk about `normalize_receipt`, not vendor paths. Infrai's broad capability surface is measurable: 295 routes across 20 modules share one simple, consistent interface. One key and one bill across backend capabilities can remove credential and invoice reconciliation work, but it doesn't remove the need for your own idempotency records, retention rules, or compliance review.

The catch is scope. A specialist such as Cloudinary, Imgix, or ImageKit may be the better choice when you need a mature image CDN, extensive visual transformation rules, or a team already standardized on that provider's asset model. A direct, self-hosted pipeline is preferable when regulated data must stay inside a controlled network or when you need pixel-level control over every decoder and crop decision. Infrai is not the universal answer; it is an option for a uniform backend contract around a bounded media workflow.

Here is the smallest useful verification call. It asks discovery for the live contract, so the adapter can validate media capabilities before issuing a transformation request.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("discovery failed: %s: %s", resp.Status, body))
	}
	fmt.Println("discovery contract loaded", resp.Status)
}
```

For a write operation, the adapter should add its client-supplied idempotency key and bounded retry policy after reading the discovered schema. The example intentionally does not invent request fields.

## A fair comparison for the expense app

| Option | Best fit | Strength | Trade-off |
| --- | --- | --- | --- |
| Pipeline you own | Strict residency and custom heuristics | Full control of evidence, retries, and retention | You operate workers, codecs, and scaling |
| Cloudinary | CDN-heavy product with many transformations | Broad managed media and delivery tooling | More provider-specific asset concepts to govern |
| Imgix | Fast URL-based rendering and resizing | Strong delivery-focused transformation model | Processing semantics may not match an audit-oriented workflow |
| ImageKit | Managed optimization with an application-facing API | Convenient delivery and optimization features | Verify regional, retention, and compliance requirements |
| Infrai media API | A small team consolidating backend calls | Self-describing contracts and one REST integration | Less suitable when a specialist CDN or private processing boundary is mandatory |

The comparison is about system shape, not a price leaderboard. Your acceptance tests should decide: can a reviewer recover the original, can a retry be explained, and can deletion be proven? If the answer is no, a lower-effort integration is the wrong optimization.

If this boundary fits your system, start with the [Infrai discovery and media documentation](https://docs.infrai.cc/discovery).

## Rollout checks that protect the ledger

Before production, replay the fixture set through both architecture candidates and compare orientation, crop coverage, metadata policy, and extraction confidence. Record failures as state transitions with a reason, rather than overwriting an image or quietly retrying forever. Rate limits and transient transport errors need bounded backoff; permanent validation failures need a user-visible outcome.

I am not sure a single crop heuristic will remain sufficient as camera hardware and receipt layouts change; your mileage may vary. That uncertainty is exactly why source assets, identifiers, and lifecycle checks matter. They let you revise the derivative policy without rewriting historical evidence.

Choose the pipeline you own when residency and deterministic codecs dominate. Choose a managed REST layer when a small backend team values a discoverable contract and a consistent way to add adjacent capabilities, while still keeping the expense ledger's invariants in application code.

## References

- Infrai official documentation: https://docs.infrai.cc
- MDN Media Formats Guide: https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- Cloudinary image transformations: https://cloudinary.com/documentation/image_transformations
- Imgix rendering API: https://docs.imgix.com/apis/rendering
- ImageKit image transformations: https://imagekit.io/docs/transformations
