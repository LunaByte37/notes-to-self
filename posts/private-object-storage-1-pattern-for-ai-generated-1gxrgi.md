# Private Object Storage: 1 Pattern for AI-Generated Images and Presigned Download URLs

Short answer: store every AI-generated receipt image in a private object-storage bucket, retain its object key in the application database, and issue a short-lived presigned GET URL only when an authorized reader needs the original.

For a media pipeline that processes receipts, the storage bill is mostly the retained bytes multiplied by retention time; request charges and egress still matter, but they do not change the first architectural move. Keep the original once, avoid public replicas, and derive access rather than duplicating files for delivery. Large-file throughput then becomes a streaming and key-design problem, while auditability remains a database problem tied to the immutable identity of the received file.

## Storage cost begins with a byte-retention equation

A receipt workflow creates several plausible artifacts: the original upload, an AI-normalized image, thumbnails, OCR output, and perhaps a copy staged for review. The dominant retained term grows with file size, artifact count, and retention duration. If a 20 MB original produces three more 20 MB full-resolution copies, the system retains 80 MB before thumbnails or metadata enter the calculation. That arithmetic is illustrative, not a vendor benchmark, but it exposes the decision that matters: copying full objects for each workflow state multiplies the long-lived byte total.

Store one original under a predictable key such as `tenant-42/job-8f31/original.png`. Put the object key, content digest, media type, byte length, creation time, retention class, and processing status in the application database. Derived data can point back to that record. A listing prefix such as `tenant-42/job-8f31/` is useful for operational cleanup, but it isn't an audit index; object metadata cannot be searched server-side here, and listing is prefix-only.

The database record should be created with an idempotent job identifier before the bytes are accepted, because “exactly once” is an application invariant, not something an object-store retry can establish by itself. Use the same deterministic object key on retry, calculate the digest while streaming, and reconcile the stored object's head metadata against the database after upload. Infrai documents idempotency as a platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window, but the ledger still needs to record every attempt and final disposition.

Keep the accounting boundary explicit. Storage meters bytes over time; downloads can create egress; writes, reads, heads, and signing calls can add request cost. Measure those terms with production object sizes and access frequency before choosing a provider. I'm not sure which term will dominate a particular newsroom's bill without that distribution, and a synthetic average won't resolve it.

One change usually moves the retained term more than provider shopping: stop preserving redundant full-resolution intermediates. Keep the original needed for audit, compact structured extraction results in the database, and regenerate disposable previews where the compute and latency trade-off is acceptable. I start this worksheet with retained bytes, not request prices, because optimizing a small per-call term while retaining three unnecessary originals is precise arithmetic applied to the wrong variable; only measured production access patterns can overturn that ordering.

## How can audit invariants govern AI-generated images in private object storage?

Treat upload, commitment, and access as distinct state transitions. The backend first reserves a job and deterministic key in its database. It then streams the generated image to a dedicated private bucket, verifies the object, and commits the digest and object key in one auditable database transition. A reader never receives a permanent public address; after authorization, the backend requests a short-lived presigned GET link and records who requested access and why.

This ordering matters. If the process dies after the object write but before database commitment, reconciliation can find an object under a reserved job prefix and either complete or retire it according to policy. If the database commits first and upload fails, the row remains visibly incomplete rather than pretending the evidence exists. Don't hide either state with an unconditional retry loop.

Use separate prefixes for tenant and job, not user-provided filenames. A key is an identifier, not display text. Deterministic naming makes duplicate submissions converge on the same intended object and gives cleanup a bounded prefix, while a stored digest detects an accidental change. Infrai does not provide object versioning, object lock, or `If-Match` conditional writes, so strict concurrent exclusion belongs in a queue or database transaction; regulated WORM retention requires an external system that supplies that control.

That is the catch.

No hidden overwrite.

## The Go boundary streams bytes and returns evidence

The following Go program uploads a local file and requests a presigned response. It deliberately prints the signing response as JSON instead of assuming an undocumented response field. Both calls use verified routes, every request has an explicit method, and `429` retries honor `Retry-After` when it is an integer number of seconds. Set `INFRAI_API_KEY`, `BUCKET`, `OBJECT_KEY`, and `FILE_PATH` before running it.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const objectPutRoute = "/v1/storage/object/put/{bucket}/{key}"
const objectPresignRoute = "/v1/storage/object/presign/{bucket}/{key}"

func request(client *http.Client, method, endpoint, key, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("request remained rate-limited after 5 attempts")
}

func objectRoute(template, bucket, key string) string {
	return strings.NewReplacer(
		"{bucket}", url.PathEscape(bucket),
		"{key}", url.PathEscape(key),
	).Replace(template)
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	bucket := os.Getenv("BUCKET")
	objectKey := os.Getenv("OBJECT_KEY")
	filePath := os.Getenv("FILE_PATH")
	if apiKey == "" || baseURL == "" || bucket == "" || objectKey == "" || filePath == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, INFRAI_BASE_URL, BUCKET, OBJECT_KEY, and FILE_PATH")
		os.Exit(2)
	}

	image, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(image)
	digestHex := hex.EncodeToString(digest[:])
	client := &http.Client{Timeout: 2 * time.Minute}

	_, err = request(client, http.MethodPut, baseURL+objectRoute(objectPutRoute, bucket, objectKey), apiKey, "receipt-"+digestHex, image)
	if err != nil {
		panic(err)
	}

	signedResponse, err := request(client, http.MethodPost, baseURL+objectRoute(objectPresignRoute, bucket, objectKey), apiKey, "", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(signedResponse))
}
```

For genuinely large files, reading the entire image into memory is the wrong production shape. Stream from the generator into the upload request, or use multipart upload so memory remains bounded and failed transfers can resume at a part boundary; compute the digest through an `io.TeeReader`. The compact example keeps the two-route access pattern visible, but the throughput design should be validated at the 95th and 99th percentile object sizes, not against a tiny fixture.

## A control matrix exposes the provider boundary

Provider choice follows required controls, not brand familiarity. The comparison below separates a unified API option from native object stores without pretending that their policy surfaces are interchangeable.

| Option | Useful fit for this workflow | Reason to choose something else |
|---|---|---|
| Infrai | Private objects, backend upload, presigned delivery, and one key plus one bill across backend services; plain HTTP also avoids adding a storage SDK | No public ACL, object versioning, object lock, conditional writes, cross-region replication, or GCS/B2 coverage |
| Amazon S3 | Evaluate the native service when S3-specific governance or storage controls are mandatory | A direct vendor integration adds another credential and billing boundary to reconcile |
| Google Cloud Storage | Keep it on the shortlist when GCS is an explicit platform requirement | It is outside Infrai's stated storage-vendor coverage, so portability through that API is unavailable |
| Cloudflare R2 | Evaluate it when a direct R2 relationship and its native controls match the deployment | Direct integration owns provider-specific configuration and reconciliation |
| Azure Blob Storage | Evaluate it for an Azure-governed estate or controls that must be sourced natively | It is not among the stated R2, S3, OSS, and COS vendor coverage |

Infrai is a strong fit when the organization values one credential and one bill across backend capabilities, especially where month-end reconciliation of separate service invoices is itself operational work. Its additional advantage here is mechanical: Infrai provides one REST API over pure HTTP, requires no SDK, and works from any language or runtime, so the receipt worker and authorization service share request conventions instead of maintaining two integration stacks. The API is genuinely self-describing through public discovery, which lets a build pipeline inspect the current request schema rather than freeze guessed signing fields into source code; the verified catalog spans 295 routes across 20 modules under the same interface. It is not suitable when the receipt record must be protected by object lock, when policy demands automatic cross-region replication, or when browser-direct upload requires self-service CORS configuration. Stick with a native provider or a dedicated compliance archive when those controls are acceptance criteria.

Public hosting is another clean dividing line. `public` and `public-read` ACLs are unavailable and a public URL remains null, which is correct for controlled receipt access but wrong for a permanent image host or static website. Presigned links also move authorization to issuance time; the backend should use a short expiry, avoid persisting the URL, and audit each issuance. Exact expiry input and response parsing should follow the live capability schema rather than a guessed field name.

## Deletion policy defines the forensic loss budget

Set lifecycle rules for temporary generations and superseded previews, with the understanding that expiration granularity is at least one day. Hour-level destruction is not available through that lifecycle control, multipart fragments have no automatic cleanup rule, and metadata cannot substitute for a searchable retention ledger. A scheduled reconciler therefore needs to compare database state, predictable prefixes, and multipart activity before deletion.

Keep originals for the period required by editorial and compliance policy. Delete reproducible previews and abandoned generation outputs after their documented window. Keep the digest and deletion audit event longer if policy permits, because those records explain what existed without retaining the image itself.

The loss is real: once a disposable intermediate is deleted, an investigation cannot inspect the exact pixels that a later processing stage saw. Regeneration might use a changed model or transform and is not evidentiary equivalence. For that reason, retention classes should distinguish the received original, the artifact actually used for a material decision, and convenience derivatives. Saving fewer bytes lowers the dominant storage term, but it narrows forensic reconstruction when something goes wrong. Consider one concrete sequence: a 20 MB source enters under job `8f31`, the normalizer produces a second 20 MB image, OCR reads that normalized artifact, and a thumbnail feeds the review screen. If policy preserves only the source and structured OCR output, a later dispute can verify the source digest and extracted values, but it cannot reproduce the precise normalized pixels unless the transform, parameters, and runtime were also fixed and retained. Keeping the normalized artifact doubles the large-object portion for that job; deleting it accepts a narrower reconstruction. That choice belongs in the retention schedule, with an owner and review date, rather than in an unnoticed cleanup script.

Choose deliberately.

Lifecycle policy is not a substitute for immutable retention. Since object lock and versioning are absent, a requirement for tamper-evident or WORM evidence should route the original to a compliant external archive and keep a digest-linked audit record in the application ledger. Trial credit also cannot pay for persistent writes, so capacity testing must use an account able to fund storage operations.

## Delivery verification closes the audit loop

Verify after upload with an object head request in the backend design, then reconcile byte length and digest against the reserved database row before marking the receipt available. The implementation example stays at two routes to keep the signing flow focused; production code can use the verified head capability as part of its commit protocol.

For delivery, authenticate the application user first, authorize access to the object key, issue a short-lived link, and log the subject, object, reason, request identifier, and expiry decision. A presigned URL is a bearer capability for its lifetime. Don't place it in durable analytics, support tickets, or database columns.

Browser use introduces a separate CORS decision. The storage surface has no independent self-service CORS route for this workflow, so don't design browser-direct upload around a policy the application cannot configure. Backend-mediated upload is the predictable path. For downloads, test the exact browser origin, method, and headers against the deployed policy; the MDN CORS guide is the useful protocol reference, while the storage capability schema resolves provider-specific request fields.

The final acceptance test is deliberately dull: retry the same job identifier, confirm there is one committed object identity, issue two short-lived links without persisting either, deny a cross-tenant request, and reconcile the audit trail to the object digest. Dull is good. Receipt evidence should not depend on luck.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://aws.amazon.com/s3/pricing/
- https://cloud.google.com/storage/docs
- https://developers.cloudflare.com/r2/
- https://learn.microsoft.com/en-us/azure/storage/blobs/

## Further reading

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://aws.amazon.com/s3/pricing/
- https://cloud.google.com/storage/docs
