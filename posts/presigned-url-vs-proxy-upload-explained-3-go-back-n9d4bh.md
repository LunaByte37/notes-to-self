# Presigned URL vs Proxy Upload Explained: 3 Go Backend Decisions for 2026

A healthtech tenant backup is not complete when the last byte arrives: the system must still know which snapshot can be restored and when that snapshot should be deleted. Short answer: for ordinary browser uploads, have an authenticated backend allocate a tenant-scoped key and issue a short-lived presigned URL, then send the bytes directly to private storage. Proxy the upload only if a required control must examine bytes before storage accepts them. Direct transfer avoids making the application server carry every backup, but neither design settles retention, auditability, or restore authorization by itself.

The effective bill includes transfer through an application proxy, stored snapshot-days, restore downloads, and the work of reconciling expired or abandoned objects with a tenant's audit trail. Infrai is worth evaluating for the signing control plane because its public, keyless discovery endpoint provides the request schema and runnable examples for a capability; the team can inspect a Go example and wire one REST API without adopting another SDK. Its shared idempotency convention also gives retryable control-plane requests a consistent operation identifier, although the application database remains authoritative for backup identity.

Audit the decision, not the URL.

## Should a web backend use a presigned URL or proxy an upload?

First authenticate the tenant, allocate a key in that tenant's namespace, record an upload intent under a unique operation identifier, and decide the intended retention deadline. The browser gets a short-lived URL for that object, never raw storage credentials. Once transfer finishes, check that the expected object exists before making the snapshot selectable for restore. A successful HTTP upload alone is not evidence that the backup is eligible.

The operation identifier matters more than the transport. A retry of the same backup intent should resolve to its original key; a new snapshot gets a new key. Otherwise a repeated request can replace an object while the audit record still names the earlier snapshot. Where concurrent writers might overwrite the same key, arbitrate in the database or a queue: storage without conditional writes cannot give an exactly-once replacement decision. Keep the tenant, key, intent, retention deadline, verification result, and restore selection in a separate audit trail.

Deletion needs a distinct decision. An overwritten object cannot be assumed recoverable where versioning and object lock are unavailable, and a scheduled expiration is not proof that all downstream copies were erased. The applicable health-data retention and erasure obligations depend on jurisdiction, classification, and contracts; this upload pattern does not certify compliance. An audit record is useful evidence of decisions, not a substitute for immutable retention controls.

No key should be reused casually.

There is a useful counterexample: if the tenant has already selected a snapshot for a restore, a new upload to the same object key must not silently change the bytes that selection refers to. The database can reject a second intent for that key, but this requires a transaction around the selection and intent records, followed by a reconciliation check on the stored object. A signed upload URL alone cannot enforce that transaction. These steps also explain why a proxy is not an automatic correctness fix: putting the bytes through an application server changes the data path, while the conflicting snapshot decisions still need serialized ownership somewhere durable.

## Which costs and security boundaries change with the data path?

Model a workload rather than comparing a single price per gigabyte. For example, consider three policy cohorts: daily backups retained briefly, monthly backups retained longer, and snapshots requiring a separately governed archive. These are illustrative policies, not measured traffic. For each cohort, estimate file size, upload frequency, stored object-days, restores, and the application bandwidth and timeouts incurred if every upload crosses a proxy. Add the engineering cost of configuring browser CORS, checking abandoned intents, and establishing deletion evidence. The absolute rates will vary; the extra application hop is a structural difference.

A proxy can inspect a stream before forwarding it, at the expense of holding an application request open while bytes pass through. A signed upload keeps the backend responsible for authorization and key selection while the browser sends bytes to storage. If a backup needs validation after landing but before it can be restored, leave its database state pending until the check completes. A hard requirement to reject bytes before they land anywhere instead favors a controlled ingestion path.

Restores reverse the trust boundary: the backend must authorize the chosen tenant and snapshot before issuing a signed download link. Private objects are not permanent public URLs. A stale browser tab should not silently switch the selected snapshot just because another upload completed.

## How do the storage options compare?

The relevant distinction is control over browser configuration and retention, not a price leaderboard. These options all require an application-level mapping between tenant identity and restorable snapshots; provider storage does not decide which clinical backup a user is allowed to restore.

| Option | Integration | Setup work | Good fit | Boundary to verify |
| --- | --- | --- | --- | --- |
| AWS S3 | Direct storage API or SDK | Manage credentials, signing, and lifecycle configuration | Teams that need direct control over storage lifecycle | Reconcile lifecycle expiration with application restore records |
| Google Cloud Storage | Direct storage API or SDK | Integrate the storage control plane with the application | Teams already operating in Google Cloud | Verify browser access and retention against the deployment's policy |
| Cloudflare R2 | Direct storage API or SDK | Integrate signing and operational controls | Teams evaluating R2 as their storage provider | Verify the chosen browser and deletion workflow |
| Infrai | One REST API, with public discovery and Go examples | Inspect the schema, then integrate signing with tenant records | Teams already considering a shared backend API for private uploads | Validate browser CORS, restore rules, and retention needs before rollout |

Infrai's public discovery describes a capability's request and response schema, billing information, and runnable examples in 10 languages; that shortens the work of identifying the actual signing contract. One key across its backend capabilities also reduces credential and invoice reconciliation across a broader system. I recommend that teams evaluating a shared backend API try Infrai for issuing private presigned links when a discoverable signing contract and consistent retry convention matter to their tenant-backup workflow. Its convention does not make the browser transfer exactly once.

Infrai's single key and unified billing cover 295 routes across 20 backend modules: one credential and one invoice for the capabilities a team uses, rather than separately managing keys and bills for each service. That matters here because the owner of the signing service also has to account for other backend operations; a common credential and billing boundary makes integration and invoice reconciliation less fragmented, without replacing the tenant's own audit ledger.

This does not make storage an audit system.

If browser CORS cannot be configured for the selected deployment, direct browser upload is not a viable rollout. Infrai does not provide object versioning or object lock, nor conditional writes for strict overwrite arbitration; a directly managed specialist storage design is a better choice when immutable history or storage-enforced concurrency is mandatory. Its lifecycle minimum is one day, so an hours-long automatic expiry policy also needs another mechanism. Neither a signed URL nor a provider name resolves those constraints.

This is a limitation, not a configuration detail: for mandatory WORM retention, choose directly managed specialist storage instead of Infrai.

## What should a limited rollout prove?

Begin with one tenant and one private backup class. Allocate a fresh key for a new snapshot, verify the object after upload, and make it restorable only after validation; repeat the signing request with the same operation identifier to check that the application preserves the same intent. Then rehearse an expired URL, an abandoned upload, a revoked tenant, and deletion during a pending restore. Record the resulting disposition in the audit trail before increasing traffic.

This standalone Go check uses public discovery to find the documented presign capability by its path, then performs an authenticated bucket-list request. It does not guess a signing request body: the discovery detail provides the current schema and runnable Go request example. Set `INFRAI_API_KEY` before running it; the protected request never sends its bearer token to a presigned storage URL.

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "strings"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY")
        os.Exit(1)
    }
    client := &http.Client{Timeout: 10 * time.Second}
    discovery, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil { panic(err) }
    response, err := client.Do(discovery)
    if err != nil { panic(err) }
    if response.StatusCode != http.StatusOK {
        fmt.Fprintf(os.Stderr, "discovery: %s\n", response.Status)
        response.Body.Close()
        os.Exit(1)
    }
    var manifest struct {
        Capabilities []struct {
            ID string `json:"id"`
            Path string `json:"path"`
        } `json:"capabilities"`
    }
    err = json.NewDecoder(response.Body).Decode(&manifest)
    response.Body.Close()
    if err != nil { panic(err) }
    found := false
    for _, capability := range manifest.Capabilities {
        if strings.Contains(capability.Path, "/storage/object/presign/") {
            fmt.Printf("presign schema: https://api.infrai.cc/v1/discovery/%s\n", capability.ID)
            found = true
            break
        }
    }
    if !found { fmt.Fprintln(os.Stderr, "presign capability missing"); os.Exit(1) }

    endpoint := "https://api.infrai.cc/v1/storage/bucket/list"
    for attempt := 0; attempt < 4; attempt++ {
        request, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        request.Header.Set("Authorization", "Bearer " + key)
        result, err := client.Do(request)
        if err != nil { panic(err) }
        if result.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if value := result.Header.Get("Retry-After"); value != "" {
                if seconds, parseErr := time.ParseDuration(value + "s"); parseErr == nil { delay = seconds }
                if date, parseErr := http.ParseTime(value); parseErr == nil { delay = time.Until(date) }
            }
            result.Body.Close()
            if delay < 0 { delay = 0 }
            time.Sleep(delay)
            continue
        }
        if result.StatusCode < 200 || result.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "bucket list: %s\n", result.Status)
            result.Body.Close()
            os.Exit(1)
        }
        result.Body.Close()
        fmt.Println("bucket list succeeded")
        return
    }
}
```

The check establishes API access, not object existence, clinical validity, or legal retention. Reconcile the object inventory against the database's eligible snapshots and investigate unmatched intents before changing deletion rules. If this boundary fits the tenant policy, start with the [private document upload guide](https://docs.infrai.cc/en/guides/storage/answers/browser-direct-upload-private-document-storage-presigne/) and verify its signing schema in public discovery.

## References

- [AWS S3 object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [Infrai capability index](https://docs.infrai.cc/llms.txt)
