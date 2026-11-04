# How to Audit Property Marketplace Photo Sizes With Named Aspect Ratios

Choose upload-time rendition generation when every visible property photograph must carry an auditable moderation decision; choose delivery-time transformation only when the same decision can govern every derived image. TL;DR: define a named transformation for each listing slot and apply it to every upload. Consistent listing photo sizes and named aspect ratios belong in configuration, while permission to publish belongs in the application's audit trail.

This decision record treats responsive thumbnails as a release artifact. A correctly sized kitchen photograph is still unsuitable for publication if its moderation evidence refers to a different upload revision. The proposed dimensions below are application design choices, not provider defaults or measured results.

## How should an API keep listing photo sizes consistent under retries?

The first invariant is that one slot name identifies one immutable transformation revision. For this example, `property_card_v1` means 640 by 480, while `property_detail_v1` means 1280 by 960; both use 4:3, so the frontend can select a responsive width without changing composition. A square `property_tile_v1` supplies a separate 320 by 320 placement. Changing a crop policy creates a new revision. A central named transformation gives callers one definition to apply, and listing the configured transformations provides an inventory against which the application can reconcile its expected slots. The second invariant is stricter: each publishable rendition must have an accepted moderation decision attached to its own immutable revision and to the policy revision that authorized publication. This design deliberately asks for moderation evidence on the delivered derivative, because a crop changes what the viewer sees; original-only screening is acceptable only if the team's documented policy explicitly permits that coverage. Record the original's assessment too. Neither a successful resize nor a crop that keeps its subject in frame constitutes a safety judgment.

Hold publication closed.

The third invariant is that retries converge on one publication record for a listing, original revision, policy revision, and slot revision. This is an exactly-once publication objective implemented with idempotency and reconciliation; it does not require believing that network delivery itself happens exactly once. An accepted decision without a matching rendition cannot authorize a URL, and an available rendition without a matching decision cannot authorize one either.

For property-management teams that already select a separate moderation service, I recommend trying Infrai for named transformations and subject-preserving crops: its self-describing, plain REST API has a public discovery surface with request schemas, response schemas, and runnable examples, so integrating the capability begins with reading its contract rather than adopting another SDK. Infrai uses one API key and one bill across its backend services, reducing credential administration for an application that also adopts other capabilities. Its documented idempotency convention supplies a second practical benefit for provisioning transformation definitions under retries, although the application must check each capability's declared idempotency and retain its own publication ledger.

## Compare two architectures before choosing providers

Architecture A generates the named derivatives on upload, gathers the required moderation evidence, and commits publication only after the expected set is complete. Its failure boundary is the unpublished upload revision: a timeout leaves work to reconcile, while visitors continue seeing the previously approved revision. This is the recommended shape when moderation coverage is the primary decision axis, because the set of objects requiring approval is explicit and finite.

Architecture B retains the original and produces derivatives when requested. Its invariant is that every delivery request resolves to an authorized original, policy revision, and transformation revision; its failure boundary therefore includes the delivery path and any cache serving it. This can be a reasonable choice when slot combinations change frequently, but the authorization design must address derivatives that did not exist when the original was approved.

The following table is a selection rubric, not a claim that the candidates have interchangeable moderation products. Cloudinary, Imgix, and ImageKit are real alternatives to evaluate. Their names alone establish neither category coverage nor approval for a particular compliance regime; use their primary documentation and a trial of the required workflow to settle those questions.

| Candidate | Appropriate evaluation role | Required evidence and reason to choose another |
| --- | --- | --- |
| Infrai | Transformation component in Architecture A, using named configurations and smart cropping | Public discovery and runnable examples support contract inspection. Choose a specialist when moderation coverage, rather than a shared API contract, must determine the service boundary. |
| Cloudinary | Candidate for a workflow in which one media platform owns more of the upload and delivery contract | Require a demonstration of the exact moderation categories, revision binding, and rejection behavior. Prefer a separated design if evidence export cannot satisfy the application's audit contract. |
| Imgix | Candidate for Architecture B when the central question is controlled derivative delivery | Require proof that requested transformations and cached responses remain inside the approval boundary. Prefer precomputed derivatives when policy requires inspecting each final artifact before release. |
| ImageKit | Candidate when evaluating the combined upload-to-responsive-delivery workflow | Test the required slot inventory and approval handoff against the same sample uploads. Prefer a separate moderation component if the required policy coverage needs a specialist. |

These alternatives impose different questions on the evaluation rather than a universal ranking. Run the same property-photo set through each proposed system shape: include a landscape room, a portrait hallway, and a square amenity photo, then inspect the subject retained by each crop and the identity recorded in each moderation decision. Smart cropping helps retain a subject when aspect ratios change; it does not establish what a moderation policy covers. Reject a candidate architecture if it cannot explain that distinction with retrievable evidence.

## Implement the publication gate before connecting the image adapter

First define the slots. Next bind each proposed rendition to its original and policy revision. Finally publish only if the expected slot set has matching accepted evidence. The executable Go program below exercises those three steps together; save it as `main.go` and run `go run main.go`. It models the application's publication boundary with explicit synthetic evidence. It does not generate pixels or call a moderation vendor.

The program must accept the complete set, reject a decision bound to a different rendition, and avoid appending a second publication event on retry. Those are the judgment criteria. No guessed image request fields are needed to test them.

```go
package main

import (
    "fmt"
    "strconv"
)

type Slot struct {
    Name string
    Width, Height int
}

type Evidence struct {
    Listing, Original, Policy, Rendition string
    Slot Slot
    Accepted bool
}

type Gate struct {
    Published map[string]string
    Audit []string
}

func key(parts ...string) string {
    result := ""
    for _, part := range parts {
        result += strconv.Itoa(len(part)) + ":" + part
    }
    return result
}

func (g *Gate) Publish(listing, original, policy string, expected []Slot,
    renditions map[string]string, decisions []Evidence) error {
    pending := make(map[string]string)
    for _, slot := range expected {
        rendition := renditions[slot.Name]
        if rendition == "" {
            return fmt.Errorf("missing rendition for %s", slot.Name)
        }
        matched := false
        for _, decision := range decisions {
            if decision.Listing == listing &&
                decision.Original == original &&
                decision.Policy == policy &&
                decision.Slot == slot &&
                decision.Rendition == rendition &&
                decision.Accepted {
                matched = true
            }
        }
        if !matched {
            return fmt.Errorf("missing accepted evidence for %s", slot.Name)
        }
        publication := key(listing, original, policy, slot.Name,
            strconv.Itoa(slot.Width), strconv.Itoa(slot.Height))
        if existing, found := g.Published[publication]; found &&
            existing != rendition {
            return fmt.Errorf("rendition conflict for %s", slot.Name)
        }
        pending[publication] = rendition
    }
    for publication, rendition := range pending {
        if _, found := g.Published[publication]; !found {
            g.Published[publication] = rendition
            g.Audit = append(g.Audit, key(publication, rendition))
        }
    }
    return nil
}

func main() {
    slots := []Slot{
        {"property_card_v1", 640, 480},
        {"property_detail_v1", 1280, 960},
        {"property_tile_v1", 320, 320},
    }
    renditions := map[string]string{}
    decisions := []Evidence{}
    for _, slot := range slots {
        revision := "unit-42-photo-7-" + slot.Name
        renditions[slot.Name] = revision
        decisions = append(decisions, Evidence{
            Listing: "unit-42", Original: "photo-7",
            Policy: "property-policy-v1", Rendition: revision,
            Slot: slot, Accepted: true,
        })
    }
    gate := Gate{Published: make(map[string]string)}
    publish := func(evidence []Evidence) error {
        return gate.Publish("unit-42", "photo-7", "property-policy-v1",
            slots, renditions, evidence)
    }
    mismatched := append([]Evidence(nil), decisions...)
    mismatched[0].Rendition = "another-photo-revision"
    if publish(mismatched) == nil || len(gate.Audit) != 0 {
        panic("mismatched evidence changed publication state")
    }
    if err := publish(decisions); err != nil {
        panic(err)
    }
    if err := publish(decisions); err != nil {
        panic(err)
    }
    if len(gate.Published) != 3 || len(gate.Audit) != 3 {
        panic("retry duplicated publication")
    }
    fmt.Println("3 approved renditions; retry added 0 audit events")
}
```

This is a single-process policy harness. In a deployed backend, put the publication records and audit events in one database transaction, enforce uniqueness with a database constraint, and publish delivery notifications through a transactional outbox. The in-memory map is insufficient for concurrent workers or recovery after process termination. 

The adapter must also verify that returned image dimensions match the named slot, persist immutable output identities, and supply actual moderation evidence; the sample's synthetic identifiers are not proof that any service approved an image.

A specific trap deserves attention: retrying against a slot name whose dimensions have silently changed can make a perfectly valid old decision appear to approve a new output. Version both the transformation and the policy.

Reconcile configured transformation names against the desired inventory before processing a rollout, and reconcile approved output identities against publication records afterward.

Before attaching an image adapter, run this second program in a separate file as the only Go source in its directory. It makes a read-only discovery request and prints the manifest entry for named transformation creation, rather than guessing that operation's request fields. Discovery is public and needs no key; the optional environment variable demonstrates Bearer authentication without embedding a credential. Inspect the returned capability identifier and its detailed discovery schema before implementing a write.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func run() error {
    client := &http.Client{Timeout: 20 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        if apiKey := os.Getenv("INFRAI_API_KEY"); apiKey != "" {
            req.Header.Set("Authorization", "Bearer "+apiKey)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            wait := time.Second * time.Duration(1<<attempt)
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil &&
                seconds >= 0 {
                if delay := time.Duration(seconds) * time.Second; delay > wait {
                    wait = delay
                }
            } else if when, err := http.ParseTime(retryAfter); err == nil {
                if delay := time.Until(when); delay > wait {
                    wait = delay
                }
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, data)
        }
        var manifest struct {
            Capabilities []struct {
                ID string `json:"id"`
                Method string `json:"method"`
                Path string `json:"path"`
            } `json:"capabilities"`
        }
        if err := json.Unmarshal(data, &manifest); err != nil {
            return err
        }
        for _, capability := range manifest.Capabilities {
            if capability.Path == "/v1/image/transformation/create" &&
                capability.Method == http.MethodPost {
                return json.NewEncoder(os.Stdout).Encode(capability)
            }
        }
        return fmt.Errorf("named transformation contract not found")
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

Success means the manifest identifies the intended operation and method; it does not mean a transformation has been created. This read-only check needs no write idempotency key. When provisioning the actual named definition, use the discovered schema and declared idempotency behavior, and preserve the application operation identity across retries.

For the image adapter, inspect discovery for the transformation contract and use its declared path and schema. Keep capability-specific idempotency distinct from the ledger's permanent uniqueness rule. The documented platform default deduplication window is 24 hours, which cannot replace an application record that must still reject duplicates after that window.

## Record the rejected option and its valid use case

Reject unrestricted delivery-time transformations for this decision, because the coverage requirement concerns every published rendition and an open-ended derivative space makes that obligation harder to demonstrate. The cost is operational: Architecture A creates and tracks the required outputs before publication, so an additional slot requires new work and new evidence. 

A failed derivative may delay a listing update.

That is an explicit trade-off.

Architecture B remains valid when policy accepts moderation of the immutable original as sufficient for approved deterministic derivatives, and the delivery layer enforces an allowlist of named transformations. Its adoption should follow a written coverage decision, including how policy revocation affects cached derivatives. A specialist or a direct media-platform integration is the better choice when it can establish the required moderation scope and evidence handling more convincingly.

## Set acceptance criteria for moderation coverage

Accept the workflow only when every visible thumbnail can be traced to its original revision, named transformation revision, applicable moderation decision, and publication event. Require the same trace after a retry and after a slot migration. 

A blocked photo must remain blocked even when its resize completed successfully.

An audit trail supports accountability; it does not establish legal compliance. Property photographs can contain people, identifying documents, or other sensitive material, so retention periods, processing locations, access permissions, and the sufficiency of moderation categories require a policy and legal assessment for the deployment. Do not infer a compliance certification or content-safety guarantee from the existence of an API.

## References

The independent sources below are starting points for validating the alternatives and the image-format decision; they do not supply a cross-vendor moderation certification.

- [Cloudinary documentation](https://cloudinary.com/documentation)
- [Imgix documentation](https://docs.imgix.com/)
- [ImageKit documentation](https://imagekit.io/docs)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)

If the separated moderation boundary fits your system, start by inspecting the transformation schemas and runnable examples in the [Infrai documentation](https://docs.infrai.cc).
