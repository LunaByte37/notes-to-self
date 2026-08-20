# Node.js Semantic Search Plus LLM Classifier for Media Candidate Docs

Use semantic retrieval to find the small set of rubric passages that can justify a media candidate score, rerank those passages, and let an LLM classify only within a validated rubric; keep the provider behind ports so a model change does not rewrite the scoring ledger.

Short answer: a Node.js classifier for media candidates should treat embeddings, reranking, and classification as separate stages, with an abstain path and an idempotent decision record at the end. The important output is not merely a topic label or score. It is a reproducible explanation of which rubric version supported that decision.

This is an architecture decision record, not a recipe for making model output look certain. In a hiring workflow, a plausible label attached to the wrong rubric can send a reel to the wrong editor, hide a qualified candidate, or make a later review impossible. I care about that boundary more than a marginal change in retrieval latency.

## Decision and invariants

The concrete job is to score candidates for a media role against a controlled rubric. A document might be a resume, a portfolio note, or an interview transcript. The service should classify evidence such as `editing`, `production`, or `audience_growth`, then produce a score only when the evidence supports the applicable rubric criteria.

The work identity should include `candidate_id`, `document_id`, `content_hash`, `rubric_version`, and `classifier_version`. Hash those values into an idempotency key. A corrected resume or revised rubric is a new decision, not a silent mutation of the old one. This makes the audit trail useful: an investigator can reconstruct what the system knew, what it was asked, and which result was committed.

Four invariants matter:

1. Retrieval returns immutable rubric passages with stable IDs and a rubric version.
2. Reranking changes evidence order, never the allowed label set.
3. Classification returns a closed-schema result that the application validates before scoring.
4. Persistence accepts one committed result for one work identity, even if the request is retried.

The fourth invariant is where many AI pipelines become careless. A timeout does not tell the worker whether the upstream model completed. Repeating an inference call may be acceptable; repeating its business effect is not. RFC 9110 describes HTTP semantics and retry considerations, but the application still needs a unique constraint, compare-and-set, or transaction around the decision write.

The service must also preserve a bounded failure result. No relevant rubric evidence means `needs_review`, not a guessed topic. An invalid label, a score outside the permitted range, or conflicting evidence means the same. The classifier can be probabilistic; the state transition cannot be vague.

## What should Node.js use to score media candidates with embeddings, reranking, and an LLM?

The worker should orchestrate narrow interfaces: an embedder converts the candidate document and rubric passages into vectors, a retriever finds likely criteria, a reranker orders those criteria against the actual scoring question, and a classifier selects from the approved labels. The application then validates and stores the result.

Embeddings are for recall. They are not a verdict. A portfolio may use the word “storytelling” while a rubric passage uses “editorial narrative”; vector similarity can locate both, but it cannot decide whether the candidate demonstrated the required production responsibility. Reranking gets the evidence into a more useful order. The LLM receives that bounded evidence and a schema with explicit abstention.

Here is the critical path in Go. The production worker may be written in Node.js, but the domain ports below keep provider-specific request shapes out of the decision core. That is the portability boundary: adapters change; the identity of a decision and its evidence do not.

```go
package scoring

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

type Evidence struct {
	ID      string  `json:"id"`
	Topic   string  `json:"topic"`
	Excerpt string  `json:"excerpt"`
	Score   float64 `json:"score"`
}

type Result struct {
	CandidateID   string     `json:"candidate_id"`
	DocumentID    string     `json:"document_id"`
	RubricVersion string     `json:"rubric_version"`
	Label         string     `json:"label"`
	Score         int        `json:"score"`
	Evidence      []Evidence `json:"evidence"`
	IdempotencyKey string    `json:"idempotency_key"`
}

type Retriever interface {
	Find(context.Context, string, string) ([]Evidence, error)
}

type Reranker interface {
	Order(context.Context, string, []Evidence) ([]Evidence, error)
}

type Classifier interface {
	Classify(context.Context, string, []Evidence, []string) (string, int, error)
}

func key(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func Score(ctx context.Context, candidateID, documentID, body, rubricVersion string,
	retriever Retriever, reranker Reranker, classifier Classifier) (Result, error) {
	evidence, err := retriever.Find(ctx, body, rubricVersion)
	if err != nil {
		return Result{}, fmt.Errorf("retrieve rubric: %w", err)
	}
	if len(evidence) == 0 {
		return Result{}, errors.New("needs_review: no rubric evidence")
	}
	evidence, err = reranker.Order(ctx, body, evidence)
	if err != nil {
		return Result{}, fmt.Errorf("rerank rubric: %w", err)
	}
	allowed := []string{"editing", "production", "audience_growth", "needs_review"}
	label, score, err := classifier.Classify(ctx, body, evidence, allowed)
	if err != nil {
		return Result{}, fmt.Errorf("classify candidate: %w", err)
	}
	if !contains(allowed, label) || score < 0 || score > 100 {
		return Result{}, errors.New("needs_review: classifier result is outside schema")
	}
	contentHash := key(body)
	return Result{
		CandidateID: candidateID, DocumentID: documentID,
		RubricVersion: rubricVersion, Label: label, Score: score,
		Evidence: evidence,
		IdempotencyKey: key(candidateID, documentID, contentHash, rubricVersion, "classifier-v1"),
	}, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
```

The code intentionally does not claim exactly-once inference. It creates an exactly-once decision effect when the repository writes `IdempotencyKey` under a uniqueness rule. The write should be transactional with the status transition that makes a candidate available to the next review step; an outbox can carry notifications without making the classifier responsible for delivery semantics.

## Failure boundaries that change the score

The first failure is taxonomy drift. If an embedding index contains passages from rubric version 11 while the classifier prompt names version 12, the response may be valid JSON and still be invalid evidence. Picture a media hiring rubric that moves “audience retention” from a supporting signal to a scored criterion: the same candidate document can retrieve the same vector neighbor, receive a different score interpretation, and look like a model regression even though the model was given a different contract. The decision record must therefore bind the query embedding, passage IDs, passage version, reranker version, classifier version, and allowed-label schema together. A worker that retries after an HTTP 429 must reuse that identity, and a worker that sees a changed rubric must create a new identity. Publish passages and vectors as one versioned set, switch the active version atomically, and retain the prior set long enough to reconcile decisions made during the transition. Don't let a mutable “current rubric” pointer become the only evidence that a score ever existed.

The second failure is a convincing near miss. A candidate may retrieve “video editing” because the document describes editing software, while the role requires editorial judgment under a newsroom deadline. Reranking should see the scoring question, not just the raw document. Store the ordered evidence and the reranker version so reviewers can tell retrieval failure from classification failure.

The third failure is schema compliance without business compliance. `{"label":"production","score":82}` is syntactically fine, but it does not prove that the evidence supports production ownership. Require evidence IDs, require a reason code or bounded rationale appropriate to the organization’s retention policy, and reject a score when the evidence threshold is not met.

Keep personal data exposure narrow. An audit trail should retain enough identifiers to explain a decision, but compliance rules may require redaction, access controls, retention limits, and deletion workflows for candidate materials. “Auditable” does not mean “store every prompt forever.”

One short rule: abstain early.

Do not guess.

## Option comparison and portability boundary

Portability is not achieved by putting a common function name over incompatible behavior. It comes from deciding which semantics belong to the application: vector dimensions, distance interpretation, evidence IDs, allowed labels, score calibration, timeout policy, and persistence rules. An adapter can translate a provider response; it cannot repair a rubric that was not versioned.

| Boundary | Useful when | Ownership and trade-off |
|---|---|---|
| Postgres with a vector extension | Candidate metadata and rubric passages already live together | The team owns index selection, query tuning, backups, and capacity; the vector can remain beside its source metadata |
| Managed vector store | Retrieval operations should be separated from the primary database | The team gains a managed retrieval boundary but must reconcile IDs, versions, deletes, and consistency across stores |
| Direct model adapters | A particular approved model contract is a requirement | Model behavior and request semantics are explicit, while replacement requires another adapter and renewed evaluation |
| Internal model gateway | Several teams need one approved access boundary | Provider replacement is easier for callers, but gateway behavior, quotas, audit fields, and failure semantics become platform responsibilities |

The rejected option is a single prompt containing every rubric and asking for a score. It is valid for a small, stable taxonomy, a human-reviewed queue, and low consequence output. It is a poor fit when rubric versions change, evidence must be inspected, or the same candidate can be processed again after a timeout. A large prompt also makes it harder to distinguish missing retrieval from a model decision.

Your mileage may vary. A retrieval threshold that is reasonable for scripted video roles may be wrong for audience analytics, and I would not choose a universal top-k value without a labeled set drawn from the actual media corpus. Record those thresholds as configuration, evaluate false positives and abstentions separately, and change one boundary at a time.

## Operating the decision record

Observability should follow the stages rather than report only end-to-end latency. Emit correlation IDs, rubric and classifier versions, candidate and document identifiers in protected form, evidence IDs, retrieval and rerank scores, validation outcomes, retry counts, and the final persistence result. Do not log raw candidate text by default.

Test the architecture with fixtures that exercise disagreement: a document that matches two topics, a rubric passage that changed between versions, an empty retrieval result, a malformed label, a score of 101, and two workers processing the same idempotency key. The last fixture is especially important. A green model test cannot prove that two concurrent consumers produce one business effect.

Deployment should make rubric publication and classifier rollout independently reversible. Shadow a new classifier against the same immutable evidence before allowing it to write, compare label and abstention rates, and preserve the old decision basis. If a policy change is urgent, the ledger still needs a clear effective version; urgency is not a reason to erase history.

The decision rule is therefore straightforward: choose the retrieval and model components that satisfy the organization’s residency, review, and operational constraints, but expose them through ports that return stable evidence and validated decisions. Prefer the boundary that your team can reconcile. A provider can be replaced; an untraceable score cannot be responsibly explained after the fact.

## References

- https://www.rfc-editor.org/rfc/rfc9110
- https://github.com/pgvector/pgvector
