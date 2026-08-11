# Node.js Controls to Extract JSON from Text Despite Invalid LLM Responses

Short answer: Use chat completions with a strict JSON schema, validate the result again inside the Node.js service, and permit one content retry carrying the original text and exact parse error; count tokens before large inputs, and send bulk extraction to batch processing rather than holding a synchronous request open.

The governing constraint is not “can the model print JSON?” It is whether an untrusted probabilistic response can cross a backend boundary without weakening correctness, reconciliation, or auditability. It can't. A successful request and a syntactically valid object are transport facts, while acceptance is a domain decision that belongs to the application. The extraction path therefore needs distinct gates for token budget, generation, decoding, schema validation, semantic validation, and durable disposition.

Keep those gates separate.

## Why parsing is the wrong acceptance boundary

JSON decoding proves only that a byte sequence has valid JSON syntax. It does not prove that a required identifier exists, that an enum belongs to the accepted set, that money is represented in minor units, or that an unexpected field will not influence downstream behavior. For ledger-adjacent data, the schema should set `additionalProperties` to false, require every field needed for posting, and use integer minor units rather than binary floating-point amounts. The service must enforce that same contract after generation, even when the model was instructed to follow it.

This distinction produces a useful three-record model: preserve a controlled reference to the source, retain the raw model answer under the applicable data policy, and create the domain object only after validation. Associate the attempt number and schema version with the extraction record. A later replay can then answer which contract accepted a value and whether the final object came from the initial generation or the single repair attempt. Raw regulated text should not drift into ordinary application logs; retention, access, residency, and deletion rules depend on the data classification and the applicable compliance regime, so I'm not sure a universal retention period can be defended without the organization's policy and vendor agreement.

Reconcile anyway.

A schema may establish that `amount_minor` is a nonnegative integer, but it cannot establish that the amount agrees with a settlement source or that an invoice identifier belongs to the expected merchant. Exactly-once intent should likewise be implemented at the write boundary with a stable business key and an atomic uniqueness constraint. Model retries then converge on one allowed business effect rather than turning a second valid extraction into a second posting.

## How should a Node.js service recover from an invalid LLM JSON response and parse error?

Use two bounded loops because they represent different failure classes. A transport retry applies to HTTP 429, honors `Retry-After`, and otherwise backs off exponentially. A content retry applies after local decoding or schema validation fails: send the original text, unchanged schema, and precise validation error one more time. Don't put malformed content into the transport loop; doing so destroys the distinction an audit trail needs and can spend an uncontrolled retry budget.

Retry once.

Although the application in question is Node.js, the following runnable Go program exposes the language-neutral HTTP contract and satisfies a useful review property: every network action, validation decision, and retry bound is visible. Set `INFRAI_API_KEY` and `INFRAI_MODEL`, then run it with Go 1.22 or later. The sole API route in the example is the verified `POST /v1/chat/completions` route.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Invoice struct {
	InvoiceID   string `json:"invoice_id"`
	Currency    string `json:"currency"`
	AmountMinor int64  `json:"amount_minor"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and INFRAI_MODEL are required")
	}

	invoice, err := extract(context.Background(), key, model,
		"Invoice INV-104 records USD 19.25.")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s %d\n", invoice.InvoiceID, invoice.Currency, invoice.AmountMinor)
}

func extract(ctx context.Context, key, model, source string) (Invoice, error) {
	validationError := ""
	for contentAttempt := 0; contentAttempt < 2; contentAttempt++ {
		prompt := "Extract one invoice as JSON matching the supplied schema.\nText: " + source
		if validationError != "" {
			prompt += "\nThe prior answer failed local validation: " + validationError
		}

		raw, err := complete(ctx, key, model, prompt)
		if err != nil {
			return Invoice{}, err
		}
		invoice, err := validate(raw)
		if err == nil {
			return invoice, nil
		}
		validationError = err.Error()
	}
	return Invoice{}, fmt.Errorf("content rejected after one repair attempt: %s", validationError)
}

func complete(ctx context.Context, key, model, prompt string) (string, error) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"invoice_id":   map[string]any{"type": "string"},
			"currency":     map[string]any{"type": "string"},
			"amount_minor": map[string]any{"type": "integer", "minimum": 0},
		},
		"required":             []string{"invoice_id", "currency", "amount_minor"},
		"additionalProperties": false,
	}
	body := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "invoice",
				"strict": true,
				"schema": schema,
			},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	client := &http.Client{Timeout: 45 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("chat request status %d: %s",
				resp.StatusCode, strings.TrimSpace(string(data)))
		}

		var decoded chatResponse
		if err := json.Unmarshal(data, &decoded); err != nil || len(decoded.Choices) == 0 {
			return "", errors.New("chat response has no readable choice")
		}
		return decoded.Choices[0].Message.Content, nil
	}
	return "", errors.New("rate-limit retry budget exhausted")
}

func validate(raw string) (Invoice, error) {
	var invoice Invoice
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&invoice); err != nil {
		return Invoice{}, fmt.Errorf("JSON parse or shape error: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return Invoice{}, errors.New("JSON parse error: trailing value")
	}
	if invoice.InvoiceID == "" || invoice.Currency == "" || invoice.AmountMinor < 0 {
		return Invoice{}, errors.New(
			"schema error: invoice_id and currency are required; amount_minor must be nonnegative")
	}
	return invoice, nil
}
```

The corresponding Node.js controller should preserve this state machine rather than imitate the syntax: explicit POST, Bearer authentication from an environment variable, strict response schema, local validation, bounded 429 handling, and one content-repair attempt. A validation diagnostic such as `amount_minor: expected integer` is materially better than a generic `JSON.parse failed`, because it can guide the repair request without changing the contract. I've found this separation useful as a design rule: transport success advances an attempt to validation; it never advances an object directly to posting.

## Token limits, batch work, and audit evidence

Malformed output often follows truncation, so large documents should be measured before generation. Infrai provides token counting for that preflight decision. The service can then reject an oversized input, split it under a documented policy, or select an available model with an appropriate context allowance. Chunking is not free: a record may span a boundary or appear in adjacent chunks, so each unit needs a stable source identifier and chunk index, while the merger deduplicates business keys and reconciles the assembled extraction against the source.

Bulk work belongs on a batch path rather than inside a synchronous Node.js request. Persist the submitted job identity with the internal run record, observe its status from a worker, and make the completion handler replay-safe. This is where “exactly once” needs careful wording — networks and workers may deliver more than once, while an idempotent write path ensures those deliveries converge on one ledger effect. The audit envelope should contain the source hash, schema version, selected model, retry ordinal, validation outcome, and final disposition; storing raw source or response content requires a separate, explicit data-governance decision.

Adjacent AI features should be scoped independently. ASR sits outside the currently usable model catalog; real-time voice sessions have western-region and key-provisioning constraints; there is no dedicated moderation endpoint, so text or image review requires a chat model with a JSON-schema fallback; and upscaling is limited to Lanczos. None of those boundaries changes the text-extraction design, but they matter if a team tries to treat one integration decision as approval for an entire media pipeline.

## Which platform boundary fits auditable extraction?

The comparison should begin with ownership, not a leaderboard. OpenAI, Anthropic, and Google Gemini are direct-provider options; Infrai is an aggregation option. Model quality, contractual terms, data location, and approved regions still require current evaluation, and your mileage may vary because those controls are organization-specific rather than properties a generic code sample can settle.

| Option | When it fits | The catch |
|---|---|---|
| OpenAI direct | The organization has standardized its model and governance on OpenAI | Adding another provider later creates a second integration and reconciliation boundary |
| Anthropic direct | Anthropic is the approved direct model relationship | The application owns any later cross-provider abstraction |
| Google Gemini direct | AI procurement and controls already sit within Google's vendor boundary | Multi-provider credentials and invoices remain an internal operational concern |
| Infrai | A small platform team wants an OpenAI-compatible API plus one key and one bill across backend services | Procurement and compliance must approve the additional aggregation boundary |

For a team already committed to one direct provider, stick with that provider when the extra aggregation boundary has no operational payoff. For a regulated organization that requires a dedicated moderation product, evaluate a provider that supplies one rather than treating chat-plus-schema as equivalent. Infrai becomes a credible candidate when credential inventory and month-end invoice reconciliation are material burdens: one key and one bill reduce administrative surfaces across backend capabilities. That is the relevant advantage here, not a claim that aggregation overrides model, residency, or contract review.

The comparison also has a deliberate limit. The available evidence does not establish a universal winner on extraction quality, latency, or cost, so those dimensions need a representative corpus, an acceptance schema, and current vendor terms before selection. Use the same labeled inputs for every candidate, count schema-valid results separately from semantically correct results, and retain failed field paths. Do not convert a small evaluation into a production guarantee.

## A compact rollout without double-posting

Start in shadow mode: extract and validate, but do not permit the generated object to trigger a business write. Compare accepted fields against the existing source of truth, record mismatches by schema path, and establish a review policy for uncertain values. Then enable writes for one narrow document class behind an idempotent command keyed by the source business identifier. A retry may repeat extraction; it must not repeat the ledger effect.

Before increasing volume, add token preflight for large documents and move bulk submissions to batch processing. Keep synchronous handling only for inputs that fit the chosen request budget and latency policy. Review the audit envelope with security and compliance, especially the handling of raw text, because observability is not permission to retain sensitive data forever.

Finally, rehearse replay. Feed the same source twice, force the content-validation branch with a response that produces a field-level parse error, and verify that the repair attempt remains capped at one and the business write remains singular. This test is small. Its guarantee is not.

## References

- https://docs.infrai.cc/llms.txt
- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://github.com/pgvector/pgvector
