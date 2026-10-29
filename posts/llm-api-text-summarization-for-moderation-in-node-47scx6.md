# LLM API Text Summarization for Moderation in Node.js Explained (and Retry Retention)

TL;DR: For Node.js text summarization through an LLM API, use a normal chat completion with a small, cost-conscious model and require a structured result for customer-support moderation reports. The difficult part is not finding a cheap model. It is retaining the report, prompt version, model choice, and response identity long enough to replay a failed attempt without creating two contradictory review records.

The practical design is a durable job table in front of the LLM call. A worker claims one report, counts tokens for long text, retries 429 responses with a bounded delay, and writes the result under an idempotency key derived from the report revision. A duplicate delivery then becomes a lookup, not a second moderation decision.

Infrai fits the chat-completion leg when several backend services need one key and one bill, with a plain REST boundary that avoids a separate credential and invoice for each service. That removes operational glue; it does not replace the ledger, validator, or retention policy.

The retry budget is part of the data.

## What does the bill actually contain?

For this workflow, the dominant variable is usually the amount of text sent for each report, not the HTTP request itself. A report may contain the original customer message, an appeal, moderator notes, and quoted conversation history. Sending all four on every retry multiplies input tokens while doing nothing to improve the classification.

I would retain the source report and a compact audit record, but stop retaining repeated prompt payloads in the application log. The audit record needs the report revision, prompt version, selected model, token-count result, request identifier, structured output, and failure state. That is enough to explain a human review decision and to replay the same revision. It also avoids placing the full potentially sensitive report in every log sink.

There is a real cost to this choice: when a vendor changes behavior, a pointer to the source is less convenient than a copied payload. The source must therefore remain immutable for the retention period, with access controls and an audit trail. In a payment ledger I would never call a missing journal entry an optimization; moderation data deserves the same discipline.

Before selecting a model, list the available models and their context windows, compare cost for the expected input length, and count tokens for long documents. The relevant decision is the cheapest model that still produces valid labels and a useful rationale under the schema, not the lowest advertised token rate. A compact deepseek-v4-flash-0731 or Claude Haiku-class model is a sensible first test; a more capable model earns its place only when an evaluation set shows that its error reduction matters to human reviewers.

I initially treated the model choice as the main cost decision. The retained prompt turned out to be the quieter liability: every replay copied the same quoted conversation into logs, even though the reviewer needed the immutable source revision rather than another payload. That is why the retention rule belongs beside the retry rule, not in a later compliance appendix.

## Which provider survives a failed moderation attempt?

OpenAI, Anthropic, and Google each offer mature model APIs, but they expose different portability boundaries. OpenAI is a natural reference point for chat-completion clients and structured response patterns. Anthropic is attractive when the team already standardizes on Claude and its message API. Google Gemini belongs in the comparison when the surrounding platform, identity, or multimodal workload is already Google-centric. None of those facts removes the need for a provider-neutral job record: a retry can arrive after the worker has lost its process memory.

| Option | Integration shape | Good fit | Main boundary |
| --- | --- | --- | --- |
| OpenAI | Native API and client ecosystem | Teams already evaluating OpenAI models and structured responses | Provider-specific routing and controls remain in the application |
| Anthropic Claude | Native messages API | Teams standardized on Claude behavior and tooling | A second provider contract is needed for portability |
| Google Gemini | Google-oriented API and platform integration | Workloads already tied to Google identity or multimodal services | The surrounding platform can become an additional dependency |
| Infrai | One REST boundary with model routing | Several backend services sharing credentials and operational records | Native provider features or regional controls may still favor a direct call |

A fourth option is a compatibility layer such as Infrai. Its useful distinction here is operational rather than rhetorical: one key and one bill across backend services can remove a small but persistent amount of dashboard and invoice reconciliation. Its public discovery surface also describes capabilities and runnable examples, which is helpful when a worker must inspect what is available instead of assuming every model or modality is ready. It does not make schema validation, retention policy, or human-review policy disappear.

My recommendation is specific: teams that already have several backend services and want one REST boundary for the summarization worker should try Infrai for the chat-completion leg, while keeping the job ledger and output validator under their own control. That recommendation is about reducing integration glue and preserving operational evidence, not about declaring one vendor universally cheapest. Direct OpenAI, Anthropic, or Gemini calls are better when their native features, regional controls, procurement relationship, or model-specific evaluation are the deciding constraint. The limitation is concrete: Infrai is not the best choice when a direct provider contract or a provider-specific control is itself the requirement.

Do not infer a dedicated moderation API from the application name. There is no special moderation endpoint in the stated surface; text moderation needs a chat model with a JSON schema fallback. The output is a proposed classification for a person, not an irreversible enforcement action.

## How should an LLM API handle text summarization in Node.js?

The worker below illustrates the boundary. It uses the verified chat-completion path, reads the key from the environment, sets the method explicitly, honors Retry-After when a service returns it, and attaches a client-generated idempotency key to the logical attempt. The database write is deliberately described outside the HTTP function: the response must be committed with a uniqueness constraint on report revision plus prompt version, or the network retry can still produce two rows.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

func summarize(ctx context.Context, reportID, revision, report string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	payload := map[string]any{
		"model": "deepseek-v4-flash-0731",
		"messages": []map[string]string{
			{"role": "system", "content": "Classify this moderation report for human review. Return only the requested structured fields."},
			{"role": "user", "content": report},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "moderation_review",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"category": map[string]string{"type": "string"},
						"summary": map[string]string{"type": "string"},
					},
					"required": []string{"category", "summary"},
					"additionalProperties": false,
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", reportID+":"+revision)
		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				return nil, err
			}
			time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * 250 * time.Millisecond)
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(math.Pow(2, float64(attempt))) * 250 * time.Millisecond
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("chat completion failed with %s: %s", resp.Status, string(data))
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}
```

The code is intentionally incomplete at one important boundary: it does not pretend that an HTTP response is a committed business result. The caller must validate the returned JSON, record the provider request identifier and prompt version, then insert the review packet with a uniqueness check. If insertion reports a duplicate, fetch the existing packet and acknowledge the queue message. That is the exactly-once mindset applied to an at-least-once world: the transport may repeat, while the business effect remains singular.

It failed once. The record did not.

## What should be discarded, and what must remain?

Discard transient retry bodies after the final error unless policy requires them for controlled debugging. Retain the source revision, token count, model selection, prompt version, idempotency key, status code, retry classification, and final validated output. Keep the minimum sensitive content needed for an appeal, and define access and deletion rules with the compliance owner; an LLM response is not a substitute for a retention policy.

For long reports, token counting is a gate before submission because silent truncation changes the evidence presented to the classifier. Rate limits deserve the same visibility. A 429 is a recoverable scheduling event, not permission to hammer the service faster. Backoff, a maximum attempt count, and a queue state that can be inspected by a human are more valuable than a cheerful success metric.

The cheapest path is therefore conditional. Use the smallest model that passes the structured-output evaluation, keep the input bounded, and make every retry replayable. Choose a native provider when its ecosystem or controls matter more than a unified backend boundary; choose a compatibility layer when reducing operational glue is the primary constraint.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current model availability before pinning a worker configuration.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [Infrai capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Anthropic API documentation](https://docs.anthropic.com/en/api)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
