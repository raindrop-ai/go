# Raindrop Go SDK

Go SDK for Raindrop AI observability.

## Install

```bash
go get github.com/raindrop-ai/go
```

Source code and releases live in this repository: [github.com/raindrop-ai/go](https://github.com/raindrop-ai/go).

## Payload size limits

As of `v0.1.4`, text fields (event input/output, event property and
attachment values, tool span I/O, stringified span properties) are capped at
**1,000,000 bytes per field by default** and
truncated with a `...[truncated by raindrop]` marker. The cap is enforced
before serialization, so oversized payloads cost the cap — not the payload —
on your calling goroutine, and large events land truncated instead of being
rejected at the ingest size limit. Tune it via:

```go
client, err := raindrop.New(
	raindrop.WithWriteKey("rk_..."),
	raindrop.WithMaxTextFieldChars(250_000),
)
```

All outbound HTTP carries finite deadlines — even when a custom
`WithHTTPClient` has no `Timeout` — and `Close()` runs its final flush under
a hard deadline (`WithCloseTimeout`, default 10s) so a dead network can
never wedge your process exit.

## Quick Start: Interaction API

The Go SDK follows the same core manual workflow as the TypeScript SDK:

1. `Begin()` to create an interaction and log the initial input
2. update it with `SetProperty`, `SetProperties`, `SetInput`, or `AddAttachments`
3. `Finish()` with the final output

```go
package main

import (
	"context"
	"log"

	raindrop "github.com/raindrop-ai/go"
)

func main() {
	client, err := raindrop.New(
		raindrop.WithWriteKey("rk_..."),
		raindrop.WithEndpoint("https://api.raindrop.ai/v1/"),
		raindrop.WithDebug(true),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	eventID := "evt_123"

	interaction := client.Begin(ctx, raindrop.BeginOptions{
		EventID: eventID,
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Can you suggest a calm Saturday morning in San Francisco?",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
	})

	_ = interaction.SetProperty("stage", "planning")
	_ = interaction.SetInput("Can you suggest a calm Saturday morning near Dolores Park?")
	_ = interaction.AddAttachments([]raindrop.Attachment{{
		Type:  "text",
		Role:  "output",
		Name:  "summary",
		Value: "The user wants coffee, a short walk, and a low-key pace.",
	}})

	_ = interaction.Finish(raindrop.FinishOptions{
		Output: "Coffee near Valencia, then a relaxed walk through Dolores Park.",
	})
}
```

### Updating An Interaction

```go
_ = interaction.SetProperty("stage", "retrieving")
_ = interaction.SetProperties(map[string]any{
	"surface": "chat",
	"region":  "us-west",
})
_ = interaction.SetInput("Can you make it a little more local?")
_ = interaction.AddAttachments([]raindrop.Attachment{{
	Type:  "text",
	Role:  "input",
	Name:  "preferences",
	Value: "Prefers coffee, a quiet walk, and no museum stops.",
}})
```

### Resuming An Interaction

`ResumeInteraction()` is only for recovering an active in-memory interaction created by `Begin()` in the same process. It is not a cross-process restore mechanism.

```go
interaction := client.Begin(ctx, raindrop.BeginOptions{
	EventID: "evt_123",
	UserID:  "user-123",
	Event:   "chat_message",
	Input:   "Can you suggest a morning plan?",
})

resumed := client.ResumeInteraction("evt_123")
_ = resumed.SetProperty("stage", "follow-up")
_ = resumed.Finish(raindrop.FinishOptions{
	Output: "Here is a shorter version of that itinerary.",
})
```

## Single-Shot Tracking

```go
_ = client.TrackAI(ctx, raindrop.AIEvent{
	UserID:  "user-123",
	Event:   "chat_message",
	Input:   "What is a calm Saturday morning plan in San Francisco?",
	Output:  "Coffee near Valencia, then a relaxed walk through Dolores Park.",
	Model:   "gpt-4o",
	ConvoID: "conv-123",
	Properties: map[string]any{
		"ai.usage.prompt_tokens":     10,
		"ai.usage.completion_tokens": 5,
	},
})
```

Use `TrackEvent()` for non-AI events:

```go
_ = client.TrackEvent(ctx, raindrop.Event{
	UserID: "user-123",
	Event:  "session_started",
	Properties: map[string]any{
		"entrypoint": "dashboard",
	},
})
```

### Feature Flags

Attach the feature-flag variants active when an event happened with the
optional `FeatureFlags` (a `map[string]string`). It is available on the
`Event`, `AIEvent`, `BeginOptions`, `PatchOptions`, and `FinishOptions`
surfaces (and via `interaction.SetFeatureFlags`), and serializes to the
top-level `feature_flags` object on the wire. Omit it and no key is sent.

```go
_ = client.TrackAI(ctx, raindrop.AIEvent{
	UserID: "user-123",
	Event:  "chat_message",
	Input:  "How do I enable reasoning?",
	Output: "Toggle it in Settings.",
	Model:  "gpt-4o",
	FeatureFlags: map[string]string{
		"prompt-version": "v2",
		"cohort":         "beta",
	},
})
```

## Tool Spans

```go
tool := interaction.StartToolSpan("weather_lookup", raindrop.ToolOptions{
	Input: map[string]any{"location": "San Francisco"},
})
tool.SetOutput(map[string]any{"forecast": "sunny"})
tool.End()

_, _ = raindrop.WithTool(interaction, "coffee_search", raindrop.ToolOptions{
	Input: map[string]any{"query": "best coffee near Dolores Park"},
}, func() (map[string]any, error) {
	return map[string]any{"winner": "Ritual Coffee Roasters"}, nil
})
```

## Generic Spans

```go
_ = interaction.WithSpan(raindrop.SpanOptions{
	Name:       "llm_turn",
	Properties: map[string]any{"provider": "openai"},
}, func(ctx context.Context, span *raindrop.Span) error {
	if span != nil {
		span.SetAttributes(raindrop.StringAttr("ai.model.id", "gpt-4o"))
	}
	_ = ctx
	return nil
})
```

## Signals, Users, And Traces

```go
_ = client.TrackSignal(ctx, raindrop.Signal{
	EventID:   "evt_123",
	Name:      "thumbs_up",
	Type:      "feedback",
	Sentiment: "POSITIVE",
})

_ = client.Identify(ctx, raindrop.User{
	UserID: "user-123",
	Traits: map[string]any{"plan": "paid"},
})

span := client.StartSpan(ctx, raindrop.SpanOptions{
	Name:    "llm_call",
	EventID: "evt_123",
})
defer span.End()
span.SetAttributes(raindrop.StringAttr("ai.model.id", "gpt-4o"))
```

## Standalone Tracer

```go
tracer := client.Tracer(map[string]any{"job_id": "batch-123"})

_ = tracer.WithSpan(raindrop.SpanOptions{
	Name:       "offline_enrichment",
	Properties: map[string]any{"step": "embed"},
}, func(ctx context.Context, span *raindrop.Span) error {
	if span != nil {
		span.SetAttributes(raindrop.StringAttr("job.kind", "offline"))
	}
	_ = ctx
	return nil
})

tracer.TrackTool(raindrop.TrackToolOptions{
	Name:       "vector_lookup",
	Input:      map[string]any{"query": "mission coffee"},
	Output:     map[string]any{"winner": "Ritual Coffee Roasters"},
	Properties: map[string]any{"step": "retrieve"},
})
```

## Options

- `WithWriteKey(string)`: Sets the Raindrop write key. When empty and no local Workshop URL resolves, the client becomes a no-op.
- `WithEndpoint(string)`: Overrides the base API endpoint. Defaults to `https://api.raindrop.ai/v1/`.
- `WithProjectID(string)`: Scopes telemetry to a Raindrop project by attaching the `X-Raindrop-Project-Id` header to every outbound request. The value is trimmed and validated against `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`. When unset, no header is sent and the backend routes events to the org's default project; an invalid value is ignored with a logged warning (no header is sent) so a misconfiguration never breaks ingestion.
- `WithLocalWorkshopURL(string)`: Pins the local Workshop daemon URL, suppressing env vars and the auto-detect probe. Pass an empty string to revert to inherit-from-env behavior.
- `WithDisableLocalWorkshop()`: Opts out of the local mirror entirely, even if `RAINDROP_LOCAL_DEBUGGER` / `RAINDROP_WORKSHOP` is set or a daemon is listening on the default port.
- `WithDebug(bool)`: Enables debug logging.
- `WithHTTPClient(*http.Client)`: Uses a custom HTTP client.
- `WithPartialFlushInterval(time.Duration)`: Sets the periodic pending-event flush interval. Defaults to `1s`.
- `WithTraceFlushInterval(time.Duration)`: Sets the trace batch flush interval. Defaults to `1s`.
- `WithTraceBatchSize(int)`: Sets the maximum spans per trace request. Defaults to `50`.
- `WithTraceQueueSize(int)`: Sets the maximum queued spans before oldest spans are dropped. Defaults to `5000`.
- `WithServiceName(string)`: Overrides the OTLP service name. Defaults to `raindrop.go-sdk`.
- `WithServiceVersion(string)`: Overrides the OTLP service version and default library version.
- `WithLibraryVersion(string)`: Overrides the `$context.library.version` event metadata.
- `WithMaxTextFieldChars(int)`: Sets the per-field byte cap applied to event input/output, event property and attachment values, and serialized tool span content before serialization. Defaults to `1000000`. Non-positive values are ignored.
- `WithCloseTimeout(time.Duration)`: Sets the hard deadline for `Close()`'s final flush; once it passes, in-flight sends are aborted and remaining payloads are dropped. Defaults to `10s`. Non-positive values are ignored.
- `WithLogger(*slog.Logger)`: Uses a custom structured logger.

## Routing To A Project

By default, telemetry lands in your org's `default` project. Pass
`WithProjectID` to route every event, signal, identify call, and trace to a
named project instead:

```go
client, err := raindrop.New(
	raindrop.WithWriteKey("rk_..."),
	raindrop.WithProjectID("checkout-bot"),
)
```

When set to a valid slug, the `X-Raindrop-Project-Id` header is attached to
every outbound request (including the local Workshop mirror). When unset, no
header is sent and the backend falls back to the default project, so existing
callers are unaffected. The slug is trimmed and validated against
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`; an invalid value is ignored with a
logged warning and no header is sent, so a misconfigured project ID can never
break telemetry shipping.

## Local Workshop Mirror

When a local Workshop daemon URL resolves, every cloud-bound POST is also mirrored to the local URL so events show up in a local Workshop instance during development.

Resolution precedence (highest priority first):

1. `WithLocalWorkshopURL(url)` option
2. `WithDisableLocalWorkshop()` option (suppresses env + probe)
3. `RAINDROP_LOCAL_DEBUGGER` env var (URL)
4. `RAINDROP_WORKSHOP` env var (URL or boolean: `1`/`true`/`yes`/`on` enables the default URL; `0`/`false`/`no`/`off` disables)
5. TCP probe of `127.0.0.1:5899` with a 100ms timeout (one-time, at `New()`)
6. None of the above &rarr; local mirror disabled

The local POST uses a 2s timeout, no retries, and errors are logged at `debug` level only so a slow or broken local daemon never affects the cloud path. When `WithWriteKey` is empty but a local URL resolves, the client ships to the local mirror only.

## Behavior

- Events are sent to `/events/track_partial`.
- Finalized events always send `is_pending=false`.
- Signal payloads go to `/signals/track`.
- User identify payloads go to `/users/identify`.
- Traces are sent as OTLP JSON to `/traces`.
- When `WithProjectID` is set to a valid slug, every outbound request carries the `X-Raindrop-Project-Id` header; otherwise the header is omitted.
- `Begin()`/`Finish()` is the recommended flow for new code.
- `ResumeInteraction()` is only for recovering an active interaction handle in the same process.
- Empty `writeKey` with no local Workshop URL resolved disables all shipping without raising errors.

## Versioning

This repository uses normal Go module tagging:

```bash
git tag v0.1.0
git push origin v0.1.0
```

That tag format matches the module path `github.com/raindrop-ai/go`.
