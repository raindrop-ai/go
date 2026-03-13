# `raindrop-go`

Go SDK for Raindrop AI observability.

## Install

```bash
go get github.com/invisible-tools/go-raindrop
```

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

	raindrop "github.com/invisible-tools/go-raindrop"
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

- `WithWriteKey(string)`: Sets the Raindrop write key. If omitted, the client becomes a noop client.
- `WithEndpoint(string)`: Overrides the base API endpoint. Defaults to `https://api.raindrop.ai/v1/`.
- `WithDebug(bool)`: Enables debug logging.
- `WithHTTPClient(*http.Client)`: Uses a custom HTTP client.
- `WithPartialFlushInterval(time.Duration)`: Sets the periodic pending-event flush interval. Defaults to `1s`.
- `WithTraceFlushInterval(time.Duration)`: Sets the trace batch flush interval. Defaults to `1s`.
- `WithTraceBatchSize(int)`: Sets the maximum spans per trace request. Defaults to `50`.
- `WithTraceQueueSize(int)`: Sets the maximum queued spans before oldest spans are dropped. Defaults to `5000`.
- `WithServiceName(string)`: Overrides the OTLP service name. Defaults to `raindrop.go-sdk`.
- `WithServiceVersion(string)`: Overrides the OTLP service version and default library version.
- `WithLibraryVersion(string)`: Overrides the `$context.library.version` event metadata.
- `WithLogger(*slog.Logger)`: Uses a custom structured logger.

## Behavior

- Events are sent to `/events/track_partial`.
- Finalized events always send `is_pending=false`.
- Signal payloads go to `/signals/track`.
- User identify payloads go to `/users/identify`.
- Traces are sent as OTLP JSON to `/traces`.
- `Begin()`/`Finish()` is the recommended flow for new code.
- `ResumeInteraction()` is only for recovering an active interaction handle in the same process.
- Empty `writeKey` disables all shipping without raising errors.

## Versioning

This module lives in a monorepo subdirectory. Release tags should use the Go subdirectory tag format:

```bash
git tag v0.1.0
git push origin v0.1.0
```

That tag format matches the module path `github.com/invisible-tools/go-raindrop`.
