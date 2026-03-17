package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTrackAISendsTrackPartialPayload(t *testing.T) {
	var received trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/track_partial" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_123",
		UserID:  "user-123",
		Event:   "ai_generation",
		Input:   "What is the capital of France?",
		Output:  "Paris",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
		Properties: map[string]any{
			"ai.usage.prompt_tokens":     10,
			"ai.usage.completion_tokens": 5,
		},
	})
	if err != nil {
		t.Fatalf("track ai: %v", err)
	}

	if received.EventID != "evt_123" {
		t.Fatalf("unexpected event id: %s", received.EventID)
	}
	if received.AIData == nil || received.AIData.Model != "gpt-4o" {
		t.Fatalf("missing ai_data model: %#v", received.AIData)
	}
	if received.IsPending {
		t.Fatalf("expected finalized event")
	}
	contextValue, ok := received.Properties["$context"].(map[string]any)
	if !ok {
		t.Fatalf("missing $context: %#v", received.Properties)
	}
	library, ok := contextValue["library"].(map[string]any)
	if !ok || library["name"] != defaultLibraryName {
		t.Fatalf("unexpected library context: %#v", contextValue)
	}
}

func TestInteractionBuffersAndFlushesOnFinish(t *testing.T) {
	var requests []trackPartialPayload
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload trackPartialPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		mu.Lock()
		requests = append(requests, payload)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithPartialFlushInterval(10*time.Millisecond))
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_interaction",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Hello",
		Model:   "gpt-4o",
	})

	if err := interaction.SetProperties(map[string]any{"stage": "processing"}); err != nil {
		t.Fatalf("set properties: %v", err)
	}
	if err := interaction.Finish(FinishOptions{
		Output: "Hi there!",
	}); err != nil {
		t.Fatalf("finish interaction: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(requests))
	}
	if requests[0].Properties["stage"] != "processing" {
		t.Fatalf("properties were not merged: %#v", requests[0].Properties)
	}
	if requests[0].AIData == nil || requests[0].AIData.Output != "Hi there!" {
		t.Fatalf("missing finish output: %#v", requests[0].AIData)
	}
	if requests[0].IsPending {
		t.Fatalf("expected finalized event")
	}
}

func TestTrackSignalAndIdentifyUseExpectedEndpoints(t *testing.T) {
	var signalCalls int
	var identifyCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/signals/track":
			signalCalls++
		case "/users/identify":
			identifyCalls++
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	if err := client.TrackSignal(context.Background(), Signal{
		EventID:   "evt_123",
		Name:      "thumbs_up",
		Type:      "feedback",
		Sentiment: "POSITIVE",
	}); err != nil {
		t.Fatalf("track signal: %v", err)
	}

	if err := client.Identify(context.Background(), User{
		UserID: "user-123",
		Traits: map[string]any{"plan": "paid"},
	}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	if signalCalls != 1 {
		t.Fatalf("expected 1 signal call, got %d", signalCalls)
	}
	if identifyCalls != 1 {
		t.Fatalf("expected 1 identify call, got %d", identifyCalls)
	}
}

func TestTraceShippingUsesOTLPJSON(t *testing.T) {
	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/traces":
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &received); err != nil {
				t.Fatalf("unmarshal traces payload: %v", err)
			}
		case "/events/track_partial":
			_, _ = io.ReadAll(r.Body)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	span := client.StartSpan(context.Background(), SpanOptions{
		Name:    "llm_call",
		EventID: "evt_123",
	})
	span.SetAttributes(StringAttr("ai.model.id", "gpt-4o"), IntAttr("ai.usage.prompt_tokens", 10))
	span.End()

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].TraceID == "" || spans[0].SpanID == "" {
		t.Fatalf("missing span ids: %#v", spans[0])
	}
	if spans[0].Status == nil || spans[0].Status.Code != SpanStatusOK {
		t.Fatalf("unexpected status: %#v", spans[0].Status)
	}
	foundPromptTokens := false
	for _, attr := range spans[0].Attributes {
		if attr.Key == "ai.usage.prompt_tokens" {
			foundPromptTokens = true
			if attr.Value.IntValue != "10" {
				t.Fatalf("expected intValue as string, got %#v", attr)
			}
		}
	}
	if !foundPromptTokens {
		t.Fatalf("missing ai.usage.prompt_tokens attribute: %#v", spans[0].Attributes)
	}
}

func TestInteractionHelpersMergePropertiesAndAttachments(t *testing.T) {
	var received trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_helpers",
		UserID:  "user-123",
		Input:   "hello",
	})

	if got := interaction.GetEventID(); got != "evt_helpers" {
		t.Fatalf("unexpected event id: %s", got)
	}
	if err := interaction.SetProperty("stage", "planning"); err != nil {
		t.Fatalf("set property: %v", err)
	}
	if err := interaction.AddAttachments([]Attachment{{
		Type:  "text",
		Role:  "output",
		Name:  "reasoning-summary",
		Value: "Short summary",
	}}); err != nil {
		t.Fatalf("add attachments: %v", err)
	}
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish interaction: %v", err)
	}

	if received.Properties["stage"] != "planning" {
		t.Fatalf("missing merged property: %#v", received.Properties)
	}
	if len(received.Attachments) != 1 || received.Attachments[0].Name != "reasoning-summary" {
		t.Fatalf("missing attachment: %#v", received.Attachments)
	}
}

func TestResumeInteractionAndSetInputFinalizeEvent(t *testing.T) {
	var received trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	_ = client.Begin(context.Background(), BeginOptions{
		EventID: "evt_resumed",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "first input",
	})
	interaction := client.ResumeInteraction("evt_resumed")
	if err := interaction.SetInput("resumed input"); err != nil {
		t.Fatalf("set input: %v", err)
	}
	if err := interaction.SetProperty("stage", "resumed"); err != nil {
		t.Fatalf("set property: %v", err)
	}
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if received.EventID != "evt_resumed" {
		t.Fatalf("unexpected event id: %s", received.EventID)
	}
	if received.AIData == nil || received.AIData.Input != "resumed input" || received.AIData.Output != "done" {
		t.Fatalf("unexpected ai_data: %#v", received.AIData)
	}
	if received.Properties["stage"] != "resumed" {
		t.Fatalf("missing resumed property: %#v", received.Properties)
	}
	if received.IsPending {
		t.Fatalf("expected resumed interaction to finalize event")
	}
}

func TestToolHelpersShipToolShapedSpans(t *testing.T) {
	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/traces":
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &received); err != nil {
				t.Fatalf("unmarshal traces payload: %v", err)
			}
		case "/events/track_partial":
			_, _ = io.ReadAll(r.Body)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_tools",
		UserID:  "user-123",
		Input:   "hello",
	})

	reasoningSpan := interaction.StartSpan(SpanOptions{
		Name:       "plan_synthesis",
		Properties: map[string]any{"stage": "planning"},
	})
	reasoningSpan.End()

	toolSpan := interaction.StartToolSpan("weather_lookup", ToolOptions{
		Input: map[string]any{"location": "San Francisco"},
		Properties: map[string]any{
			"user_id": "user-123",
		},
	})
	toolSpan.SetOutput(map[string]any{"forecast": "sunny"})
	toolSpan.End()

	start := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(250 * time.Millisecond)
	interaction.TrackTool(TrackToolOptions{
		Name:       "coffee_search",
		Input:      map[string]any{"query": "best coffee"},
		Output:     map[string]any{"winner": "Ritual"},
		Properties: map[string]any{"convo_id": "conv-123"},
		StartTime:  start,
		EndTime:    end,
	})

	_, err := WithTool(interaction, "park_check", ToolOptions{
		Input: map[string]any{"location": "Dolores Park"},
	}, func() (map[string]any, error) {
		return map[string]any{"recommendation": "yes"}, nil
	})
	if err != nil {
		t.Fatalf("with tool: %v", err)
	}

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 4 {
		t.Fatalf("expected 4 spans, got %d", len(spans))
	}

	var foundWeather bool
	var foundCoffee bool
	var foundReasoning bool
	var foundWrappedTool bool
	for _, span := range spans {
		attrMap := make(map[string]otlpAnyValue, len(span.Attributes))
		for _, attr := range span.Attributes {
			attrMap[attr.Key] = attr.Value
		}

		switch span.Name {
		case "plan_synthesis":
			foundReasoning = true
		case "weather_lookup":
			foundWeather = true
			if attrMap["traceloop.span.kind"].StringValue != "tool" {
				t.Fatalf("weather span missing tool kind: %#v", attrMap)
			}
			if attrMap["traceloop.association.properties.event_id"].StringValue != "evt_tools" {
				t.Fatalf("weather span missing event association: %#v", attrMap)
			}
			if attrMap["traceloop.entity.output"].StringValue == "" {
				t.Fatalf("weather span missing output: %#v", attrMap)
			}
		case "coffee_search":
			foundCoffee = true
			if attrMap["traceloop.entity.duration_ms"].IntValue != "250" {
				t.Fatalf("coffee span missing duration: %#v", attrMap)
			}
			if attrMap["traceloop.association.properties.convo_id"].StringValue != "conv-123" {
				t.Fatalf("coffee span missing convo_id: %#v", attrMap)
			}
		case "park_check":
			foundWrappedTool = true
			if attrMap["traceloop.entity.output"].StringValue == "" {
				t.Fatalf("wrapped tool missing output: %#v", attrMap)
			}
		}
	}

	if !foundWeather || !foundCoffee || !foundReasoning || !foundWrappedTool {
		t.Fatalf("missing expected tool spans: %#v", spans)
	}
}

func TestWithSpanAndTracerCarryAssociationProperties(t *testing.T) {
	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/traces":
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &received); err != nil {
				t.Fatalf("unmarshal traces payload: %v", err)
			}
		case "/events/track_partial":
			_, _ = io.ReadAll(r.Body)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_span",
		UserID:  "user-123",
		Input:   "hello",
	})
	if err := interaction.WithSpan(SpanOptions{
		Name:       "llm_call",
		Properties: map[string]any{"convo_id": "conv-123"},
	}, func(ctx context.Context, span *Span) error {
		child := interaction.StartToolSpan("lookup", ToolOptions{
			Parent: span,
			Input:  map[string]any{"query": "coffee"},
		})
		child.SetOutput(map[string]any{"winner": "Ritual"})
		child.End()
		_ = ctx
		return nil
	}); err != nil {
		t.Fatalf("interaction with span: %v", err)
	}

	tracer := client.Tracer(map[string]any{"job_id": "batch-123"})
	if err := tracer.WithSpan(SpanOptions{
		Name:       "batch_work",
		Properties: map[string]any{"step": "embed"},
	}, func(ctx context.Context, span *Span) error {
		span.SetAttributes(StringAttr("job.kind", "offline"))
		_ = ctx
		return nil
	}); err != nil {
		t.Fatalf("tracer with span: %v", err)
	}
	tracer.TrackTool(TrackToolOptions{
		Name:       "batch_lookup",
		Input:      map[string]any{"query": "weather"},
		Output:     map[string]any{"forecast": "sunny"},
		Properties: map[string]any{"step": "tool"},
		Duration:   125 * time.Millisecond,
	})

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 4 {
		t.Fatalf("expected 4 spans, got %d", len(spans))
	}

	var foundInteractionSpan bool
	var foundTracerSpan bool
	var foundTracerTool bool
	for _, span := range spans {
		attrMap := make(map[string]otlpAnyValue, len(span.Attributes))
		for _, attr := range span.Attributes {
			attrMap[attr.Key] = attr.Value
		}

		switch span.Name {
		case "llm_call":
			foundInteractionSpan = true
			if attrMap["traceloop.association.properties.convo_id"].StringValue != "conv-123" {
				t.Fatalf("interaction span missing convo property: %#v", attrMap)
			}
			if attrMap["ai.telemetry.metadata.raindrop.eventId"].StringValue != "evt_span" {
				t.Fatalf("interaction span missing event id: %#v", attrMap)
			}
		case "batch_work":
			foundTracerSpan = true
			if attrMap["traceloop.association.properties.job_id"].StringValue != "batch-123" {
				t.Fatalf("tracer span missing global property: %#v", attrMap)
			}
			if attrMap["traceloop.association.properties.step"].StringValue != "embed" {
				t.Fatalf("tracer span missing local property: %#v", attrMap)
			}
		case "batch_lookup":
			foundTracerTool = true
			if attrMap["traceloop.association.properties.job_id"].StringValue != "batch-123" {
				t.Fatalf("tracer tool missing global property: %#v", attrMap)
			}
			if attrMap["traceloop.entity.duration_ms"].IntValue != "125" {
				t.Fatalf("tracer tool missing duration: %#v", attrMap)
			}
		}
	}

	if !foundInteractionSpan || !foundTracerSpan || !foundTracerTool {
		t.Fatalf("missing expected spans: %#v", spans)
	}
}

func TestWithToolRunsWrappedFunctionWithoutActiveInteraction(t *testing.T) {
	called := false

	result, err := WithTool[string](nil, "coffee_search", ToolOptions{}, func() (string, error) {
		called = true
		return "Ritual Coffee Roasters", nil
	})
	if err != nil {
		t.Fatalf("with tool: %v", err)
	}
	if !called {
		t.Fatalf("expected wrapped function to execute")
	}
	if result != "Ritual Coffee Roasters" {
		t.Fatalf("unexpected result: %s", result)
	}
}

func TestWithSpanRunsWrappedFunctionWithoutActiveClient(t *testing.T) {
	var interaction *Interaction
	interactionCalled := false

	err := interaction.WithSpan(SpanOptions{Name: "untracked"}, func(ctx context.Context, span *Span) error {
		interactionCalled = true
		if ctx == nil {
			t.Fatalf("expected non-nil context")
		}
		if span != nil {
			t.Fatalf("expected nil span when client is unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("interaction with span: %v", err)
	}
	if !interactionCalled {
		t.Fatalf("expected interaction callback to execute")
	}

	var tracer *Tracer
	tracerCalled := false
	err = tracer.WithSpan(SpanOptions{Name: "untracked"}, func(ctx context.Context, span *Span) error {
		tracerCalled = true
		if ctx == nil {
			t.Fatalf("expected non-nil context")
		}
		if span != nil {
			t.Fatalf("expected nil span when tracer is unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tracer with span: %v", err)
	}
	if !tracerCalled {
		t.Fatalf("expected tracer callback to execute")
	}
}

func TestRetryOnRetryableStatuses(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := attempts.Add(1)
		if current < 3 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "retry me", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	client.transport.sleep = func(context.Context, time.Duration) error { return nil }
	client.transport.jitterFraction = 0
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_retry",
		UserID:  "user-123",
	}); err != nil {
		t.Fatalf("track ai: %v", err)
	}

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestEventFlushRetainsBufferedPatchWhenRequestFails(t *testing.T) {
	var requests atomic.Int32
	var received trackPartialPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/track_partial" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if requests.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusInternalServerError)
			return
		}

		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	client.transport.maxAttempts = 1
	client.transport.sleep = func(context.Context, time.Duration) error { return nil }
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_retry_flush",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "original input",
		Properties: map[string]any{
			"phase": "start",
		},
	})
	if err := interaction.Finish(FinishOptions{
		Output: "final output",
	}); err == nil {
		t.Fatalf("expected first finish flush to fail")
	}

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry flush: %v", err)
	}

	if requests.Load() != 2 {
		t.Fatalf("expected 2 requests, got %d", requests.Load())
	}
	if received.EventID != "evt_retry_flush" {
		t.Fatalf("unexpected event id: %s", received.EventID)
	}
	if received.AIData == nil || received.AIData.Input != "original input" || received.AIData.Output != "final output" {
		t.Fatalf("expected original and final data to be preserved: %#v", received.AIData)
	}
	if received.Properties["phase"] != "start" {
		t.Fatalf("expected original properties to survive retry: %#v", received.Properties)
	}
	if received.IsPending {
		t.Fatalf("expected finalized event")
	}
}

func TestEventFlushRetainsPendingPatchWhenPayloadCannotBeBuiltYet(t *testing.T) {
	var requests atomic.Int32
	var received trackPartialPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/track_partial" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_missing_user",
		Input:   "draft input",
		Event:   "chat_message",
	})

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush without user id: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("expected unsendable patch to remain buffered without HTTP call")
	}

	if err := interaction.Patch(PatchOptions{
		UserID: "user-123",
	}); err != nil {
		t.Fatalf("patch user id: %v", err)
	}
	if err := interaction.Finish(FinishOptions{
		Output: "completed output",
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if requests.Load() != 1 {
		t.Fatalf("expected 1 request after user id attached, got %d", requests.Load())
	}
	if received.AIData == nil || received.AIData.Input != "draft input" || received.AIData.Output != "completed output" {
		t.Fatalf("expected buffered input/output to be preserved: %#v", received.AIData)
	}
}

func TestPendingFlushDoesNotOverwriteNewerStickyState(t *testing.T) {
	firstRequestStarted := make(chan struct{})
	allowFirstRequest := make(chan struct{})
	var requestCount atomic.Int32
	var finalized trackPartialPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/track_partial" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var payload trackPartialPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}

		switch requestCount.Add(1) {
		case 1:
			close(firstRequestStarted)
			<-allowFirstRequest
		case 3:
			finalized = payload
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_sticky",
		UserID:  "user-old",
		Event:   "chat_message",
		Input:   "hello",
	})

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- client.Flush(context.Background())
	}()

	<-firstRequestStarted

	if err := interaction.Patch(PatchOptions{UserID: "user-new"}); err != nil {
		t.Fatalf("patch user id: %v", err)
	}

	close(allowFirstRequest)
	if err := <-flushDone; err != nil {
		t.Fatalf("first flush: %v", err)
	}

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("second pending flush: %v", err)
	}

	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if finalized.UserID != "user-new" {
		t.Fatalf("expected newest sticky user id, got %q", finalized.UserID)
	}
}

func TestTraceFlushRestoresBatchWhenExportFails(t *testing.T) {
	var requests atomic.Int32
	var received exportTraceServiceRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/traces" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if requests.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusInternalServerError)
			return
		}

		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal traces payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	client.transport.maxAttempts = 1
	client.transport.sleep = func(context.Context, time.Duration) error { return nil }
	defer func() { _ = client.Close() }()

	span := client.StartSpan(context.Background(), SpanOptions{
		Name:    "retry_trace",
		EventID: "evt_trace_retry",
	})
	span.SetAttributes(StringAttr("ai.model.id", "gpt-4o"))
	span.End()

	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected first trace flush to fail")
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry trace flush: %v", err)
	}

	if requests.Load() != 2 {
		t.Fatalf("expected 2 trace requests, got %d", requests.Load())
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 1 || spans[0].Name != "retry_trace" {
		t.Fatalf("expected failed batch to be retried intact: %#v", spans)
	}
}

func TestPeriodicFlushTimerWorks(t *testing.T) {
	var attempts atomic.Int32
	done := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events/track_partial" {
			if attempts.Add(1) == 1 {
				close(done)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithPartialFlushInterval(10*time.Millisecond))
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_pending",
		UserID:  "user-123",
		Input:   "hello",
	})
	if interaction == nil {
		t.Fatalf("expected interaction")
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected periodic flush")
	}
}

func TestCloseFlushesPendingEventsAndSpans(t *testing.T) {
	var eventCalls atomic.Int32
	var traceCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/events/track_partial":
			eventCalls.Add(1)
		case "/traces":
			traceCalls.Add(1)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithPartialFlushInterval(100*time.Millisecond), WithTraceFlushInterval(100*time.Millisecond))

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_close",
		UserID:  "user-123",
		Input:   "hello",
	})
	if interaction == nil {
		t.Fatalf("expected interaction")
	}

	span := client.StartSpan(context.Background(), SpanOptions{
		Name:    "close_flush",
		EventID: "evt_close",
	})
	span.End()

	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	if eventCalls.Load() == 0 {
		t.Fatalf("expected pending event to flush on close")
	}
	if traceCalls.Load() == 0 {
		t.Fatalf("expected spans to flush on close")
	}
}

func TestNoopClientWithoutWriteKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("expected no requests for noop client")
	}))
	defer server.Close()

	client, err := New(
		WithEndpoint(server.URL+"/"),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{UserID: "user-123"}); err != nil {
		t.Fatalf("track ai: %v", err)
	}
	if err := client.TrackSignal(context.Background(), Signal{Name: "thumbs_up"}); err != nil {
		t.Fatalf("track signal: %v", err)
	}
	if err := client.Identify(context.Background(), User{UserID: "user-123"}); err != nil {
		t.Fatalf("identify: %v", err)
	}
}

func TestBeginIsNilSafe(t *testing.T) {
	var client *Client

	interaction := client.Begin(nil, BeginOptions{
		UserID: "user-123",
		Event:  "chat_message",
		Input:  "hello",
	})

	if interaction == nil {
		t.Fatalf("expected interaction")
	}
	if interaction.client != nil {
		t.Fatalf("expected nil client on nil-safe interaction")
	}
	if interaction.ctx == nil {
		t.Fatalf("expected background context")
	}
}

func TestConcurrentUsageIsSafe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithPartialFlushInterval(5*time.Millisecond), WithTraceFlushInterval(5*time.Millisecond))
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			eventID := strings.Join([]string{"evt", strconv.Itoa(index)}, "-")
			interaction := client.Begin(context.Background(), BeginOptions{
				EventID: eventID,
				UserID:  "user-123",
				Input:   "hello",
			})
			_ = interaction.SetProperties(map[string]any{"index": index})
			_ = interaction.Finish(FinishOptions{Output: "done"})
			span := client.StartSpan(context.Background(), SpanOptions{Name: "worker", EventID: eventID})
			span.SetAttributes(IntAttr("worker.index", int64(index)))
			span.End()
		}(i)
	}
	wg.Wait()

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func newTestClient(t *testing.T, endpoint string, opts ...Option) *Client {
	t.Helper()

	allOpts := []Option{
		WithWriteKey("rk_test"),
		WithEndpoint(endpoint),
		WithDebug(false),
		WithPartialFlushInterval(0),
		WithTraceFlushInterval(0),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	allOpts = append(allOpts, opts...)

	client, err := New(allOpts...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func validOTLPPayload() exportTraceServiceRequest {
	return exportTraceServiceRequest{
		ResourceSpans: []resourceSpans{{
			Resource: resource{Attributes: []otlpKeyValue{
				{Key: "service.name", Value: otlpAnyValue{StringValue: "openrouter"}},
			}},
			ScopeSpans: []scopeSpans{{
				Scope: scope{Name: "openrouter", Version: "1.0"},
				Spans: []otlpSpan{{
					TraceID:           "dGVzdHRyYWNlaWQx",
					SpanID:            "dGVzdHNwYW4x",
					Name:              "chat_completion",
					StartTimeUnixNano: "1000000000",
					EndTimeUnixNano:   "2000000000",
				}},
			}},
		}},
	}
}

func TestOTLPHandlerForwardsValidPayload(t *testing.T) {
	var received exportTraceServiceRequest
	var gotRequest atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/traces" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		gotRequest.Store(true)
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("unmarshal: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	payload, _ := json.Marshal(validOTLPPayload())
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	client.OTLPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !gotRequest.Load() {
		t.Fatal("upstream never received the request")
	}
	if len(received.ResourceSpans) != 1 {
		t.Fatalf("expected 1 resourceSpans, got %d", len(received.ResourceSpans))
	}
	// Verify incoming service.name was preserved (overlay wins over defaults)
	attrs := received.ResourceSpans[0].Resource.Attributes
	var foundServiceName string
	for _, attr := range attrs {
		if attr.Key == "service.name" {
			foundServiceName = attr.Value.StringValue
		}
	}
	if foundServiceName != "openrouter" {
		t.Fatalf("expected service.name=openrouter, got %q", foundServiceName)
	}
}

func TestOTLPHandlerRejectsInvalidJSON(t *testing.T) {
	var gotRequest atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequest.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("not json"))
	rec := httptest.NewRecorder()
	client.OTLPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if gotRequest.Load() {
		t.Fatal("upstream should not have received a request")
	}
}

func TestOTLPHandlerRejectsWrongMethod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not have received a request")
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	req := httptest.NewRequest(http.MethodGet, "/v1/traces", nil)
	rec := httptest.NewRecorder()
	client.OTLPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestOTLPHandlerNoopWhenDisabled(t *testing.T) {
	var gotRequest atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequest.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithWriteKey(""))
	defer func() { _ = client.Close() }()

	payload, _ := json.Marshal(validOTLPPayload())
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	client.OTLPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotRequest.Load() {
		t.Fatal("upstream should not have received a request when disabled")
	}
}

func TestOTLPHandlerEmptyResourceSpans(t *testing.T) {
	var gotRequest atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequest.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	payload, _ := json.Marshal(exportTraceServiceRequest{ResourceSpans: []resourceSpans{}})
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	client.OTLPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotRequest.Load() {
		t.Fatal("upstream should not have received a request for empty payload")
	}
}
