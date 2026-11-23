package raindrop

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// maskRecorder is a tag mask that records every call.
type maskRecorder struct {
	mu    sync.Mutex
	calls []MaskContext
}

func (m *maskRecorder) mask(value any, ctx MaskContext) any {
	m.mu.Lock()
	m.calls = append(m.calls, ctx)
	m.mu.Unlock()
	label := string(ctx.Kind) + " " + ctx.Field
	switch value.(type) {
	case string:
		return "<MASKED " + label + ">"
	case []Attachment:
		return []Attachment{{Type: "text", Role: "input", Value: "<MASKED " + label + ">"}}
	default:
		return map[string]any{"masked": label}
	}
}

func (m *maskRecorder) fields() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.calls))
	for i, c := range m.calls {
		out[i] = string(c.Kind) + " " + c.Field
	}
	return out
}

func decodeBatch(t *testing.T, rs *recordingServer, route string) []map[string]any {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var out []map[string]any
	for _, body := range rs.bodies[route] {
		var batch []map[string]any
		if err := json.Unmarshal(body, &batch); err == nil {
			out = append(out, batch...)
			continue
		}
		var single map[string]any
		if err := json.Unmarshal(body, &single); err != nil {
			t.Fatalf("decode %s: %v", route, err)
		}
		out = append(out, single)
	}
	return out
}

func TestMaskCompleteEventContentOnly(t *testing.T) {
	server := newRecordingServer(t, nil)
	rec := &maskRecorder{}
	client := newTestClient(t, server.URL+"/", WithMask(rec.mask))
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_mask", UserID: "SECRET-jane", Event: "chat", Model: "mock-gpt", ConvoID: "c1",
		Input: "Contact me at SECRET-jane", Output: "Card 4111 1111 1111 1111",
		Properties:  map[string]any{"note": "SECRET-jane"},
		Attachments: []Attachment{{Type: "text", Role: "input", Value: "SECRET-jane"}},
	}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)

	events := decodeBatch(t, server, "/events/track")
	if len(events) != 1 {
		t.Fatalf("events = %d", len(events))
	}
	event := events[0]
	ai := event["ai_data"].(map[string]any)
	if ai["input"] != "<MASKED event ai_data.input>" || ai["output"] != "<MASKED event ai_data.output>" {
		t.Fatalf("ai_data = %#v", ai)
	}
	if ai["model"] != "mock-gpt" || ai["convo_id"] != "c1" {
		t.Fatalf("routing fields changed: %#v", ai)
	}
	if event["user_id"] != "SECRET-jane" || event["event"] != "chat" || event["event_id"] != "evt_mask" {
		t.Fatalf("identity changed: %#v", event)
	}
	props := event["properties"].(map[string]any)
	if props["masked"] != "event properties" || props["note"] != nil {
		t.Fatalf("properties = %#v", props)
	}
	if props["$context"] == nil {
		t.Fatalf("the SDK's own $context must survive the mask: %#v", props)
	}
	attachments := event["attachments"].([]any)
	if len(attachments) != 1 || attachments[0].(map[string]any)["value"] != "<MASKED event attachments>" {
		t.Fatalf("attachments = %#v", attachments)
	}
	want := []string{"event properties", "event ai_data.input", "event ai_data.output", "event attachments"}
	if got := rec.fields(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mask calls = %v, want %v", got, want)
	}
}

func TestMaskGetsCopiesNotCallerData(t *testing.T) {
	server := newRecordingServer(t, nil)
	mask := func(value any, ctx MaskContext) any {
		if m, ok := value.(map[string]any); ok {
			m["nested"].(map[string]any)["email"] = "<gone>"
			m["list"].([]any)[0] = "<gone>"
			return m
		}
		return value
	}
	client := newTestClient(t, server.URL+"/", WithMask(mask))
	defer func() { _ = client.Close() }()

	nested := map[string]any{"email": "SECRET-jane"}
	list := []any{"SECRET-jane"}
	if err := client.TrackEvent(context.Background(), Event{
		UserID: "u1", Event: "e", Properties: map[string]any{"nested": nested, "list": list},
	}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	if nested["email"] != "SECRET-jane" || list[0] != "SECRET-jane" {
		t.Fatalf("mask changed the caller's data: %#v %#v", nested, list)
	}
	props := decodeBatch(t, server, "/events/track")[0]["properties"].(map[string]any)
	if props["nested"].(map[string]any)["email"] != "<gone>" || props["list"].([]any)[0] != "<gone>" {
		t.Fatalf("mask output not sent: %#v", props)
	}
}

func TestMaskRunsOnEveryPartialRequest(t *testing.T) {
	server := newRecordingServer(t, nil)
	rec := &maskRecorder{}
	client := newTestClient(t, server.URL+"/", WithMask(rec.mask))
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_partial", UserID: "u1", Event: "chat", Input: "SECRET-jane",
		Properties: map[string]any{"note": "SECRET-jane"},
	})
	flushOrFail(t, client)
	if err := interaction.SetProperty("stage", "SECRET-jane"); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	if err := interaction.Finish(FinishOptions{
		Output:      "SECRET-bob",
		Attachments: []Attachment{{Type: "text", Role: "output", Value: "SECRET-bob"}},
	}); err != nil {
		t.Fatal(err)
	}

	partials := server.partials(t)
	if len(partials) != 3 {
		t.Fatalf("partials = %d", len(partials))
	}
	for i, p := range partials {
		raw, _ := json.Marshal(p)
		if strings.Contains(string(raw), "SECRET") {
			t.Fatalf("request %d leaked content: %s", i, raw)
		}
		if p["event_id"] != "evt_partial" {
			t.Fatalf("request %d identity changed: %s", i, raw)
		}
		props := p["properties"].(map[string]any)
		if props["$context"] == nil {
			t.Fatalf("request %d lost $context: %#v", i, props)
		}
		// Begin and SetProperty carry properties; Finish carries none, so its
		// request has only the SDK's own.
		if carried := i < 2; carried != (props["masked"] == "event properties") {
			t.Fatalf("request %d properties = %#v", i, props)
		}
	}
	if got := partials[0]["ai_data"].(map[string]any)["input"]; got != "<MASKED event ai_data.input>" {
		t.Fatalf("begin input = %#v", got)
	}
	last := partials[2]
	if got := last["ai_data"].(map[string]any)["output"]; got != "<MASKED event ai_data.output>" {
		t.Fatalf("finish output = %#v", got)
	}
	if got := last["attachments"].([]any); len(got) != 1 {
		t.Fatalf("finish attachments = %#v", got)
	}
}

func TestMaskSignalPropertiesIncludeCommentAndAfter(t *testing.T) {
	server := newRecordingServer(t, nil)
	var seen map[string]any
	mask := func(value any, ctx MaskContext) any {
		if ctx.Kind == MaskKindSignal && ctx.Field == "properties" {
			seen = value.(map[string]any)
			return map[string]any{"masked": "signal properties"}
		}
		t.Errorf("unexpected mask call %v", ctx)
		return value
	}
	client := newTestClient(t, server.URL+"/", WithMask(mask))
	defer func() { _ = client.Close() }()

	if err := client.TrackSignal(context.Background(), Signal{
		EventID: "evt_sig", Name: "feedback", Comment: "reach me at SECRET-jane", After: "call 555-123-4567",
		Properties: map[string]any{"note": "SECRET-bob"},
	}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)

	want := map[string]any{"note": "SECRET-bob", "comment": "reach me at SECRET-jane", "after": "call 555-123-4567"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("mask got %#v, want %#v", seen, want)
	}
	signal := decodeBatch(t, server, "/signals/track")[0]
	if signal["comment"] != nil || signal["after"] != nil {
		t.Fatalf("comment/after sent outside properties: %#v", signal)
	}
	if signal["event_id"] != "evt_sig" || signal["signal_name"] != "feedback" {
		t.Fatalf("identity changed: %#v", signal)
	}
	if props := signal["properties"].(map[string]any); props["masked"] != "signal properties" || len(props) != 1 {
		t.Fatalf("properties = %#v", props)
	}
}

func TestSignalWithoutMaskKeepsCommentAndAfter(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	if err := client.TrackSignal(context.Background(), Signal{EventID: "e", Name: "n", Comment: "c", After: "a"}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	signal := decodeBatch(t, server, "/signals/track")[0]
	if signal["comment"] != "c" || signal["after"] != "a" || signal["properties"] != nil {
		t.Fatalf("signal = %#v", signal)
	}
}

func TestMaskIdentifyTraits(t *testing.T) {
	server := newRecordingServer(t, nil)
	rec := &maskRecorder{}
	client := newTestClient(t, server.URL+"/", WithMask(rec.mask))
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{
		UserID: "SECRET-jane", Traits: map[string]any{"email": "SECRET-jane", "plan": "pro"},
	}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	user := decodeBatch(t, server, "/users/identify")[0]
	if user["user_id"] != "SECRET-jane" {
		t.Fatalf("user id changed: %#v", user)
	}
	if traits := user["traits"].(map[string]any); traits["masked"] != "identify traits" || traits["email"] != nil {
		t.Fatalf("traits = %#v", traits)
	}
}

func TestMaskSpanContentAndStatusMessage(t *testing.T) {
	var received exportTraceServiceRequest
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool { return false })
	rec := &maskRecorder{}
	client := newTestClient(t, server.URL+"/", WithMask(rec.mask))
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_span", UserID: "u1", Input: "hi"})
	interaction.TrackTool(TrackToolOptions{
		Name:       "lookup",
		Input:      map[string]any{"q": "SECRET-jane"},
		Output:     "SECRET-bob",
		Error:      errors.New("failed for SECRET-jane"),
		Properties: map[string]any{"note": "SECRET-jane", "count": 3},
	})
	flushOrFail(t, client)

	server.mu.Lock()
	bodies := server.bodies["/traces"]
	server.mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("trace requests = %d", len(bodies))
	}
	if err := json.Unmarshal(bodies[0], &received); err != nil {
		t.Fatal(err)
	}
	span := received.ResourceSpans[0].ScopeSpans[0].Spans[0]
	attrs := map[string]otlpAnyValue{}
	for _, attr := range span.Attributes {
		attrs[attr.Key] = attr.Value
	}
	for key, want := range map[string]string{
		"traceloop.entity.input":                 "<MASKED span traceloop.entity.input>",
		"traceloop.entity.output":                "<MASKED span traceloop.entity.output>",
		"traceloop.association.properties.note":  "<MASKED span traceloop.association.properties.note>",
		"traceloop.entity.name":                  "lookup",
		"ai.telemetry.metadata.raindrop.eventId": "evt_span",
	} {
		if got := attrs[key].StringValue; got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if got := attrs["traceloop.association.properties.count"].IntValue; got != "3" {
		t.Fatalf("numeric property copy changed: %q", got)
	}
	if span.Status == nil || span.Status.Message != "<MASKED span status.message>" || span.Status.Code != SpanStatusError {
		t.Fatalf("status = %#v", span.Status)
	}
	if strings.Contains(string(bodies[0]), "SECRET") {
		t.Fatalf("span leaked content: %s", bodies[0])
	}
}

func TestMaskFailsClosedOnPanic(t *testing.T) {
	server := newRecordingServer(t, nil)
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithLogger(logger), WithMask(func(value any, ctx MaskContext) any {
		panic(errors.New("cannot mask SECRET-jane"))
	}))
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		UserID: "u1", Event: "chat", Input: "SECRET-jane", Output: "SECRET-bob",
		Properties:  map[string]any{"note": "SECRET-jane"},
		Attachments: []Attachment{{Type: "text", Role: "input", Value: "SECRET-jane"}},
	}); err != nil {
		t.Fatalf("a failing mask must not fail the call: %v", err)
	}
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_fail", UserID: "u1", Input: "SECRET-jane",
		Attachments: []Attachment{{Type: "text", Role: "input", Value: "SECRET-jane"}}})
	interaction.TrackTool(TrackToolOptions{Name: "t", Input: "SECRET-jane", Error: errors.New("SECRET-jane")})
	if err := client.Identify(context.Background(), User{UserID: "u1", Traits: map[string]any{"email": "SECRET-jane"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.TrackSignal(context.Background(), Signal{EventID: "e", Name: "n", Comment: "SECRET-jane"}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)

	server.mu.Lock()
	for route, bodies := range server.bodies {
		for _, body := range bodies {
			if strings.Contains(string(body), "SECRET") {
				t.Errorf("%s leaked content: %s", route, body)
			}
		}
	}
	server.mu.Unlock()

	event := decodeBatch(t, server, "/events/track")[0]
	ai := event["ai_data"].(map[string]any)
	if ai["input"] != MaskFailed || ai["output"] != MaskFailed {
		t.Fatalf("ai_data = %#v", ai)
	}
	props := event["properties"].(map[string]any)
	if props["error"] != MaskFailed || props["note"] != nil || props["$context"] == nil {
		t.Fatalf("properties = %#v", props)
	}
	if event["attachments"] != nil {
		t.Fatalf("attachments = %#v", event["attachments"])
	}
	partial := server.partials(t)[0]
	if got, ok := partial["attachments"].([]any); !ok || len(got) != 0 {
		t.Fatalf("partial attachments = %#v", partial["attachments"])
	}
	if got := decodeBatch(t, server, "/users/identify")[0]["traits"].(map[string]any); got["error"] != MaskFailed {
		t.Fatalf("traits = %#v", got)
	}
	if got := decodeBatch(t, server, "/signals/track")[0]["properties"].(map[string]any); got["error"] != MaskFailed {
		t.Fatalf("signal properties = %#v", got)
	}
	if server.count("/traces") != 1 {
		t.Fatalf("span not shipped")
	}

	out := logs.String()
	if !strings.Contains(out, "mask function failed") || !strings.Contains(out, "*errors.errorString") {
		t.Fatalf("expected a warning naming the panic type, got %q", out)
	}
	if strings.Contains(out, "SECRET") || strings.Contains(out, "cannot mask") {
		t.Fatalf("warning quotes the content: %q", out)
	}
}

func TestMaskFailsClosedOnWrongType(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/", WithMask(func(value any, ctx MaskContext) any {
		switch value.(type) {
		case string:
			return 42
		case []Attachment:
			return []any{}
		default:
			return map[string]any{"bad": make(chan int)}
		}
	}))
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		UserID: "u1", Input: "SECRET-jane", Properties: map[string]any{"note": "x"},
	}); err != nil {
		t.Fatal(err)
	}
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_wrong", UserID: "u1", Input: "x",
		Attachments: []Attachment{{Type: "text", Role: "input", Value: "x"}}})
	_ = interaction
	flushOrFail(t, client)

	event := decodeBatch(t, server, "/events/track")[0]
	if got := event["ai_data"].(map[string]any)["input"]; got != MaskFailed {
		t.Fatalf("input = %#v", got)
	}
	if got := event["properties"].(map[string]any)["error"]; got != MaskFailed {
		t.Fatalf("an unencodable map must fail closed, got %#v", event["properties"])
	}
	if got := server.partials(t)[0]["attachments"].([]any); len(got) != 0 {
		t.Fatalf("attachments = %#v", got)
	}
}

func TestNoMaskLeavesPayloadsUnchanged(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/", WithMask(nil))
	defer func() { _ = client.Close() }()
	if err := client.TrackAI(context.Background(), AIEvent{UserID: "u1", Input: "in", Properties: map[string]any{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	event := decodeBatch(t, server, "/events/track")[0]
	if event["ai_data"].(map[string]any)["input"] != "in" || event["properties"].(map[string]any)["k"] != "v" {
		t.Fatalf("event = %#v", event)
	}
}

func TestSpanContentAttributeKeys(t *testing.T) {
	for key, want := range map[string]bool{
		"traceloop.entity.input":                true,
		"traceloop.entity.output":               true,
		"gen_ai.tool.call.arguments":            true,
		"ai.prompt.system":                      true,
		"error.message":                         true,
		"ai.subagent.result":                    true,
		"traceloop.association.properties.note": true,
		"ai.telemetry.metadata.traceloop.association.properties.note": true,
		"ai.telemetry.metadata.raindrop.properties":                   true,
		"traceloop.association.properties.user_id":                    false,
		"traceloop.association.properties.raindrop.app.commit_sha":    false,
		"ai.telemetry.metadata.raindrop.eventId":                      false,
		"ai.telemetry.metadata.input":                                 false,
		"raindrop.app.commit_sha":                                     false,
		"traceloop.entity.name":                                       false,
		"gen_ai.request.model":                                        false,
		"traceloop.entity.duration_ms":                                false,
		"raindrop.handoff.childEventId":                               false,
		"input":                                                       true,
	} {
		if got := isSpanContentAttributeKey(key); got != want {
			t.Errorf("isSpanContentAttributeKey(%q) = %v, want %v", key, got, want)
		}
	}
}
