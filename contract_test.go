package raindrop

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingServer captures every request body by route.
type recordingServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies map[string][][]byte
}

func newRecordingServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request) bool) *recordingServer {
	t.Helper()
	rs := &recordingServer{bodies: make(map[string][][]byte)}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rs.mu.Lock()
		rs.bodies[r.URL.Path] = append(rs.bodies[r.URL.Path], body)
		rs.mu.Unlock()
		if respond != nil && respond(w, r) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *recordingServer) partials(t *testing.T) []map[string]any {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]map[string]any, 0, len(rs.bodies["/events/track_partial"]))
	for _, body := range rs.bodies["/events/track_partial"] {
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode partial: %v", err)
		}
		out = append(out, payload)
	}
	return out
}

func (rs *recordingServer) count(route string) int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.bodies[route])
}

func warnLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), &buf
}

func TestPartialRequestsCarryTheEventStart(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_start", UserID: "user", Input: "hi"})
	flushOrFail(t, client)
	time.Sleep(5 * time.Millisecond)
	if err := interaction.SetProperty("stage", "retrieval"); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	if err := interaction.Finish(FinishOptions{Output: "hello"}); err != nil {
		t.Fatal(err)
	}

	partials := server.partials(t)
	if len(partials) != 3 {
		t.Fatalf("got %d partial requests, want 3", len(partials))
	}
	first := partials[0]["timestamp"]
	for i, payload := range partials {
		if payload["timestamp"] != first {
			t.Fatalf("request %d timestamp %v, want the begin time %v (CONTRACT 9.1)", i, payload["timestamp"], first)
		}
	}
	if _, err := time.Parse(time.RFC3339, first.(string)); err != nil {
		t.Fatalf("timestamp %v: %v", first, err)
	}
}

func TestCallerTimestampIsSentUnchangedAndOnlyMovesEarlier(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	start := time.Date(2026, 10, 1, 14, 0, 0, 0, time.UTC)
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_ts", UserID: "user", Input: "hi", Timestamp: start})
	if err := interaction.Patch(PatchOptions{Timestamp: start.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)
	if err := interaction.Patch(PatchOptions{Timestamp: start.Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}

	partials := server.partials(t)
	if got := partials[0]["timestamp"]; got != "2026-10-01T14:00:00.000Z" {
		t.Fatalf("first timestamp = %v, want the caller's start unchanged", got)
	}
	if got := partials[len(partials)-1]["timestamp"]; got != "2026-10-01T13:00:00.000Z" {
		t.Fatalf("last timestamp = %v, want the earlier start", got)
	}
}

func TestResumedEventSendsOnlyWhatItIsGiven(t *testing.T) {
	server := newRecordingServer(t, nil)
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithLogger(logger))
	defer func() { _ = client.Close() }()

	interaction := client.Resume("evt_from_elsewhere", ResumeOptions{})
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	partials := server.partials(t)
	if len(partials) != 1 {
		t.Fatalf("got %d partial requests, want 1 (a resumed update must not be dropped)", len(partials))
	}
	payload := partials[0]
	for _, field := range []string{"user_id", "event", "timestamp"} {
		if _, present := payload[field]; present {
			t.Fatalf("resumed update carried %q: %v (CONTRACT 9.2)", field, payload)
		}
	}
	if payload["is_pending"] != false {
		t.Fatalf("is_pending = %v, want false", payload["is_pending"])
	}
	if !strings.Contains(logs.String(), "resumed event without a user id") {
		t.Fatalf("expected a warning for the resumed update without a user, got %q", logs.String())
	}
	if strings.Contains(logs.String(), "no input, output or attachments") {
		t.Fatalf("a resumed event is exempt from the empty-finish warning, got %q", logs.String())
	}
}

func TestResumeOptionsIdentityIsSent(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Resume("evt_resume_identity", ResumeOptions{UserID: "user", ConvoID: "convo", Event: "chat"})
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}
	payload := server.partials(t)[0]
	if payload["user_id"] != "user" || payload["event"] != "chat" {
		t.Fatalf("resume identity not sent: %v", payload)
	}
	if ai, _ := payload["ai_data"].(map[string]any); ai["convo_id"] != "convo" {
		t.Fatalf("resume convo not sent: %v", payload)
	}
}

func TestEventsWithoutAUserAreNotSentAndWarn(t *testing.T) {
	server := newRecordingServer(t, nil)
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithLogger(logger))
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{Event: "chat", Input: "hi"}); err != nil {
		t.Fatal(err)
	}
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_anon", Event: "chat", Input: "hi"})
	_ = interaction.Finish(FinishOptions{Output: "hello"})
	flushOrFail(t, client)

	if server.count("/events/track") != 0 || server.count("/events/track_partial") != 0 {
		t.Fatalf("an event without a user id was sent (CONTRACT 10.1)")
	}
	if !strings.Contains(logs.String(), "no user id") {
		t.Fatalf("expected a missing-user warning, got %q", logs.String())
	}
}

func TestTrackCallsBatchIntoOneRequest(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	for i := 0; i < 5; i++ {
		if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"}); err != nil {
			t.Fatal(err)
		}
	}
	if server.count("/events/track") != 0 {
		t.Fatalf("TrackEvent sent on the caller (CONTRACT 1.1)")
	}
	flushOrFail(t, client)
	if got := server.count("/events/track"); got != 1 {
		t.Fatalf("got %d events/track requests, want 1 batch (CONTRACT 7.2)", got)
	}
	var batch []trackEventPayload
	server.mu.Lock()
	body := server.bodies["/events/track"][0]
	server.mu.Unlock()
	if err := json.Unmarshal(body, &batch); err != nil || len(batch) != 5 {
		t.Fatalf("batch = %d events (%v), want 5", len(batch), err)
	}
}

func TestFlushWaitsForABackgroundSendInFlight(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return false
	})
	client := newTestClient(t, server.URL+"/", WithPartialFlushInterval(10*time.Millisecond))
	defer func() { _ = client.Close() }()

	if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("background send never started")
	}
	done := make(chan error, 1)
	go func() { done <- client.Flush(context.Background()) }()
	select {
	case <-done:
		t.Fatal("Flush returned while a background send was in flight (CONTRACT 1.2)")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush never returned")
	}
}

func TestRetryRules(t *testing.T) {
	t.Run("408 is retried", func(t *testing.T) {
		var attempts atomic.Int32
		server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusRequestTimeout)
				return true
			}
			return false
		})
		client := newTestClient(t, server.URL+"/")
		client.transport.sleep = func(context.Context, time.Duration) error { return nil }
		defer func() { _ = client.Close() }()
		_ = client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"})
		flushOrFail(t, client)
		if attempts.Load() != 2 {
			t.Fatalf("attempts = %d, want 2", attempts.Load())
		}
	})

	t.Run("permanent 4xx is dropped with a warning", func(t *testing.T) {
		for _, status := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusRequestEntityTooLarge} {
			var attempts atomic.Int32
			server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				attempts.Add(1)
				w.WriteHeader(status)
				return true
			})
			logger, logs := warnLogger()
			client := newTestClient(t, server.URL+"/", WithLogger(logger))
			client.transport.sleep = func(context.Context, time.Duration) error { return nil }
			_ = client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"})
			if err := client.Flush(context.Background()); err != nil {
				t.Fatalf("HTTP %d: a permanent rejection is a warned drop, not a Flush error: %v", status, err)
			}
			if attempts.Load() != 1 {
				t.Fatalf("HTTP %d retried: %d attempts", status, attempts.Load())
			}
			if !strings.Contains(logs.String(), "permanent") {
				t.Fatalf("HTTP %d: expected a drop warning, got %q", status, logs.String())
			}
			_ = client.Close()
		}
	})

	t.Run("503 with Retry-After pauses other sends", func(t *testing.T) {
		var attempts atomic.Int32
		server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			if attempts.Add(1) == 1 {
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		})
		client := newTestClient(t, server.URL+"/")
		var slept []time.Duration
		var mu sync.Mutex
		client.transport.sleep = func(_ context.Context, d time.Duration) error {
			mu.Lock()
			slept = append(slept, d)
			mu.Unlock()
			return nil
		}
		defer func() { _ = client.Close() }()
		_ = client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"})
		flushOrFail(t, client)
		mu.Lock()
		defer mu.Unlock()
		if len(slept) == 0 || slept[0] < 1500*time.Millisecond {
			t.Fatalf("waits %v: the retry must wait for Retry-After", slept)
		}
		if !client.transport.pausedUntil.After(time.Now()) {
			t.Fatalf("a 503 with Retry-After must pause every send to the server (CONTRACT 15.2)")
		}
	})
}

func TestIngestRejectedItemsWarn(t *testing.T) {
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[],"rejected":[{"index":0,"event_id":"e1","issues":[{"path":"user_id","message":"Required"}]}]}`))
		return true
	})
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithLogger(logger))
	defer func() { _ = client.Close() }()

	_ = client.TrackEvent(context.Background(), Event{UserID: "user", Event: "click"})
	flushOrFail(t, client)
	if !strings.Contains(logs.String(), "event e1 (user_id: Required)") {
		t.Fatalf("expected the rejected item in a warning, got %q", logs.String())
	}
}

func TestFinishWarnings(t *testing.T) {
	server := newRecordingServer(t, nil)
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithLogger(logger))
	defer func() { _ = client.Close() }()

	empty := client.Begin(context.Background(), BeginOptions{EventID: "evt_empty", UserID: "user"})
	if err := empty.Finish(FinishOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "no input, output or attachments") {
		t.Fatalf("expected the empty-finish warning, got %q", logs.String())
	}
	if server.count("/events/track_partial") != 1 {
		t.Fatalf("an empty finish is still sent (CONTRACT 14.2)")
	}

	twice := client.Begin(context.Background(), BeginOptions{EventID: "evt_twice", UserID: "user", Input: "hi"})
	_ = twice.Finish(FinishOptions{Output: "one"})
	_ = client.Finish(context.Background(), "evt_twice", FinishOptions{Output: "two"})
	if !strings.Contains(logs.String(), "already finished") {
		t.Fatalf("expected the repeated-finish warning, got %q", logs.String())
	}
}

func TestTrackToolUnderASavedParent(t *testing.T) {
	var received exportTraceServiceRequest
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		return false
	})
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{EventID: "evt_tool", UserID: "user", Input: "hi"})
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	interaction.TrackTool(TrackToolOptions{
		Name:      "web_search",
		StartTime: start,
		Duration:  1500 * time.Millisecond,
		ParentContext: &SpanContext{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:  "00f067aa0ba902b7",
		},
	})
	flushOrFail(t, client)

	server.mu.Lock()
	body := server.bodies["/traces"][0]
	server.mu.Unlock()
	if err := json.Unmarshal(body, &received); err != nil {
		t.Fatal(err)
	}
	span := received.ResourceSpans[0].ScopeSpans[0].Spans[0]
	traceID, _ := base64.StdEncoding.DecodeString(span.TraceID)
	parentID, _ := base64.StdEncoding.DecodeString(span.ParentSpanID)
	if hex.EncodeToString(traceID) != "4bf92f3577b34da6a3ce929d0e0e4736" || hex.EncodeToString(parentID) != "00f067aa0ba902b7" {
		t.Fatalf("span not under the saved parent: trace %x parent %x", traceID, parentID)
	}
	if span.StartTimeUnixNano != "1767225600000000000" || span.EndTimeUnixNano != "1767225601500000000" {
		t.Fatalf("span times %s..%s, want start plus duration exactly", span.StartTimeUnixNano, span.EndTimeUnixNano)
	}
}

func TestSubagentRoundTrip(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	launcher := client.Begin(context.Background(), BeginOptions{EventID: "evt_launcher", UserID: "user", Input: "research X"})
	dispatch := launcher.Subagent(SubagentOptions{Name: "researcher", ChildEventID: "evt_child", Input: "find sources"})
	if dispatch.ChildEventID != "evt_child" || dispatch.Headers[HandoffHeader] == "" || dispatch.Headers["traceparent"] == "" {
		t.Fatalf("incomplete carrier: %#v", dispatch)
	}
	_ = launcher.Finish(FinishOptions{Output: "dispatched"})

	run := client.ResumeSubagent(context.Background(), dispatch.Headers, ResumeSubagentOptions{})
	run.Interaction.TrackTool(TrackToolOptions{Name: "web_search", Duration: time.Millisecond})
	if err := run.Finish(FinishOptions{Output: "found 3"}); err != nil {
		t.Fatal(err)
	}
	flushOrFail(t, client)

	var child map[string]any
	for _, payload := range server.partials(t) {
		if payload["event_id"] == "evt_child" {
			child = payload
		}
	}
	if child == nil || child["user_id"] != "user" {
		t.Fatalf("child event missing or without the carrier's user: %v", child)
	}

	attrs := map[string]map[string]string{}
	server.mu.Lock()
	for _, body := range server.bodies["/traces"] {
		var request exportTraceServiceRequest
		_ = json.Unmarshal(body, &request)
		for _, rs := range request.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					values := map[string]string{}
					for _, kv := range span.Attributes {
						values[kv.Key] = kv.Value.StringValue
					}
					attrs[span.Name] = values
				}
			}
		}
	}
	server.mu.Unlock()
	dispatchSpan := attrs[defaultDispatchToolName]
	if dispatchSpan[handoffModeAttribute] != "detached" || dispatchSpan[handoffChildEventIDAttribute] != "evt_child" {
		t.Fatalf("dispatch span lacks the hand-off attributes: %v", dispatchSpan)
	}
	childSpan := attrs["web_search"]
	if childSpan[handoffParentEventIDAttribute] != "evt_launcher" || childSpan[handoffParentSpanIDAttribute] == "" {
		t.Fatalf("child span lacks the reverse link: %v", childSpan)
	}
}

func TestResumeSubagentWithoutACarrierRunsUnlinked(t *testing.T) {
	server := newRecordingServer(t, nil)
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	run := client.ResumeSubagent(context.Background(), map[string]string{"x-raindrop-handoff": "garbage"}, ResumeSubagentOptions{UserID: "user"})
	if err := run.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}
	if server.count("/events/track_partial") != 1 {
		t.Fatalf("a child with an unreadable carrier must still report its own event")
	}
}

// -- CONTRACT 1.2.1: one deadline, what happens to payloads when it passes --

func TestOneSettingGovernsFlushAndClose(t *testing.T) {
	for name, opt := range map[string]Option{
		"WithFlushTimeout": WithFlushTimeout(250 * time.Millisecond),
		"WithCloseTimeout": WithCloseTimeout(250 * time.Millisecond),
	} {
		client := newTestClient(t, "http://localhost:1/", opt)
		if got := client.deadline(); got != 250*time.Millisecond {
			t.Errorf("%s: deadline = %s, want 250ms", name, got)
		}
		_ = client.Close()
	}
	client := newTestClient(t, "http://localhost:1/")
	defer func() { _ = client.Close() }()
	if got := client.deadline(); got != defaultFlushTimeout {
		t.Errorf("default deadline = %s, want %s", got, defaultFlushTimeout)
	}
}

func TestFlushDeadlineKeepsUnsentPayloadsQueued(t *testing.T) {
	var answered atomic.Int32
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if answered.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithFlushTimeout(300*time.Millisecond), WithLogger(logger))
	defer func() { _ = client.Close() }()

	if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "kept"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err := client.Flush(context.Background())
	if !errors.Is(err, ErrUndelivered) {
		t.Fatalf("Flush() error = %v, want ErrUndelivered", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Flush took %s, want about its 300ms deadline", elapsed)
	}
	if !strings.Contains(logs.String(), "flush returned before all telemetry was delivered") {
		t.Fatalf("no warning logged: %s", logs.String())
	}

	// The event waited out the Retry-After in the queue, not in the bin.
	time.Sleep(1100 * time.Millisecond)
	flushOrFail(t, client)
	server.mu.Lock()
	bodies := server.bodies["/events/track"]
	server.mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatalf("events/track bodies = %d, want the same batch twice (a 503, then delivered)", len(bodies))
	}
}

func TestFlushDeadlineDropsACutOffRequest(t *testing.T) {
	release := make(chan struct{})
	var answered atomic.Int32
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if answered.Add(1) == 1 {
			<-release // hold the first request past the deadline
		}
		return false
	})
	t.Cleanup(func() { close(release) })
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithFlushTimeout(300*time.Millisecond), WithLogger(logger))
	defer func() { _ = client.Close() }()

	if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "cut-off"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrUndelivered) {
		t.Fatalf("Flush() error = %v, want ErrUndelivered", err)
	}
	if !strings.Contains(logs.String(), "dropping") {
		t.Fatalf("no drop warning: %s", logs.String())
	}
	// It may already have reached ingest, so it is not sent again.
	flushOrFail(t, client)
	if got := server.count("/events/track"); got != 1 {
		t.Fatalf("events/track requests = %d, want 1 (a cut-off request is not resent)", got)
	}
}

func TestCloseDropsWhatIsLeftAtTheDeadline(t *testing.T) {
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	})
	logger, logs := warnLogger()
	client := newTestClient(t, server.URL+"/", WithFlushTimeout(300*time.Millisecond), WithLogger(logger))

	if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "left"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := client.Close(); !errors.Is(err, ErrUndelivered) {
		t.Fatalf("Close() error = %v, want ErrUndelivered", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Close took %s, want about its 300ms deadline", elapsed)
	}
	if !strings.Contains(logs.String(), "dropping") {
		t.Fatalf("no drop warning: %s", logs.String())
	}
	client.track.mu.Lock()
	got := len(client.track.items)
	client.track.mu.Unlock()
	if got != 0 {
		t.Fatalf("%d items left after Close, want 0 (dropped)", got)
	}
}

func TestPermanentRejectionIsNotAFlushFailure(t *testing.T) {
	server := newRecordingServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusBadRequest)
		return true
	})
	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()
	if err := client.TrackEvent(context.Background(), Event{UserID: "user", Event: "bad"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v, want nil (delivered and rejected)", err)
	}
}
