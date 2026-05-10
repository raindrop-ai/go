package raindrop

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// silentClient builds a Client wired to the supplied option set with all
// auto-detect signals neutralized so each fan-out test starts from a clean
// "nothing configured" baseline.
func silentClient(t *testing.T, opts ...Option) *Client {
	t.Helper()
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	base := []Option{
		WithDisableLocalWorkshop(),
		WithDebug(false),
		WithPartialFlushInterval(0),
		WithTraceFlushInterval(0),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	client, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func TestFanOutNoKeyNoLocalNoHTTP(t *testing.T) {
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("expected no requests when neither key nor local URL set: %s", r.URL.Path)
	}))
	defer cloud.Close()

	client := silentClient(t, WithEndpoint(cloud.URL+"/"))
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify: %v", err)
	}
	if err := client.TrackSignal(context.Background(), Signal{EventID: "evt", Name: "thumbs_up"}); err != nil {
		t.Fatalf("track signal: %v", err)
	}
	if err := client.TrackAI(context.Background(), AIEvent{EventID: "evt", UserID: "user-123"}); err != nil {
		t.Fatalf("track ai: %v", err)
	}
}

func TestFanOutKeyOnlyShipsCloudOnly(t *testing.T) {
	var cloudCalls atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cloud.Close()

	client := silentClient(t,
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	if got := cloudCalls.Load(); got != 1 {
		t.Fatalf("expected 1 cloud POST, got %d", got)
	}
}

func TestFanOutKeyPlusLocalDualShips(t *testing.T) {
	var cloudCalls atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cloud.Close()

	var localPaths []string
	var localMu sync.Mutex
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localMu.Lock()
		localPaths = append(localPaths, r.URL.Path)
		localMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer local.Close()

	client := silentClient(t,
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopUrl(local.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	if got := cloudCalls.Load(); got != 1 {
		t.Fatalf("expected 1 cloud POST, got %d", got)
	}
	localMu.Lock()
	defer localMu.Unlock()
	if len(localPaths) != 1 || localPaths[0] != "/users/identify" {
		t.Fatalf("expected 1 local POST to /users/identify, got %#v", localPaths)
	}
}

func TestFanOutLocalOnlyWhenNoKey(t *testing.T) {
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("expected no cloud POST when no write key: %s", r.URL.Path)
	}))
	defer cloud.Close()

	var localPaths []string
	var localMu sync.Mutex
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localMu.Lock()
		localPaths = append(localPaths, r.URL.Path)
		localMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer local.Close()

	client := silentClient(t,
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopUrl(local.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify: %v", err)
	}
	if err := client.TrackSignal(context.Background(), Signal{EventID: "evt", Name: "thumbs_up"}); err != nil {
		t.Fatalf("track signal: %v", err)
	}

	localMu.Lock()
	defer localMu.Unlock()
	if len(localPaths) != 2 {
		t.Fatalf("expected 2 local POSTs, got %#v", localPaths)
	}
	wantOrder := map[string]bool{"/users/identify": false, "/signals/track": false}
	for _, p := range localPaths {
		if _, ok := wantOrder[p]; !ok {
			t.Fatalf("unexpected local path: %s", p)
		}
		wantOrder[p] = true
	}
	for path, seen := range wantOrder {
		if !seen {
			t.Fatalf("missing local POST for %s: %#v", path, localPaths)
		}
	}
}

func TestFanOutLocalFailureDoesNotBreakCloud(t *testing.T) {
	var cloudCalls atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cloud.Close()

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	local.Close() // close immediately so connections are refused

	client := silentClient(t,
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopUrl(local.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify with broken local mirror should not bubble: %v", err)
	}
	if got := cloudCalls.Load(); got != 1 {
		t.Fatalf("expected cloud POST to still succeed, got %d", got)
	}
}

func TestFanOutLocalReturns500DoesNotBreakCloud(t *testing.T) {
	var cloudCalls atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cloud.Close()

	var localCalls atomic.Int32
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localCalls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer local.Close()

	client := silentClient(t,
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopUrl(local.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123"}); err != nil {
		t.Fatalf("identify with failing local mirror should not bubble: %v", err)
	}
	if got := cloudCalls.Load(); got != 1 {
		t.Fatalf("expected 1 cloud POST, got %d", got)
	}
	if got := localCalls.Load(); got != 1 {
		t.Fatalf("expected 1 local POST attempt, got %d", got)
	}
}

func TestFanOutTracesAlsoMirror(t *testing.T) {
	var cloudCalls atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traces" {
			cloudCalls.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cloud.Close()

	var localTraceCalls atomic.Int32
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traces" {
			localTraceCalls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer local.Close()

	client := silentClient(t,
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopUrl(local.URL+"/"),
	)
	defer func() { _ = client.Close() }()

	span := client.StartSpan(context.Background(), SpanOptions{Name: "llm_call", EventID: "evt_trace"})
	span.End()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := cloudCalls.Load(); got != 1 {
		t.Fatalf("expected 1 cloud /traces POST, got %d", got)
	}
	if got := localTraceCalls.Load(); got != 1 {
		t.Fatalf("expected 1 local /traces POST, got %d", got)
	}
}

func TestNewLocalOnlyClientIsEnabled(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	client, err := New(
		WithLocalWorkshopUrl("http://workshop.local:5899/v1/"),
		WithDebug(false),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if !client.Enabled() {
		t.Fatalf("expected local-only client to be enabled")
	}
	if client.transport.localBaseURL != "http://workshop.local:5899/v1/" {
		t.Fatalf("expected local URL on transport, got %q", client.transport.localBaseURL)
	}
	if client.transport.baseURL != DefaultEndpoint {
		t.Fatalf("expected cloud baseURL to remain default, got %q", client.transport.baseURL)
	}
}

func TestNewLocalOnlyFromEnvIsEnabled(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://workshop.env:5899/v1/")
	t.Setenv(WorkshopEnvVar, "")

	client, err := New(
		WithDebug(false),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if !client.Enabled() {
		t.Fatalf("expected env-driven local-only client to be enabled")
	}
	if client.transport.localBaseURL != "http://workshop.env:5899/v1/" {
		t.Fatalf("expected env URL on transport, got %q", client.transport.localBaseURL)
	}
}

func TestExplicitWithEndpointPreservedAlongsideLocal(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://from-env:9999/v1/")
	t.Setenv(WorkshopEnvVar, "")

	client, err := New(
		WithWriteKey("rk_test"),
		WithEndpoint("http://explicit.cloud:8888/v1/"),
		WithDebug(false),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if got := client.transport.baseURL; got != "http://explicit.cloud:8888/v1/" {
		t.Fatalf("expected explicit endpoint to win, got %q", got)
	}
	if got := client.transport.localBaseURL; got != "http://from-env:9999/v1/" {
		t.Fatalf("expected env URL to populate local mirror, got %q", got)
	}
}

func TestEventBufferGoroutineRunsForLocalOnlyClient(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	var localCalls atomic.Int32
	done := make(chan struct{})
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events/track_partial" {
			if localCalls.Add(1) == 1 {
				close(done)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer local.Close()

	client, err := New(
		WithLocalWorkshopUrl(local.URL+"/"),
		WithPartialFlushInterval(10*time.Millisecond),
		WithDebug(false),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_local_only",
		UserID:  "user-123",
		Input:   "hello",
	})
	if interaction == nil {
		t.Fatalf("expected interaction")
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected periodic flush to drive local POST")
	}
}
