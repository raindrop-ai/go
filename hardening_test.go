package raindrop

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hungHandler blocks every request until the client gives up (context
// cancellation / deadline) or the test ends, simulating a dead-slow API.
// The body is drained first so the server can detect client disconnects
// (background read) and the request context fires on abort.
func hungHandler(release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
}

func TestCloseBoundedWithHungServer(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hungHandler(release)(w, r)
	}))
	defer server.Close()
	// Registered after server.Close so it runs FIRST: hung handlers must be
	// released before server.Close waits on them.
	defer close(release)

	client := newTestClient(t, server.URL+"/",
		WithCloseTimeout(250*time.Millisecond),
		WithPartialFlushInterval(time.Hour),
		WithTraceFlushInterval(time.Hour),
	)

	// Buffer a pending event and a span without triggering an inline flush.
	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_hung_close",
		UserID:  "user-123",
		Input:   "hello",
	})
	if interaction == nil {
		t.Fatalf("expected interaction")
	}
	span := client.StartSpan(context.Background(), SpanOptions{Name: "hung_span", EventID: "evt_hung_close"})
	span.End()

	start := time.Now()
	err := client.Close()
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Fatalf("Close took %s against a hung server; expected the %s deadline to bound it", elapsed, 250*time.Millisecond)
	}
	if err == nil {
		t.Fatalf("expected Close to report the aborted flush")
	}
	if hits.Load() == 0 {
		t.Fatalf("expected Close to attempt the final flush")
	}
}

func TestCloseBoundedWhenTickerFlushInFlight(t *testing.T) {
	requestStarted := make(chan struct{}, 4)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		hungHandler(release)(w, r)
	}))
	defer server.Close()
	defer close(release)

	client := newTestClient(t, server.URL+"/",
		WithCloseTimeout(250*time.Millisecond),
		WithPartialFlushInterval(20*time.Millisecond),
		WithTraceFlushInterval(time.Hour),
	)

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_inflight",
		UserID:  "user-123",
		Input:   "hello",
	})
	if interaction == nil {
		t.Fatalf("expected interaction")
	}

	// Wait until the background ticker flush is parked inside the hung POST.
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("ticker flush never reached the server")
	}

	start := time.Now()
	_ = client.Close()
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Fatalf("Close took %s while a ticker flush was in flight; runCtx cancellation should abort it", elapsed)
	}
}

func TestRetryAfterDelayClamped(t *testing.T) {
	transport := &retryingHTTPClient{
		baseDelay:      time.Second,
		jitterFraction: 0,
	}

	tests := []struct {
		name       string
		previous   error
		want       time.Duration
		wantAtMost time.Duration
	}{
		{
			name:     "small retry-after honored",
			previous: &httpStatusError{RetryAfter: 5 * time.Second},
			want:     5 * time.Second,
		},
		{
			name:     "huge retry-after clamped",
			previous: &httpStatusError{RetryAfter: 24 * time.Hour},
			want:     maxRetryAfterDelay,
		},
		{
			name:     "no retry-after falls back to backoff",
			previous: &httpStatusError{},
			want:     time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transport.retryDelay(1, tt.previous); got != tt.want {
				t.Fatalf("retryDelay = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPerAttemptTimeoutBoundsClientWithoutTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(hungHandler(release))
	defer server.Close()
	defer close(release)

	// The zero-value http.Client has NO timeout: without the per-attempt
	// deadline this request would hang forever.
	client := newTestClient(t, server.URL+"/", WithHTTPClient(&http.Client{}))
	client.transport.attemptTimeout = 100 * time.Millisecond
	client.transport.maxAttempts = 1
	defer func() { _ = client.Close() }()

	start := time.Now()
	err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_no_timeout",
		UserID:  "user-123",
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected the bounded attempt to fail against a hung server")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("request took %s with a timeout-less client; expected the per-attempt deadline to bound it", elapsed)
	}
}

func TestPerAttemptTimeoutFailuresAreRetried(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			hungHandler(release)(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	defer close(release)

	client := newTestClient(t, server.URL+"/", WithHTTPClient(&http.Client{}))
	client.transport.attemptTimeout = 50 * time.Millisecond
	client.transport.sleep = func(context.Context, time.Duration) error { return nil }
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_retry_after_timeout",
		UserID:  "user-123",
	}); err != nil {
		t.Fatalf("expected attempt timeouts to stay retryable: %v", err)
	}
	if hits.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", hits.Load())
	}
}

func TestSpanEndDoesNotBlockOnBatchThresholdFlush(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/traces" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		hungHandler(release)(w, r)
	}))
	defer server.Close()
	defer releaseServer()

	client := newTestClient(t, server.URL+"/",
		WithTraceBatchSize(2),
		WithTraceFlushInterval(time.Hour),
		WithCloseTimeout(250*time.Millisecond),
	)

	first := client.StartSpan(context.Background(), SpanOptions{Name: "first"})
	first.End()

	// The second End crosses the batch threshold. It must hand the flush to
	// the background goroutine instead of POSTing (with retries) inline on
	// the caller — the host's hot path.
	second := client.StartSpan(context.Background(), SpanOptions{Name: "second"})
	start := time.Now()
	second.End()
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("Span.End blocked the caller for %s on a threshold flush against a hung server", elapsed)
	}

	releaseServer()
	_ = client.Close()
}

func TestLocalMirrorRespectsContextDeadline(t *testing.T) {
	t.Run("expired context skips the mirror entirely", func(t *testing.T) {
		var hits atomic.Int32
		mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer mirror.Close()

		client := newTestClient(t, "http://localhost:1/")
		defer func() { _ = client.Close() }()
		client.transport.localBaseURL = mirror.URL + "/"
		client.transport.localClient = &http.Client{Timeout: localMirrorTimeout}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		client.transport.postLocalMirror(ctx, "events/track_partial", []byte(`{}`))

		if hits.Load() != 0 {
			t.Fatalf("expected no mirror POST after the deadline, got %d", hits.Load())
		}
	})

	t.Run("remaining deadline bounds a slow mirror", func(t *testing.T) {
		release := make(chan struct{})
		mirror := httptest.NewServer(hungHandler(release))
		defer mirror.Close()
		defer close(release)

		client := newTestClient(t, "http://localhost:1/")
		defer func() { _ = client.Close() }()
		client.transport.localBaseURL = mirror.URL + "/"
		client.transport.localClient = &http.Client{Timeout: localMirrorTimeout}

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		start := time.Now()
		client.transport.postLocalMirror(ctx, "events/track_partial", []byte(`{}`))
		elapsed := time.Since(start)

		if elapsed >= localMirrorTimeout {
			t.Fatalf("mirror POST ran %s; expected the context deadline to clamp the 2s client timeout", elapsed)
		}
	})

	t.Run("cancelled caller context does not abort a healthy mirror", func(t *testing.T) {
		var hits atomic.Int32
		mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer mirror.Close()

		client := newTestClient(t, "http://localhost:1/")
		defer func() { _ = client.Close() }()
		client.transport.localBaseURL = mirror.URL + "/"
		client.transport.localClient = &http.Client{Timeout: localMirrorTimeout}

		// Deadline in the future, but cancellation about to happen: the
		// mirror send is detached from cancellation once it starts.
		client.transport.postLocalMirror(context.Background(), "events/track_partial", []byte(`{}`))
		if hits.Load() != 1 {
			t.Fatalf("expected 1 mirror POST, got %d", hits.Load())
		}
	})
}

func TestWithCloseTimeoutValidation(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{"positive applied", 3 * time.Second, 3 * time.Second},
		{"zero ignored", 0, defaultCloseTimeout},
		{"negative ignored", -time.Second, defaultCloseTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, "http://localhost:1/", WithCloseTimeout(tt.timeout))
			defer func() { _ = client.Close() }()
			if client.closeTimeout != tt.want {
				t.Fatalf("closeTimeout = %s, want %s", client.closeTimeout, tt.want)
			}
		})
	}
}
