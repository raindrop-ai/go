package raindrop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type traceAttempt struct {
	name   string
	body   string
	at     time.Duration
	status int
}

func (a traceAttempt) String() string { return fmt.Sprintf("%s@%s=%d", a.name, a.at, a.status) }

// traceIngest records every /traces attempt and answers with status(at).
type traceIngest struct {
	mu       sync.Mutex
	attempts []traceAttempt
}

func (s *traceIngest) handler(now func() time.Duration, status func(at time.Duration, name string) int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req exportTraceServiceRequest
		_ = json.Unmarshal(body, &req)
		at, name := now(), req.ResourceSpans[0].ScopeSpans[0].Spans[0].Name
		code := status(at, name)
		s.mu.Lock()
		s.attempts = append(s.attempts, traceAttempt{name, string(body), at, code})
		s.mu.Unlock()
		if code == http.StatusServiceUnavailable && at == 0 {
			w.Header().Set("Retry-After", "7")
		}
		w.WriteHeader(code)
	}
}

func (s *traceIngest) delivered(want int) []traceAttempt {
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := 0
		for _, a := range s.attempts {
			if a.status < 300 {
				got++
			}
		}
		out := append([]traceAttempt(nil), s.attempts...)
		s.mu.Unlock()
		if got >= want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRejectedTraceBatchDoesNotBlockLaterBatches(t *testing.T) {
	ingest := &traceIngest{}
	server := httptest.NewServer(ingest.handler(func() time.Duration { return 0 }, func(_ time.Duration, name string) int {
		if name == "rejected" {
			return http.StatusBadRequest
		}
		return http.StatusNoContent
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithTraceBatchSize(1), WithTraceFlushInterval(10*time.Millisecond))
	defer func() { _ = client.Close() }()
	client.StartSpan(context.Background(), SpanOptions{Name: "rejected"}).End()
	client.StartSpan(context.Background(), SpanOptions{Name: "later"}).End()

	ingest.delivered(1)
	time.Sleep(50 * time.Millisecond) // a few more ticks: the 400 must not be resent
	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	if len(ingest.attempts) != 2 || ingest.attempts[0].name != "rejected" || ingest.attempts[1].name != "later" {
		t.Fatalf("want one attempt of the rejected batch, then the later batch delivered; got %+v", ingest.attempts)
	}
}

func TestTraceOutageThenRecoveryDeliversEveryBatchOnce(t *testing.T) {
	const outage = 240 * time.Second
	var mu sync.Mutex
	var elapsed time.Duration
	var delays []time.Duration
	now := func() time.Duration { mu.Lock(); defer mu.Unlock(); return elapsed }

	ingest := &traceIngest{}
	server := httptest.NewServer(ingest.handler(now, func(at time.Duration, _ string) int {
		if at < outage {
			return http.StatusServiceUnavailable
		}
		return http.StatusNoContent
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithTraceBatchSize(1), WithTraceFlushInterval(time.Hour))
	defer func() { _ = client.Close() }()
	start := time.Now()
	client.traces.now = func() time.Time { return start.Add(now()) }
	queued := make(chan struct{})
	client.transport.sleep = func(ctx context.Context, d time.Duration) error {
		<-queued // later batches are queued before the head's first backoff ends
		mu.Lock()
		defer mu.Unlock()
		delays = append(delays, d)
		elapsed += d
		return ctx.Err()
	}

	for _, name := range []string{"a", "b", "c"} {
		client.StartSpan(context.Background(), SpanOptions{Name: name}).End()
	}
	close(queued)
	attempts := ingest.delivered(3)

	perName := map[string][]traceAttempt{}
	for _, a := range attempts {
		perName[a.name] = append(perName[a.name], a)
	}
	head := perName["a"]
	for _, name := range []string{"a", "b", "c"} {
		got := perName[name]
		if len(got) == 0 || got[len(got)-1].status >= 300 {
			t.Fatalf("batch %s not delivered: %+v", name, got)
		}
		if name != "a" && len(got) != 1 {
			t.Fatalf("batch %s sent %d times; only the head batch should probe during the outage", name, len(got))
		}
	}
	for _, a := range head {
		if a.body != head[0].body {
			t.Fatalf("retry body differs from the first attempt")
		}
	}
	if last := head[len(head)-1].at; last < outage || last > outage+maxTraceRetryDelay || len(head) > 20 {
		t.Fatalf("head batch: %d attempts, delivered at %s: %v", len(head), last, head)
	}

	mu.Lock()
	defer mu.Unlock()
	if delays[0] != 7*time.Second {
		t.Fatalf("first delay %s, want Retry-After 7s", delays[0])
	}
	for i, d := range delays[1:] {
		ceiling := min(maxTraceRetryDelay, time.Second<<(i+1))
		if d < ceiling/2 || d > ceiling {
			t.Fatalf("delay %d = %s, want within [%s, %s]", i+2, d, ceiling/2, ceiling)
		}
	}
}

func TestFlushReportsRejectedTraceBatchAndDeliversLaterOnes(t *testing.T) {
	ingest := &traceIngest{}
	server := httptest.NewServer(ingest.handler(func() time.Duration { return 0 }, func(_ time.Duration, name string) int {
		if name == "rejected" {
			return http.StatusBadRequest
		}
		return http.StatusNoContent
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithTraceBatchSize(10), WithTraceFlushInterval(time.Hour))
	defer func() { _ = client.Close() }()
	client.StartSpan(context.Background(), SpanOptions{Name: "rejected"}).End()
	client.StartSpan(context.Background(), SpanOptions{Name: "later"}).End()
	client.traces.mu.Lock()
	client.traces.maxBatchSize = 1 // two batches, without kicking the background flusher
	client.traces.mu.Unlock()

	err := client.Flush(context.Background())
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("Flush error = %v, want the 400 of the dropped batch", err)
	}
	if got := ingest.delivered(1); len(got) != 2 || got[1].name != "later" || got[1].status != http.StatusNoContent {
		t.Fatalf("want the later batch delivered after the rejected one; got %+v", got)
	}
}

func TestCloseResendsBatchCancelledNearRetryBudget(t *testing.T) {
	var shift atomic.Int64
	var calls atomic.Int32
	var delivered atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // lets the server notice the client hanging up
		if calls.Add(1) == 1 {
			shift.Store(int64(traceRetryBudget - 100*time.Millisecond))
			<-r.Context().Done() // in flight until Close cancels it
			return
		}
		delivered.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithTraceBatchSize(1), WithTraceFlushInterval(time.Hour))
	client.traces.now = func() time.Time { return time.Now().Add(time.Duration(shift.Load())) }
	client.StartSpan(context.Background(), SpanOptions{Name: "a"}).End()
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	_ = client.Close()
	if delivered.Load() != 1 {
		t.Fatalf("batch cancelled by Close was not re-sent by Close's final flush (%d deliveries)", delivered.Load())
	}
}

func TestDropWarningsAreRateLimited(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	var logs safeBuffer
	client := newTestClient(t, server.URL+"/", WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	defer func() { _ = client.Close() }()
	for i := 0; i < 20; i++ {
		_ = client.Begin(context.Background(), BeginOptions{Event: "e", UserID: "u"}).Finish(FinishOptions{Output: "x"})
		client.StartSpan(context.Background(), SpanOptions{Name: "s"}).End()
		_ = client.Flush(context.Background())
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("got %d drop warnings for 40 rejected payloads, want 1:\n%s", n, logs.String())
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
