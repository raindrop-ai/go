package raindrop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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
