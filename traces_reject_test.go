package raindrop

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

func TestRejectedTraceBatchIsDroppedAndLaterSpansAreDelivered(t *testing.T) {
	var mu sync.Mutex
	var attempts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req exportTraceServiceRequest
		_ = json.Unmarshal(body, &req)
		name := req.ResourceSpans[0].ScopeSpans[0].Spans[0].Name
		mu.Lock()
		attempts = append(attempts, name)
		mu.Unlock()
		if name == "rejected" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()
	buffer := newTraceBuffer(client, 0, 1, 100) // one span per batch, no background flusher
	buffer.queue = []otlpSpan{{Name: "rejected"}, {Name: "later"}}

	err := buffer.Flush(context.Background())
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("Flush() error = %v, want the 400 of the rejected batch", err)
	}
	if err := buffer.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush() error = %v, want nil (rejected batch must not be resent)", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"rejected", "later"}; !reflect.DeepEqual(attempts, want) {
		t.Fatalf("trace attempts = %v, want %v (rejected batch sent once, later span delivered)", attempts, want)
	}
}
