package raindrop

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type recordedRequest struct {
	path       string
	projectID  string
	hasProject bool
}

// newProjectIDRecordingServer records the request path and the
// X-Raindrop-Project-Id header (presence + value) for every inbound request.
func newProjectIDRecordingServer(t *testing.T) (*httptest.Server, func() []recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	var records []recordedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		values := r.Header.Values(projectIDHeader)
		rec := recordedRequest{path: r.URL.Path, hasProject: len(values) > 0}
		if rec.hasProject {
			rec.projectID = values[0]
		}
		mu.Lock()
		records = append(records, rec)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	snapshot := func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		out := make([]recordedRequest, len(records))
		copy(out, records)
		return out
	}
	return server, snapshot
}

// exerciseEveryRequestSite drives each outbound request path the SDK can emit:
// events/track_partial, signals/track, users/identify, and the OTLP /traces
// export. This is the coverage that proves the project-id header is attached
// everywhere, not just on one endpoint.
func exerciseEveryRequestSite(t *testing.T, client *Client) {
	t.Helper()
	ctx := context.Background()

	if err := client.TrackAI(ctx, AIEvent{
		EventID: "evt_project",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "hello",
		Output:  "hi",
	}); err != nil {
		t.Fatalf("track ai: %v", err)
	}
	if err := client.TrackSignal(ctx, Signal{EventID: "evt_project", Name: "thumbs_up", Type: "feedback"}); err != nil {
		t.Fatalf("track signal: %v", err)
	}
	if err := client.Identify(ctx, User{UserID: "user-123", Traits: map[string]any{"plan": "paid"}}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	span := client.StartSpan(ctx, SpanOptions{Name: "llm_call", EventID: "evt_project"})
	span.SetAttributes(StringAttr("ai.model.id", "gpt-4o"))
	span.End()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestNormalizeProjectID(t *testing.T) {
	maxLenSlug := strings.Repeat("a", 63) // 63 chars is the longest valid slug

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "default", "default"},
		{"hyphenated", "my-project", "my-project"},
		{"single-char", "a", "a"},
		{"alphanumeric", "abc123", "abc123"},
		{"interior-hyphens", "a-b-c", "a-b-c"},
		{"single-digit", "0", "0"},
		{"letter-digit", "z9", "z9"},
		{"max-length-63", maxLenSlug, maxLenSlug},
		{"trims-surrounding-whitespace", "  my-project  ", "my-project"},
		{"empty", "", ""},
		{"whitespace-only", "   ", ""},
		{"leading-hyphen", "-leading", ""},
		{"trailing-hyphen", "trailing-", ""},
		{"uppercase", "UPPER", ""},
		{"underscore", "with_underscore", ""},
		{"space", "with space", ""},
		{"unicode", "ünicode", ""},
		{"too-long-64", strings.Repeat("a", 64), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeProjectID(tc.in, nil); got != tc.want {
				t.Fatalf("normalizeProjectID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestProjectIDHeaderAttachedToEveryRequestWhenValid(t *testing.T) {
	server, snapshot := newProjectIDRecordingServer(t)
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithProjectID("my-project"))
	defer func() { _ = client.Close() }()

	exerciseEveryRequestSite(t, client)

	records := snapshot()
	wantPaths := map[string]bool{
		"/events/track_partial": false,
		"/signals/track":        false,
		"/users/identify":       false,
		"/traces":               false,
	}
	for _, rec := range records {
		if _, ok := wantPaths[rec.path]; ok {
			wantPaths[rec.path] = true
		}
		if !rec.hasProject || rec.projectID != "my-project" {
			t.Fatalf("request to %s missing project header: %#v", rec.path, rec)
		}
	}
	for path, seen := range wantPaths {
		if !seen {
			t.Fatalf("expected a request to %s, got %#v", path, records)
		}
	}
}

func TestProjectIDHeaderOmittedWhenUnset(t *testing.T) {
	server, snapshot := newProjectIDRecordingServer(t)
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	exerciseEveryRequestSite(t, client)

	records := snapshot()
	if len(records) == 0 {
		t.Fatalf("expected at least one request")
	}
	for _, rec := range records {
		if rec.hasProject {
			t.Fatalf("request to %s unexpectedly carried project header %q", rec.path, rec.projectID)
		}
	}
}

func TestProjectIDHeaderTrimsSurroundingWhitespace(t *testing.T) {
	server, snapshot := newProjectIDRecordingServer(t)
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithProjectID("  trimmed-project  "))
	defer func() { _ = client.Close() }()

	if err := client.TrackSignal(context.Background(), Signal{EventID: "evt", Name: "thumbs_up"}); err != nil {
		t.Fatalf("track signal: %v", err)
	}

	records := snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 request, got %d", len(records))
	}
	if !records[0].hasProject || records[0].projectID != "trimmed-project" {
		t.Fatalf("expected trimmed project header, got %#v", records[0])
	}
}

func TestProjectIDHeaderOmittedAndWarnedWhenInvalid(t *testing.T) {
	server, snapshot := newProjectIDRecordingServer(t)
	defer server.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	client := newTestClient(t, server.URL+"/",
		WithProjectID("Bad Slug"),
		WithLogger(logger),
	)
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123"}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	records := snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 request, got %d", len(records))
	}
	if records[0].hasProject {
		t.Fatalf("invalid project_id must omit the header, got %#v", records[0])
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "invalid project_id") {
		t.Fatalf("expected warning about invalid project_id, got %q", logged)
	}
	if !strings.Contains(logged, "Bad Slug") {
		t.Fatalf("expected warning to include the rejected value, got %q", logged)
	}
}

func TestProjectIDHeaderAttachedToLocalMirror(t *testing.T) {
	cloud, cloudSnapshot := newProjectIDRecordingServer(t)
	defer cloud.Close()
	local, localSnapshot := newProjectIDRecordingServer(t)
	defer local.Close()

	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	client, err := New(
		WithWriteKey("rk_test"),
		WithEndpoint(cloud.URL+"/"),
		WithLocalWorkshopURL(local.URL+"/"),
		WithProjectID("mirror-project"),
		WithDebug(false),
		WithPartialFlushInterval(0),
		WithTraceFlushInterval(0),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Identify(context.Background(), User{UserID: "user-123"}); err != nil {
		t.Fatalf("identify: %v", err)
	}

	for _, tc := range []struct {
		name     string
		snapshot func() []recordedRequest
	}{
		{"cloud", cloudSnapshot},
		{"local mirror", localSnapshot},
	} {
		records := tc.snapshot()
		if len(records) != 1 {
			t.Fatalf("%s: expected 1 request, got %d", tc.name, len(records))
		}
		if !records[0].hasProject || records[0].projectID != "mirror-project" {
			t.Fatalf("%s: expected project header, got %#v", tc.name, records[0])
		}
	}
}
