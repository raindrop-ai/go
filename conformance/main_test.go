package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	raindrop "github.com/raindrop-ai/go"
)

func TestAppGitArgAcceptsPublicVocabulary(t *testing.T) {
	for _, value := range []any{
		false,
		map[string]any{},
		map[string]any{
			"commit_sha":       "0123456789abcdef0123456789abcdef01234567",
			"commit_dirty":     true,
			"branch":           "main",
			"source_directory": "/app",
			"detect_branch":    true,
			"auto_detect":      false,
		},
	} {
		if option, err := appGitArg(map[string]any{"app_git": value}); err != nil || option == nil {
			t.Fatalf("app_git %#v: option=%v err=%v", value, option != nil, err)
		}
	}
}

func TestAppGitArgRejectsUnknownAndInvalidOptions(t *testing.T) {
	for _, value := range []any{
		true,
		"false",
		map[string]any{"unknown": true},
		map[string]any{"commit_dirty": "true"},
		map[string]any{"commit_sha": false},
	} {
		if _, err := appGitArg(map[string]any{"app_git": value}); err == nil {
			t.Fatalf("app_git %#v unexpectedly accepted", value)
		}
	}
}

func TestDriverAppGitUsesPublicSDKOption(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	var payload struct {
		Properties map[string]any `json:"properties"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events/track" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		var batch []json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch) != 1 {
			t.Errorf("decode payload: %v (%d items)", err, len(batch))
		} else if err := json.Unmarshal(batch[0], &payload); err != nil {
			t.Errorf("decode event: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	t.Setenv("RAINDROP_WRITE_KEY", "rk_test")
	t.Setenv("RAINDROP_SINK_URL", server.URL)

	d := &driver{}
	if err := d.stepInit(map[string]any{"app_git": map[string]any{
		"commit_sha":   commit,
		"commit_dirty": true,
		"branch":       "driver-branch",
	}}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer func() { _ = d.client.Close() }()
	if err := d.stepTrack(context.Background(), map[string]any{
		"event_id": "driver-event",
		"user_id":  "driver-user",
	}); err != nil {
		t.Fatalf("track: %v", err)
	}
	if err := d.client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := payload.Properties["raindrop.app.commit_sha"]; got != commit {
		t.Fatalf("commit = %#v", got)
	}
	if got := payload.Properties["raindrop.app.commit_dirty"]; got != true {
		t.Fatalf("dirty = %#v", got)
	}
	if got := payload.Properties["raindrop.app.branch"]; got != "driver-branch" {
		t.Fatalf("branch = %#v", got)
	}
}

func TestTestMasksMatchTheHarnessTable(t *testing.T) {
	ctx := raindrop.MaskContext{Kind: raindrop.MaskKindSignal, Field: "properties"}
	tag, err := testMask("tag")
	if err != nil {
		t.Fatal(err)
	}
	if got := tag("x", raindrop.MaskContext{Kind: raindrop.MaskKindEvent, Field: "ai_data.input"}); got != "<MASKED event ai_data.input>" {
		t.Fatalf("tag string = %#v", got)
	}
	if got, ok := tag(map[string]any{"a": 1}, ctx).(map[string]any); !ok || len(got) != 1 || got["masked"] != "signal properties" {
		t.Fatalf("tag object = %#v", got)
	}
	if got, ok := tag([]raindrop.Attachment{{Value: "x"}}, ctx).([]raindrop.Attachment); !ok || len(got) != 0 {
		t.Fatalf("tag attachments = %#v", got)
	}

	suffix, err := testMask("suffix")
	if err != nil {
		t.Fatal(err)
	}
	if got := suffix("hi", ctx); got != "hi"+maskSuffix {
		t.Fatalf("suffix string = %#v", got)
	}
	object := map[string]any{"a": "b"}
	if got, ok := suffix(object, ctx).(map[string]any); !ok || got["a"] != "b" {
		t.Fatalf("suffix object = %#v", got)
	}

	throw, err := testMask("throw")
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("throw mask did not panic")
			}
		}()
		throw("x", ctx)
	}()

	for _, bad := range []any{"", "TAG", true, nil} {
		if _, err := testMask(bad); err == nil {
			t.Fatalf("mask %#v unexpectedly accepted", bad)
		}
	}
}
