package raindrop

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestTrackAISerializesFeatureFlags proves the public FeatureFlags surface
// serializes to a top-level `feature_flags` string→string object on the wire —
// the ratified shape (dawn ingest TrackEventSchema / raindrop-js core
// event-shipper). Multibyte values must round-trip to the wire unmangled.
func TestTrackAISerializesFeatureFlags(t *testing.T) {
	var rawBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/track_partial" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_flags",
		UserID:  "flags-u1",
		Event:   "chat",
		Input:   "How do I enable reasoning?",
		Output:  "Toggle it in Settings.",
		Model:   "mock-gpt",
		FeatureFlags: map[string]string{
			"prompt-version": "v2",
			"locale-label":   "café-日本語",
		},
	})
	if err != nil {
		t.Fatalf("track ai: %v", err)
	}

	// Decode into a shape-agnostic map so the assertion is on the actual wire
	// JSON, not the SDK's own payload struct.
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &wire); err != nil {
		t.Fatalf("unmarshal wire body: %v", err)
	}
	rawFlags, ok := wire["feature_flags"]
	if !ok {
		t.Fatalf("feature_flags key absent from wire body: %s", rawBody)
	}
	var flags map[string]string
	if err := json.Unmarshal(rawFlags, &flags); err != nil {
		t.Fatalf("feature_flags is not a string→string object: %v (%s)", err, rawFlags)
	}
	if flags["prompt-version"] != "v2" {
		t.Fatalf("prompt-version: got %q, want v2", flags["prompt-version"])
	}
	if flags["locale-label"] != "café-日本語" {
		t.Fatalf("locale-label: got %q, want café-日本語", flags["locale-label"])
	}
}

// TestOmittedFeatureFlagsLeaveBodyUnchanged proves the additive guarantee:
// existing callers that never set FeatureFlags produce a wire body with NO
// `feature_flags` key — byte-identical to the pre-change payload (the field is
// json:",omitempty" and nil is never populated).
func TestOmittedFeatureFlagsLeaveBodyUnchanged(t *testing.T) {
	var rawBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	if err := client.TrackAI(context.Background(), AIEvent{
		EventID: "evt_noflags",
		UserID:  "user-123",
		Event:   "ai_generation",
		Input:   "hello",
		Output:  "world",
		Model:   "gpt-4o",
	}); err != nil {
		t.Fatalf("track ai: %v", err)
	}

	if strings.Contains(string(rawBody), "feature_flags") {
		t.Fatalf("omitted flags leaked a feature_flags key onto the wire: %s", rawBody)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &wire); err != nil {
		t.Fatalf("unmarshal wire body: %v", err)
	}
	if _, ok := wire["feature_flags"]; ok {
		t.Fatalf("feature_flags key present despite omission: %s", rawBody)
	}
}

// TestInteractionFeatureFlagsMergeAcrossPartials proves flags set on the
// partial lifecycle (Begin/SetFeatureFlags) survive to the finalized flush and
// merge (later keys win) rather than replacing.
func TestInteractionFeatureFlagsMergeAcrossPartials(t *testing.T) {
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
		EventID: "evt_merge",
		UserID:  "user-123",
		Input:   "hi",
		FeatureFlags: map[string]string{
			"prompt-version": "v1",
			"cohort":         "beta",
		},
	})
	if err := interaction.SetFeatureFlags(map[string]string{"prompt-version": "v2"}); err != nil {
		t.Fatalf("set feature flags: %v", err)
	}
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if received.FeatureFlags["prompt-version"] != "v2" {
		t.Fatalf("later flag did not win: %#v", received.FeatureFlags)
	}
	if received.FeatureFlags["cohort"] != "beta" {
		t.Fatalf("earlier flag not retained on merge: %#v", received.FeatureFlags)
	}
	if received.IsPending {
		t.Fatalf("expected finalized event")
	}
}

// TestFeatureFlagsClonedFromCaller proves the SDK defensively copies the
// caller's map, so post-call mutation cannot alter buffered/serialized flags.
func TestFeatureFlagsClonedFromCaller(t *testing.T) {
	var received trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()

	flags := map[string]string{"prompt-version": "v2"}
	if err := client.TrackAI(context.Background(), AIEvent{
		EventID:      "evt_clone",
		UserID:       "user-123",
		FeatureFlags: flags,
	}); err != nil {
		t.Fatalf("track ai: %v", err)
	}
	flags["prompt-version"] = "mutated"

	if received.FeatureFlags["prompt-version"] != "v2" {
		t.Fatalf("caller mutation leaked into shipped flags: %#v", received.FeatureFlags)
	}
}
