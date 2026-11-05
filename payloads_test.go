package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
)

// decodeEventPayloads reads an event request body in either shape: one
// object (events/track_partial) or an array of them (events/track batches).
func decodeEventPayloads(t *testing.T, body io.Reader) []trackPartialPayload {
	t.Helper()
	raw, err := io.ReadAll(body)
	if err != nil {
		t.Errorf("read payload: %v", err)
		return nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var batch []trackPartialPayload
		if err := json.Unmarshal(raw, &batch); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		return batch
	}
	var single trackPartialPayload
	if err := json.Unmarshal(raw, &single); err != nil {
		t.Errorf("decode payload: %v", err)
	}
	return []trackPartialPayload{single}
}

// flushOrFail delivers queued telemetry now: TrackEvent, TrackAI,
// TrackSignal and Identify only enqueue (CONTRACT 1.1).
func flushOrFail(t *testing.T, client *Client) {
	t.Helper()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}
