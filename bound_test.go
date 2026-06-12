package raindrop

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestCapText(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		limit      int
		want       string
		wantMarker bool
	}{
		{
			name:  "short string untouched",
			input: "hello",
			limit: 100,
			want:  "hello",
		},
		{
			name:  "exactly at limit untouched",
			input: strings.Repeat("x", 100),
			limit: 100,
			want:  strings.Repeat("x", 100),
		},
		{
			name:       "long string capped with marker within limit",
			input:      strings.Repeat("x", 10_000),
			limit:      100,
			wantMarker: true,
		},
		{
			name:  "limit smaller than marker hard slices without marker",
			input: strings.Repeat("x", 100),
			limit: 10,
			want:  strings.Repeat("x", 10),
		},
		{
			name:  "non-positive limit is a no-op",
			input: "hello",
			limit: 0,
			want:  "hello",
		},
		{
			name:       "multibyte runes never split mid-rune",
			input:      strings.Repeat("é", 200), // 2 bytes per rune
			limit:      101,                      // cut would land mid-rune without backoff
			wantMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := capText(tt.input, tt.limit)
			if tt.limit > 0 && len(got) > tt.limit {
				t.Fatalf("result exceeds limit: len=%d limit=%d", len(got), tt.limit)
			}
			if tt.want != "" && got != tt.want {
				t.Fatalf("unexpected result: %q", got)
			}
			if tt.wantMarker {
				if !strings.HasSuffix(got, truncationMarker) {
					t.Fatalf("missing truncation marker, tail=%q", got[max(0, len(got)-40):])
				}
				if !strings.HasPrefix(tt.input, got[:len(got)-len(truncationMarker)]) {
					t.Fatalf("capped prefix does not match input")
				}
			}
			if !utf8.ValidString(got) {
				t.Fatalf("result is not valid UTF-8: %q", got)
			}
		})
	}
}

func TestStringifyValueSmallPayloadsMatchMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"flat map", map[string]any{"q": "hello", "n": 3, "ok": true, "none": nil}},
		{"nested", map[string]any{"args": []any{map[string]any{"q": "coffee", "f": 1.5}}}},
		{"marshaler passes through", map[string]any{"at": time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
		{"struct passes through", struct {
			Name string `json:"name"`
			N    int    `json:"n"`
		}{Name: "x", N: 7}},
		{"typed map", map[string]int{"a": 1}},
		{"typed slice", []int{1, 2, 3}},
		{"raw message preserved", map[string]any{"raw": json.RawMessage(`{"k":1}`)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, err := json.Marshal(tt.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := stringifyValue(tt.value, defaultMaxTextFieldChars)
			if got != string(want) {
				t.Fatalf("bounded stringify diverged for small payload:\n got %s\nwant %s", got, want)
			}
		})
	}
}

func TestStringifyValueBoundsHugePayloads(t *testing.T) {
	rows := make([]any, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		rows = append(rows, "order-timeline-entry "+strings.Repeat("z", 200))
	}

	tests := []struct {
		name  string
		value any
		limit int
	}{
		{"huge string leaf", map[string]any{"text": strings.Repeat("y", 10_000_000)}, 5_000},
		{"huge array of rows", map[string]any{"rows": rows}, 5_000}, // ~2.2 MB
		{"huge raw string", strings.Repeat("z", 5_000_000), 5_000},
		{"huge byte slice", []byte(strings.Repeat("b", 5_000_000)), 5_000},
		{"huge typed string slice", func() any {
			out := make([]string, 50_000)
			for i := range out {
				out[i] = strings.Repeat("s", 100)
			}
			return out
		}(), 5_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringifyValue(tt.value, tt.limit)
			if len(got) > tt.limit {
				t.Fatalf("output exceeds limit: len=%d limit=%d", len(got), tt.limit)
			}
			if !strings.Contains(got, truncationMarker) {
				t.Fatalf("missing truncation marker in bounded output, tail=%q", got[max(0, len(got)-60):])
			}
		})
	}
}

func TestStringifyValueCostProportionalToCap(t *testing.T) {
	// The worst-case failure mode: a multi-MB payload serialized inline on
	// the caller. The pruning walk must stop after ~limit bytes of work, so
	// even a 1M-entry map costs milliseconds, not a full-payload encode.
	huge := make(map[string]any, 1_000_000)
	for i := 0; i < 1_000_000; i++ {
		huge["key_"+strconv.Itoa(i)] = i
	}

	start := time.Now()
	got := stringifyValue(huge, 2_000)
	elapsed := time.Since(start)

	if len(got) > 2_000 {
		t.Fatalf("output exceeds limit: %d", len(got))
	}
	if elapsed > time.Second {
		t.Fatalf("bounded stringify walked the full payload: took %s for a 1M-entry map with a 2KB cap", elapsed)
	}
}

func TestStringifyValueScalarShortcuts(t *testing.T) {
	tests := []struct {
		name  string
		value any
		limit int
		want  string
	}{
		{"nil", nil, 100, ""},
		{"short string verbatim", "hello", 100, "hello"},
		{"short bytes verbatim", []byte("hi"), 100, "hi"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringifyValue(tt.value, tt.limit); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBoundedCloneDepthCapped(t *testing.T) {
	deep := map[string]any{}
	leaf := deep
	for i := 0; i < 2*boundedWalkMaxDepth; i++ {
		next := map[string]any{}
		leaf["child"] = next
		leaf = next
	}
	leaf["v"] = "end"

	got := stringifyValue(deep, defaultMaxTextFieldChars)
	// json.Marshal HTML-escapes "<" to \u003c; match on the inner text.
	if !strings.Contains(got, "max depth:") {
		t.Fatalf("expected depth cap placeholder in %s", got)
	}
}

func TestEventTextFieldsCappedBeforeSend(t *testing.T) {
	const limit = 120
	big := strings.Repeat("x", 50_000)

	tests := []struct {
		name string
		send func(t *testing.T, client *Client)
	}{
		{
			name: "TrackAI caps input and output",
			send: func(t *testing.T, client *Client) {
				if err := client.TrackAI(context.Background(), AIEvent{
					EventID: "evt_cap",
					UserID:  "user-123",
					Input:   big,
					Output:  big,
				}); err != nil {
					t.Fatalf("track ai: %v", err)
				}
			},
		},
		{
			name: "Begin/SetInput/Finish caps input and output",
			send: func(t *testing.T, client *Client) {
				interaction := client.Begin(context.Background(), BeginOptions{
					EventID: "evt_cap",
					UserID:  "user-123",
					Input:   big,
				})
				if err := interaction.SetInput(big); err != nil {
					t.Fatalf("set input: %v", err)
				}
				if err := interaction.Finish(FinishOptions{Output: big}); err != nil {
					t.Fatalf("finish: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var received trackPartialPayload
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if err := json.Unmarshal(body, &received); err != nil {
					t.Errorf("unmarshal payload: %v", err)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL+"/", WithMaxTextFieldChars(limit))
			defer func() { _ = client.Close() }()

			tt.send(t, client)

			if received.AIData == nil {
				t.Fatalf("missing ai_data: %#v", received)
			}
			for field, value := range map[string]string{
				"input":  received.AIData.Input,
				"output": received.AIData.Output,
			} {
				if len(value) > limit {
					t.Fatalf("%s not capped: len=%d", field, len(value))
				}
				if !strings.HasSuffix(value, truncationMarker) {
					t.Fatalf("%s missing truncation marker, tail=%q", field, value[max(0, len(value)-40):])
				}
			}
		})
	}
}

func TestEventPropertiesAndAttachmentsCappedBeforeSend(t *testing.T) {
	const limit = 120
	big := strings.Repeat("x", 50_000)
	bigAttachment := Attachment{Type: "code", Role: "input", Name: "dump.txt", Value: big}
	bigProperties := map[string]any{
		"transcript": big,
		"payload":    map[string]any{"rows": []any{big, big}},
		"small":      "ok",
	}

	tests := []struct {
		name string
		send func(t *testing.T, client *Client)
	}{
		{
			name: "TrackAI caps properties and attachments",
			send: func(t *testing.T, client *Client) {
				if err := client.TrackAI(context.Background(), AIEvent{
					EventID:     "evt_props_cap",
					UserID:      "user-123",
					Properties:  bigProperties,
					Attachments: []Attachment{bigAttachment},
				}); err != nil {
					t.Fatalf("track ai: %v", err)
				}
			},
		},
		{
			name: "SetProperty and AddAttachments cap on patch",
			send: func(t *testing.T, client *Client) {
				interaction := client.Begin(context.Background(), BeginOptions{
					EventID: "evt_props_cap",
					UserID:  "user-123",
					Input:   "hello",
				})
				for key, value := range bigProperties {
					if err := interaction.SetProperty(key, value); err != nil {
						t.Fatalf("set property: %v", err)
					}
				}
				if err := interaction.AddAttachments([]Attachment{bigAttachment}); err != nil {
					t.Fatalf("add attachments: %v", err)
				}
				if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
					t.Fatalf("finish: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var received trackPartialPayload
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if err := json.Unmarshal(body, &received); err != nil {
					t.Errorf("unmarshal payload: %v", err)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL+"/", WithMaxTextFieldChars(limit))
			defer func() { _ = client.Close() }()

			tt.send(t, client)

			transcript, _ := received.Properties["transcript"].(string)
			if len(transcript) > limit {
				t.Fatalf("transcript property not capped: len=%d", len(transcript))
			}
			if !strings.HasSuffix(transcript, truncationMarker) {
				t.Fatalf("transcript property missing truncation marker, tail=%q", transcript[max(0, len(transcript)-40):])
			}

			structured, err := json.Marshal(received.Properties["payload"])
			if err != nil {
				t.Fatalf("marshal structured property: %v", err)
			}
			// The pruned clone carries ~limit bytes of data plus pruning
			// slack and JSON syntax overhead; the original is ~100KB.
			if len(structured) > 2_000 {
				t.Fatalf("structured property not bounded: len=%d", len(structured))
			}
			if !strings.Contains(string(structured), truncationMarker) {
				t.Fatalf("structured property missing truncation marker: %s", structured)
			}

			if got := received.Properties["small"]; got != "ok" {
				t.Fatalf("small property changed: %#v", got)
			}

			if len(received.Attachments) != 1 {
				t.Fatalf("expected 1 attachment, got %d", len(received.Attachments))
			}
			attachment := received.Attachments[0]
			if len(attachment.Value) > limit {
				t.Fatalf("attachment value not capped: len=%d", len(attachment.Value))
			}
			if !strings.HasSuffix(attachment.Value, truncationMarker) {
				t.Fatalf("attachment value missing truncation marker, tail=%q", attachment.Value[max(0, len(attachment.Value)-40):])
			}
			if attachment.Type != "code" || attachment.Role != "input" || attachment.Name != "dump.txt" {
				t.Fatalf("attachment metadata changed: %#v", attachment)
			}
		})
	}
}

func TestToolSpanPayloadsCappedBeforeSend(t *testing.T) {
	const limit = 200
	bigText := strings.Repeat("p", 100_000)
	bigOutput := map[string]any{"rows": []any{strings.Repeat("r", 1_000_000)}}

	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traces" {
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &received); err != nil {
				t.Errorf("unmarshal traces payload: %v", err)
			}
		} else {
			_, _ = io.ReadAll(r.Body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithMaxTextFieldChars(limit))
	defer func() { _ = client.Close() }()

	interaction := client.Begin(context.Background(), BeginOptions{
		EventID: "evt_big_tool",
		UserID:  "user-123",
		Input:   "tool probe",
	})
	start := time.Now()
	interaction.TrackTool(TrackToolOptions{
		Name:       "fetch_order_timeline",
		Input:      map[string]any{"q": "probe", "noise": bigText},
		Output:     bigOutput,
		Properties: map[string]any{"transcript": bigText},
		Duration:   42 * time.Millisecond,
	})
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Fatalf("TrackTool spent %s on the caller for a ~1MB payload; expected cost proportional to the cap", elapsed)
	}
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	var toolSpan *otlpSpan
	for i := range spans {
		if spans[i].Name == "fetch_order_timeline" {
			toolSpan = &spans[i]
		}
	}
	if toolSpan == nil {
		t.Fatalf("missing tool span: %#v", spans)
	}

	checked := 0
	for _, attr := range toolSpan.Attributes {
		switch attr.Key {
		case "traceloop.entity.input", "traceloop.entity.output", "traceloop.association.properties.transcript":
			checked++
			if got := len(attr.Value.StringValue); got > limit {
				t.Fatalf("%s not capped: len=%d limit=%d", attr.Key, got, limit)
			}
			if !strings.Contains(attr.Value.StringValue, truncationMarker) {
				t.Fatalf("%s missing truncation marker: %q", attr.Key, attr.Value.StringValue)
			}
		}
	}
	if checked != 3 {
		t.Fatalf("expected input, output, and property attributes, checked %d", checked)
	}
}

func TestWithMaxTextFieldCharsValidation(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{"positive value applied", 123, 123},
		{"zero ignored", 0, defaultMaxTextFieldChars},
		{"negative ignored", -5, defaultMaxTextFieldChars},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, "http://localhost:1/", WithMaxTextFieldChars(tt.limit))
			defer func() { _ = client.Close() }()
			if got := client.textFieldLimit(); got != tt.want {
				t.Fatalf("textFieldLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}
