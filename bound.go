package raindrop

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"unicode/utf8"
)

// Payload bounds.
//
// Text fields (event input/output, event property and attachment values,
// tool span I/O, stringified properties) are capped BEFORE serialization so
// the cost of an oversized payload on the calling goroutine is proportional
// to the cap, not the payload: raw strings are length-checked in O(1) and
// structured values are pruned by a budgeted walk before json.Marshal ever
// sees them. Without this, a multi-MB tool
// result is fully marshaled inline on the caller's hot path and then
// rejected at the ingest size limit anyway.
const (
	defaultMaxTextFieldChars = 1_000_000
	truncationMarker         = "...[truncated by raindrop]"
	boundedWalkMaxDepth      = 12
)

// textFieldLimit returns the per-field byte cap for this client, nil-safe.
func (c *Client) textFieldLimit() int {
	if c == nil || c.maxTextFieldChars <= 0 {
		return defaultMaxTextFieldChars
	}
	return c.maxTextFieldChars
}

// capText bounds s to limit bytes. The result, truncation marker included,
// never exceeds limit; when limit is too small to fit the marker the string
// is hard-sliced without it. The length check is O(1) and the slice copies
// at most limit bytes, so multi-MB strings cost O(limit) here. Cuts back off
// to a UTF-8 rune boundary so the result is never mid-rune-invalid.
func capText(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	cut := limit
	if limit > len(truncationMarker) {
		cut = limit - len(truncationMarker)
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	if limit > len(truncationMarker) {
		return s[:cut] + truncationMarker
	}
	return s[:cut]
}

// stringifyValue renders an arbitrary user value as a string no longer than
// limit bytes, paying serialization cost proportional to the limit for
// string/map/slice shaped payloads: the value is pruned by boundedClone
// before json.Marshal runs, so a multi-MB tool result never gets fully
// encoded on the caller's goroutine. Values that carry their own marshaling
// (json.Marshaler, encoding.TextMarshaler) and plain structs pass through
// the pruning walk untouched — for those the final capText still bounds the
// OUTPUT, though not the encode cost.
func stringifyValue(value any, limit int) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return capText(typed, limit)
	case []byte:
		if len(typed) > limit {
			// Convert only a prefix (O(limit)); len > limit forces the marker.
			return capText(string(typed[:limit+1]), limit)
		}
		return string(typed)
	default:
		// Slack covers JSON syntax overhead (quotes, braces, escapes) so
		// payloads near the limit don't get pruned twice.
		budget := limit + len(truncationMarker) + 256
		clone := boundedClone(value, &budget, 0)
		encoded, err := json.Marshal(clone)
		if err == nil {
			return capText(string(encoded), limit)
		}
		return capText(fmt.Sprint(clone), limit)
	}
}

// capValue bounds a single event property value to roughly limit bytes while
// keeping its structured JSON shape: string leaves are capped with the
// marker and containers are pruned by the same budgeted walk used for tool
// span content. Values with custom marshaling pass through the walk
// untouched (their JSON form is preserved), so the bound is best-effort for
// those.
func capValue(value any, limit int) any {
	if value == nil || limit <= 0 {
		return value
	}
	if text, ok := value.(string); ok {
		return capText(text, limit)
	}
	// Same slack as stringifyValue: JSON syntax overhead should not cause
	// near-limit values to be pruned twice.
	budget := limit + len(truncationMarker) + 256
	return boundedClone(value, &budget, 0)
}

// capProperties clones an event property map with every value bounded by
// capValue. Nil stays nil so the patch merge logic keeps treating "no
// properties" and "empty properties" differently.
func capProperties(properties map[string]any, limit int) map[string]any {
	if properties == nil {
		return nil
	}
	out := make(map[string]any, len(properties))
	for key, value := range properties {
		out[key] = capValue(value, limit)
	}
	return out
}

// capAttachments clones attachments with each Value bounded to limit bytes.
// Attachment values carry the bulk (code, transcripts); the remaining fields
// are short metadata and pass through unchanged.
func capAttachments(attachments []Attachment, limit int) []Attachment {
	out := cloneAttachments(attachments)
	for i := range out {
		out[i].Value = capText(out[i].Value, limit)
	}
	return out
}

// boundedClone shallow-prunes a payload to roughly *budget bytes of data.
// String leaves are capped, and the walk stops descending once the budget is
// consumed, so the clone — and therefore its encoding — is O(budget)
// regardless of payload shape. Every visited node charges a little budget,
// bounding the walk itself on huge collections of small values. Types with
// custom marshaling pass through untouched so their JSON form is preserved.
func boundedClone(v any, budget *int, depth int) any {
	if *budget <= 0 {
		return truncationMarker
	}
	switch typed := v.(type) {
	case nil:
		*budget -= 8
		return nil
	case string:
		if len(typed) > *budget {
			out := capText(typed, *budget)
			*budget = 0
			return out
		}
		*budget -= max(len(typed), 1)
		return typed
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64, json.Number:
		*budget -= 8
		return typed
	case []byte:
		if len(typed) > *budget {
			out := typed[:*budget]
			*budget = 0
			return out
		}
		*budget -= max(len(typed), 1)
		return typed
	}

	if depth >= boundedWalkMaxDepth {
		*budget -= 16
		return fmt.Sprintf("<max depth: %T>", v)
	}

	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			if *budget <= 0 {
				out["..."] = truncationMarker
				break
			}
			// Cap the key BEFORE walking the value so a budget-draining
			// value can't corrupt its own key.
			cappedKey := key
			if len(key) > *budget {
				cappedKey = capText(key, *budget)
				*budget = 0
			} else {
				*budget -= max(len(key), 1)
			}
			out[cappedKey] = boundedClone(value, budget, depth+1)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, value := range typed {
			if *budget <= 0 {
				out = append(out, truncationMarker)
				break
			}
			out = append(out, boundedClone(value, budget, depth+1))
		}
		return out
	}

	// Custom marshaling must be preserved verbatim (time.Time,
	// json.RawMessage, ...): pass through and charge a token so unbounded
	// sequences of such values still terminate the walk.
	if _, ok := v.(json.Marshaler); ok {
		*budget -= 16
		return v
	}
	if _, ok := v.(encoding.TextMarshaler); ok {
		*budget -= 16
		return v
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			*budget -= 8
			return v
		}
		return boundedClone(rv.Elem().Interface(), budget, depth+1)
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			if *budget <= 0 {
				out["..."] = truncationMarker
				break
			}
			key, _ := boundedClone(fmt.Sprint(iter.Key().Interface()), budget, depth+1).(string)
			out[key] = boundedClone(iter.Value().Interface(), budget, depth+1)
		}
		return out
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			// Named byte slices marshal as base64 strings; keep that shape.
			return boundedClone(rv.Bytes(), budget, depth)
		}
		length := rv.Len()
		out := make([]any, 0, length)
		for i := 0; i < length; i++ {
			if *budget <= 0 {
				out = append(out, truncationMarker)
				break
			}
			out = append(out, boundedClone(rv.Index(i).Interface(), budget, depth+1))
		}
		return out
	}

	// Structs and exotic kinds: leave for encoding/json (tags, embedding,
	// omitempty are its business). Charge a token so huge collections of
	// them still terminate; the post-marshal capText bounds the output.
	*budget -= 16
	return v
}
