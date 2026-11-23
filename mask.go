package raindrop

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MaskKind names the payload a masked value comes from.
type MaskKind string

const (
	// MaskKindEvent is an event: fields "ai_data.input", "ai_data.output"
	// (strings), "properties" (map[string]any) and "attachments"
	// ([]Attachment).
	MaskKindEvent MaskKind = "event"
	// MaskKindSignal is a signal: field "properties" (map[string]any), with
	// the signal's Comment and After in it under "comment" and "after".
	MaskKindSignal MaskKind = "signal"
	// MaskKindIdentify is an Identify call: field "traits" (map[string]any).
	MaskKindIdentify MaskKind = "identify"
	// MaskKindSpan is a trace span: the field is a content attribute's name
	// (tool input and output, for example "traceloop.entity.input", and the
	// span's copies of the caller's properties) or "status.message"; the
	// value is a string.
	MaskKindSpan MaskKind = "span"
)

// MaskContext says where a masked value comes from.
type MaskContext struct {
	Kind  MaskKind
	Field string
}

// MaskFunc receives one field's value and returns the value to send in its
// place. Strings must come back as strings, properties and traits as
// map[string]any, and attachments as []Attachment. Maps and slices are
// copies the function may change.
//
// It runs synchronously, once per field, as each payload is built for
// sending. It never sees identity or routing fields (event, user and
// conversation ids, event and signal names, model, timestamps) or the SDK's
// own properties.
type MaskFunc func(value any, ctx MaskContext) any

// MaskFailed is sent in place of a string field the mask function failed on.
// A failed map field is sent as {"error": MaskFailed}, failed attachments as
// an empty list.
const MaskFailed = "<MASK FAILED>"

// WithMask installs a masking function for content the built-in options
// can't predict (names, medical terms, internal ids). It covers event input,
// output, properties and attachments, signal properties (with the comment
// and after text), identify traits, and span content attributes and status
// messages, including every request of a Begin/Patch/Finish event.
//
// It fails closed: if fn panics or returns the wrong type, the field is sent
// as MaskFailed (see there) and the payload still ships. The warning names
// only the panic's or value's type, never the content. The mask never
// causes a panic or an error in the caller. A nil fn turns masking off.
func WithMask(fn MaskFunc) Option {
	return func(cfg *config) error {
		cfg.mask = fn
		return nil
	}
}

// maskString passes one string field through the mask function.
func (c *Client) maskString(kind MaskKind, field, value string) string {
	if c == nil || c.mask == nil || value == "" {
		return value
	}
	ctx := MaskContext{Kind: kind, Field: field}
	result, reason := c.callMask(value, ctx)
	if reason == "" {
		if s, ok := result.(string); ok {
			return s
		}
		reason = fmt.Sprintf("it returned %T, not a string", result)
	}
	c.warnMaskFailed(ctx, reason)
	return MaskFailed
}

// maskMap passes one object field (properties, traits) through the mask
// function. An empty map is not passed.
func (c *Client) maskMap(kind MaskKind, field string, value map[string]any) map[string]any {
	if c == nil || c.mask == nil || len(value) == 0 {
		return value
	}
	ctx := MaskContext{Kind: kind, Field: field}
	result, reason := c.callMask(deepCopyMap(value), ctx)
	if reason == "" {
		if m, ok := result.(map[string]any); ok {
			if _, err := json.Marshal(m); err == nil {
				out := deepCopyMap(m)
				if out == nil {
					out = map[string]any{}
				}
				return out
			}
			reason = "it returned a map that cannot be encoded as JSON"
		} else {
			reason = fmt.Sprintf("it returned %T, not a map[string]any", result)
		}
	}
	c.warnMaskFailed(ctx, reason)
	return map[string]any{"error": MaskFailed}
}

// maskAttachments passes an event's attachments through the mask function.
// An empty list is not passed.
func (c *Client) maskAttachments(value []Attachment) []Attachment {
	if c == nil || c.mask == nil || len(value) == 0 {
		return value
	}
	ctx := MaskContext{Kind: MaskKindEvent, Field: "attachments"}
	result, reason := c.callMask(cloneAttachments(value), ctx)
	if reason == "" {
		if list, ok := result.([]Attachment); ok {
			out := make([]Attachment, len(list))
			copy(out, list)
			return out
		}
		reason = fmt.Sprintf("it returned %T, not a []Attachment", result)
	}
	c.warnMaskFailed(ctx, reason)
	return []Attachment{}
}

// callMask runs the mask function, turning a panic into a failure reason
// that names only the panic value's type.
func (c *Client) callMask(value any, ctx MaskContext) (result any, reason string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			reason = fmt.Sprintf("it panicked (%T)", recovered)
		}
	}()
	return c.mask(value, ctx), ""
}

func (c *Client) warnMaskFailed(ctx MaskContext, reason string) {
	c.warn("mask.failed",
		"raindrop: mask function failed; sending "+MaskFailed+" in its place",
		"kind", string(ctx.Kind), "field", ctx.Field, "reason", reason)
}

// maskSpan passes a finished span's content attributes and status message
// through the mask function, one string at a time.
func (c *Client) maskSpan(attributes []otlpKeyValue, status *otlpStatus) ([]otlpKeyValue, *otlpStatus) {
	if c == nil || c.mask == nil {
		return attributes, status
	}
	for i := range attributes {
		if !isSpanContentAttributeKey(attributes[i].Key) {
			continue
		}
		attributes[i].Value = c.maskSpanValue(attributes[i].Key, attributes[i].Value)
	}
	if status != nil && status.Message != "" {
		masked := *status
		masked.Message = c.maskString(MaskKindSpan, "status.message", status.Message)
		status = &masked
	}
	return attributes, status
}

func (c *Client) maskSpanValue(key string, value otlpAnyValue) otlpAnyValue {
	if value.StringValue != "" {
		value.StringValue = c.maskString(MaskKindSpan, key, value.StringValue)
		return value
	}
	if value.ArrayValue != nil {
		values := make([]otlpAnyValue, len(value.ArrayValue.Values))
		for i, item := range value.ArrayValue.Values {
			values[i] = c.maskSpanValue(key, item)
		}
		value.ArrayValue = &otlpArrayValue{Values: values}
	}
	return value
}

// Span content attributes (the conformance harness's span content key
// rule): copies of the caller's event properties, else anything outside the
// identifier prefixes that is a known content key or whose last segment
// names content.
var (
	spanContentSuffixes = map[string]struct{}{
		"input": {}, "output": {}, "inputs": {}, "outputs": {}, "args": {}, "arguments": {},
		"result": {}, "results": {}, "content": {}, "text": {}, "messages": {}, "prompt": {},
		"completion": {}, "instructions": {}, "system_instructions": {}, "reasoning": {},
		"reasoningText": {}, "thinking": {}, "toolCalls": {}, "tool_calls": {}, "object": {},
		"value": {}, "values": {}, "intent": {}, "query": {}, "tool_input": {},
	}
	spanContentKeys = map[string]struct{}{
		"ai.prompt.system": {}, "ai.subagent.result": {}, "agent.session.title": {},
		"error.message": {}, "exception.message": {}, "exception.stacktrace": {},
	}
	spanNonContentPrefixes = []string{
		"traceloop.association.properties.",
		"ai.telemetry.metadata.",
		"ai.settings.context.",
		"raindrop.app.",
	}
	spanPropertyCopyPrefixes = []string{
		"ai.telemetry.metadata.traceloop.association.properties.",
		"traceloop.association.properties.",
	}
	spanOwnPropertyNames = map[string]struct{}{
		"user_id": {}, "event_id": {}, "convo_id": {}, "event_name": {},
	}
)

func isSpanContentAttributeKey(key string) bool {
	if key == "ai.telemetry.metadata.raindrop.properties" {
		return true
	}
	for _, prefix := range spanPropertyCopyPrefixes {
		if name, ok := strings.CutPrefix(key, prefix); ok && name != "" {
			if _, own := spanOwnPropertyNames[name]; !own && !strings.HasPrefix(name, "raindrop.") {
				return true
			}
			break
		}
	}
	for _, prefix := range spanNonContentPrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	if _, ok := spanContentKeys[key]; ok {
		return true
	}
	last := key
	if i := strings.LastIndexByte(key, '.'); i >= 0 {
		last = key[i+1:]
	}
	_, ok := spanContentSuffixes[last]
	return ok
}

// deepCopyMap copies nested maps and slices, so the mask function can change
// its value without touching the caller's data. Other values are shared.
func deepCopyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = deepCopyValue(value)
	}
	return dst
}

func deepCopyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return deepCopyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = deepCopyValue(item)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(typed))
		for i, item := range typed {
			out[i] = deepCopyMap(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}
