package raindrop

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

const (
	SpanStatusUnset = 0
	SpanStatusOK    = 1
	SpanStatusError = 2
)

type Attribute struct {
	Key   string
	Value otlpAnyValue
}

type otlpAnyValue struct {
	StringValue string          `json:"stringValue,omitempty"`
	IntValue    string          `json:"intValue,omitempty"`
	DoubleValue *float64        `json:"doubleValue,omitempty"`
	BoolValue   *bool           `json:"boolValue,omitempty"`
	ArrayValue  *otlpArrayValue `json:"arrayValue,omitempty"`
}

type otlpArrayValue struct {
	Values []otlpAnyValue `json:"values"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

type otlpSpan struct {
	TraceID           string         `json:"traceId"`
	SpanID            string         `json:"spanId"`
	ParentSpanID      string         `json:"parentSpanId,omitempty"`
	Name              string         `json:"name"`
	StartTimeUnixNano string         `json:"startTimeUnixNano"`
	EndTimeUnixNano   string         `json:"endTimeUnixNano"`
	Attributes        []otlpKeyValue `json:"attributes,omitempty"`
	Status            *otlpStatus    `json:"status,omitempty"`
}

type exportTraceServiceRequest struct {
	ResourceSpans []resourceSpans `json:"resourceSpans"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type scopeSpans struct {
	Scope scope      `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type scope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type spanIDs struct {
	TraceIDB64      string
	SpanIDB64       string
	ParentSpanIDB64 string
}

func StringAttr(key, value string) Attribute {
	return Attribute{Key: key, Value: otlpAnyValue{StringValue: value}}
}

func IntAttr(key string, value int64) Attribute {
	return Attribute{Key: key, Value: otlpAnyValue{IntValue: strconv.FormatInt(value, 10)}}
}

func FloatAttr(key string, value float64) Attribute {
	return Attribute{Key: key, Value: otlpAnyValue{DoubleValue: &value}}
}

func BoolAttr(key string, value bool) Attribute {
	return Attribute{Key: key, Value: otlpAnyValue{BoolValue: &value}}
}

func StringSliceAttr(key string, values []string) Attribute {
	otlpValues := make([]otlpAnyValue, 0, len(values))
	for _, value := range values {
		otlpValues = append(otlpValues, otlpAnyValue{StringValue: value})
	}
	return Attribute{
		Key: key,
		Value: otlpAnyValue{
			ArrayValue: &otlpArrayValue{Values: otlpValues},
		},
	}
}

func createSpanIDs(parent *Span) (spanIDs, error) {
	traceID := ""
	if parent != nil {
		traceID = parent.ids.TraceIDB64
	} else {
		var err error
		traceID, err = randomID(16)
		if err != nil {
			return spanIDs{}, err
		}
	}

	spanID, err := randomID(8)
	if err != nil {
		return spanIDs{}, err
	}

	ids := spanIDs{
		TraceIDB64: traceID,
		SpanIDB64:  spanID,
	}
	if parent != nil {
		ids.ParentSpanIDB64 = parent.ids.SpanIDB64
	}
	return ids, nil
}

// spanIDsUnder re-parents fresh span ids under a saved parent: its trace id
// and its span id as the parent. Invalid ids are rejected so a malformed
// saved context never ships a span nobody can attach.
func spanIDsUnder(parent SpanContext, ids spanIDs) (spanIDs, error) {
	traceID, err := hex.DecodeString(parent.TraceID)
	if err != nil || len(traceID) != 16 {
		return spanIDs{}, fmt.Errorf("raindrop: parent trace id %q is not 32 hex characters", parent.TraceID)
	}
	spanID, err := hex.DecodeString(parent.SpanID)
	if err != nil || len(spanID) != 8 {
		return spanIDs{}, fmt.Errorf("raindrop: parent span id %q is not 16 hex characters", parent.SpanID)
	}
	ids.TraceIDB64 = base64.StdEncoding.EncodeToString(traceID)
	ids.ParentSpanIDB64 = base64.StdEncoding.EncodeToString(spanID)
	return ids, nil
}

func randomID(length int) (string, error) {
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

func unixNanoString(at time.Time) string {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return strconv.FormatInt(at.UnixNano(), 10)
}

func buildExportTraceServiceRequest(spans []otlpSpan, serviceName, serviceVersion string) exportTraceServiceRequest {
	return exportTraceServiceRequest{
		ResourceSpans: []resourceSpans{
			{
				Resource: resource{
					Attributes: []otlpKeyValue{
						{Key: "service.name", Value: otlpAnyValue{StringValue: serviceName}},
						{Key: "service.version", Value: otlpAnyValue{StringValue: serviceVersion}},
					},
				},
				ScopeSpans: []scopeSpans{
					{
						Scope: scope{Name: serviceName, Version: serviceVersion},
						Spans: spans,
					},
				},
			},
		},
	}
}
