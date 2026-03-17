package raindrop

import (
	"crypto/rand"
	"encoding/base64"
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

func defaultResourceAttributes(serviceName, serviceVersion string) []otlpKeyValue {
	return []otlpKeyValue{
		{Key: "service.name", Value: otlpAnyValue{StringValue: serviceName}},
		{Key: "service.version", Value: otlpAnyValue{StringValue: serviceVersion}},
	}
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
