package raindrop

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sort"

	otelattribute "go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const maxOTLPBodyBytes = 5 << 20 // 5 MB

type OTelSpanExporter struct {
	client *Client
}

func (c *Client) OTelSpanExporter() *OTelSpanExporter {
	return &OTelSpanExporter{client: c}
}

func (e *OTelSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e == nil || e.client == nil || !e.client.enabled || len(spans) == 0 {
		return nil
	}
	if err := e.client.ensureOpen(); err != nil {
		return err
	}

	payload := buildOTelExportTraceServiceRequest(spans, e.client.serviceName, e.client.version)
	if len(payload.ResourceSpans) == 0 {
		return nil
	}
	return e.client.transport.postJSON(ctx, "traces", payload)
}

func (e *OTelSpanExporter) Shutdown(context.Context) error {
	return nil
}

// OTLPHandler returns an http.Handler that accepts OTLP/HTTP JSON trace
// payloads and forwards them to Raindrop. Mount it at your preferred path
// (typically "/v1/traces").
func (c *Client) OTLPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if c == nil || !c.enabled {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
			return
		}
		if err := c.ensureOpen(); err != nil {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxOTLPBodyBytes))
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		var req exportTraceServiceRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid OTLP JSON", http.StatusBadRequest)
			return
		}
		if len(req.ResourceSpans) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}

		defaults := defaultResourceAttributes(c.serviceName, c.version)
		for i := range req.ResourceSpans {
			req.ResourceSpans[i].Resource.Attributes = mergeOTLPAttributes(
				defaults,
				req.ResourceSpans[i].Resource.Attributes,
			)
		}

		if err := c.transport.postJSON(r.Context(), "traces", req); err != nil {
			c.debugLog("OTLPHandler: failed to forward traces", "error", err)
			http.Error(w, "failed to forward traces", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func OTelToolAttributes(name string, input any, output any, properties map[string]any) []otelattribute.KeyValue {
	attrs := make([]otelattribute.KeyValue, 0, 2+len(properties)+2)
	attrs = append(attrs, otelattribute.String("traceloop.span.kind", "tool"))
	if name != "" {
		attrs = append(attrs, otelattribute.String("traceloop.entity.name", name))
	}
	if input != nil {
		attrs = append(attrs, otelattribute.String("traceloop.entity.input", stringifyValue(input)))
	}
	if output != nil {
		attrs = append(attrs, otelattribute.String("traceloop.entity.output", stringifyValue(output)))
	}
	attrs = append(attrs, otelToolPropertyAttributes(properties)...)
	return attrs
}

func buildOTelExportTraceServiceRequest(spans []sdktrace.ReadOnlySpan, defaultServiceName, defaultServiceVersion string) exportTraceServiceRequest {
	resourceSpansList := make([]resourceSpans, 0, len(spans))

	for _, span := range spans {
		converted, ok := convertOTelSpan(span)
		if !ok {
			continue
		}

		resourceAttrs := []otlpKeyValue(nil)
		if resource := span.Resource(); resource != nil {
			resourceAttrs = convertOTelKeyValues(resource.Attributes())
		}
		resourceAttrs = mergeOTLPAttributes(
			defaultResourceAttributes(defaultServiceName, defaultServiceVersion),
			resourceAttrs,
		)

		scopeInfo := span.InstrumentationScope()
		scopeName := scopeInfo.Name
		if scopeName == "" {
			scopeName = defaultServiceName
		}
		scopeVersion := scopeInfo.Version
		if scopeVersion == "" {
			scopeVersion = defaultServiceVersion
		}

		resourceSpansList = append(resourceSpansList, resourceSpans{
			Resource: resource{Attributes: resourceAttrs},
			ScopeSpans: []scopeSpans{
				{
					Scope: scope{
						Name:    scopeName,
						Version: scopeVersion,
					},
					Spans: []otlpSpan{converted},
				},
			},
		})
	}

	return exportTraceServiceRequest{ResourceSpans: resourceSpansList}
}

func convertOTelSpan(span sdktrace.ReadOnlySpan) (otlpSpan, bool) {
	spanContext := span.SpanContext()
	if !spanContext.IsValid() {
		return otlpSpan{}, false
	}

	attributes := convertOTelKeyValues(span.Attributes())
	if kind := span.SpanKind().String(); kind != "" && kind != "unspecified" {
		attributes = append(attributes, otlpKeyValue{
			Key:   "otel.span.kind",
			Value: otlpAnyValue{StringValue: kind},
		})
	}

	parent := span.Parent()

	return otlpSpan{
		TraceID:           encodeOTelTraceID(spanContext.TraceID()),
		SpanID:            encodeOTelSpanID(spanContext.SpanID()),
		ParentSpanID:      encodeOTelParentSpanID(parent),
		Name:              span.Name(),
		StartTimeUnixNano: unixNanoString(span.StartTime()),
		EndTimeUnixNano:   unixNanoString(span.EndTime()),
		Attributes:        attributes,
		Status:            convertOTelStatus(span.Status()),
	}, true
}

func convertOTelStatus(status sdktrace.Status) *otlpStatus {
	switch status.Code {
	case otelcodes.Error:
		return &otlpStatus{
			Code:    SpanStatusError,
			Message: status.Description,
		}
	case otelcodes.Ok:
		return &otlpStatus{Code: SpanStatusOK}
	default:
		return &otlpStatus{Code: SpanStatusUnset}
	}
}

func convertOTelKeyValues(attrs []otelattribute.KeyValue) []otlpKeyValue {
	if len(attrs) == 0 {
		return nil
	}

	converted := make([]otlpKeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if !attr.Valid() {
			continue
		}
		converted = append(converted, otlpKeyValue{
			Key:   string(attr.Key),
			Value: convertOTelValue(attr.Value),
		})
	}
	return converted
}

func convertOTelValue(value otelattribute.Value) otlpAnyValue {
	switch value.Type() {
	case otelattribute.BOOL:
		boolValue := value.AsBool()
		return otlpAnyValue{BoolValue: &boolValue}
	case otelattribute.INT64:
		return otlpAnyValue{IntValue: stringifyValue(value.AsInt64())}
	case otelattribute.FLOAT64:
		floatValue := value.AsFloat64()
		return otlpAnyValue{DoubleValue: &floatValue}
	case otelattribute.STRING:
		return otlpAnyValue{StringValue: value.AsString()}
	case otelattribute.BOOLSLICE:
		values := make([]otlpAnyValue, 0, len(value.AsBoolSlice()))
		for _, item := range value.AsBoolSlice() {
			boolValue := item
			values = append(values, otlpAnyValue{BoolValue: &boolValue})
		}
		return otlpAnyValue{ArrayValue: &otlpArrayValue{Values: values}}
	case otelattribute.INT64SLICE:
		values := make([]otlpAnyValue, 0, len(value.AsInt64Slice()))
		for _, item := range value.AsInt64Slice() {
			values = append(values, otlpAnyValue{IntValue: stringifyValue(item)})
		}
		return otlpAnyValue{ArrayValue: &otlpArrayValue{Values: values}}
	case otelattribute.FLOAT64SLICE:
		values := make([]otlpAnyValue, 0, len(value.AsFloat64Slice()))
		for _, item := range value.AsFloat64Slice() {
			floatValue := item
			values = append(values, otlpAnyValue{DoubleValue: &floatValue})
		}
		return otlpAnyValue{ArrayValue: &otlpArrayValue{Values: values}}
	case otelattribute.STRINGSLICE:
		values := make([]otlpAnyValue, 0, len(value.AsStringSlice()))
		for _, item := range value.AsStringSlice() {
			values = append(values, otlpAnyValue{StringValue: item})
		}
		return otlpAnyValue{ArrayValue: &otlpArrayValue{Values: values}}
	default:
		return otlpAnyValue{StringValue: value.Emit()}
	}
}

func otelToolPropertyAttributes(properties map[string]any) []otelattribute.KeyValue {
	if len(properties) == 0 {
		return nil
	}

	keys := make([]string, 0, len(properties))
	for key := range properties {
		if key != "" && properties[key] != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	attrs := make([]otelattribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		attrKey := "traceloop.association.properties." + key
		switch typed := properties[key].(type) {
		case string:
			attrs = append(attrs, otelattribute.String(attrKey, typed))
		case bool:
			attrs = append(attrs, otelattribute.Bool(attrKey, typed))
		case int:
			attrs = append(attrs, otelattribute.Int(attrKey, typed))
		case int8:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case int16:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case int32:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case int64:
			attrs = append(attrs, otelattribute.Int64(attrKey, typed))
		case uint:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case uint8:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case uint16:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case uint32:
			attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
		case uint64:
			if typed <= uint64(^uint64(0)>>1) {
				attrs = append(attrs, otelattribute.Int64(attrKey, int64(typed)))
			} else {
				attrs = append(attrs, otelattribute.String(attrKey, stringifyValue(typed)))
			}
		case float32:
			attrs = append(attrs, otelattribute.Float64(attrKey, float64(typed)))
		case float64:
			attrs = append(attrs, otelattribute.Float64(attrKey, typed))
		case []string:
			attrs = append(attrs, otelattribute.StringSlice(attrKey, typed))
		default:
			attrs = append(attrs, otelattribute.String(attrKey, stringifyValue(typed)))
		}
	}

	return attrs
}

func mergeOTLPAttributes(base []otlpKeyValue, overlay []otlpKeyValue) []otlpKeyValue {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}

	valuesByKey := make(map[string]otlpAnyValue, len(base)+len(overlay))
	for _, attr := range base {
		valuesByKey[attr.Key] = attr.Value
	}
	for _, attr := range overlay {
		valuesByKey[attr.Key] = attr.Value
	}

	keys := make([]string, 0, len(valuesByKey))
	for key := range valuesByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	merged := make([]otlpKeyValue, 0, len(keys))
	for _, key := range keys {
		merged = append(merged, otlpKeyValue{Key: key, Value: valuesByKey[key]})
	}
	return merged
}

func encodeOTelTraceID(id oteltrace.TraceID) string {
	return base64.StdEncoding.EncodeToString(id[:])
}

func encodeOTelSpanID(id oteltrace.SpanID) string {
	return base64.StdEncoding.EncodeToString(id[:])
}

func encodeOTelParentSpanID(parent oteltrace.SpanContext) string {
	if !parent.IsValid() {
		return ""
	}
	return encodeOTelSpanID(parent.SpanID())
}
