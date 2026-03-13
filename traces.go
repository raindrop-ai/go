package raindrop

import (
	"context"
	"sync"
	"time"
)

type SpanOptions struct {
	Name       string
	EventID    string
	Parent     *Span
	Properties map[string]any
	Attributes []Attribute
	StartTime  time.Time
}

type ToolOptions struct {
	Parent     *Span
	Properties map[string]any
	Input      any
	StartTime  time.Time
}

type TrackToolOptions struct {
	Name       string
	Parent     *Span
	Input      any
	Output     any
	Error      error
	Properties map[string]any
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
}

type Span struct {
	client  *Client
	ids     spanIDs
	name    string
	eventID string
	start   time.Time

	mu     sync.Mutex
	attrs  []Attribute
	status *otlpStatus
	ended  bool
}

type ToolSpan struct {
	span   *Span
	input  any
	output any
}

type Tracer struct {
	client     *Client
	ctx        context.Context
	properties map[string]any
}

type traceBuffer struct {
	client *Client

	mu           sync.Mutex
	queue        []otlpSpan
	maxBatchSize int
	maxQueueSize int

	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type spanContextKey struct{}

func newTraceBuffer(client *Client, flushEvery time.Duration, maxBatchSize, maxQueueSize int) *traceBuffer {
	buffer := &traceBuffer{
		client:       client,
		maxBatchSize: maxBatchSize,
		maxQueueSize: maxQueueSize,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	if client != nil && client.enabled && flushEvery > 0 {
		buffer.ticker = time.NewTicker(flushEvery)
		go buffer.run()
	}
	return buffer
}

func (b *traceBuffer) run() {
	defer close(b.doneCh)
	for {
		select {
		case <-b.stopCh:
			if b.ticker != nil {
				b.ticker.Stop()
			}
			return
		case <-b.ticker.C:
			_ = b.Flush(context.Background())
		}
	}
}

func (b *traceBuffer) Enqueue(span otlpSpan) {
	b.mu.Lock()
	if len(b.queue) >= b.maxQueueSize {
		copy(b.queue, b.queue[1:])
		b.queue = b.queue[:len(b.queue)-1]
	}
	b.queue = append(b.queue, span)
	flushNow := len(b.queue) >= b.maxBatchSize
	b.mu.Unlock()

	if flushNow {
		_ = b.Flush(context.Background())
	}
}

func (b *traceBuffer) Flush(ctx context.Context) error {
	for {
		batch := b.takeBatch()
		if len(batch) == 0 {
			return nil
		}

		payload := buildExportTraceServiceRequest(batch, b.client.serviceName, b.client.version)
		if err := b.client.transport.postJSON(ctx, "traces", payload); err != nil {
			b.restoreBatch(batch)
			return err
		}
	}
}

func (b *traceBuffer) Stop(ctx context.Context) error {
	b.stopOnce.Do(func() {
		close(b.stopCh)
	})
	if b.ticker != nil {
		<-b.doneCh
	}
	return b.Flush(ctx)
}

func (b *traceBuffer) takeBatch() []otlpSpan {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.queue) == 0 {
		return nil
	}

	size := b.maxBatchSize
	if size <= 0 || size > len(b.queue) {
		size = len(b.queue)
	}

	batch := make([]otlpSpan, size)
	copy(batch, b.queue[:size])
	b.queue = append([]otlpSpan{}, b.queue[size:]...)
	return batch
}

func (b *traceBuffer) restoreBatch(batch []otlpSpan) {
	if len(batch) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	restored := make([]otlpSpan, 0, len(batch)+len(b.queue))
	restored = append(restored, batch...)
	restored = append(restored, b.queue...)
	if b.maxQueueSize > 0 && len(restored) > b.maxQueueSize {
		restored = restored[:b.maxQueueSize]
	}
	b.queue = restored
}

func (c *Client) StartSpan(ctx context.Context, opts SpanOptions) *Span {
	if c == nil || !c.enabled {
		return &Span{}
	}
	if err := c.ensureOpen(); err != nil {
		return &Span{}
	}

	parent := opts.Parent
	if parent == nil {
		parent = SpanFromContext(ctx)
	}

	ids, err := createSpanIDs(parent)
	if err != nil {
		c.debugLog("failed to create span IDs", "error", err)
		return &Span{}
	}

	start := opts.StartTime
	if start.IsZero() {
		start = time.Now().UTC()
	}

	span := &Span{
		client:  c,
		ids:     ids,
		name:    opts.Name,
		eventID: opts.EventID,
		start:   start,
		attrs:   append(append([]Attribute{}, opts.Attributes...), toolPropertyAttributes(opts.Properties)...),
	}
	return span
}

func (c *Client) Tracer(properties map[string]any) *Tracer {
	if c == nil {
		return &Tracer{}
	}
	return &Tracer{
		client:     c,
		ctx:        context.Background(),
		properties: cloneMap(properties),
	}
}

func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, spanContextKey{}, span)
}

func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanContextKey{}).(*Span)
	return span
}

func (i *Interaction) StartSpan(opts SpanOptions) *Span {
	if i == nil || i.client == nil {
		return &Span{}
	}
	if opts.EventID == "" {
		opts.EventID = i.eventID
	}
	return i.client.StartSpan(i.ctx, opts)
}

func (i *Interaction) WithSpan(opts SpanOptions, fn func(context.Context, *Span) error) error {
	if fn == nil {
		return nil
	}
	if i == nil || i.client == nil {
		return fn(context.Background(), nil)
	}

	span := i.StartSpan(opts)
	ctx := ContextWithSpan(i.ctx, span)
	err := fn(ctx, span)
	if err != nil {
		span.SetError(err)
	}
	span.End()
	return err
}

func (i *Interaction) StartToolSpan(name string, opts ToolOptions) *ToolSpan {
	if i == nil || i.client == nil {
		return &ToolSpan{}
	}
	properties := cloneMap(opts.Properties)
	if properties == nil {
		properties = make(map[string]any, 1)
	}
	if i.eventID != "" {
		if _, exists := properties["event_id"]; !exists {
			properties["event_id"] = i.eventID
		}
	}
	span := i.StartSpan(SpanOptions{
		Name:       name,
		EventID:    i.eventID,
		Parent:     opts.Parent,
		StartTime:  opts.StartTime,
		Attributes: buildToolAttributes(name, opts.Input, nil, 0, properties),
	})
	return &ToolSpan{
		span:  span,
		input: opts.Input,
	}
}

func (t *Tracer) StartSpan(opts SpanOptions) *Span {
	if t == nil || t.client == nil {
		return &Span{}
	}
	opts.Properties = mergeMaps(t.properties, opts.Properties)
	return t.client.StartSpan(t.ctx, opts)
}

func (t *Tracer) WithSpan(opts SpanOptions, fn func(context.Context, *Span) error) error {
	if fn == nil {
		return nil
	}
	if t == nil || t.client == nil {
		return fn(context.Background(), nil)
	}

	span := t.StartSpan(opts)
	ctx := ContextWithSpan(t.ctx, span)
	err := fn(ctx, span)
	if err != nil {
		span.SetError(err)
	}
	span.End()
	return err
}

func (t *Tracer) TrackTool(opts TrackToolOptions) {
	if t == nil || t.client == nil {
		return
	}
	opts.Properties = mergeMaps(t.properties, opts.Properties)

	startTime := opts.StartTime
	endTime := opts.EndTime
	if startTime.IsZero() && !endTime.IsZero() && opts.Duration > 0 {
		startTime = endTime.Add(-opts.Duration)
	}
	if startTime.IsZero() && opts.Duration > 0 {
		startTime = time.Now().UTC().Add(-opts.Duration)
	}

	span := t.StartSpan(SpanOptions{
		Name:       opts.Name,
		Parent:     opts.Parent,
		StartTime:  startTime,
		Attributes: buildToolAttributes(opts.Name, opts.Input, opts.Output, 0, opts.Properties),
	})
	if opts.Error != nil {
		span.SetError(opts.Error)
	}
	if opts.Duration > 0 {
		span.SetAttributes(IntAttr("traceloop.entity.duration_ms", opts.Duration.Milliseconds()))
	} else if !startTime.IsZero() && !endTime.IsZero() {
		span.SetAttributes(IntAttr("traceloop.entity.duration_ms", endTime.Sub(startTime).Milliseconds()))
	}
	if !endTime.IsZero() {
		span.EndAt(endTime)
		return
	}
	span.End()
}

func (i *Interaction) TrackTool(opts TrackToolOptions) {
	if i == nil || i.client == nil || opts.Name == "" {
		return
	}
	startTime := opts.StartTime
	endTime := opts.EndTime
	if startTime.IsZero() && !endTime.IsZero() && opts.Duration > 0 {
		startTime = endTime.Add(-opts.Duration)
	}
	if startTime.IsZero() && opts.Duration > 0 {
		startTime = time.Now().UTC().Add(-opts.Duration)
	}

	toolSpan := i.StartToolSpan(opts.Name, ToolOptions{
		Parent:     opts.Parent,
		Properties: opts.Properties,
		Input:      opts.Input,
		StartTime:  startTime,
	})
	toolSpan.SetOutput(opts.Output)
	if opts.Error != nil {
		toolSpan.SetError(opts.Error)
	}
	if opts.Duration > 0 {
		toolSpan.span.SetAttributes(IntAttr("traceloop.entity.duration_ms", opts.Duration.Milliseconds()))
	} else if !startTime.IsZero() && !endTime.IsZero() {
		toolSpan.span.SetAttributes(IntAttr("traceloop.entity.duration_ms", endTime.Sub(startTime).Milliseconds()))
	}
	if !endTime.IsZero() {
		toolSpan.EndAt(endTime)
		return
	}
	toolSpan.End()
}

func (s *Span) SetAttributes(attrs ...Attribute) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attrs = append(s.attrs, attrs...)
}

func (s *Span) SetError(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = &otlpStatus{Code: SpanStatusError, Message: err.Error()}
}

func (s *Span) End() {
	s.EndAt(time.Now().UTC())
}

func (s *Span) EndAt(endTime time.Time) {
	if s == nil || s.client == nil || !s.client.enabled {
		return
	}

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true

	attributes := make([]otlpKeyValue, 0, len(s.attrs)+1)
	if s.eventID != "" {
		attributes = append(attributes, otlpKeyValue{
			Key:   "ai.telemetry.metadata.raindrop.eventId",
			Value: otlpAnyValue{StringValue: s.eventID},
		})
	}
	for _, attr := range s.attrs {
		attributes = append(attributes, otlpKeyValue{Key: attr.Key, Value: attr.Value})
	}

	status := s.status
	if status == nil {
		status = &otlpStatus{Code: SpanStatusOK}
	}
	span := otlpSpan{
		TraceID:           s.ids.TraceIDB64,
		SpanID:            s.ids.SpanIDB64,
		ParentSpanID:      s.ids.ParentSpanIDB64,
		Name:              s.name,
		StartTimeUnixNano: unixNanoString(s.start),
		EndTimeUnixNano:   unixNanoString(endTime),
		Attributes:        attributes,
		Status:            status,
	}
	s.mu.Unlock()

	s.client.traces.Enqueue(span)
}

func (s *ToolSpan) SetInput(input any) {
	if s == nil {
		return
	}
	s.input = input
	if s.span != nil {
		s.span.SetAttributes(StringAttr("traceloop.entity.input", stringifyValue(input)))
	}
}

func (s *ToolSpan) SetOutput(output any) {
	if s == nil {
		return
	}
	s.output = output
	if s.span != nil {
		s.span.SetAttributes(StringAttr("traceloop.entity.output", stringifyValue(output)))
	}
}

func (s *ToolSpan) SetError(err error) {
	if s == nil || s.span == nil || err == nil {
		return
	}
	s.span.SetError(err)
}

func (s *ToolSpan) End() {
	if s == nil || s.span == nil {
		return
	}
	if !s.span.start.IsZero() {
		s.span.SetAttributes(IntAttr("traceloop.entity.duration_ms", time.Since(s.span.start).Milliseconds()))
	}
	s.span.End()
}

func (s *ToolSpan) EndAt(endTime time.Time) {
	if s == nil || s.span == nil {
		return
	}
	if !s.span.start.IsZero() && !endTime.IsZero() {
		s.span.SetAttributes(IntAttr("traceloop.entity.duration_ms", endTime.Sub(s.span.start).Milliseconds()))
	}
	s.span.EndAt(endTime)
}

func WithTool[T any](interaction *Interaction, name string, opts ToolOptions, fn func() (T, error)) (T, error) {
	var zero T
	if fn == nil {
		return zero, nil
	}
	if interaction == nil || interaction.client == nil {
		return fn()
	}

	toolSpan := interaction.StartToolSpan(name, opts)
	result, err := fn()
	if err != nil {
		toolSpan.SetError(err)
		toolSpan.End()
		return zero, err
	}
	toolSpan.SetOutput(result)
	toolSpan.End()
	return result, nil
}

func buildToolAttributes(name string, input any, output any, duration time.Duration, properties map[string]any) []Attribute {
	attrs := []Attribute{
		StringAttr("traceloop.span.kind", "tool"),
		StringAttr("traceloop.entity.name", name),
	}
	if input != nil {
		attrs = append(attrs, StringAttr("traceloop.entity.input", stringifyValue(input)))
	}
	if output != nil {
		attrs = append(attrs, StringAttr("traceloop.entity.output", stringifyValue(output)))
	}
	if duration > 0 {
		attrs = append(attrs, IntAttr("traceloop.entity.duration_ms", duration.Milliseconds()))
	}
	attrs = append(attrs, toolPropertyAttributes(properties)...)
	return attrs
}

func toolPropertyAttributes(properties map[string]any) []Attribute {
	if len(properties) == 0 {
		return nil
	}
	attrs := make([]Attribute, 0, len(properties))
	for key, value := range properties {
		if key == "" || value == nil {
			continue
		}
		attrKey := "traceloop.association.properties." + key
		switch typed := value.(type) {
		case string:
			attrs = append(attrs, StringAttr(attrKey, typed))
		case bool:
			attrs = append(attrs, BoolAttr(attrKey, typed))
		case int:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case int8:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case int16:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case int32:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case int64:
			attrs = append(attrs, IntAttr(attrKey, typed))
		case uint:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case uint8:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case uint16:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case uint32:
			attrs = append(attrs, IntAttr(attrKey, int64(typed)))
		case uint64:
			if typed <= uint64(^uint64(0)>>1) {
				attrs = append(attrs, IntAttr(attrKey, int64(typed)))
			} else {
				attrs = append(attrs, StringAttr(attrKey, stringifyValue(typed)))
			}
		case float32:
			attrs = append(attrs, FloatAttr(attrKey, float64(typed)))
		case float64:
			attrs = append(attrs, FloatAttr(attrKey, typed))
		case []string:
			attrs = append(attrs, StringSliceAttr(attrKey, typed))
		default:
			attrs = append(attrs, StringAttr(attrKey, stringifyValue(typed)))
		}
	}
	return attrs
}
