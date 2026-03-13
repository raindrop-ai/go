package raindrop

import (
	"context"
	"sync"
	"time"
)

type eventPatch struct {
	EventName   string
	UserID      string
	ConvoID     string
	Input       string
	Output      string
	Model       string
	Properties  map[string]any
	Attachments []Attachment
	IsPending   *bool
	Timestamp   time.Time
}

type stickyEventData struct {
	EventName string
	UserID    string
	ConvoID   string
	IsPending *bool
}

type eventBuffer struct {
	client *Client

	mu      sync.Mutex
	buffers map[string]eventPatch
	sticky  map[string]stickyEventData

	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func newEventBuffer(client *Client, flushEvery time.Duration) *eventBuffer {
	buffer := &eventBuffer{
		client:  client,
		buffers: make(map[string]eventPatch),
		sticky:  make(map[string]stickyEventData),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	if client != nil && client.enabled && flushEvery > 0 {
		buffer.ticker = time.NewTicker(flushEvery)
		go buffer.run()
	}
	return buffer
}

func (b *eventBuffer) run() {
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

func (b *eventBuffer) Patch(ctx context.Context, eventID string, patch eventPatch) error {
	b.mu.Lock()
	existing := b.buffers[eventID]
	sticky := b.sticky[eventID]

	merged := mergeEventPatches(existing, patch)
	if merged.IsPending == nil {
		if sticky.IsPending != nil {
			value := *sticky.IsPending
			merged.IsPending = &value
		} else {
			pending := true
			merged.IsPending = &pending
		}
	}

	b.buffers[eventID] = merged
	b.sticky[eventID] = mergeStickyEventData(sticky, merged)
	flushNow := merged.IsPending != nil && !*merged.IsPending
	b.mu.Unlock()

	if flushNow {
		return b.flushOne(ctx, eventID)
	}
	return nil
}

func (b *eventBuffer) Flush(ctx context.Context) error {
	b.mu.Lock()
	ids := make([]string, 0, len(b.buffers))
	for eventID := range b.buffers {
		ids = append(ids, eventID)
	}
	b.mu.Unlock()

	var firstErr error
	for _, eventID := range ids {
		if err := b.flushOne(ctx, eventID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *eventBuffer) Stop(ctx context.Context) error {
	b.stopOnce.Do(func() {
		close(b.stopCh)
	})
	if b.ticker != nil {
		<-b.doneCh
	}
	return b.Flush(ctx)
}

func (b *eventBuffer) flushOne(ctx context.Context, eventID string) error {
	patch, sticky, ok := b.take(eventID)
	if !ok {
		return nil
	}

	payload := b.client.buildTrackPartialPayload(eventID, patch, sticky)
	if payload == nil {
		b.restore(eventID, patch)
		return nil
	}

	if err := b.client.transport.postJSON(ctx, "events/track_partial", payload); err != nil {
		b.restore(eventID, patch)
		return err
	}

	if payload.IsPending {
		return nil
	}

	b.mu.Lock()
	if _, stillBuffered := b.buffers[eventID]; !stillBuffered {
		delete(b.sticky, eventID)
	}
	b.mu.Unlock()
	return nil
}

func (b *eventBuffer) take(eventID string) (eventPatch, stickyEventData, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	patch, ok := b.buffers[eventID]
	if !ok {
		return eventPatch{}, stickyEventData{}, false
	}
	delete(b.buffers, eventID)
	return patch, b.sticky[eventID], true
}

func (b *eventBuffer) restore(eventID string, patch eventPatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	current := b.buffers[eventID]
	b.buffers[eventID] = mergeEventPatches(patch, current)
}

func mergeEventPatches(target, source eventPatch) eventPatch {
	out := target
	if source.EventName != "" {
		out.EventName = source.EventName
	}
	if source.UserID != "" {
		out.UserID = source.UserID
	}
	if source.ConvoID != "" {
		out.ConvoID = source.ConvoID
	}
	if source.Input != "" {
		out.Input = source.Input
	}
	if source.Output != "" {
		out.Output = source.Output
	}
	if source.Model != "" {
		out.Model = source.Model
	}
	if !source.Timestamp.IsZero() {
		out.Timestamp = source.Timestamp
	}
	if source.IsPending != nil {
		value := *source.IsPending
		out.IsPending = &value
	}
	if target.Properties != nil || source.Properties != nil {
		out.Properties = cloneMap(target.Properties)
		if out.Properties == nil {
			out.Properties = make(map[string]any, len(source.Properties))
		}
		for key, value := range source.Properties {
			out.Properties[key] = value
		}
	}
	if len(source.Attachments) > 0 {
		out.Attachments = append(cloneAttachments(target.Attachments), source.Attachments...)
	}
	return out
}

func mergeStickyEventData(existing stickyEventData, patch eventPatch) stickyEventData {
	out := existing
	if patch.EventName != "" {
		out.EventName = patch.EventName
	}
	if patch.UserID != "" {
		out.UserID = patch.UserID
	}
	if patch.ConvoID != "" {
		out.ConvoID = patch.ConvoID
	}
	if patch.IsPending != nil {
		value := *patch.IsPending
		out.IsPending = &value
	}
	return out
}

type trackPartialPayload struct {
	EventID     string         `json:"event_id"`
	UserID      string         `json:"user_id"`
	Event       string         `json:"event"`
	Timestamp   string         `json:"timestamp"`
	AIData      *aiDataPayload `json:"ai_data,omitempty"`
	Properties  map[string]any `json:"properties"`
	Attachments []Attachment   `json:"attachments"`
	IsPending   bool           `json:"is_pending"`
}

type aiDataPayload struct {
	Input   string `json:"input,omitempty"`
	Output  string `json:"output,omitempty"`
	Model   string `json:"model,omitempty"`
	ConvoID string `json:"convo_id,omitempty"`
}

func (c *Client) buildTrackPartialPayload(eventID string, patch eventPatch, sticky stickyEventData) *trackPartialPayload {
	if c == nil {
		return nil
	}

	userID := patch.UserID
	if userID == "" {
		userID = sticky.UserID
	}
	if userID == "" {
		c.debugLog("skipping track_partial: missing userID", "event_id", eventID)
		return nil
	}

	eventName := patch.EventName
	if eventName == "" {
		eventName = sticky.EventName
	}
	if eventName == "" {
		eventName = defaultEventName
	}

	convoID := patch.ConvoID
	if convoID == "" {
		convoID = sticky.ConvoID
	}

	properties := cloneMap(patch.Properties)
	if properties == nil {
		properties = make(map[string]any, 1)
	}
	properties["$context"] = cloneMap(c.contextData)

	attachments := cloneAttachments(patch.Attachments)
	if attachments == nil {
		attachments = []Attachment{}
	}

	isPending := true
	if patch.IsPending != nil {
		isPending = *patch.IsPending
	} else if sticky.IsPending != nil {
		isPending = *sticky.IsPending
	}

	payload := &trackPartialPayload{
		EventID:     eventID,
		UserID:      userID,
		Event:       eventName,
		Timestamp:   iso8601Timestamp(patch.Timestamp),
		Properties:  properties,
		Attachments: attachments,
		IsPending:   isPending,
	}

	if patch.Input != "" || patch.Output != "" || patch.Model != "" || convoID != "" {
		payload.AIData = &aiDataPayload{
			Input:   patch.Input,
			Output:  patch.Output,
			Model:   patch.Model,
			ConvoID: convoID,
		}
	}
	return payload
}
