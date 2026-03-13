package raindrop

import (
	"context"
	"time"
)

type Signal struct {
	EventID      string
	Name         string
	Type         string
	Sentiment    string
	Timestamp    time.Time
	Properties   map[string]any
	AttachmentID string
}

type signalPayload struct {
	EventID      string         `json:"event_id"`
	SignalName   string         `json:"signal_name"`
	SignalType   string         `json:"signal_type,omitempty"`
	Sentiment    string         `json:"sentiment,omitempty"`
	Timestamp    string         `json:"timestamp,omitempty"`
	Properties   map[string]any `json:"properties,omitempty"`
	AttachmentID string         `json:"attachment_id,omitempty"`
}

func (c *Client) TrackSignal(ctx context.Context, signal Signal) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}

	payload := []signalPayload{
		{
			EventID:      signal.EventID,
			SignalName:   signal.Name,
			SignalType:   signalTypeOrDefault(signal.Type),
			Sentiment:    signal.Sentiment,
			Timestamp:    optionalTimestamp(signal.Timestamp),
			Properties:   cloneMap(signal.Properties),
			AttachmentID: signal.AttachmentID,
		},
	}
	return c.transport.postJSON(ctx, "signals/track", payload)
}

func signalTypeOrDefault(value string) string {
	if value == "" {
		return "default"
	}
	return value
}
