package raindrop

import (
	"context"
	"time"
)

type Attachment struct {
	Type     string `json:"type"`
	Role     string `json:"role"`
	Name     string `json:"name,omitempty"`
	Value    string `json:"value"`
	Language string `json:"language,omitempty"`
}

type Event struct {
	EventID      string
	UserID       string
	Event        string
	Timestamp    time.Time
	Properties   map[string]any
	Attachments  []Attachment
	FeatureFlags map[string]string
}

type AIEvent struct {
	EventID      string
	UserID       string
	Event        string
	Timestamp    time.Time
	Input        string
	Output       string
	Model        string
	ConvoID      string
	Properties   map[string]any
	Attachments  []Attachment
	FeatureFlags map[string]string
}

type BeginOptions struct {
	EventID      string
	UserID       string
	Event        string
	Timestamp    time.Time
	Input        string
	Model        string
	ConvoID      string
	Properties   map[string]any
	Attachments  []Attachment
	FeatureFlags map[string]string
}

type PatchOptions struct {
	UserID       string
	Event        string
	Timestamp    time.Time
	Input        string
	Output       string
	Model        string
	ConvoID      string
	Properties   map[string]any
	Attachments  []Attachment
	FeatureFlags map[string]string
	IsPending    *bool
}

type FinishOptions struct {
	Timestamp    time.Time
	Output       string
	Model        string
	Properties   map[string]any
	Attachments  []Attachment
	FeatureFlags map[string]string
}

type Interaction struct {
	client  *Client
	ctx     context.Context
	eventID string
}

func (c *Client) TrackEvent(ctx context.Context, event Event) error {
	eventID := event.EventID
	if eventID == "" {
		var err error
		eventID, err = newEventID()
		if err != nil {
			return err
		}
	}
	done := false
	return c.Patch(ctx, eventID, PatchOptions{
		UserID:       event.UserID,
		Event:        eventNameOrDefault(event.Event),
		Timestamp:    event.Timestamp,
		Properties:   cloneMap(event.Properties),
		Attachments:  cloneAttachments(event.Attachments),
		FeatureFlags: cloneStringMap(event.FeatureFlags),
		IsPending:    &done,
	})
}

func (c *Client) TrackAI(ctx context.Context, event AIEvent) error {
	eventID := event.EventID
	if eventID == "" {
		var err error
		eventID, err = newEventID()
		if err != nil {
			return err
		}
	}
	done := false
	return c.Patch(ctx, eventID, PatchOptions{
		UserID:       event.UserID,
		Event:        eventNameOrDefault(event.Event),
		Timestamp:    event.Timestamp,
		Input:        event.Input,
		Output:       event.Output,
		Model:        event.Model,
		ConvoID:      event.ConvoID,
		Properties:   cloneMap(event.Properties),
		Attachments:  cloneAttachments(event.Attachments),
		FeatureFlags: cloneStringMap(event.FeatureFlags),
		IsPending:    &done,
	})
}

func (c *Client) Begin(ctx context.Context, opts BeginOptions) *Interaction {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return &Interaction{ctx: ctx}
	}
	eventID := opts.EventID
	if eventID == "" {
		if generated, err := newEventID(); err == nil {
			eventID = generated
		}
	}
	pending := true
	_ = c.Patch(ctx, eventID, PatchOptions{
		UserID:       opts.UserID,
		Event:        eventNameOrDefault(opts.Event),
		Timestamp:    opts.Timestamp,
		Input:        opts.Input,
		Model:        opts.Model,
		ConvoID:      opts.ConvoID,
		Properties:   cloneMap(opts.Properties),
		Attachments:  cloneAttachments(opts.Attachments),
		FeatureFlags: cloneStringMap(opts.FeatureFlags),
		IsPending:    &pending,
	})
	interaction := &Interaction{client: c, ctx: ctx, eventID: eventID}
	if eventID != "" {
		c.interactions.Store(eventID, interaction)
	}
	return interaction
}

func (c *Client) ResumeInteraction(eventID string) *Interaction {
	if c == nil || eventID == "" {
		return &Interaction{}
	}
	if interaction, ok := c.interactions.Load(eventID); ok {
		if typed, ok := interaction.(*Interaction); ok {
			return typed
		}
	}
	return &Interaction{
		client:  c,
		ctx:     context.Background(),
		eventID: eventID,
	}
}

func (c *Client) Patch(ctx context.Context, eventID string, opts PatchOptions) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if eventID == "" {
		return nil
	}

	// Cap text fields BEFORE buffering so multi-MB inputs, outputs, property
	// values, and attachment values never enter the merge/serialize pipeline
	// at full size: the cost on the caller stays proportional to the cap.
	limit := c.textFieldLimit()
	return c.events.Patch(ctx, eventID, eventPatch{
		EventName:    opts.Event,
		UserID:       opts.UserID,
		Timestamp:    opts.Timestamp,
		Input:        capText(opts.Input, limit),
		Output:       capText(opts.Output, limit),
		Model:        opts.Model,
		ConvoID:      opts.ConvoID,
		Properties:   capProperties(opts.Properties, limit),
		Attachments:  capAttachments(opts.Attachments, limit),
		FeatureFlags: cloneStringMap(opts.FeatureFlags),
		IsPending:    opts.IsPending,
	})
}

func (c *Client) Finish(ctx context.Context, eventID string, opts FinishOptions) error {
	done := false
	return c.Patch(ctx, eventID, PatchOptions{
		Timestamp:    opts.Timestamp,
		Output:       opts.Output,
		Model:        opts.Model,
		Properties:   cloneMap(opts.Properties),
		Attachments:  cloneAttachments(opts.Attachments),
		FeatureFlags: cloneStringMap(opts.FeatureFlags),
		IsPending:    &done,
	})
}

func (i *Interaction) EventID() string {
	if i == nil {
		return ""
	}
	return i.eventID
}

func (i *Interaction) GetEventID() string {
	return i.EventID()
}

func (i *Interaction) Patch(opts PatchOptions) error {
	if i == nil || i.client == nil {
		return nil
	}
	return i.client.Patch(i.ctx, i.eventID, opts)
}

func (i *Interaction) SetProperties(properties map[string]any) error {
	return i.Patch(PatchOptions{Properties: cloneMap(properties)})
}

func (i *Interaction) SetFeatureFlags(featureFlags map[string]string) error {
	return i.Patch(PatchOptions{FeatureFlags: cloneStringMap(featureFlags)})
}

func (i *Interaction) SetProperty(key string, value any) error {
	if key == "" {
		return nil
	}
	return i.SetProperties(map[string]any{key: value})
}

func (i *Interaction) AddAttachments(attachments []Attachment) error {
	return i.Patch(PatchOptions{Attachments: cloneAttachments(attachments)})
}

func (i *Interaction) SetInput(input string) error {
	return i.Patch(PatchOptions{Input: input})
}

func (i *Interaction) Finish(opts FinishOptions) error {
	if i == nil || i.client == nil {
		return nil
	}
	err := i.client.Finish(i.ctx, i.eventID, opts)
	if err == nil && i.eventID != "" {
		i.client.interactions.Delete(i.eventID)
	}
	return err
}

func eventNameOrDefault(name string) string {
	if name == "" {
		return defaultEventName
	}
	return name
}
