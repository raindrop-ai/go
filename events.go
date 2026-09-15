package raindrop

import (
	"context"
	"sync"
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
	EventID     string
	UserID      string
	Event       string
	Timestamp   time.Time
	Properties  map[string]any
	Attachments []Attachment
}

type AIEvent struct {
	EventID     string
	UserID      string
	Event       string
	Timestamp   time.Time
	Input       string
	Output      string
	Model       string
	ConvoID     string
	Properties  map[string]any
	Attachments []Attachment
}

type BeginOptions struct {
	EventID     string
	UserID      string
	Event       string
	Timestamp   time.Time
	Input       string
	Model       string
	ConvoID     string
	Properties  map[string]any
	Attachments []Attachment
}

type PatchOptions struct {
	UserID      string
	Event       string
	Timestamp   time.Time
	Input       string
	Output      string
	Model       string
	ConvoID     string
	Properties  map[string]any
	Attachments []Attachment
	IsPending   *bool
}

type FinishOptions struct {
	Timestamp   time.Time
	Output      string
	Model       string
	Properties  map[string]any
	Attachments []Attachment
}

type Interaction struct {
	client  *Client
	ctx     context.Context
	eventID string
	mu      sync.RWMutex
	appGit  appGitSnapshot
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
		UserID:      event.UserID,
		Event:       eventNameOrDefault(event.Event),
		Timestamp:   event.Timestamp,
		Properties:  cloneMap(event.Properties),
		Attachments: cloneAttachments(event.Attachments),
		IsPending:   &done,
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
		UserID:      event.UserID,
		Event:       eventNameOrDefault(event.Event),
		Timestamp:   event.Timestamp,
		Input:       event.Input,
		Output:      event.Output,
		Model:       event.Model,
		ConvoID:     event.ConvoID,
		Properties:  cloneMap(event.Properties),
		Attachments: cloneAttachments(event.Attachments),
		IsPending:   &done,
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
	appGit := appGitWithPropertyOverrides(c.appGitSnapshot(), opts.Properties)
	pending := true
	_ = c.patch(ctx, eventID, PatchOptions{
		UserID:      opts.UserID,
		Event:       eventNameOrDefault(opts.Event),
		Timestamp:   opts.Timestamp,
		Input:       opts.Input,
		Model:       opts.Model,
		ConvoID:     opts.ConvoID,
		Properties:  cloneMap(opts.Properties),
		Attachments: cloneAttachments(opts.Attachments),
		IsPending:   &pending,
	}, appGit)
	interaction := &Interaction{client: c, ctx: ctx, eventID: eventID, appGit: appGit}
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
	appGit := c.appGitSnapshot()
	if buffered, ok := c.events.appGitSnapshot(eventID); ok {
		appGit = buffered
	}
	return &Interaction{
		client:  c,
		ctx:     context.Background(),
		eventID: eventID,
		appGit:  appGit,
	}
}

func (c *Client) Patch(ctx context.Context, eventID string, opts PatchOptions) error {
	return c.patch(ctx, eventID, opts, c.appGitSnapshot())
}

func (c *Client) patch(ctx context.Context, eventID string, opts PatchOptions, appGit appGitSnapshot) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if eventID == "" {
		return nil
	}
	// Public Client.Patch/Finish calls targeting an interaction must update the
	// same effective operation provenance used by Interaction spans.
	if stored, ok := c.interactions.Load(eventID); ok {
		if interaction, ok := stored.(*Interaction); ok {
			appGit = interaction.updateAppGit(opts.Properties)
		}
	}

	// Cap text fields BEFORE buffering so multi-MB inputs, outputs, property
	// values, and attachment values never enter the merge/serialize pipeline
	// at full size: the cost on the caller stays proportional to the cap.
	limit := c.textFieldLimit()
	return c.events.Patch(ctx, eventID, eventPatch{
		EventName:   opts.Event,
		UserID:      opts.UserID,
		Timestamp:   opts.Timestamp,
		Input:       capText(opts.Input, limit),
		Output:      capText(opts.Output, limit),
		Model:       opts.Model,
		ConvoID:     opts.ConvoID,
		Properties:  capProperties(opts.Properties, limit),
		Attachments: capAttachments(opts.Attachments, limit),
		IsPending:   opts.IsPending,
		AppGit:      &appGit,
	})
}

func (c *Client) Finish(ctx context.Context, eventID string, opts FinishOptions) error {
	done := false
	return c.Patch(ctx, eventID, PatchOptions{
		Timestamp:   opts.Timestamp,
		Output:      opts.Output,
		Model:       opts.Model,
		Properties:  cloneMap(opts.Properties),
		Attachments: cloneAttachments(opts.Attachments),
		IsPending:   &done,
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
	appGit := i.updateAppGit(opts.Properties)
	return i.client.patch(i.ctx, i.eventID, opts, appGit)
}

func (i *Interaction) SetProperties(properties map[string]any) error {
	return i.Patch(PatchOptions{Properties: cloneMap(properties)})
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
	done := false
	appGit := i.updateAppGit(opts.Properties)
	err := i.client.patch(i.ctx, i.eventID, PatchOptions{
		Timestamp:   opts.Timestamp,
		Output:      opts.Output,
		Model:       opts.Model,
		Properties:  cloneMap(opts.Properties),
		Attachments: cloneAttachments(opts.Attachments),
		IsPending:   &done,
	}, appGit)
	if err == nil && i.eventID != "" {
		i.client.interactions.Delete(i.eventID)
	}
	return err
}

func (i *Interaction) updateAppGit(properties map[string]any) appGitSnapshot {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.appGit = appGitWithPropertyOverrides(i.appGit, properties)
	return cloneAppGitSnapshot(i.appGit)
}

func (i *Interaction) appGitSnapshot() appGitSnapshot {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return cloneAppGitSnapshot(i.appGit)
}

func (c *Client) appGitSnapshot() appGitSnapshot {
	if c == nil {
		return emptyAppGitSnapshot()
	}
	return c.appGit.snapshot()
}

func eventNameOrDefault(name string) string {
	if name == "" {
		return defaultEventName
	}
	return name
}
