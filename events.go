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

	// spanAttributes go on every span this interaction starts: for a
	// detached sub-agent's own run, its reverse link to the launcher
	// (CONTRACT 13.2).
	spanAttributes []Attribute
}

// TrackEvent queues a complete plain event for events/track. It never does
// network I/O on the caller (CONTRACT 1.1): delivery happens on the
// background goroutine, Flush or Close. An event without a user id is not
// sent (CONTRACT 10.1).
func (c *Client) TrackEvent(ctx context.Context, event Event) error {
	return c.trackComplete(event.EventID, event.UserID, event.Event, event.Timestamp,
		event.Properties, event.Attachments, nil)
}

// TrackAI queues a complete AI event for events/track; see TrackEvent.
func (c *Client) TrackAI(ctx context.Context, event AIEvent) error {
	limit := c.textFieldLimit()
	var ai *aiDataPayload
	if event.Input != "" || event.Output != "" || event.Model != "" || event.ConvoID != "" {
		ai = &aiDataPayload{
			Input:   capText(event.Input, limit),
			Output:  capText(event.Output, limit),
			Model:   event.Model,
			ConvoID: event.ConvoID,
		}
	}
	return c.trackComplete(event.EventID, event.UserID, event.Event, event.Timestamp,
		event.Properties, event.Attachments, ai)
}

type trackEventPayload struct {
	EventID     string         `json:"event_id"`
	UserID      string         `json:"user_id"`
	Event       string         `json:"event"`
	Timestamp   string         `json:"timestamp"`
	Properties  map[string]any `json:"properties"`
	AIData      *aiDataPayload `json:"ai_data,omitempty"`
	Attachments []Attachment   `json:"attachments,omitempty"`
}

func (c *Client) trackComplete(eventID, userID, name string, timestamp time.Time, properties map[string]any, attachments []Attachment, ai *aiDataPayload) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if userID == "" {
		c.warn("track.missing_user", "raindrop: not sending event: it has no user id", "event", name)
		return nil
	}
	if eventID == "" {
		generated, err := newEventID()
		if err != nil {
			return err
		}
		eventID = generated
	}
	limit := c.textFieldLimit()
	props := capProperties(properties, limit)
	if props == nil {
		props = make(map[string]any, 1)
	}
	for key, value := range appGitWithPropertyOverrides(c.appGitSnapshot(), properties).properties {
		if _, exists := props[key]; !exists {
			props[key] = value
		}
	}
	props["$context"] = cloneMap(c.contextData)
	c.track.Enqueue(routeEventsTrack, trackEventPayload{
		EventID:     eventID,
		UserID:      userID,
		Event:       eventNameOrDefault(name),
		Timestamp:   iso8601Timestamp(timestamp),
		Properties:  props,
		AIData:      ai,
		Attachments: capAttachments(attachments, limit),
	})
	return nil
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

// ResumeOptions is identity to attach when continuing an event begun
// elsewhere. Every field is optional: what is omitted is never sent, so the
// user, conversation and name ingest already stored are kept.
type ResumeOptions struct {
	UserID  string
	ConvoID string
	Event   string
}

// ResumeInteraction returns a handle for eventID. Within this process it
// returns the open interaction; for an event begun elsewhere (another
// process, a queue worker, a later request) it continues that event by id,
// like Resume with no options.
func (c *Client) ResumeInteraction(eventID string) *Interaction {
	return c.Resume(eventID, ResumeOptions{})
}

// Resume continues an event begun elsewhere using only its id (CONTRACT
// 9.2). Later Patch and Finish calls are sent even without a user id, and
// carry only the fields they (or opts) supply.
func (c *Client) Resume(eventID string, opts ResumeOptions) *Interaction {
	if c == nil || eventID == "" {
		return &Interaction{}
	}
	if interaction, ok := c.interactions.Load(eventID); ok {
		if typed, ok := interaction.(*Interaction); ok {
			return typed
		}
	}
	c.events.markResumed(eventID, opts)
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
