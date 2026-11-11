// Command conformance is the raindrop-sdk-harness driver for the Go SDK
// (DEV-1144).
//
// A thin CLI that maps the harness's language-neutral step vocabulary onto the
// *public* Go SDK API — no internals, no test hooks. It speaks driver protocol
// major 1, documented in the harness README ("Step vocabulary & driver
// protocol"):
//
//   - `--describe` prints a JSON handshake object to stdout and exits 0.
//   - Otherwise it reads a JSON array of steps from stdin, executes them in
//     order against a client configured *only* from the environment, prints
//     one `{"step": <n>, "ms": <elapsed>}` timing line per completed step,
//     and exits 0 on success.
//   - An unsupported step prints `unsupported:<step>` as the last stdout line
//     and exits 3.
//
// Client configuration comes only from the environment:
//
//   - RAINDROP_SINK_URL   — ingest base URL (the SDK appends /v1/<route>).
//   - RAINDROP_WRITE_KEY  — bearer write key.
//   - RAINDROP_PROJECT_ID — optional project slug.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	raindrop "github.com/raindrop-ai/go"
)

const (
	driverVersion = "1.2.0"
	protocolMajor = 1
	sdkName       = "go"
)

// Canonical capability keys (harness capabilities.yaml) this SDK supports.
//
// An omitted key is neither supported nor structurally not-applicable, so the
// runner skips scenarios that require it.
var (
	capabilities = []string{
		"app_git.environment",
		"app_git.contextual_auto_detection",
		"events.track",
		"events.track_ai",
		"events.track_partial",
		// Resume an event begun elsewhere by id (Client.Resume, CONTRACT 9.2).
		"events.resume",
		// The Go API takes an empty UserID, so an omitted user_id reaches the
		// SDK as "" and its own handling is what the wire shows (CONTRACT 10.1).
		"events.optional_user_id",
		"app_git.config",
		"identify",
		// `signal` maps to the public Client.TrackSignal surface (DEV-1201:
		// the capability went active once signal UUIDs populate on Query API
		// reads; signal scenarios are experimental until promoted).
		"signal",
		// SDK warnings go to RAINDROP_HARNESS_LOG through a slog handler
		// (CONTRACT 14).
		"logs.capture",
		// Interaction.TrackTool with StartTime/EndTime/Duration and a saved
		// ParentContext (CONTRACT 12).
		"traces.retroactive_span",
		"traces.span_parent",
		// Interaction.Subagent and Client.ResumeSubagent (CONTRACT 13).
		"subagents.detached_dispatch",
		"subagents.detached_child",
		// init.flush_timeout_ms -> WithFlushTimeout, the one Flush/Close
		// deadline (CONTRACT 1.2.1).
		"lifecycle.flush_timeout",
		// `wait`: sleep without an SDK call, so background delivery is
		// observable without an explicit flush.
		"timing.wait",
	}
	notApplicable = []string{"wrapper.capture"}
	// A not_applicable claim must argue "not fixable" (README policy).
	notApplicableReasons = map[string]string{
		"wrapper.capture": "core SDK driven by direct calls; a framework capture path cannot exist by design",
	}
	// traces.otlp is deliberately NOT declared and NOT not_applicable: the SDK
	// ships OTLP spans (otlp.go/traces.go) but the harness cannot drive trace
	// emission yet — a visible gap tracked as DEV-1153.
)

// unsupportedSteps carry a capability the driver does not advertise. Reaching
// one is the exit-3 "unsupported" path. (Currently empty: every step in the
// v1 vocabulary that this SDK can express is mapped.)
var unsupportedSteps = map[string]bool{}

type describeOutput struct {
	SDKName             string            `json:"sdk_name"`
	SDKVersion          string            `json:"sdk_version"`
	DriverVersion       string            `json:"driver_version"`
	Protocol            int               `json:"protocol"`
	Capabilities        []string          `json:"capabilities"`
	NotApplicable       []string          `json:"not_applicable"`
	NotApplicableReason map[string]string `json:"not_applicable_reasons"`
}

type timingLine struct {
	Step int     `json:"step"`
	Ms   float64 `json:"ms"`
}

// driver owns the single SDK client and (at most one) open interaction.
type driver struct {
	client      *raindrop.Client
	interaction *raindrop.Interaction
	// carriers holds dispatch_subagent carriers by child event id.
	carriers map[string]map[string]string
}

// cleanArgs drops the reserved `max_ms` timing arg (handled by the runner,
// never a payload field) and treats an explicit JSON null on any arg as
// omitted — per the driver protocol, null must never reach an SDK call.
func cleanArgs(args map[string]any) map[string]any {
	out := make(map[string]any, len(args))
	for k, v := range args {
		if k == "max_ms" || v == nil {
			continue
		}
		out[k] = v
	}
	return out
}

func stringArg(args map[string]any, key string) (string, error) {
	v, ok := args[key]
	if !ok {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("arg %q: expected string, got %T", key, v)
	}
	return s, nil
}

func objectArg(args map[string]any, key string) (map[string]any, error) {
	v, ok := args[key]
	if !ok {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("arg %q: expected object, got %T", key, v)
	}
	return m, nil
}

// timeArg parses the optional `timestamp` arg (an ISO 8601 string) into a
// time.Time; the zero value means "absent" (the SDK then stamps current time).
func timeArg(args map[string]any) (time.Time, error) {
	v, ok := args["timestamp"]
	if !ok {
		return time.Time{}, nil
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("arg \"timestamp\": expected ISO 8601 string, got %T", v)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("arg \"timestamp\": %w", err)
	}
	return t, nil
}

// attachmentsArg maps the harness attachments array onto the SDK's Attachment
// struct. The public struct carries type/role/value/name/language only; other
// (forward-compat) keys have no public representation and are not forwarded.
func attachmentsArg(args map[string]any) ([]raindrop.Attachment, error) {
	v, ok := args["attachments"]
	if !ok {
		return nil, nil
	}
	items, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("arg \"attachments\": expected array, got %T", v)
	}
	out := make([]raindrop.Attachment, 0, len(items))
	for i, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("attachments[%d]: expected object, got %T", i, raw)
		}
		var att raindrop.Attachment
		for key, dst := range map[string]*string{
			"type":     &att.Type,
			"role":     &att.Role,
			"name":     &att.Name,
			"value":    &att.Value,
			"language": &att.Language,
		} {
			if fv, ok := item[key]; ok && fv != nil {
				s, ok := fv.(string)
				if !ok {
					return nil, fmt.Errorf("attachments[%d].%s: expected string, got %T", i, key, fv)
				}
				*dst = s
			}
		}
		out = append(out, att)
	}
	return out, nil
}

func (d *driver) requireClient() (*raindrop.Client, error) {
	if d.client == nil {
		return nil, fmt.Errorf("step executed before init")
	}
	return d.client, nil
}

// errUnsupported marks the exit-3 protocol path for a step whose capability
// the driver does not advertise.
type errUnsupported struct{ step string }

func (e errUnsupported) Error() string { return "unsupported step: " + e.step }

func (d *driver) execute(ctx context.Context, name string, args map[string]any) error {
	if unsupportedSteps[name] {
		return errUnsupported{step: name}
	}
	args = cleanArgs(args)
	switch name {
	case "init":
		return d.stepInit(args)
	case "track":
		return d.stepTrack(ctx, args)
	case "track_ai":
		return d.stepTrackAI(ctx, args)
	case "begin":
		return d.stepBegin(ctx, args)
	case "patch":
		return d.stepPatch(args)
	case "finish":
		return d.stepFinish(args)
	case "resume":
		return d.stepResume(args)
	case "track_tool":
		return d.stepTrackTool(args)
	case "dispatch_subagent":
		return d.stepDispatchSubagent(args)
	case "run_subagent":
		return d.stepRunSubagent(ctx, args)
	case "identify":
		return d.stepIdentify(ctx, args)
	case "signal":
		return d.stepSignal(ctx, args)
	case "flush":
		return d.stepFlush(ctx)
	case "close":
		return d.stepClose()
	case "wait":
		return stepWait(args)
	default:
		return errUnsupported{step: name}
	}
}

// -- lifecycle -------------------------------------------------------------

func (d *driver) stepInit(args map[string]any) error {
	if d.client != nil {
		return fmt.Errorf("init: client already constructed")
	}
	writeKey := strings.TrimSpace(os.Getenv("RAINDROP_WRITE_KEY"))
	if writeKey == "" {
		// An empty write key + WithDisableLocalWorkshop() yields a disabled
		// client: every step "succeeds" while nothing ships. Hard config error.
		return errors.New("init: RAINDROP_WRITE_KEY is required: an empty key builds a disabled client and the run becomes a silent no-op")
	}
	opts := []raindrop.Option{
		raindrop.WithLogger(harnessLogger()),
		raindrop.WithWriteKey(writeKey),
		// The harness measures the SDK↔sink exchange alone; a locally running
		// Workshop daemon must not be probed or dual-shipped to.
		raindrop.WithDisableLocalWorkshop(),
	}
	appGitOption, err := appGitArg(args)
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	if appGitOption != nil {
		opts = append(opts, appGitOption)
	}
	if raw, ok := args["flush_timeout_ms"]; ok {
		ms, ok := raw.(float64)
		if !ok || ms < 1 {
			return fmt.Errorf("init: arg \"flush_timeout_ms\": expected a positive number of milliseconds, got %v", raw)
		}
		opts = append(opts, raindrop.WithFlushTimeout(time.Duration(ms)*time.Millisecond))
	}
	sink := strings.TrimRight(strings.TrimSpace(os.Getenv("RAINDROP_SINK_URL")), "/")
	if sink == "" {
		// A missing sink must never fall through to the SDK's production
		// default: a conformance run pointed at prod would ship test traffic
		// with a real-looking bearer key. Hard config error instead.
		return errors.New("init: RAINDROP_SINK_URL is required: refusing to run against the SDK's default production endpoint")
	}
	opts = append(opts, raindrop.WithEndpoint(sink+"/v1/"))
	if projectID := os.Getenv("RAINDROP_PROJECT_ID"); projectID != "" {
		opts = append(opts, raindrop.WithProjectID(projectID))
	}
	client, err := raindrop.New(opts...)
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	d.client = client
	return nil
}

func appGitArg(args map[string]any) (raindrop.Option, error) {
	raw, ok := args["app_git"]
	if !ok {
		return nil, nil
	}
	if enabled, ok := raw.(bool); ok {
		if !enabled {
			return raindrop.WithAppGitDisabled(), nil
		}
		return nil, fmt.Errorf("arg \"app_git\": expected false or object, got true")
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("arg \"app_git\": expected false or object, got %T", raw)
	}
	known := map[string]bool{
		"commit_sha": true, "commit_dirty": true, "branch": true,
		"source_directory": true, "detect_branch": true, "auto_detect": true,
	}
	for key := range values {
		if !known[key] {
			return nil, fmt.Errorf("arg \"app_git\": unknown option %q", key)
		}
	}
	var config raindrop.AppGitOptions
	var err error
	if config.CommitSHA, err = stringArg(values, "commit_sha"); err != nil {
		return nil, err
	}
	if config.Branch, err = stringArg(values, "branch"); err != nil {
		return nil, err
	}
	if config.SourceDirectory, err = stringArg(values, "source_directory"); err != nil {
		return nil, err
	}
	for key, destination := range map[string]**bool{
		"commit_dirty":  &config.CommitDirty,
		"detect_branch": &config.DetectBranch,
		"auto_detect":   &config.AutoDetect,
	} {
		if value, present := values[key]; present {
			boolean, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("arg %q: expected boolean, got %T", key, value)
			}
			*destination = &boolean
		}
	}
	return raindrop.WithAppGit(config), nil
}

func (d *driver) stepFlush(ctx context.Context) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	return ignoreUndelivered(client.Flush(ctx))
}

func (d *driver) stepClose() error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	return ignoreUndelivered(client.Close())
}

// stepWait holds the process for args.ms milliseconds without an SDK call.
func stepWait(args map[string]any) error {
	ms, ok := args["ms"].(float64)
	if !ok || ms <= 0 {
		return fmt.Errorf("wait: arg \"ms\" must be a positive number, got %v", args["ms"])
	}
	time.Sleep(time.Duration(ms * float64(time.Millisecond)))
	return nil
}

// ignoreUndelivered drops ErrUndelivered: it is safe to ignore by contract
// (CONTRACT 1.2.1), the SDK has already warned, and the scenario asserts
// what reached the wire. Any other error still fails the step.
func ignoreUndelivered(err error) error {
	if errors.Is(err, raindrop.ErrUndelivered) {
		return nil
	}
	return err
}

// -- events ------------------------------------------------------------------

func (d *driver) stepTrack(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	return client.TrackEvent(ctx, raindrop.Event{
		EventID:     event.eventID,
		UserID:      event.userID,
		Event:       event.event,
		Timestamp:   event.timestamp,
		Properties:  event.properties,
		Attachments: event.attachments,
	})
}

func (d *driver) stepTrackAI(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	return client.TrackAI(ctx, raindrop.AIEvent{
		EventID:     event.eventID,
		UserID:      event.userID,
		Event:       event.event,
		Timestamp:   event.timestamp,
		Input:       event.input,
		Output:      event.output,
		Model:       event.model,
		ConvoID:     event.convoID,
		Properties:  event.properties,
		Attachments: event.attachments,
	})
}

func (d *driver) stepIdentify(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	userID, err := stringArg(args, "user_id")
	if err != nil {
		return err
	}
	traits, err := objectArg(args, "traits")
	if err != nil {
		return err
	}
	if traits == nil {
		// `traits` is required — an empty object is explicit and valid, but a
		// missing one is an authoring error the driver should not paper over.
		return fmt.Errorf("identify: missing required arg \"traits\"")
	}
	return client.Identify(ctx, raindrop.User{UserID: userID, Traits: traits})
}

func (d *driver) stepSignal(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	// The harness `signal` step's event_id/name land on signals/track as
	// event_id/signal_name (signal_type defaults server-convention "default")
	// via the public Client.TrackSignal surface.
	eventID, err := stringArg(args, "event_id")
	if err != nil {
		return err
	}
	name, err := stringArg(args, "name")
	if err != nil {
		return err
	}
	if eventID == "" || name == "" {
		return fmt.Errorf("signal: missing required arg %q", map[bool]string{true: "event_id", false: "name"}[eventID == ""])
	}
	sig := raindrop.Signal{EventID: eventID, Name: name}
	if v, err := stringArg(args, "signal_type"); err != nil {
		return err
	} else if v != "" {
		sig.Type = v
	}
	if v, err := stringArg(args, "sentiment"); err != nil {
		return err
	} else if v != "" {
		sig.Sentiment = v
	}
	if v, err := stringArg(args, "timestamp"); err != nil {
		return err
	} else if v != "" {
		ts, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return fmt.Errorf("signal: arg \"timestamp\" is not RFC3339: %w", perr)
		}
		sig.Timestamp = ts
	}
	if props, err := objectArg(args, "properties"); err != nil {
		return err
	} else if props != nil {
		sig.Properties = props
	}
	if v, err := stringArg(args, "attachment_id"); err != nil {
		return err
	} else if v != "" {
		sig.AttachmentID = v
	}
	if sig.Comment, err = stringArg(args, "comment"); err != nil {
		return err
	}
	if sig.After, err = stringArg(args, "after"); err != nil {
		return err
	}
	return client.TrackSignal(ctx, sig)
}

// -- partial (begin/patch/finish) lifecycle ---------------------------------

func (d *driver) stepBegin(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	if d.interaction != nil {
		return fmt.Errorf("begin: an interaction is already open")
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	d.interaction = client.Begin(ctx, raindrop.BeginOptions{
		EventID:     event.eventID,
		UserID:      event.userID,
		Event:       event.event,
		Timestamp:   event.timestamp,
		Input:       event.input,
		Model:       event.model,
		ConvoID:     event.convoID,
		Properties:  event.properties,
		Attachments: event.attachments,
	})
	return nil
}

func (d *driver) stepPatch(args map[string]any) error {
	if d.interaction == nil {
		return fmt.Errorf("patch: no open interaction")
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	return d.interaction.Patch(raindrop.PatchOptions{
		UserID:      event.userID,
		Event:       event.event,
		Timestamp:   event.timestamp,
		Input:       event.input,
		Output:      event.output,
		Model:       event.model,
		ConvoID:     event.convoID,
		Properties:  event.properties,
		Attachments: event.attachments,
	})
}

func (d *driver) stepFinish(args map[string]any) error {
	if d.interaction == nil {
		return fmt.Errorf("finish: no open interaction")
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	err = d.interaction.Finish(raindrop.FinishOptions{
		Timestamp:   event.timestamp,
		Output:      event.output,
		Model:       event.model,
		Properties:  event.properties,
		Attachments: event.attachments,
	})
	d.interaction = nil
	return err
}

func (d *driver) stepResume(args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	if d.interaction != nil {
		return fmt.Errorf("resume: an interaction is already open")
	}
	event, err := eventFields(args)
	if err != nil {
		return err
	}
	if event.eventID == "" {
		return fmt.Errorf("resume: missing required arg \"event_id\"")
	}
	// CONTRACT 9.2: bind to an event begun elsewhere by id. Nothing is sent
	// now; later patch/finish carry only what they (or this step) supply.
	d.interaction = client.Resume(event.eventID, raindrop.ResumeOptions{
		UserID:  event.userID,
		ConvoID: event.convoID,
		Event:   event.event,
	})
	return nil
}

func (d *driver) stepTrackTool(args map[string]any) error {
	if d.interaction == nil {
		return fmt.Errorf("track_tool: no open interaction")
	}
	name, err := stringArg(args, "name")
	if err != nil {
		return err
	}
	opts := raindrop.TrackToolOptions{Name: name}
	if value, present := args["input"]; present {
		opts.Input = value
	}
	if value, present := args["output"]; present {
		opts.Output = value
	}
	if message, err := stringArg(args, "error"); err != nil {
		return err
	} else if message != "" {
		opts.Error = errors.New(message)
	}
	for key, destination := range map[string]*time.Time{"start_time": &opts.StartTime, "end_time": &opts.EndTime} {
		raw, err := stringArg(args, key)
		if err != nil {
			return err
		}
		if raw == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return fmt.Errorf("track_tool: arg %q is not RFC3339: %w", key, err)
		}
		*destination = parsed
	}
	if value, present := args["duration_ms"]; present {
		ms, ok := value.(float64)
		if !ok {
			return fmt.Errorf("track_tool: arg \"duration_ms\" must be a number, got %T", value)
		}
		opts.Duration = time.Duration(ms * float64(time.Millisecond))
	}
	if parent, err := objectArg(args, "parent"); err != nil {
		return err
	} else if parent != nil {
		traceID, _ := parent["trace_id"].(string)
		spanID, _ := parent["span_id"].(string)
		opts.ParentContext = &raindrop.SpanContext{TraceID: traceID, SpanID: spanID}
	}
	d.interaction.TrackTool(opts)
	return nil
}

func (d *driver) stepDispatchSubagent(args map[string]any) error {
	if d.interaction == nil {
		return fmt.Errorf("dispatch_subagent: no open interaction")
	}
	name, err := stringArg(args, "name")
	if err != nil {
		return err
	}
	childEventID, err := stringArg(args, "child_event_id")
	if err != nil {
		return err
	}
	opts := raindrop.SubagentOptions{Name: name, ChildEventID: childEventID}
	if value, present := args["input"]; present {
		opts.Input = value
	}
	dispatch := d.interaction.Subagent(opts)
	if d.carriers == nil {
		d.carriers = make(map[string]map[string]string)
	}
	d.carriers[childEventID] = dispatch.Headers
	return nil
}

func (d *driver) stepRunSubagent(ctx context.Context, args map[string]any) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	if d.interaction != nil {
		return fmt.Errorf("run_subagent: the client has an open interaction")
	}
	var headers map[string]string
	if source, err := stringArg(args, "carrier_from"); err != nil {
		return err
	} else if source != "" {
		var ok bool
		if headers, ok = d.carriers[source]; !ok {
			return fmt.Errorf("run_subagent: no carrier was dispatched for %q", source)
		}
	} else {
		raw, err := objectArg(args, "headers")
		if err != nil {
			return err
		}
		headers = make(map[string]string, len(raw))
		for key, value := range raw {
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("run_subagent: header %q must be a string", key)
			}
			headers[key] = text
		}
	}
	userID, err := stringArg(args, "user_id")
	if err != nil {
		return err
	}
	output, err := stringArg(args, "output")
	if err != nil {
		return err
	}
	run := client.ResumeSubagent(ctx, headers, raindrop.ResumeSubagentOptions{UserID: userID})
	if toolName, err := stringArg(args, "tool_name"); err != nil {
		return err
	} else if toolName != "" {
		// The child's work: one tool span while the run is open, which
		// carries the hand-off link back to the launcher.
		run.Interaction.TrackTool(raindrop.TrackToolOptions{Name: toolName, Duration: time.Millisecond})
	}
	return run.Finish(raindrop.FinishOptions{Output: output})
}

// harnessLogger writes SDK log records to RAINDROP_HARNESS_LOG as JSON lines
// ({"level", "message"}) for the runner (CONTRACT 14). Without the variable
// the SDK logs nowhere, keeping the driver's stdout protocol clean.
func harnessLogger() *slog.Logger {
	path := os.Getenv("RAINDROP_HARNESS_LOG")
	if path == "" {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return slog.New(&harnessLogHandler{path: path})
}

type harnessLogHandler struct {
	path  string
	attrs []slog.Attr
}

func (h *harnessLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *harnessLogHandler) Handle(_ context.Context, record slog.Record) error {
	level := "info"
	switch {
	case record.Level >= slog.LevelError:
		level = "error"
	case record.Level >= slog.LevelWarn:
		level = "warn"
	case record.Level < slog.LevelInfo:
		level = "debug"
	}
	message := record.Message
	for _, attr := range h.attrs {
		message += " " + attr.String()
	}
	record.Attrs(func(attr slog.Attr) bool {
		message += " " + attr.String()
		return true
	})
	line, err := json.Marshal(map[string]string{"level": level, "message": message})
	if err != nil {
		return err
	}
	file, err := os.OpenFile(h.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(line, '\n'))
	return err
}

func (h *harnessLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &harnessLogHandler{path: h.path, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *harnessLogHandler) WithGroup(string) slog.Handler { return h }

// eventArgs is the union of the harness's event-shaped step args; each step
// handler forwards only the fields its SDK call accepts.
type eventArgs struct {
	eventID     string
	userID      string
	event       string
	input       string
	output      string
	model       string
	convoID     string
	timestamp   time.Time
	properties  map[string]any
	attachments []raindrop.Attachment
}

func eventFields(args map[string]any) (eventArgs, error) {
	var out eventArgs
	var err error
	for key, dst := range map[string]*string{
		"event_id": &out.eventID,
		"user_id":  &out.userID,
		"event":    &out.event,
		"input":    &out.input,
		"output":   &out.output,
		"model":    &out.model,
		"convo_id": &out.convoID,
	} {
		if *dst, err = stringArg(args, key); err != nil {
			return out, err
		}
	}
	if out.timestamp, err = timeArg(args); err != nil {
		return out, err
	}
	if out.properties, err = objectArg(args, "properties"); err != nil {
		return out, err
	}
	if out.attachments, err = attachmentsArg(args); err != nil {
		return out, err
	}
	return out, nil
}

// -- protocol ----------------------------------------------------------------

func describe() error {
	return json.NewEncoder(os.Stdout).Encode(describeOutput{
		SDKName:             sdkName,
		SDKVersion:          raindrop.Version,
		DriverVersion:       driverVersion,
		Protocol:            protocolMajor,
		Capabilities:        capabilities,
		NotApplicable:       notApplicable,
		NotApplicableReason: notApplicableReasons,
	})
}

func runSteps(raw []byte) int {
	var steps []map[string]map[string]any
	if err := json.Unmarshal(raw, &steps); err != nil {
		fmt.Fprintf(os.Stderr, "driver: invalid step array on stdin: %v\n", err)
		return 1
	}
	d := &driver{}
	ctx := context.Background()
	for index, step := range steps {
		if len(step) != 1 {
			fmt.Fprintf(os.Stderr, "driver: step %d: expected a single-key map, got %d keys\n", index, len(step))
			return 1
		}
		var name string
		var args map[string]any
		for k, v := range step {
			name, args = k, v
		}
		start := time.Now()
		if err := d.execute(ctx, name, args); err != nil {
			var unsupported errUnsupported
			if ok := errorAs(err, &unsupported); ok {
				fmt.Fprintf(os.Stdout, "unsupported:%s\n", unsupported.step)
				return 3
			}
			fmt.Fprintf(os.Stderr, "driver: step %d (%s): %v\n", index, name, err)
			return 1
		}
		elapsedMs := float64(time.Since(start)) / float64(time.Millisecond)
		line, err := json.Marshal(timingLine{Step: index, Ms: elapsedMs})
		if err != nil {
			fmt.Fprintf(os.Stderr, "driver: step %d (%s): encoding timing line: %v\n", index, name, err)
			return 1
		}
		fmt.Fprintln(os.Stdout, string(line))
	}
	return 0
}

// errorAs is errors.As specialized to errUnsupported (returned by value).
func errorAs(err error, target *errUnsupported) bool {
	u, ok := err.(errUnsupported)
	if ok {
		*target = u
	}
	return ok
}

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--describe" {
			if err := describe(); err != nil {
				fmt.Fprintf(os.Stderr, "driver: --describe: %v\n", err)
				os.Exit(1)
			}
			return
		}
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "driver: reading stdin: %v\n", err)
		os.Exit(1)
	}
	os.Exit(runSteps(raw))
}
