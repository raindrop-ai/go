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
	"os"
	"strings"
	"time"

	raindrop "github.com/raindrop-ai/go"
)

const (
	driverVersion = "1.0.0"
	protocolMajor = 1
	sdkName       = "go"
)

// Canonical capability keys (harness capabilities.yaml) this SDK supports.
//
// An omitted key is neither supported nor structurally not-applicable, so the
// runner skips scenarios that require it.
var (
	capabilities = []string{
		"events.track",
		"events.track_ai",
		// Factually true delivery mode: TrackEvent/TrackAI ship begin-style
		// to events/track_partial (DEV-1149) — declaring it runs the
		// wrap-*-partial scenarios against the route this SDK actually uses.
		"events.track_ai_partial",
		"events.track_partial",
		"identify",
		// `feature_flags` maps to the public FeatureFlags map on the
		// track/track_ai/begin/patch surfaces (DEV-1214): the SDK ships it as a
		// top-level `feature_flags` string→string object on its
		// events/track_partial body, matching dawn's ingest schema and the
		// raindrop-js core event-shipper wire key.
		"events.feature_flags",
		// `signal` maps to the public Client.TrackSignal surface (DEV-1201:
		// the capability went active once signal UUIDs populate on Query API
		// reads; signal scenarios are experimental until promoted).
		"signal",
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

// stringMapArg maps a harness object arg whose values are all strings onto a
// Go map[string]string (the shape of the SDK's public feature-flag surface).
// A non-string value is refused loudly rather than coerced (fleet
// non-negotiable: never silently drop or mangle a step arg).
func stringMapArg(args map[string]any, key string) (map[string]string, error) {
	v, ok := args[key]
	if !ok {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("arg %q: expected object, got %T", key, v)
	}
	out := make(map[string]string, len(m))
	for k, raw := range m {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("arg %q[%q]: expected string value, got %T", key, k, raw)
		}
		out[k] = s
	}
	return out, nil
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
		return d.stepInit()
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
	case "identify":
		return d.stepIdentify(ctx, args)
	case "signal":
		return d.stepSignal(ctx, args)
	case "flush":
		return d.stepFlush(ctx)
	case "close":
		return d.stepClose()
	default:
		return errUnsupported{step: name}
	}
}

// -- lifecycle -------------------------------------------------------------

func (d *driver) stepInit() error {
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
		raindrop.WithWriteKey(writeKey),
		// The harness measures the SDK↔sink exchange alone; a locally running
		// Workshop daemon must not be probed or dual-shipped to.
		raindrop.WithDisableLocalWorkshop(),
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

func (d *driver) stepFlush(ctx context.Context) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	return client.Flush(ctx)
}

func (d *driver) stepClose() error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	return client.Close()
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
		EventID:      event.eventID,
		UserID:       event.userID,
		Event:        event.event,
		Timestamp:    event.timestamp,
		Properties:   event.properties,
		Attachments:  event.attachments,
		FeatureFlags: event.featureFlags,
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
		EventID:      event.eventID,
		UserID:       event.userID,
		Event:        event.event,
		Timestamp:    event.timestamp,
		Input:        event.input,
		Output:       event.output,
		Model:        event.model,
		ConvoID:      event.convoID,
		Properties:   event.properties,
		Attachments:  event.attachments,
		FeatureFlags: event.featureFlags,
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
	// The public Signal struct carries no comment/after fields — refuse
	// loudly rather than silently drop (fleet non-negotiable).
	for _, unmappable := range []string{"comment", "after"} {
		if _, present := args[unmappable]; present {
			return fmt.Errorf("signal: arg %q is not mappable onto the public TrackSignal surface", unmappable)
		}
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
		EventID:      event.eventID,
		UserID:       event.userID,
		Event:        event.event,
		Timestamp:    event.timestamp,
		Input:        event.input,
		Model:        event.model,
		ConvoID:      event.convoID,
		Properties:   event.properties,
		Attachments:  event.attachments,
		FeatureFlags: event.featureFlags,
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
		UserID:       event.userID,
		Event:        event.event,
		Timestamp:    event.timestamp,
		Input:        event.input,
		Output:       event.output,
		Model:        event.model,
		ConvoID:      event.convoID,
		Properties:   event.properties,
		Attachments:  event.attachments,
		FeatureFlags: event.featureFlags,
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
		Timestamp:    event.timestamp,
		Output:       event.output,
		Model:        event.model,
		Properties:   event.properties,
		Attachments:  event.attachments,
		FeatureFlags: event.featureFlags,
	})
	d.interaction = nil
	return err
}

// eventArgs is the union of the harness's event-shaped step args; each step
// handler forwards only the fields its SDK call accepts.
type eventArgs struct {
	eventID      string
	userID       string
	event        string
	input        string
	output       string
	model        string
	convoID      string
	timestamp    time.Time
	properties   map[string]any
	attachments  []raindrop.Attachment
	featureFlags map[string]string
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
	if out.featureFlags, err = stringMapArg(args, "feature_flags"); err != nil {
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
