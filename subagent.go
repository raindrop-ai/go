package raindrop

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strings"
)

// Detached sub-agent hand-off (CONTRACT 13). A detached sub-agent is work
// launched from one event that reports as its own event, possibly in another
// process or service. Nothing is nested: the launcher records a dispatch
// span, the child its own event and trace, and the two are joined by
// attributes and a carrier passed with the launch.

const (
	// HandoffHeader carries the hand-off fields in W3C baggage form. Send it
	// with the "baggage" and "traceparent" headers the carrier also holds.
	HandoffHeader = "x-raindrop-handoff"

	carrierEventID        = "raindrop-event-id"
	carrierChildEventID   = "raindrop-child-event-id"
	carrierName           = "raindrop-handoff-name"
	carrierConvoID        = "raindrop-convo-id"
	carrierUserID         = "raindrop-user-id"
	carrierTraceID        = "raindrop-trace-id"
	carrierDispatchSpanID = "raindrop-dispatch-span-id"

	handoffModeAttribute          = "raindrop.handoff.mode"
	handoffChildEventIDAttribute  = "raindrop.handoff.childEventId"
	handoffNameAttribute          = "raindrop.handoff.name"
	handoffParentEventIDAttribute = "raindrop.handoff.parentEventId"
	handoffParentSpanIDAttribute  = "raindrop.handoff.parentSpanId"
	agentRoleAttribute            = "raindrop.agent.role"

	defaultDispatchToolName = "dispatch_subagent"
	defaultSubagentName     = "subagent"
)

// SubagentOptions describes a detached sub-agent launch.
type SubagentOptions struct {
	// Name is the sub-agent's name, shown on the launcher's event.
	Name string
	// ChildEventID is the child's own event id. It must exist before the
	// launch so the link resolves while the child is queued; one is minted
	// when empty (a job id works too).
	ChildEventID string
	// Input is what the sub-agent was asked to do, recorded on the dispatch.
	Input any
	// ToolName names the dispatch span; defaults to "dispatch_subagent".
	ToolName string
}

// SubagentDispatch is a recorded launch: the child's event id and the carrier
// headers to pass to it (x-raindrop-handoff, baggage and traceparent).
type SubagentDispatch struct {
	ChildEventID string
	Headers      map[string]string
}

// Subagent records a detached sub-agent dispatch on the interaction and
// returns the carrier for the child (CONTRACT 13.1).
func (i *Interaction) Subagent(opts SubagentOptions) SubagentDispatch {
	childEventID := opts.ChildEventID
	if childEventID == "" {
		childEventID, _ = newEventID()
	}
	name := opts.Name
	if name == "" {
		name = defaultSubagentName
	}
	dispatch := SubagentDispatch{ChildEventID: childEventID}
	if i == nil || i.client == nil {
		return dispatch
	}
	toolName := opts.ToolName
	if toolName == "" {
		toolName = defaultDispatchToolName
	}

	tool := i.StartToolSpan(toolName, ToolOptions{Input: opts.Input})
	tool.span.SetAttributes(
		StringAttr(handoffModeAttribute, "detached"),
		StringAttr(handoffChildEventIDAttribute, childEventID),
		StringAttr(handoffNameAttribute, name),
	)
	// The dispatch's result is the job handle; the child's answer belongs to
	// the child's own event.
	tool.SetOutput(map[string]any{"jobId": childEventID, "status": "accepted"})
	traceID, spanID := hexIDs(tool.span.ids)
	tool.End()

	fields := [][2]string{
		{carrierEventID, i.eventID},
		{carrierChildEventID, childEventID},
		{carrierName, name},
	}
	if sticky, ok := i.client.events.stickyFor(i.eventID); ok {
		if sticky.ConvoID != "" {
			fields = append(fields, [2]string{carrierConvoID, sticky.ConvoID})
		}
		if sticky.UserID != "" {
			fields = append(fields, [2]string{carrierUserID, sticky.UserID})
		}
	}
	if traceID != "" && spanID != "" {
		fields = append(fields, [2]string{carrierTraceID, traceID}, [2]string{carrierDispatchSpanID, spanID})
	}
	baggage := encodeBaggage(fields)
	dispatch.Headers = map[string]string{HandoffHeader: baggage, "baggage": baggage}
	if traceID != "" && spanID != "" {
		dispatch.Headers["traceparent"] = "00-" + traceID + "-" + spanID + "-01"
	}
	return dispatch
}

// ResumeSubagentOptions are the child's own event fields. Values the carrier
// names win; these fill what it leaves out.
type ResumeSubagentOptions struct {
	UserID  string
	Event   string
	Input   string
	ConvoID string
}

// SubagentRun is a detached sub-agent's own run: its event (Interaction),
// whose every span carries the reverse link to the launcher.
type SubagentRun struct {
	Interaction *Interaction
}

// Finish closes the child's event.
func (r *SubagentRun) Finish(opts FinishOptions) error {
	if r == nil {
		return nil
	}
	return r.Interaction.Finish(opts)
}

// ResumeSubagent opens a detached sub-agent's own event from the carrier it
// was launched with (CONTRACT 13.2): headers from Subagent, from any SDK. A
// missing or unreadable carrier never fails the work: the child runs as an
// unlinked event.
func (c *Client) ResumeSubagent(ctx context.Context, headers map[string]string, opts ResumeSubagentOptions) *SubagentRun {
	carrier := parseCarrier(headers)
	eventID := carrier[carrierChildEventID]
	if eventID == "" {
		eventID, _ = newEventID()
	}
	userID := firstNonEmpty(carrier[carrierUserID], opts.UserID)
	convoID := firstNonEmpty(carrier[carrierConvoID], opts.ConvoID)
	name := firstNonEmpty(carrier[carrierName], defaultSubagentName)
	interaction := c.Begin(ctx, BeginOptions{
		EventID: eventID,
		UserID:  userID,
		Event:   firstNonEmpty(opts.Event, name),
		Input:   opts.Input,
		ConvoID: convoID,
	})
	if parent := carrier[carrierEventID]; parent != "" {
		attrs := []Attribute{
			StringAttr(handoffParentEventIDAttribute, parent),
			StringAttr(handoffNameAttribute, name),
			StringAttr(agentRoleAttribute, "subagent"),
		}
		if spanID := carrier[carrierDispatchSpanID]; spanID != "" {
			attrs = append(attrs, StringAttr(handoffParentSpanIDAttribute, spanID))
		}
		interaction.spanAttributes = attrs
	}
	return &SubagentRun{Interaction: interaction}
}

// parseCarrier reads the hand-off fields from x-raindrop-handoff, else from
// baggage, filling trace ids from traceparent when the baggage has none.
// Header names match case-insensitively.
func parseCarrier(headers map[string]string) map[string]string {
	lookup := make(map[string]string, len(headers))
	for key, value := range headers {
		lookup[strings.ToLower(key)] = value
	}
	fields := decodeBaggage(lookup[HandoffHeader])
	if fields[carrierEventID] == "" {
		fields = decodeBaggage(lookup["baggage"])
	}
	if parts := strings.Split(lookup["traceparent"], "-"); len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		if fields[carrierTraceID] == "" {
			fields[carrierTraceID] = parts[1]
		}
		if fields[carrierDispatchSpanID] == "" {
			fields[carrierDispatchSpanID] = parts[2]
		}
	}
	return fields
}

func encodeBaggage(fields [][2]string) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		if field[1] == "" {
			continue
		}
		parts = append(parts, field[0]+"="+url.PathEscape(field[1]))
	}
	return strings.Join(parts, ",")
}

func decodeBaggage(header string) map[string]string {
	out := map[string]string{}
	for _, member := range strings.Split(header, ",") {
		member = strings.TrimSpace(member)
		if semi := strings.IndexByte(member, ';'); semi >= 0 {
			member = member[:semi]
		}
		key, value, ok := strings.Cut(member, "=")
		if !ok || !strings.HasPrefix(strings.TrimSpace(key), "raindrop-") {
			continue
		}
		if decoded, err := url.PathUnescape(strings.TrimSpace(value)); err == nil {
			out[strings.TrimSpace(key)] = decoded
		}
	}
	return out
}

func hexIDs(ids spanIDs) (string, string) {
	traceID, err := base64.StdEncoding.DecodeString(ids.TraceIDB64)
	if err != nil || len(traceID) != 16 {
		return "", ""
	}
	spanID, err := base64.StdEncoding.DecodeString(ids.SpanIDB64)
	if err != nil || len(spanID) != 8 {
		return "", ""
	}
	return hex.EncodeToString(traceID), hex.EncodeToString(spanID)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
