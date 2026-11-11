package raindrop

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxQuotedIngestItems bounds how many items one ingest warning quotes; the
// rest are counted.
const maxQuotedIngestItems = 3

type ingestIssue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

type ingestItem struct {
	Index   *int          `json:"index"`
	EventID string        `json:"event_id"`
	Issues  []ingestIssue `json:"issues"`
	Fields  []string      `json:"fields"`
	Message string        `json:"message"`
}

type ingestReport struct {
	Rejected []ingestItem `json:"rejected"`
	Warnings []ingestItem `json:"warnings"`
}

// reportIngestResponse warns about what ingest rejected or changed in an
// accepted (2xx) request. Rejected items were invalid and not stored;
// warnings list input accepted with changes, such as ignored fields. The
// request still counts as delivered (CONTRACT 7.1): an empty, non-JSON or
// unexpected body is ignored (CONTRACT 14.2).
func (c *retryingHTTPClient) reportIngestResponse(path string, body []byte) {
	if len(body) == 0 || c.logger == nil {
		return
	}
	var report ingestReport
	if err := json.Unmarshal(body, &report); err != nil {
		return
	}
	if len(report.Rejected) > 0 {
		c.warnings.warn(c.logger, path+".ingest_rejected",
			"raindrop: ingest rejected items, which were not stored",
			"route", path, "count", len(report.Rejected), "items", quoteIngestItems(report.Rejected, describeRejected))
	}
	if len(report.Warnings) > 0 {
		c.warnings.warn(c.logger, path+".ingest_warnings",
			"raindrop: ingest accepted items with changes",
			"route", path, "count", len(report.Warnings), "items", quoteIngestItems(report.Warnings, describeIngestWarning))
	}
}

func ingestItemLabel(item ingestItem) string {
	switch {
	case item.EventID != "":
		return "event " + item.EventID
	case item.Index != nil:
		return fmt.Sprintf("item %d", *item.Index)
	default:
		return "item"
	}
}

func describeRejected(item ingestItem) string {
	parts := make([]string, 0, len(item.Issues))
	for _, issue := range item.Issues {
		if issue.Path != "" {
			parts = append(parts, issue.Path+": "+issue.Message)
		} else {
			parts = append(parts, issue.Message)
		}
	}
	detail := strings.Join(parts, "; ")
	if detail == "" {
		detail = "invalid"
	}
	return ingestItemLabel(item) + " (" + detail + ")"
}

func describeIngestWarning(item ingestItem) string {
	if item.Message != "" {
		return ingestItemLabel(item) + ": " + item.Message
	}
	if len(item.Fields) > 0 {
		return ingestItemLabel(item) + ": ignored " + strings.Join(item.Fields, ", ")
	}
	return ingestItemLabel(item) + ": accepted with changes"
}

func quoteIngestItems(items []ingestItem, describe func(ingestItem) string) string {
	n := min(len(items), maxQuotedIngestItems)
	quoted := make([]string, 0, n)
	for _, item := range items[:n] {
		quoted = append(quoted, describe(item))
	}
	out := strings.Join(quoted, " | ")
	if len(items) > n {
		out += fmt.Sprintf(" (+%d more)", len(items)-n)
	}
	return out
}
