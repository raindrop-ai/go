package raindrop

import (
	"crypto/rand"
	"fmt"
	"time"
)

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func mergeMaps(base map[string]any, overlay map[string]any) map[string]any {
	switch {
	case len(base) == 0 && len(overlay) == 0:
		return nil
	case len(base) == 0:
		return cloneMap(overlay)
	case len(overlay) == 0:
		return cloneMap(base)
	}

	merged := cloneMap(base)
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func cloneAttachments(src []Attachment) []Attachment {
	if len(src) == 0 {
		return nil
	}
	dst := make([]Attachment, len(src))
	copy(dst, src)
	return dst
}

func iso8601Timestamp(at time.Time) string {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return at.UTC().Format(time.RFC3339Nano)
}

func optionalTimestamp(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339Nano)
}

func newEventID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16],
	), nil
}
