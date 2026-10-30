package raindrop

import (
	"log/slog"
	"sync"
	"time"
)

// rateLimitedWarnInterval caps each distinct warning family to one log line
// per interval: failure paths fire per event or per batch, and under
// sustained failure (dead network, bad key) they would otherwise flood the
// host's logs.
const rateLimitedWarnInterval = 30 * time.Second

type warnLimiter struct {
	mu   sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}

func newWarnLimiter() *warnLimiter {
	return &warnLimiter{last: make(map[string]time.Time), now: time.Now}
}

// warn logs msg at Warn level unless the same key fired within the interval.
// Warnings are never gated on debug mode: they mean telemetry was lost or
// changed (CONTRACT 14.2).
func (l *warnLimiter) warn(logger *slog.Logger, key, msg string, args ...any) {
	if l == nil || logger == nil {
		return
	}
	l.mu.Lock()
	now := l.now()
	if last, ok := l.last[key]; ok && now.Sub(last) < rateLimitedWarnInterval {
		l.mu.Unlock()
		return
	}
	l.last[key] = now
	l.mu.Unlock()
	logger.Warn(msg, args...)
}

func (c *Client) warn(key, msg string, args ...any) {
	if c == nil || c.transport == nil {
		return
	}
	c.transport.warnings.warn(c.logger, key, msg, args...)
}
