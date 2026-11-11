package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Outbound HTTP bounds: telemetry must never wedge the host app. Every
// attempt carries a finite deadline even when the caller injected an
// http.Client without a Timeout, and server-controlled Retry-After values
// are clamped so a misbehaving endpoint cannot park a flush goroutine.
const (
	// defaultAttemptTimeout bounds a single POST attempt when neither the
	// configured http.Client nor the caller's context imposes a deadline
	// (http.Client zero value has NO timeout).
	defaultAttemptTimeout = 30 * time.Second
	// maxRetryAfterDelay caps how long a server-provided Retry-After header
	// can delay the next attempt.
	maxRetryAfterDelay = 30 * time.Second
	// maxIngestResponseBytes bounds how much of a 2xx body is read to look
	// for ingest's rejected/warnings report.
	maxIngestResponseBytes = 1 << 20

	// projectIDHeader routes telemetry to a specific Raindrop project when set.
	projectIDHeader = "X-Raindrop-Project-Id"
)

// projectIDSlugPattern bounds a project_id to a DNS-label-style slug.
var projectIDSlugPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// normalizeProjectID trims and validates a configured project_id. An empty or
// whitespace-only value yields "" (no header is sent). An invalid value is
// dropped with a warning rather than risking an ingest-time HTTP 400, so a
// misconfigured project_id can never break telemetry shipping.
func normalizeProjectID(raw string, logger *slog.Logger) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if !projectIDSlugPattern.MatchString(trimmed) {
		if logger != nil {
			logger.Warn(
				"raindrop: ignoring invalid project_id; no X-Raindrop-Project-Id header will be sent",
				"project_id", trimmed,
				"pattern", projectIDSlugPattern.String(),
			)
		}
		return ""
	}
	return trimmed
}

type retryingHTTPClient struct {
	baseURL        string
	localBaseURL   string
	writeKey       string
	projectID      string
	client         *http.Client
	localClient    *http.Client
	debug          bool
	logger         *slog.Logger
	maxAttempts    int
	baseDelay      time.Duration
	jitterFraction float64
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error
	randomFloat    func() float64
	warnings       *warnLimiter

	// pausedUntil is when sending may resume after a 429 or 503 carrying
	// Retry-After: the server asked for less traffic, so every send waits,
	// not just the retry of the rejected request (CONTRACT 15.2).
	pauseMu     sync.Mutex
	pausedUntil time.Time
}

// unsentError reports that the caller's deadline passed while the payload
// was waiting to be sent: before its first attempt, during a retry delay, or
// during an overload pause. Nothing was on the wire, so the payload can stay
// queued (CONTRACT 1.2.1). A deadline that ends an attempt mid-request
// surfaces as the bare context error instead: that request may already have
// reached ingest, so it is not resent.
type unsentError struct {
	cause error
}

func (e *unsentError) Error() string {
	return "raindrop: deadline passed before the payload was sent: " + e.cause.Error()
}

func (e *unsentError) Unwrap() error { return e.cause }

// isUnsent reports whether err means the payload never went out (see unsentError).
func isUnsent(err error) bool {
	var unsent *unsentError
	return errors.As(err, &unsent)
}

// isPermanentRejection reports whether err is a non-retryable HTTP status:
// ingest received the payload and rejected it for good.
func isPermanentRejection(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && !isRetryableStatus(statusErr.StatusCode)
}

type httpStatusError struct {
	StatusCode int
	Status     string
	Body       string
	RetryAfter time.Duration
}

func (e *httpStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("raindrop: %s", e.Status)
	}
	return fmt.Sprintf("raindrop: %s: %s", e.Status, e.Body)
}

func newRetryingHTTPClient(cfg config, localBaseURL string) *retryingHTTPClient {
	var localClient *http.Client
	if localBaseURL != "" {
		localClient = &http.Client{Timeout: localMirrorTimeout}
	}
	return &retryingHTTPClient{
		baseURL:        cfg.endpoint,
		localBaseURL:   localBaseURL,
		writeKey:       cfg.writeKey,
		projectID:      cfg.projectID,
		client:         cfg.httpClient,
		localClient:    localClient,
		debug:          cfg.debug,
		logger:         cfg.logger,
		maxAttempts:    cfg.retryMaxAttempts,
		baseDelay:      cfg.retryBaseDelay,
		jitterFraction: cfg.retryJitterFraction,
		attemptTimeout: defaultAttemptTimeout,
		sleep:          sleepContext,
		randomFloat:    rand.Float64,
		warnings:       newWarnLimiter(),
	}
}

func (c *retryingHTTPClient) postJSON(ctx context.Context, path string, body any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	c.postLocalMirror(ctx, path, payload)

	if c.writeKey == "" {
		return nil
	}

	url := c.baseURL + strings.TrimPrefix(path, "/")
	var lastErr error

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if attempt > 1 {
			delay := c.retryDelay(attempt-1, lastErr)
			if delay > 0 {
				if err := c.sleep(ctx, delay); err != nil {
					return &unsentError{cause: err}
				}
			}
		}
		if err := c.waitForPause(ctx); err != nil {
			return &unsentError{cause: err}
		}
		if err := ctx.Err(); err != nil {
			return &unsentError{cause: err}
		}

		done, err := c.postOnce(ctx, path, url, payload)
		if done {
			return err
		}
		lastErr = err
	}

	if lastErr != nil {
		return lastErr
	}
	return nil
}

// waitForPause holds a send until an overload pause ends, bounded by ctx.
func (c *retryingHTTPClient) waitForPause(ctx context.Context) error {
	c.pauseMu.Lock()
	until := c.pausedUntil
	c.pauseMu.Unlock()
	if wait := time.Until(until); wait > 0 {
		return c.sleep(ctx, wait)
	}
	return nil
}

func (c *retryingHTTPClient) notePause(statusCode int, retryAfter time.Duration) {
	if retryAfter <= 0 || (statusCode != http.StatusTooManyRequests && statusCode != http.StatusServiceUnavailable) {
		return
	}
	until := time.Now().Add(min(retryAfter, maxRetryAfterDelay))
	c.pauseMu.Lock()
	if until.After(c.pausedUntil) {
		c.pausedUntil = until
	}
	c.pauseMu.Unlock()
}

// isRetryableStatus reports whether a non-2xx status is transient: 408 and
// 429 are the transient 4xx, every 5xx is retried, and every other 4xx is a
// permanent rejection that must not be retried (CONTRACT 3 and 15.2).
func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500
}

// postOnce performs a single POST attempt. done reports whether the retry
// loop should stop (success, permanent failure, or caller context done);
// when done is false the returned error is the retryable lastErr.
func (c *retryingHTTPClient) postOnce(ctx context.Context, path, url string, payload []byte) (done bool, err error) {
	// Bound the attempt even when the configured http.Client carries no
	// Timeout (the zero value is unbounded — a stalled connection would hang
	// the flush goroutine, and Close, forever). context deadlines compose,
	// so an earlier caller deadline still wins.
	attemptCtx := ctx
	if c.client.Timeout == 0 {
		var cancel context.CancelFunc
		attemptCtx, cancel = context.WithTimeout(ctx, c.attemptTimeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return true, err
	}
	req.Header.Set("Authorization", "Bearer "+c.writeKey)
	req.Header.Set("Content-Type", "application/json")
	c.setProjectIDHeader(req)

	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			// The CALLER's context ended; per-attempt timeouts surface as a
			// retryable error instead.
			return true, ctx.Err()
		}
		return false, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxIngestResponseBytes))
		_ = resp.Body.Close()
		c.reportIngestResponse(path, body)
		return true, nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	statusErr := &httpStatusError{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Body:       strings.TrimSpace(string(bodyBytes)),
		RetryAfter: parseRetryAfter(resp.Header),
	}

	c.notePause(resp.StatusCode, statusErr.RetryAfter)
	if !isRetryableStatus(resp.StatusCode) {
		c.warnings.warn(c.logger, path+".rejected",
			"raindrop: ingest rejected the payload (permanent); dropping it",
			"route", path, "status", resp.StatusCode, "body", statusErr.Body)
		return true, statusErr
	}

	return false, statusErr
}

// postLocalMirror runs synchronously but does not propagate errors: the
// 2s client timeout caps the worst-case latency added to every cloud POST,
// failures are surfaced only via the debug logger, and a cancelled caller
// context can't abort the mirror once we've decided to send it. This
// matches the Python SDK's _post_local_mirror semantics. Wall deadlines are
// honored, though: during Close the mirror obeys the remaining shutdown
// budget instead of adding up to 2s per queued payload past it.
func (c *retryingHTTPClient) postLocalMirror(ctx context.Context, path string, payload []byte) {
	if c.localBaseURL == "" || c.localClient == nil {
		return
	}
	if ctx.Err() != nil {
		c.debugMirror("local mirror skipped: context done", "error", ctx.Err())
		return
	}
	// Detach from the caller's cancellation but keep its deadline.
	mirrorCtx := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		mirrorCtx, cancel = context.WithDeadline(mirrorCtx, deadline)
		defer cancel()
	}
	url := c.localBaseURL + strings.TrimPrefix(path, "/")
	req, err := http.NewRequestWithContext(mirrorCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		c.debugMirror("build local mirror request failed", "error", err)
		return
	}
	if c.writeKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.writeKey)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setProjectIDHeader(req)
	resp, err := c.localClient.Do(req)
	if err != nil {
		c.debugMirror("local mirror POST failed", "error", err, "url", url)
		return
	}
	if resp.StatusCode >= 400 {
		c.debugMirror("local mirror POST returned non-2xx", "status", resp.StatusCode, "url", url)
	}
	_ = resp.Body.Close()
}

func (c *retryingHTTPClient) debugMirror(msg string, args ...any) {
	if !c.debug || c.logger == nil {
		return
	}
	c.logger.Debug(msg, args...)
}

// setProjectIDHeader attaches the project routing header when a project_id is
// set. Values are trimmed and validated once at New(), so projectID is empty
// here for blank or invalid input and no header is sent.
func (c *retryingHTTPClient) setProjectIDHeader(req *http.Request) {
	if c.projectID != "" {
		req.Header.Set(projectIDHeader, c.projectID)
	}
}

func (c *retryingHTTPClient) retryDelay(retryNumber int, previous error) time.Duration {
	delay := c.baseDelay << (retryNumber - 1)
	if c.jitterFraction > 0 {
		jitterMin := 1 - c.jitterFraction
		jitterMax := 1 + c.jitterFraction
		factor := jitterMin + (jitterMax-jitterMin)*c.randomFloat()
		delay = time.Duration(float64(delay) * factor)
	}
	if statusErr, ok := previous.(*httpStatusError); ok && statusErr.RetryAfter > 0 {
		// Wait at least what the server asked for, clamped: an arbitrary
		// Retry-After (hours, days) would otherwise park the flush goroutine.
		delay = max(delay, min(statusErr.RetryAfter, maxRetryAfterDelay))
	}
	return delay
}

func formatEndpoint(endpoint string) string {
	if endpoint == "" {
		return DefaultEndpoint
	}
	if strings.HasSuffix(endpoint, "/") {
		return endpoint
	}
	return endpoint + "/"
}

func parseRetryAfter(headers http.Header) time.Duration {
	value := headers.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		delay := time.Until(retryAt)
		if delay < 0 {
			return 0
		}
		return delay
	}
	return 0
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
