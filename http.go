package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
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
)

type retryingHTTPClient struct {
	baseURL        string
	localBaseURL   string
	writeKey       string
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
					return err
				}
			}
		}

		done, err := c.postOnce(ctx, url, payload)
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

// postOnce performs a single POST attempt. done reports whether the retry
// loop should stop (success, permanent failure, or caller context done);
// when done is false the returned error is the retryable lastErr.
func (c *retryingHTTPClient) postOnce(ctx context.Context, url string, payload []byte) (done bool, err error) {
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
		_ = resp.Body.Close()
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

	if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
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

func (c *retryingHTTPClient) retryDelay(retryNumber int, previous error) time.Duration {
	if statusErr, ok := previous.(*httpStatusError); ok && statusErr.RetryAfter > 0 {
		// Clamp server-controlled values: an arbitrary Retry-After (hours,
		// days) would otherwise park the flush goroutine for that long.
		return min(statusErr.RetryAfter, maxRetryAfterDelay)
	}

	delay := c.baseDelay << (retryNumber - 1)
	if c.jitterFraction <= 0 {
		return delay
	}

	jitterMin := 1 - c.jitterFraction
	jitterMax := 1 + c.jitterFraction
	factor := jitterMin + (jitterMax-jitterMin)*c.randomFloat()
	return time.Duration(float64(delay) * factor)
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
