package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type retryingHTTPClient struct {
	baseURL        string
	writeKey       string
	client         *http.Client
	debug          bool
	maxAttempts    int
	baseDelay      time.Duration
	jitterFraction float64
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

func newRetryingHTTPClient(cfg config) *retryingHTTPClient {
	return &retryingHTTPClient{
		baseURL:        cfg.endpoint,
		writeKey:       cfg.writeKey,
		client:         cfg.httpClient,
		debug:          cfg.debug,
		maxAttempts:    cfg.retryMaxAttempts,
		baseDelay:      cfg.retryBaseDelay,
		jitterFraction: cfg.retryJitterFraction,
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

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.writeKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			_ = resp.Body.Close()
			return nil
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
			return statusErr
		}

		lastErr = statusErr
	}

	if lastErr != nil {
		return lastErr
	}
	return nil
}

func (c *retryingHTTPClient) retryDelay(retryNumber int, previous error) time.Duration {
	if statusErr, ok := previous.(*httpStatusError); ok && statusErr.RetryAfter > 0 {
		return statusErr.RetryAfter
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
