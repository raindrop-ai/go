package raindrop

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	DefaultEndpoint    = "https://api.raindrop.ai/v1/"
	defaultLibraryName = "raindrop-go"
	defaultServiceName = "raindrop.go-sdk"
	defaultEventName   = "ai_generation"

	// defaultFlushTimeout is the one deadline for Flush and Close
	// (CONTRACT 1.2.1): a dead or slow network must never wedge either.
	defaultFlushTimeout = 10 * time.Second
	// defaultCloseTimeout is the former name of defaultFlushTimeout.
	defaultCloseTimeout = defaultFlushTimeout
)

var ErrClosed = errors.New("raindrop: client closed")

type Client struct {
	transport         *retryingHTTPClient
	track             *trackQueue
	events            *eventBuffer
	traces            *traceBuffer
	logger            *slog.Logger
	debug             bool
	enabled           bool
	serviceName       string
	version           string
	contextData       map[string]any
	maxTextFieldChars int
	flushTimeout      time.Duration
	appGit            *appGitState

	closeOnce sync.Once
	closed    bool
	closeMu   sync.RWMutex

	interactions sync.Map
}

func New(opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	cfg.writeKey = strings.TrimSpace(cfg.writeKey)
	cfg.endpoint = formatEndpoint(cfg.endpoint)
	cfg.projectID = normalizeProjectID(cfg.projectID, cfg.logger)
	resolvedLocal := ResolveLocalWorkshopURL(cfg.localWorkshop, cfg.autoDetectLocal)

	client := &Client{
		logger:            cfg.logger,
		debug:             cfg.debug,
		enabled:           cfg.writeKey != "" || resolvedLocal != "",
		serviceName:       cfg.serviceName,
		version:           cfg.serviceVersion,
		maxTextFieldChars: cfg.maxTextFieldChars,
		flushTimeout:      cfg.flushTimeout,
		contextData: map[string]any{
			"library": map[string]any{
				"name":    cfg.libraryName,
				"version": cfg.libraryVersion,
			},
			"metadata": map[string]any{
				"goVersion": runtime.Version(),
				"goRuntime": runtime.Compiler,
			},
		},
	}
	if !client.enabled {
		cfg.appGit.enabled = false
	}
	client.appGit = newAppGitState(cfg.appGit)

	client.transport = newRetryingHTTPClient(cfg, resolvedLocal)
	client.track = newTrackQueue(client, cfg.partialFlushInterval, defaultTrackBatchSize, defaultTrackQueueSize)
	client.events = newEventBuffer(client, cfg.partialFlushInterval)
	client.traces = newTraceBuffer(client, cfg.traceFlushInterval, cfg.traceBatchSize, cfg.traceQueueSize)

	switch {
	case cfg.writeKey == "" && resolvedLocal == "":
		client.debugLog("writeKey not provided and no local Workshop URL resolved; telemetry shipping is disabled")
	case cfg.writeKey == "":
		client.debugLog("writeKey not provided; shipping locally only", "local_workshop_url", resolvedLocal)
	case resolvedLocal != "":
		client.debugLog("dual-shipping cloud + local Workshop", "local_workshop_url", resolvedLocal)
	}

	return client, nil
}

func (c *Client) Enabled() bool {
	return c != nil && c.enabled
}

// ErrUndelivered is wrapped by the error Flush and Close return when some
// telemetry enqueued before the call was not handed to ingest: the deadline
// passed, or retries ran out. It is safe to ignore: the SDK has already
// logged a warning, and nothing else changes (CONTRACT 1.2.1). A payload
// ingest rejected permanently (a 4xx other than 408 and 429) was delivered
// and is not reported here.
var ErrUndelivered = errors.New("raindrop: telemetry not delivered")

// Flush sends everything queued before it, including sends already in
// flight on the background goroutines, and returns once it was handed to
// ingest (CONTRACT 1.2). Without a deadline on ctx it is bounded by the
// flush timeout (WithFlushTimeout, default 10s), which each request's
// timeout is clamped to.
//
// When the deadline passes, payloads that were not sent yet, or were waiting
// on a retry or an overload pause, stay queued for the background sender; a
// request cut off mid-flight is dropped, since it may already have reached
// ingest. Flush then logs a warning and returns an error wrapping
// ErrUndelivered, which is safe to ignore.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.deadline())
		defer cancel()
	}

	return c.undelivered("flush", errors.Join(c.track.Flush(ctx), c.events.Flush(ctx), c.traces.Flush(ctx)))
}

// Close flushes pending telemetry and stops the background goroutines,
// under the same deadline as Flush (WithFlushTimeout, default 10s):
// typically called on the host's shutdown path, it must never wedge process
// exit on a dead or slow network. Once the deadline passes, in-flight sends
// are aborted and remaining payloads are dropped with a warning; the
// returned error wraps ErrUndelivered and is safe to ignore.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		c.closed = true
		c.closeMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), c.deadline())
		defer cancel()
		closeErr = c.undelivered("close", errors.Join(c.track.Stop(ctx), c.events.Stop(ctx), c.traces.Stop(ctx)))
	})
	return closeErr
}

// deadline is the one Flush/Close deadline (CONTRACT 1.2.1).
func (c *Client) deadline() time.Duration {
	if c.flushTimeout <= 0 {
		return defaultFlushTimeout
	}
	return c.flushTimeout
}

// undelivered logs and wraps a drain error, or returns nil.
func (c *Client) undelivered(call string, err error) error {
	if err == nil {
		return nil
	}
	c.warn(call+".undelivered",
		"raindrop: "+call+" returned before all telemetry was delivered", "error", err)
	return fmt.Errorf("%w: %w", ErrUndelivered, err)
}

func (c *Client) ensureOpen() error {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	return nil
}

func (c *Client) debugLog(msg string, args ...any) {
	if c == nil || !c.debug || c.logger == nil {
		return
	}
	c.logger.Debug(msg, args...)
}
