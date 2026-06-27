package raindrop

import (
	"context"
	"errors"
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

	// defaultCloseTimeout bounds Close's final flush so a dead or slow
	// network can never wedge the host process's exit path.
	defaultCloseTimeout = 10 * time.Second
)

var ErrClosed = errors.New("raindrop: client closed")

type Client struct {
	transport         *retryingHTTPClient
	events            *eventBuffer
	traces            *traceBuffer
	logger            *slog.Logger
	debug             bool
	enabled           bool
	serviceName       string
	version           string
	contextData       map[string]any
	maxTextFieldChars int
	closeTimeout      time.Duration

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
		closeTimeout:      cfg.closeTimeout,
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

	client.transport = newRetryingHTTPClient(cfg, resolvedLocal)
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

func (c *Client) Flush(ctx context.Context) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}

	if err := c.events.Flush(ctx); err != nil {
		return err
	}
	if err := c.traces.Flush(ctx); err != nil {
		return err
	}
	return nil
}

// Close flushes pending telemetry and stops the background goroutines,
// under a hard overall deadline (WithCloseTimeout, default 10s): typically
// called on the host's shutdown path, it must never wedge process exit on a
// dead or slow network. Once the deadline passes, in-flight sends are
// aborted and remaining payloads are dropped.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		c.closed = true
		c.closeMu.Unlock()

		timeout := c.closeTimeout
		if timeout <= 0 {
			timeout = defaultCloseTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		closeErr = errors.Join(c.events.Stop(ctx), c.traces.Stop(ctx))
	})
	return closeErr
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
