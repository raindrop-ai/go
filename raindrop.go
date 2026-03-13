package raindrop

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
)

const (
	DefaultEndpoint    = "https://api.raindrop.ai/v1/"
	defaultLibraryName = "raindrop-go"
	defaultServiceName = "raindrop.go-sdk"
	defaultEventName   = "ai_generation"
)

var ErrClosed = errors.New("raindrop: client closed")

type Client struct {
	transport   *retryingHTTPClient
	events      *eventBuffer
	traces      *traceBuffer
	logger      *slog.Logger
	debug       bool
	enabled     bool
	serviceName string
	version     string
	contextData map[string]any

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

	client := &Client{
		logger:      cfg.logger,
		debug:       cfg.debug,
		enabled:     cfg.writeKey != "",
		serviceName: cfg.serviceName,
		version:     cfg.serviceVersion,
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

	client.transport = newRetryingHTTPClient(cfg)
	client.events = newEventBuffer(client, cfg.partialFlushInterval)
	client.traces = newTraceBuffer(client, cfg.traceFlushInterval, cfg.traceBatchSize, cfg.traceQueueSize)

	if !client.enabled {
		client.debugLog("writeKey not provided; telemetry shipping is disabled")
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

func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		c.closed = true
		c.closeMu.Unlock()

		ctx := context.Background()
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
