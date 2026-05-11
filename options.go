package raindrop

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Option func(*config) error

type config struct {
	writeKey             string
	endpoint             string
	localWorkshop        LocalWorkshopConfig
	autoDetectLocal      bool
	debug                bool
	httpClient           *http.Client
	partialFlushInterval time.Duration
	traceFlushInterval   time.Duration
	traceBatchSize       int
	traceQueueSize       int
	serviceName          string
	serviceVersion       string
	libraryName          string
	libraryVersion       string
	logger               *slog.Logger
	retryMaxAttempts     int
	retryBaseDelay       time.Duration
	retryJitterFraction  float64
}

func defaultConfig() config {
	return config{
		endpoint:             DefaultEndpoint,
		localWorkshop:        LocalWorkshopConfig{Inherit: true},
		autoDetectLocal:      true,
		httpClient:           &http.Client{Timeout: 15 * time.Second},
		partialFlushInterval: time.Second,
		traceFlushInterval:   time.Second,
		traceBatchSize:       50,
		traceQueueSize:       5000,
		serviceName:          defaultServiceName,
		serviceVersion:       Version,
		libraryName:          defaultLibraryName,
		libraryVersion:       Version,
		logger:               slog.Default(),
		retryMaxAttempts:     3,
		retryBaseDelay:       time.Second,
		retryJitterFraction:  0.2,
	}
}

func WithWriteKey(writeKey string) Option {
	return func(cfg *config) error {
		cfg.writeKey = writeKey
		return nil
	}
}

func WithEndpoint(endpoint string) Option {
	return func(cfg *config) error {
		cfg.endpoint = formatEndpoint(endpoint)
		return nil
	}
}

// WithLocalWorkshopURL pins the local Workshop daemon URL, suppressing env
// vars and the auto-detect probe. Pass an empty string to revert to the
// inherit-from-env default behavior.
func WithLocalWorkshopURL(url string) Option {
	return func(cfg *config) error {
		trimmed := strings.TrimSpace(url)
		if trimmed == "" {
			cfg.localWorkshop = LocalWorkshopConfig{Inherit: true}
			return nil
		}
		cfg.localWorkshop = LocalWorkshopConfig{URL: trimmed}
		return nil
	}
}

// WithDisableLocalWorkshop opts out of the local mirror entirely, even if
// `RAINDROP_LOCAL_DEBUGGER` / `RAINDROP_WORKSHOP` is set or a daemon is
// listening on the default port.
func WithDisableLocalWorkshop() Option {
	return func(cfg *config) error {
		cfg.localWorkshop = LocalWorkshopConfig{Disabled: true}
		return nil
	}
}

func WithDebug(debug bool) Option {
	return func(cfg *config) error {
		cfg.debug = debug
		return nil
	}
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(cfg *config) error {
		if httpClient != nil {
			cfg.httpClient = httpClient
		}
		return nil
	}
}

func WithPartialFlushInterval(interval time.Duration) Option {
	return func(cfg *config) error {
		if interval > 0 {
			cfg.partialFlushInterval = interval
		}
		return nil
	}
}

func WithTraceFlushInterval(interval time.Duration) Option {
	return func(cfg *config) error {
		if interval > 0 {
			cfg.traceFlushInterval = interval
		}
		return nil
	}
}

func WithTraceBatchSize(size int) Option {
	return func(cfg *config) error {
		if size > 0 {
			cfg.traceBatchSize = size
		}
		return nil
	}
}

func WithTraceQueueSize(size int) Option {
	return func(cfg *config) error {
		if size > 0 {
			cfg.traceQueueSize = size
		}
		return nil
	}
}

func WithServiceName(name string) Option {
	return func(cfg *config) error {
		if name != "" {
			cfg.serviceName = name
		}
		return nil
	}
}

func WithServiceVersion(version string) Option {
	return func(cfg *config) error {
		if version != "" {
			cfg.serviceVersion = version
			cfg.libraryVersion = version
		}
		return nil
	}
}

func WithLibraryVersion(version string) Option {
	return func(cfg *config) error {
		if version != "" {
			cfg.libraryVersion = version
		}
		return nil
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(cfg *config) error {
		if logger != nil {
			cfg.logger = logger
		}
		return nil
	}
}
