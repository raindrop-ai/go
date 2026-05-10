package raindrop

import (
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	LocalDebuggerEnvVar     = "RAINDROP_LOCAL_DEBUGGER"
	WorkshopEnvVar          = "RAINDROP_WORKSHOP"
	DefaultLocalWorkshopURL = "http://localhost:5899/v1/"
)

const (
	probeHost          = "127.0.0.1"
	probeTimeout       = 100 * time.Millisecond
	localMirrorTimeout = 2 * time.Second
)

// probePort is var-not-const so tests can point the probe at an ephemeral
// listener without needing a real Workshop daemon on 5899.
var probePort = "5899"

// LocalWorkshopConfig represents the three states a caller can put the
// builder in: inherit (fall through to env + auto-detect), opt-out, or
// pin to an explicit URL.
type LocalWorkshopConfig struct {
	Inherit  bool
	Disabled bool
	URL      string
}

// ResolveLocalWorkshopURL applies the cross-language precedence rules and
// returns the URL to mirror cloud POSTs to ("" when local is disabled).
//
//	1. explicit URL (cfg.URL)
//	2. explicit opt-out (cfg.Disabled)
//	3. RAINDROP_LOCAL_DEBUGGER env var
//	4. RAINDROP_WORKSHOP env var (URL or boolean)
//	5. TCP probe of 127.0.0.1:5899 when autoDetect
//	6. ""
func ResolveLocalWorkshopURL(cfg LocalWorkshopConfig, autoDetect bool) string {
	if cfg.Disabled {
		return ""
	}
	if !cfg.Inherit && cfg.URL != "" {
		return formatLocalWorkshopURL(cfg.URL)
	}

	if envURL := strings.TrimSpace(os.Getenv(LocalDebuggerEnvVar)); envURL != "" {
		if formatted := formatLocalWorkshopURL(envURL); formatted != "" {
			return formatted
		}
	}

	switch v := readWorkshopEnv(); v.kind {
	case workshopEnvDisable:
		return ""
	case workshopEnvEnable:
		return DefaultLocalWorkshopURL
	case workshopEnvURL:
		if formatted := formatLocalWorkshopURL(v.url); formatted != "" {
			return formatted
		}
	}

	if autoDetect && probeDefaultWorkshop() {
		return DefaultLocalWorkshopURL
	}

	return ""
}

type workshopEnvKind int

const (
	workshopEnvNone workshopEnvKind = iota
	workshopEnvEnable
	workshopEnvDisable
	workshopEnvURL
)

type workshopEnvValue struct {
	kind workshopEnvKind
	url  string
}

func readWorkshopEnv() workshopEnvValue {
	raw := strings.TrimSpace(os.Getenv(WorkshopEnvVar))
	if raw == "" {
		return workshopEnvValue{kind: workshopEnvNone}
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return workshopEnvValue{kind: workshopEnvURL, url: raw}
	}
	switch lower {
	case "1", "true", "yes", "on":
		return workshopEnvValue{kind: workshopEnvEnable}
	case "0", "false", "no", "off":
		return workshopEnvValue{kind: workshopEnvDisable}
	}
	return workshopEnvValue{kind: workshopEnvNone}
}

// formatLocalWorkshopURL returns a normalized URL with a trailing slash, or
// "" for inputs that are not http(s) URLs (so junk env vars no-op rather
// than silently sending events to garbage hosts).
func formatLocalWorkshopURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Host == "" {
		return ""
	}
	if !strings.HasSuffix(trimmed, "/") {
		trimmed += "/"
	}
	return trimmed
}

// probeDefaultWorkshop opens a brief TCP connection to the daemon's port.
// Workshop's port is non-standard, so a listener on it is overwhelmingly
// likely to be the daemon; false positives just produce a fire-and-forget
// POST that never affects the cloud path.
func probeDefaultWorkshop() bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(probeHost, probePort), probeTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
