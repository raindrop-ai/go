package raindrop

import (
	"net"
	"testing"
)

func TestResolveExplicitURLWins(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://from-env:9999/v1/")
	t.Setenv(WorkshopEnvVar, "1")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{URL: "http://kwarg:8888/v1/"}, false)
	if got != "http://kwarg:8888/v1/" {
		t.Fatalf("expected explicit URL, got %q", got)
	}
}

func TestResolveExplicitURLAppendsTrailingSlash(t *testing.T) {
	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{URL: "http://kwarg:8888/v1"}, false)
	if got != "http://kwarg:8888/v1/" {
		t.Fatalf("expected trailing slash appended, got %q", got)
	}
}

func TestResolveExplicitDisabledOptsOut(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://from-env:9999/v1/")
	t.Setenv(WorkshopEnvVar, "1")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Disabled: true}, true)
	if got != "" {
		t.Fatalf("expected disabled to opt out, got %q", got)
	}
}

func TestResolveLocalDebuggerEnv(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://from-env:9999/v1/")
	t.Setenv(WorkshopEnvVar, "")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
	if got != "http://from-env:9999/v1/" {
		t.Fatalf("expected env URL, got %q", got)
	}
}

func TestResolveWorkshopEnvURL(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "http://workshop:7777/v1/")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
	if got != "http://workshop:7777/v1/" {
		t.Fatalf("expected workshop env URL, got %q", got)
	}
}

func TestResolveWorkshopEnvTruthyUsesDefault(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	for _, value := range []string{"1", "true", "True", "yes", "on"} {
		t.Setenv(WorkshopEnvVar, value)
		got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
		if got != DefaultLocalWorkshopURL {
			t.Fatalf("workshop env %q: expected default URL, got %q", value, got)
		}
	}
}

func TestResolveWorkshopEnvFalsyDisables(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	for _, value := range []string{"0", "false", "no", "off"} {
		t.Setenv(WorkshopEnvVar, value)
		got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, true)
		if got != "" {
			t.Fatalf("workshop env %q: expected disabled, got %q", value, got)
		}
	}
}

func TestResolveLocalDebuggerBeatsWorkshopEnv(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "http://specific:1111/v1/")
	t.Setenv(WorkshopEnvVar, "1")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
	if got != "http://specific:1111/v1/" {
		t.Fatalf("expected specific env to win, got %q", got)
	}
}

func TestResolveNoURLNoEnvNoProbe(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
	if got != "" {
		t.Fatalf("expected empty when nothing set, got %q", got)
	}
}

func TestResolveInvalidExplicitURLReturnsEmpty(t *testing.T) {
	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{URL: "not a url"}, false)
	if got != "" {
		t.Fatalf("expected invalid URL to resolve to empty, got %q", got)
	}
}

func TestResolveInvalidEnvURLFallsThrough(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "ftp://nope")
	t.Setenv(WorkshopEnvVar, "")

	got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, false)
	if got != "" {
		t.Fatalf("expected non-http env URL to resolve to empty, got %q", got)
	}
}

func TestProbeFindsListener(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	original := probePort
	probePort = port
	defer func() { probePort = original }()

	if !probeDefaultWorkshop() {
		t.Fatalf("expected probe to find listener on %s", port)
	}
	if got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, true); got != DefaultLocalWorkshopURL {
		t.Fatalf("expected probe hit to resolve to default URL, got %q", got)
	}
}

func TestProbeMissesWhenNothingListens(t *testing.T) {
	t.Setenv(LocalDebuggerEnvVar, "")
	t.Setenv(WorkshopEnvVar, "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("split host port: %v", err)
	}
	_ = listener.Close()

	original := probePort
	probePort = port
	defer func() { probePort = original }()

	if probeDefaultWorkshop() {
		t.Fatalf("expected probe to miss closed port %s", port)
	}
	if got := ResolveLocalWorkshopURL(LocalWorkshopConfig{Inherit: true}, true); got != "" {
		t.Fatalf("expected probe miss to resolve to empty, got %q", got)
	}
}
