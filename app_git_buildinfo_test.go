package raindrop

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCompiledApplicationBuildInfoWinsOverAmbientDeployment(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("real Git executable unavailable: %v", err)
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("real Go compiler unavailable: %v", err)
	}
	sdkDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("resolve SDK directory: %v", err)
	}
	sdkDirectory, err = filepath.Abs(sdkDirectory)
	if err != nil {
		t.Fatalf("absolute SDK directory: %v", err)
	}

	applicationDirectory := filepath.Join(t.TempDir(), "application")
	if err := os.MkdirAll(applicationDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	module := "module example.com/compiled-aut\n\ngo 1.21\n\nrequire github.com/raindrop-ai/go v0.0.0\n\nreplace github.com/raindrop-ai/go => " + strconv.Quote(filepath.ToSlash(sdkDirectory)) + "\n"
	if err := os.WriteFile(filepath.Join(applicationDirectory, "go.mod"), []byte(module), 0o600); err != nil {
		t.Fatal(err)
	}
	mainSource := `package main

import (
	"context"
	"os"

	raindrop "github.com/raindrop-ai/go"
)

func main() {
	client, err := raindrop.New(
		raindrop.WithWriteKey("rk_test"),
		raindrop.WithEndpoint(os.Getenv("TEST_ENDPOINT")),
		raindrop.WithDisableLocalWorkshop(),
	)
	if err != nil {
		panic(err)
	}
	if err := client.TrackEvent(context.Background(), raindrop.Event{
		EventID: "compiled-aut",
		UserID:  "test-user",
		Event:   "build_info_test",
	}); err != nil {
		panic(err)
	}
	if err := client.Close(); err != nil {
		panic(err)
	}
}
`
	if err := os.WriteFile(filepath.Join(applicationDirectory, "main.go"), []byte(mainSource), 0o600); err != nil {
		t.Fatal(err)
	}

	baseEnvironment := buildInfoTestEnvironment(sanitizedGitEnvironment(os.Environ()), nil)
	runBuildInfoCommand(t, 10*time.Second, applicationDirectory, baseEnvironment, git, "init")
	runBuildInfoCommand(t, 10*time.Second, applicationDirectory, baseEnvironment, git, "add", "go.mod", "main.go")
	runBuildInfoCommand(t, 10*time.Second, applicationDirectory, baseEnvironment, git,
		"-c", "user.name=Raindrop", "-c", "user.email=test@example.com", "commit", "-m", "compiled application")
	applicationSHA := strings.TrimSpace(runBuildInfoCommand(t, 10*time.Second, applicationDirectory, baseEnvironment, git, "rev-parse", "HEAD"))
	sdkSHA := strings.TrimSpace(runBuildInfoCommand(t, 10*time.Second, sdkDirectory, baseEnvironment, git, "rev-parse", "HEAD"))
	if applicationSHA == sdkSHA {
		t.Fatalf("fixture application commit unexpectedly equals SDK commit %s", sdkSHA)
	}

	executable := filepath.Join(t.TempDir(), "compiled-aut")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	goEnvironment := buildInfoTestEnvironment(baseEnvironment, map[string]string{
		"GOPROXY":     "off",
		"GOSUMDB":     "off",
		"GOWORK":      "off",
		"GOTOOLCHAIN": "local",
	})
	runBuildInfoCommand(t, 30*time.Second, applicationDirectory, goEnvironment, goTool, "build", "-buildvcs=true", "-o", executable, ".")

	type capturedEvent struct {
		Properties map[string]any `json:"properties"`
	}
	captured := make(chan capturedEvent, 1)
	requestErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/events/track_partial" {
			select {
			case requestErrors <- fmt.Errorf("unexpected path %q", request.URL.Path):
			default:
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var event capturedEvent
		if err := json.NewDecoder(request.Body).Decode(&event); err != nil {
			select {
			case requestErrors <- err:
			default:
			}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		captured <- event
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	observerDeploymentSHA := strings.Repeat("b", 40)
	observerCISHA := strings.Repeat("c", 40)
	childEnvironment := buildInfoTestEnvironment(goEnvironment, map[string]string{
		"TEST_ENDPOINT":              server.URL + "/",
		"VERCEL":                     "1",
		"VERCEL_GIT_COMMIT_SHA":      observerDeploymentSHA,
		"VERCEL_GIT_COMMIT_REF":      "observer-deployment",
		"GITHUB_ACTIONS":             "true",
		"GITHUB_SHA":                 observerCISHA,
		"GITHUB_REF_NAME":            "observer-ci",
		"RAINDROP_GIT_DETECT_BRANCH": "true",
	})
	runDirectory := t.TempDir() // Deliberately outside every Git repository.
	runBuildInfoCommand(t, 10*time.Second, runDirectory, childEnvironment, executable)

	select {
	case err := <-requestErrors:
		t.Fatalf("capture request: %v", err)
	case event := <-captured:
		if got := event.Properties[appCommitSHAProperty]; got != applicationSHA {
			t.Fatalf("reported commit %#v, want compiled AUT %s (SDK %s, Vercel %s, CI %s)", got, applicationSHA, sdkSHA, observerDeploymentSHA, observerCISHA)
		}
		if got := event.Properties[appBranchProperty]; got != nil {
			t.Fatalf("build info unexpectedly inherited ambient branch: %#v", got)
		}
		contextValue, ok := event.Properties["$context"].(map[string]any)
		if !ok {
			t.Fatalf("missing SDK context: %#v", event.Properties)
		}
		library, ok := contextValue["library"].(map[string]any)
		if !ok || library["name"] != defaultLibraryName || library["version"] != Version {
			t.Fatalf("SDK identity was clobbered: %#v", contextValue)
		}
		if _, exists := event.Properties["raindrop.app.release"]; exists {
			t.Fatalf("application Git reinterpreted release identity: %#v", event.Properties)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("compiled application produced no captured event")
	}
}

func buildInfoTestEnvironment(environment []string, overrides map[string]string) []string {
	removed := map[string]bool{
		"RAINDROP_COMMIT_SHA":              true,
		"RAINDROP_COMMIT_DIRTY":            true,
		"RAINDROP_BRANCH":                  true,
		"RAINDROP_GIT_SOURCE_DIRECTORY":    true,
		"RAINDROP_GIT_AUTO_DETECT":         true,
		"RAINDROP_GIT_DETECT_BRANCH":       true,
		"VERCEL":                           true,
		"VERCEL_GIT_COMMIT_SHA":            true,
		"VERCEL_GIT_COMMIT_REF":            true,
		"GITHUB_ACTIONS":                   true,
		"GITHUB_SHA":                       true,
		"GITHUB_REF_NAME":                  true,
		"GITLAB_CI":                        true,
		"CI_COMMIT_SHA":                    true,
		"CI_COMMIT_BRANCH":                 true,
		"GIT_DIR":                          true,
		"GIT_WORK_TREE":                    true,
		"GIT_COMMON_DIR":                   true,
		"GIT_INDEX_FILE":                   true,
		"GIT_OBJECT_DIRECTORY":             true,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
		"GIT_NAMESPACE":                    true,
	}
	for key := range overrides {
		removed[strings.ToUpper(key)] = true
	}
	clean := make([]string, 0, len(environment)+len(overrides))
	for _, entry := range environment {
		key, _, found := strings.Cut(entry, "=")
		if found && !removed[strings.ToUpper(key)] {
			clean = append(clean, entry)
		}
	}
	for key, value := range overrides {
		clean = append(clean, key+"="+value)
	}
	return clean
}

func runBuildInfoCommand(t *testing.T, timeout time.Duration, directory string, environment []string, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	command.Env = environment
	command.WaitDelay = 250 * time.Millisecond
	var output limitedBuffer
	output.limit = 64 << 10
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		t.Fatalf("%s %s: %v: %s", filepath.Base(name), strings.Join(args, " "), err, output.String())
	}
	if ctx.Err() != nil {
		t.Fatalf("%s %s exceeded %s", filepath.Base(name), strings.Join(args, " "), timeout)
	}
	return output.String()
}
