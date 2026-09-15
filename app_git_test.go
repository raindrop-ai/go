package raindrop

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testCommitSHA = "0123456789abcdef0123456789abcdef01234567"

func boolPointer(value bool) *bool { return &value }

func unsetEnvironment(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		value, existed := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		keyCopy, valueCopy, existedCopy := key, value, existed
		t.Cleanup(func() {
			if existedCopy {
				_ = os.Setenv(keyCopy, valueCopy)
			} else {
				_ = os.Unsetenv(keyCopy)
			}
		})
	}
}

func TestConfiguredAppGitEnrichesEventsWithoutOverwritingCaller(t *testing.T) {
	var received []trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload trackPartialPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		received = append(received, payload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dirty := true
	client := newTestClient(t, server.URL+"/", WithAppGit(AppGitOptions{
		CommitSHA:   testCommitSHA,
		CommitDirty: &dirty,
		Branch:      "main",
	}))
	dirty = false // WithAppGit copied the option value at construction.
	defer func() { _ = client.Close() }()

	if err := client.TrackEvent(context.Background(), Event{EventID: "auto", UserID: "user"}); err != nil {
		t.Fatalf("track enriched event: %v", err)
	}
	if err := client.TrackEvent(context.Background(), Event{
		EventID: "explicit",
		UserID:  "user",
		Properties: map[string]any{
			appCommitSHAProperty:   "",
			appCommitDirtyProperty: nil,
			appBranchProperty:      42,
		},
	}); err != nil {
		t.Fatalf("track explicit event: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("received %d payloads, want 2", len(received))
	}
	if got := received[0].Properties[appCommitSHAProperty]; got != testCommitSHA {
		t.Fatalf("automatic commit = %#v", got)
	}
	if got := received[0].Properties[appCommitDirtyProperty]; got != true {
		t.Fatalf("automatic dirty = %#v, want true", got)
	}
	if got := received[0].Properties[appBranchProperty]; got != "main" {
		t.Fatalf("automatic branch = %#v", got)
	}
	if got, exists := received[1].Properties[appCommitSHAProperty]; !exists || got != "" {
		t.Fatalf("explicit empty commit was overwritten: %#v", received[1].Properties)
	}
	if got, exists := received[1].Properties[appCommitDirtyProperty]; !exists || got != nil {
		t.Fatalf("explicit nil dirty was overwritten: %#v", received[1].Properties)
	}
	if got := received[1].Properties[appBranchProperty]; got != float64(42) {
		t.Fatalf("explicit invalid branch was overwritten: %#v", got)
	}
}

func TestInteractionFreezesEmptyAppGitSnapshot(t *testing.T) {
	var received []trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload trackPartialPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		received = append(received, payload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "frozen", UserID: "user"})
	client.appGit.store(appGitSnapshot{properties: map[string]any{appCommitSHAProperty: testCommitSHA}})
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if err := client.TrackEvent(context.Background(), Event{EventID: "later", UserID: "user"}); err != nil {
		t.Fatalf("track later event: %v", err)
	}

	if _, exists := received[0].Properties[appCommitSHAProperty]; exists {
		t.Fatalf("interaction picked up metadata discovered after Begin: %#v", received[0].Properties)
	}
	if got := received[1].Properties[appCommitSHAProperty]; got != testCommitSHA {
		t.Fatalf("later operation did not see resolved metadata: %#v", received[1].Properties)
	}
}

func TestOTLPAppGitSnapshotAndExplicitProperties(t *testing.T) {
	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traces" {
			_ = json.NewDecoder(r.Body).Decode(&received)
		} else {
			_, _ = io.ReadAll(r.Body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/")
	defer func() { _ = client.Close() }()
	first := client.StartSpan(context.Background(), SpanOptions{
		Name: "first",
		Properties: map[string]any{
			appCommitSHAProperty: nil,
			appBranchProperty:    "caller-branch",
		},
	})
	client.appGit.store(appGitSnapshot{properties: map[string]any{
		appCommitSHAProperty:   testCommitSHA,
		appCommitDirtyProperty: true,
		appBranchProperty:      "automatic-branch",
	}})
	first.End()
	second := client.StartSpan(context.Background(), SpanOptions{Name: "second"})
	second.End()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Fatalf("got %d spans", len(spans))
	}
	firstAttrs := attributesByKey(spans[0].Attributes)
	if value, exists := firstAttrs[appCommitSHAProperty]; !exists || value != (otlpAnyValue{}) {
		t.Fatalf("explicit nil commit was not preserved: %#v", firstAttrs)
	}
	if got := firstAttrs[appBranchProperty].StringValue; got != "caller-branch" {
		t.Fatalf("explicit branch = %q", got)
	}
	if _, exists := firstAttrs[appCommitDirtyProperty]; exists {
		t.Fatalf("span picked up metadata after StartSpan: %#v", firstAttrs)
	}
	secondAttrs := attributesByKey(spans[1].Attributes)
	if got := secondAttrs[appCommitSHAProperty].StringValue; got != testCommitSHA {
		t.Fatalf("second span commit = %q", got)
	}
	if value := secondAttrs[appCommitDirtyProperty].BoolValue; value == nil || !*value {
		t.Fatalf("second span dirty = %#v", value)
	}
}

func TestExplicitEnvironmentSurvivesAutomaticOptOut(t *testing.T) {
	t.Setenv("RAINDROP_COMMIT_SHA", "not-a-checkout-sha")
	t.Setenv("RAINDROP_COMMIT_DIRTY", "true")
	t.Setenv("RAINDROP_BRANCH", "env-branch")
	t.Setenv("RAINDROP_GIT_AUTO_DETECT", "false")

	snapshot, discover, _, _, _ := resolveAppGitConfig(appGitConfig{enabled: true})
	if discover {
		t.Fatalf("explicit environment should resolve without discovery")
	}
	if got := snapshot.properties[appCommitSHAProperty]; got != "not-a-checkout-sha" {
		t.Fatalf("explicit invalid environment commit changed: %#v", got)
	}
	if got := snapshot.properties[appCommitDirtyProperty]; got != true {
		t.Fatalf("environment dirty = %#v", got)
	}
	if got := snapshot.properties[appBranchProperty]; got != "env-branch" {
		t.Fatalf("environment branch = %#v", got)
	}
}

func TestInvalidExplicitDirtyEnvironmentDoesNotBlockSHAInference(t *testing.T) {
	t.Setenv("RAINDROP_COMMIT_DIRTY", "not-a-boolean")
	snapshot, discover, _, _, _ := resolveAppGitConfig(appGitConfig{enabled: true})
	if !discover {
		if _, hasSHA := snapshot.properties[appCommitSHAProperty]; !hasSHA {
			t.Fatalf("dirty-only environment blocked SHA inference: %#v", snapshot.properties)
		}
	}
}

func TestExplicitFieldsMergeByPrecedenceAndControlAutomaticDetection(t *testing.T) {
	t.Setenv("RAINDROP_COMMIT_SHA", testCommitSHA)
	t.Setenv("RAINDROP_BRANCH", "environment-branch")
	t.Setenv("RAINDROP_GIT_AUTO_DETECT", "false")
	clientAuto := true
	snapshot, discover, _, _, _ := resolveAppGitConfig(appGitConfig{
		enabled:     true,
		branch:      "client-branch",
		autoDetect:  &clientAuto,
		commitDirty: boolPointer(true),
	})
	if discover {
		t.Fatalf("explicit environment SHA should resolve immediately")
	}
	if got := snapshot.properties[appCommitSHAProperty]; got != testCommitSHA {
		t.Fatalf("environment SHA = %#v", got)
	}
	if got := snapshot.properties[appBranchProperty]; got != "client-branch" {
		t.Fatalf("client branch did not override environment: %#v", got)
	}
	if got := snapshot.properties[appCommitDirtyProperty]; got != true {
		t.Fatalf("client dirty missing: %#v", got)
	}

	withoutSHA := appGitWithPropertyOverrides(inferredAppGitSnapshot(map[string]any{
		appCommitSHAProperty:   testCommitSHA,
		appCommitDirtyProperty: true,
		appBranchProperty:      "inferred-branch",
	}), map[string]any{
		appCommitSHAProperty: strings.Repeat("a", 40),
		appBranchProperty:    "caller-branch",
	})
	if _, exists := withoutSHA.properties[appCommitDirtyProperty]; exists {
		t.Fatalf("operation SHA retained inferred dirty: %#v", withoutSHA.properties)
	}
	if got := withoutSHA.properties[appBranchProperty]; got != "caller-branch" {
		t.Fatalf("caller branch was not preserved: %#v", got)
	}
}

func TestClientAutomaticControlsOverrideEnvironmentDefaults(t *testing.T) {
	unsetEnvironment(t, "RAINDROP_COMMIT_SHA", "RAINDROP_COMMIT_DIRTY", "RAINDROP_BRANCH")
	t.Setenv("RAINDROP_GIT_AUTO_DETECT", "false")
	auto := true
	snapshot, discover, sourceDirectory, _, _ := resolveAppGitConfig(appGitConfig{enabled: true, branch: "explicit-branch", autoDetect: &auto})
	if !discover {
		if _, hasSHA := snapshot.properties[appCommitSHAProperty]; !hasSHA {
			t.Fatalf("client auto_detect=true did not override environment false: %#v", snapshot.properties)
		}
	} else if !filepath.IsAbs(sourceDirectory) {
		t.Fatalf("source directory was not captured absolutely: %q", sourceDirectory)
	}
	if got := snapshot.properties[appBranchProperty]; got != "explicit-branch" {
		t.Fatalf("branch-only client config was lost while resolving SHA: %#v", snapshot.properties)
	}

	t.Setenv("RAINDROP_GIT_AUTO_DETECT", "true")
	auto = false
	snapshot, discover, _, _, _ = resolveAppGitConfig(appGitConfig{enabled: true, autoDetect: &auto})
	if discover || len(snapshot.properties) != 0 {
		t.Fatalf("client auto_detect=false did not override environment true: %#v, discover=%v", snapshot.properties, discover)
	}
}

func TestProviderContextRequiredForDeploymentAndCI(t *testing.T) {
	unsetEnvironment(t, "GITLAB_CI", "CI_COMMIT_SHA", "CIRCLECI", "CIRCLE_SHA1", "BUILDKITE", "BUILDKITE_COMMIT")
	t.Setenv("GITHUB_SHA", testCommitSHA)
	t.Setenv("GITHUB_REF_NAME", "github-branch")
	t.Setenv("GITHUB_ACTIONS", "false")
	if snapshot, ok := discoverCIAppGit(true); ok {
		t.Fatalf("unmarked GitHub variables were trusted: %#v", snapshot.properties)
	}
	t.Setenv("GITHUB_ACTIONS", "true")
	if snapshot, ok := discoverCIAppGit(false); !ok || snapshot.properties[appCommitSHAProperty] != testCommitSHA {
		t.Fatalf("marked GitHub metadata missing: %#v, %v", snapshot.properties, ok)
	} else if _, exists := snapshot.properties[appBranchProperty]; exists {
		t.Fatalf("CI branch emitted without opt-in: %#v", snapshot.properties)
	}

	t.Setenv("VERCEL", "1")
	t.Setenv("VERCEL_GIT_COMMIT_SHA", testCommitSHA)
	t.Setenv("VERCEL_GIT_COMMIT_REF", "vercel-branch")
	if snapshot, ok := applicationDeploymentAppGit(true); !ok || snapshot.properties[appBranchProperty] != "vercel-branch" {
		t.Fatalf("Vercel metadata missing: %#v, %v", snapshot.properties, ok)
	}
}

func TestDiscoverLocalAppGit(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed")
	}
	directory := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		cmd := exec.Command(git, append([]string{"-C", directory}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Raindrop", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=Raindrop", "GIT_COMMITTER_EMAIL=test@example.com")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
		}
	}
	runGit("init", "-b", "app-branch")
	if err := os.WriteFile(filepath.Join(directory, "app.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "app.txt")
	runGit("commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(directory, "app.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, ok := discoverLocalAppGit(directory, true, append(os.Environ(), "GIT_TERMINAL_PROMPT=0"))
	if !ok {
		t.Fatalf("local discovery failed")
	}
	if sha, _ := snapshot.properties[appCommitSHAProperty].(string); !validAutomaticSHA(sha) {
		t.Fatalf("invalid discovered SHA: %#v", sha)
	}
	if got := snapshot.properties[appCommitDirtyProperty]; got != true {
		t.Fatalf("dirty = %#v", got)
	}
	if got := snapshot.properties[appBranchProperty]; got != "app-branch" {
		t.Fatalf("branch = %#v", got)
	}
	withoutBranch, ok := discoverLocalAppGit(directory, false, append(os.Environ(), "GIT_TERMINAL_PROMPT=0"))
	if !ok {
		t.Fatalf("local discovery without branch failed")
	}
	if _, exists := withoutBranch.properties[appBranchProperty]; exists {
		t.Fatalf("automatic branch was reported without opt-in: %#v", withoutBranch.properties)
	}
}

func TestConfiguredSourceDirectoryWinsOverInheritedGitSelectors(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	autDirectory := filepath.Join(root, "aut")
	observerDirectory := filepath.Join(root, "observer")
	autSHA := createTestRepository(t, git, autDirectory, "aut-branch", "application source\n")
	observerSHA := createTestRepository(t, git, observerDirectory, "observer-branch", "observer source\n")
	if autSHA == observerSHA {
		t.Fatalf("fixture commits unexpectedly match: %s", autSHA)
	}

	configPath := filepath.Join(root, "injected.gitconfig")
	if err := os.WriteFile(configPath, []byte("[core]\n\tworktree = "+observerDirectory+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	selectors := map[string]string{
		"GIT_DIR":                          filepath.Join(observerDirectory, ".git"),
		"GIT_WORK_TREE":                    observerDirectory,
		"GIT_COMMON_DIR":                   filepath.Join(observerDirectory, ".git"),
		"GIT_INDEX_FILE":                   filepath.Join(observerDirectory, ".git", "index"),
		"GIT_OBJECT_DIRECTORY":             filepath.Join(observerDirectory, ".git", "objects"),
		"GIT_ALTERNATE_OBJECT_DIRECTORIES": filepath.Join(observerDirectory, ".git", "objects"),
		"GIT_NAMESPACE":                    "observer",
		"GIT_CONFIG":                       configPath,
		"GIT_CONFIG_GLOBAL":                configPath,
		"GIT_CONFIG_SYSTEM":                configPath,
		"GIT_CONFIG_PARAMETERS":            "'core.worktree'='" + observerDirectory + "'",
		"GIT_CONFIG_COUNT":                 "1",
		"GIT_CONFIG_KEY_0":                 "core.worktree",
		"GIT_CONFIG_VALUE_0":               observerDirectory,
	}
	for key, value := range selectors {
		t.Setenv(key, value)
	}

	snapshot, ok := discoverLocalAppGit(autDirectory, true, os.Environ())
	if !ok {
		t.Fatalf("AUT discovery failed with inherited observer selectors")
	}
	if got := snapshot.properties[appCommitSHAProperty]; got != autSHA {
		t.Fatalf("reported observer identity: got %#v, AUT %s, observer %s", got, autSHA, observerSHA)
	}
	if got := snapshot.properties[appBranchProperty]; got != "aut-branch" {
		t.Fatalf("reported observer branch: %#v", got)
	}
	for key, value := range selectors {
		if got := os.Getenv(key); got != value {
			t.Fatalf("customer process environment %s mutated: got %q, want %q", key, got, value)
		}
	}
}

func TestExplicitSourceDirectoryBeatsAmbientDeploymentAndCI(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	autDirectory := filepath.Join(root, "aut")
	observerDirectory := filepath.Join(root, "observer")
	autSHA := createTestRepository(t, git, autDirectory, "aut-branch", "application source\n")
	observerSHA := createTestRepository(t, git, observerDirectory, "observer-branch", "observer source\n")
	unsetEnvironment(t, "RAINDROP_COMMIT_SHA", "RAINDROP_COMMIT_DIRTY", "RAINDROP_BRANCH")
	t.Setenv("VERCEL", "1")
	t.Setenv("VERCEL_GIT_COMMIT_SHA", observerSHA)
	t.Setenv("VERCEL_GIT_COMMIT_REF", "observer-vercel")
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_SHA", observerSHA)
	t.Setenv("GITHUB_REF_NAME", "observer-ci")

	for _, test := range []struct {
		name    string
		options func(*testing.T) []Option
	}{
		{
			name: "public client option",
			options: func(t *testing.T) []Option {
				return []Option{WithAppGit(AppGitOptions{SourceDirectory: autDirectory})}
			},
		},
		{
			name: "Raindrop environment option",
			options: func(t *testing.T) []Option {
				t.Setenv("RAINDROP_GIT_SOURCE_DIRECTORY", autDirectory)
				return []Option{WithAppGit(AppGitOptions{})}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var received trackPartialPayload
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&received)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := newTestClient(t, server.URL+"/", test.options(t)...)
			defer func() { _ = client.Close() }()
			deadline := time.Now().Add(2 * time.Second)
			for client.appGit.snapshot().properties[appCommitSHAProperty] != autSHA && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if err := client.TrackEvent(context.Background(), Event{EventID: "selected-aut", UserID: "user"}); err != nil {
				t.Fatal(err)
			}
			if got := received.Properties[appCommitSHAProperty]; got != autSHA {
				t.Fatalf("selected AUT commit = %#v, observer = %s", got, observerSHA)
			}
		})
	}
}

func TestUnavailableExplicitSourceDirectoryDoesNotUseAmbientIdentity(t *testing.T) {
	unsetEnvironment(t, "RAINDROP_COMMIT_SHA", "RAINDROP_COMMIT_DIRTY", "RAINDROP_BRANCH", "RAINDROP_GIT_SOURCE_DIRECTORY")
	t.Setenv("VERCEL", "true")
	t.Setenv("VERCEL_GIT_COMMIT_SHA", testCommitSHA)
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_SHA", testCommitSHA)

	var received trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL+"/", WithAppGit(AppGitOptions{SourceDirectory: filepath.Join(t.TempDir(), "missing")}))
	defer func() { _ = client.Close() }()
	time.Sleep(gitDiscoveryTimeout + gitCommandWaitDelay + 50*time.Millisecond)
	if err := client.TrackEvent(context.Background(), Event{EventID: "missing-aut", UserID: "user"}); err != nil {
		t.Fatal(err)
	}
	if _, exists := received.Properties[appCommitSHAProperty]; exists {
		t.Fatalf("unavailable selected AUT fell back to ambient identity: %#v", received.Properties)
	}
}

func TestSanitizedGitEnvironmentPreservesUnrelatedValues(t *testing.T) {
	environment := []string{
		"PATH=/usr/bin",
		"GIT_AUTHOR_NAME=Application",
		"GIT_DIR=/observer/.git",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=core.worktree",
		"GIT_CONFIG_VALUE_0=/observer",
		"GIT_TERMINAL_PROMPT=1",
	}
	clean := sanitizedGitEnvironment(environment)
	joined := strings.Join(clean, "\n")
	for _, expected := range []string{"PATH=/usr/bin", "GIT_AUTHOR_NAME=Application", "GIT_TERMINAL_PROMPT=0"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("sanitized environment lost %q: %#v", expected, clean)
		}
	}
	for _, forbidden := range []string{"GIT_DIR=", "GIT_CONFIG_COUNT=", "GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_", "GIT_TERMINAL_PROMPT=1"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("sanitized environment retained %q: %#v", forbidden, clean)
		}
	}
}

func createTestRepository(t *testing.T, git, directory, branch, contents string) string {
	t.Helper()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(git, append([]string{"-C", directory}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Raindrop", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=Raindrop", "GIT_COMMITTER_EMAIL=test@example.com")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	run("init", "-b", branch)
	if err := os.WriteFile(filepath.Join(directory, "app.txt"), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "app.txt")
	run("commit", "-m", "initial")
	return run("rev-parse", "HEAD")
}

func TestLocalRevisionSurvivesUnavailableStatus(t *testing.T) {
	directory := t.TempDir()
	fakeGit := filepath.Join(directory, "git")
	script := "#!/bin/sh\nif [ \"$3\" = \"rev-parse\" ]; then echo " + testCommitSHA + "; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(fakeGit, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	snapshot, ok := discoverLocalAppGit(directory, true, []string{"PATH=" + directory, "GIT_TERMINAL_PROMPT=0"})
	if !ok || snapshot.properties[appCommitSHAProperty] != testCommitSHA {
		t.Fatalf("known revision lost when status failed: %#v, %v", snapshot.properties, ok)
	}
	if _, exists := snapshot.properties[appCommitDirtyProperty]; exists {
		t.Fatalf("failed status reported a false clean state: %#v", snapshot.properties)
	}
	if _, exists := snapshot.properties[appBranchProperty]; exists {
		t.Fatalf("failed status reported a branch: %#v", snapshot.properties)
	}
}

func TestDirectPatchSnapshotSurvivesFlushFinishAndResume(t *testing.T) {
	var received []trackPartialPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload trackPartialPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		received = append(received, payload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithAppGitDisabled())
	defer func() { _ = client.Close() }()
	pending := true
	if err := client.Patch(context.Background(), "direct", PatchOptions{UserID: "user", IsPending: &pending}); err != nil {
		t.Fatal(err)
	}
	client.appGit.store(inferredAppGitSnapshot(map[string]any{appCommitSHAProperty: testCommitSHA}))
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Patch(context.Background(), "direct", PatchOptions{Properties: map[string]any{"stage": "two"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.ResumeInteraction("direct").Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}
	client.appGit.store(emptyAppGitSnapshot())
	if err := client.Patch(context.Background(), "direct-finish", PatchOptions{UserID: "user", IsPending: &pending}); err != nil {
		t.Fatal(err)
	}
	client.appGit.store(inferredAppGitSnapshot(map[string]any{appCommitSHAProperty: strings.Repeat("b", 40)}))
	if err := client.Finish(context.Background(), "direct-finish", FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}
	if len(received) != 4 {
		t.Fatalf("received %d partials, want 4", len(received))
	}
	for index, payload := range received {
		if _, exists := payload.Properties[appCommitSHAProperty]; exists {
			t.Fatalf("partial %d did not retain its frozen empty snapshot: %#v", index, payload.Properties)
		}
	}
	if received[1].Properties["stage"] != "two" {
		t.Fatalf("ordinary caller property lost: %#v", received[1].Properties)
	}
}

func TestOperationSHAPropagatesToChildSpanWithoutInferredContext(t *testing.T) {
	var received exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traces" {
			_ = json.NewDecoder(r.Body).Decode(&received)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithAppGitDisabled())
	defer func() { _ = client.Close() }()
	client.appGit.store(inferredAppGitSnapshot(map[string]any{
		appCommitSHAProperty:   testCommitSHA,
		appCommitDirtyProperty: true,
		appBranchProperty:      "inferred-branch",
	}))
	override := strings.Repeat("a", 40)
	parent := client.StartSpan(context.Background(), SpanOptions{
		Name:       "parent",
		Attributes: []Attribute{StringAttr(appCommitSHAProperty, override)},
		Properties: map[string]any{
			"ordinary": "kept",
		},
	})
	child := client.StartSpan(context.Background(), SpanOptions{Name: "child", Parent: parent})
	child.End()
	parent.End()
	nilOverride := client.StartSpan(context.Background(), SpanOptions{
		Name: "nil-override",
		Properties: map[string]any{
			appCommitSHAProperty: nil,
			"ordinary":           "still-here",
		},
	})
	nilOverride.End()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := received.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 3 {
		t.Fatalf("received %d spans, want 3", len(spans))
	}
	for _, span := range spans[:2] {
		attrs := attributesByKey(span.Attributes)
		if got := attrs[appCommitSHAProperty].StringValue; got != override {
			t.Fatalf("%s commit = %q", span.Name, got)
		}
		if _, exists := attrs[appCommitDirtyProperty]; exists {
			t.Fatalf("%s retained inferred dirty: %#v", span.Name, attrs)
		}
		if _, exists := attrs[appBranchProperty]; exists {
			t.Fatalf("%s retained inferred branch: %#v", span.Name, attrs)
		}
	}
	nilAttrs := attributesByKey(spans[2].Attributes)
	if value, exists := nilAttrs[appCommitSHAProperty]; !exists || value != (otlpAnyValue{}) {
		t.Fatalf("nil canonical SHA did not suppress only inference: %#v", nilAttrs)
	}
	if _, exists := nilAttrs[appCommitDirtyProperty]; exists {
		t.Fatalf("nil SHA retained inferred dirty: %#v", nilAttrs)
	}
	if got := nilAttrs["traceloop.association.properties.ordinary"].StringValue; got != "still-here" {
		t.Fatalf("ordinary caller property lost: %#v", nilAttrs)
	}
}

func TestDirectPatchOverrideSharedByOwningEventAndSpans(t *testing.T) {
	var event trackPartialPayload
	var traces exportTraceServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/events/track_partial":
			_ = json.NewDecoder(r.Body).Decode(&event)
		case "/traces":
			_ = json.NewDecoder(r.Body).Decode(&traces)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/", WithAppGitDisabled())
	defer func() { _ = client.Close() }()
	client.appGit.store(inferredAppGitSnapshot(map[string]any{
		appCommitSHAProperty:   testCommitSHA,
		appCommitDirtyProperty: true,
		appBranchProperty:      "old-branch",
	}))
	interaction := client.Begin(context.Background(), BeginOptions{EventID: "shared", UserID: "user"})
	override := strings.Repeat("c", 40)
	if err := client.Patch(context.Background(), "shared", PatchOptions{Properties: map[string]any{
		appCommitSHAProperty: override,
		"ordinary":           "event-value",
	}}); err != nil {
		t.Fatal(err)
	}
	root := client.StartSpan(context.Background(), SpanOptions{Name: "root", EventID: "shared"})
	child := client.StartSpan(context.Background(), SpanOptions{Name: "child", Parent: root})
	child.End()
	root.End()
	if err := interaction.Finish(FinishOptions{Output: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := event.Properties[appCommitSHAProperty]; got != override {
		t.Fatalf("event commit = %#v", got)
	}
	if _, exists := event.Properties[appCommitDirtyProperty]; exists {
		t.Fatalf("event retained inferred dirty: %#v", event.Properties)
	}
	if event.Properties["ordinary"] != "event-value" {
		t.Fatalf("event ordinary property lost: %#v", event.Properties)
	}
	spans := traces.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Fatalf("received %d spans, want 2", len(spans))
	}
	for _, span := range spans {
		attrs := attributesByKey(span.Attributes)
		if got := attrs[appCommitSHAProperty].StringValue; got != override {
			t.Fatalf("%s commit = %q", span.Name, got)
		}
		if _, exists := attrs[appCommitDirtyProperty]; exists {
			t.Fatalf("%s retained inferred dirty: %#v", span.Name, attrs)
		}
	}
}

func attributesByKey(attributes []otlpKeyValue) map[string]otlpAnyValue {
	out := make(map[string]otlpAnyValue, len(attributes))
	for _, attribute := range attributes {
		out[attribute.Key] = attribute.Value
	}
	return out
}
