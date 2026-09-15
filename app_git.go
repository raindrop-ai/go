package raindrop

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

const (
	appCommitSHAProperty   = "raindrop.app.commit_sha"
	appCommitDirtyProperty = "raindrop.app.commit_dirty"
	appBranchProperty      = "raindrop.app.branch"
	gitDiscoveryTimeout    = 750 * time.Millisecond
	gitDiscoveryOutputMax  = 64 * 1024
	gitCommandWaitDelay    = 100 * time.Millisecond
)

// AppGitOptions configures application Git provenance. Values are copied when
// WithAppGit is applied and are never mutated by the client. Pointer booleans
// distinguish an explicit false from an omitted setting.
type AppGitOptions struct {
	CommitSHA       string
	CommitDirty     *bool
	Branch          string
	SourceDirectory string
	DetectBranch    *bool
	AutoDetect      *bool
}

type appGitConfig struct {
	enabled         bool
	commitSHA       string
	commitDirty     *bool
	branch          string
	sourceDirectory string
	detectBranch    *bool
	autoDetect      *bool
}

// appGitSnapshot is immutable after construction. Presence in properties is
// significant: explicit empty or otherwise invalid caller values still win.
type appGitSnapshot struct {
	properties map[string]any
	inferred   map[string]bool
	attributes map[string]otlpAnyValue
}

type appGitState struct {
	current atomic.Pointer[appGitSnapshot]
}

func (s *appGitState) snapshot() appGitSnapshot {
	if s == nil {
		return appGitSnapshot{properties: map[string]any{}}
	}
	if current := s.current.Load(); current != nil {
		return cloneAppGitSnapshot(*current)
	}
	return appGitSnapshot{properties: map[string]any{}}
}

func (s *appGitState) store(snapshot appGitSnapshot) {
	copy := cloneAppGitSnapshot(snapshot)
	s.current.Store(&copy)
}

func cloneAppGitSnapshot(snapshot appGitSnapshot) appGitSnapshot {
	properties := make(map[string]any, len(snapshot.properties))
	for key, value := range snapshot.properties {
		properties[key] = value
	}
	inferred := make(map[string]bool, len(snapshot.inferred))
	for key, value := range snapshot.inferred {
		inferred[key] = value
	}
	attributes := make(map[string]otlpAnyValue, len(snapshot.attributes))
	for key, value := range snapshot.attributes {
		attributes[key] = value
	}
	return appGitSnapshot{properties: properties, inferred: inferred, attributes: attributes}
}

func newAppGitState(cfg appGitConfig) *appGitState {
	state := &appGitState{}
	initial, discover, sourceDirectory, detectBranch := resolveAppGitConfig(cfg)
	state.store(initial)
	if !discover {
		return state
	}
	fallback, _ := discoverCIAppGit(detectBranch)
	commandEnv := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")

	// Local Git is intentionally off every telemetry and shutdown path. The
	// command has a hard deadline, bounded captured output, no shell, and no
	// prompt; failure simply resolves to CI metadata or an empty snapshot.
	go func() {
		snapshot, ok := discoverLocalAppGit(sourceDirectory, detectBranch, commandEnv)
		if !ok {
			snapshot = fallback
		}
		state.store(mergeAutomaticAndExplicit(snapshot, initial))
	}()
	return state
}

func resolveAppGitConfig(cfg appGitConfig) (appGitSnapshot, bool, string, bool) {
	if !cfg.enabled {
		return emptyAppGitSnapshot(), false, "", false
	}
	explicit := mergedExplicitAppGit(cfg)
	if _, hasSHA := explicit.properties[appCommitSHAProperty]; hasSHA {
		return explicit, false, "", false
	}

	autoDetect := true
	if cfg.autoDetect != nil {
		autoDetect = *cfg.autoDetect
	} else if strings.EqualFold(strings.TrimSpace(os.Getenv("RAINDROP_GIT_AUTO_DETECT")), "false") {
		autoDetect = false
	}
	if !autoDetect {
		return explicit, false, "", false
	}

	detectBranch := false
	if cfg.detectBranch != nil {
		detectBranch = *cfg.detectBranch
	} else if strings.EqualFold(strings.TrimSpace(os.Getenv("RAINDROP_GIT_DETECT_BRANCH")), "true") {
		detectBranch = true
	}
	if snapshot, ok := applicationBuildInfoAppGit(); ok {
		return mergeAutomaticAndExplicit(snapshot, explicit), false, "", false
	}
	if snapshot, ok := applicationDeploymentAppGit(detectBranch); ok {
		return mergeAutomaticAndExplicit(snapshot, explicit), false, "", false
	}
	sourceDirectory := cfg.sourceDirectory
	if sourceDirectory == "" {
		sourceDirectory = os.Getenv("RAINDROP_GIT_SOURCE_DIRECTORY")
	}
	if sourceDirectory == "" {
		var err error
		sourceDirectory, err = os.Getwd()
		if err != nil {
			fallback, _ := discoverCIAppGit(detectBranch)
			return mergeAutomaticAndExplicit(fallback, explicit), false, "", detectBranch
		}
	} else {
		absolute, err := filepath.Abs(sourceDirectory)
		if err != nil {
			fallback, _ := discoverCIAppGit(detectBranch)
			return mergeAutomaticAndExplicit(fallback, explicit), false, "", detectBranch
		}
		sourceDirectory = absolute
	}
	return explicit, true, sourceDirectory, detectBranch
}

func mergedExplicitAppGit(cfg appGitConfig) appGitSnapshot {
	environment, _ := explicitEnvironmentAppGit()
	properties := environment.properties
	if cfg.commitSHA != "" {
		properties[appCommitSHAProperty] = cfg.commitSHA
	}
	if cfg.commitDirty != nil {
		properties[appCommitDirtyProperty] = *cfg.commitDirty
	}
	if cfg.branch != "" {
		properties[appBranchProperty] = cfg.branch
	}
	return appGitSnapshot{properties: properties, inferred: map[string]bool{}, attributes: map[string]otlpAnyValue{}}
}

func explicitEnvironmentAppGit() (appGitSnapshot, bool) {
	properties := map[string]any{}
	found := false
	if value, ok := os.LookupEnv("RAINDROP_COMMIT_SHA"); ok {
		found = true
		properties[appCommitSHAProperty] = value
	}
	if value, ok := os.LookupEnv("RAINDROP_COMMIT_DIRTY"); ok {
		found = true
		if parsed, valid := parseBool(value); valid {
			properties[appCommitDirtyProperty] = parsed
		}
	}
	if value, ok := os.LookupEnv("RAINDROP_BRANCH"); ok {
		found = true
		properties[appBranchProperty] = value
	}
	return appGitSnapshot{properties: properties, inferred: map[string]bool{}, attributes: map[string]otlpAnyValue{}}, found
}

func applicationBuildInfoAppGit() (appGitSnapshot, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return emptyAppGitSnapshot(), false
	}
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["vcs"] != "git" || !validAutomaticSHA(settings["vcs.revision"]) {
		return emptyAppGitSnapshot(), false
	}
	properties := map[string]any{appCommitSHAProperty: strings.ToLower(settings["vcs.revision"])}
	if dirty, valid := parseBool(settings["vcs.modified"]); valid {
		properties[appCommitDirtyProperty] = dirty
	}
	return inferredAppGitSnapshot(properties), true
}

func applicationDeploymentAppGit(detectBranch bool) (appGitSnapshot, bool) {
	if !environmentTrue("VERCEL") {
		return emptyAppGitSnapshot(), false
	}
	sha := os.Getenv("VERCEL_GIT_COMMIT_SHA")
	if !validAutomaticSHA(sha) {
		return emptyAppGitSnapshot(), false
	}
	properties := map[string]any{appCommitSHAProperty: strings.ToLower(sha)}
	if detectBranch {
		if branch := os.Getenv("VERCEL_GIT_COMMIT_REF"); branch != "" {
			properties[appBranchProperty] = branch
		}
	}
	return inferredAppGitSnapshot(properties), true
}

func discoverLocalAppGit(directory string, detectBranch bool, commandEnv []string) (appGitSnapshot, bool) {
	revisionOutput, complete := runGitCommand(directory, commandEnv, "rev-parse", "--verify", "HEAD")
	sha := strings.TrimSpace(revisionOutput)
	if !complete || !validAutomaticSHA(sha) {
		return emptyAppGitSnapshot(), false
	}
	properties := map[string]any{appCommitSHAProperty: strings.ToLower(sha)}
	statusOutput, statusComplete := runGitCommand(directory, commandEnv, "status", "--porcelain=v2", "--branch", "--untracked-files=normal")
	if !statusComplete {
		return inferredAppGitSnapshot(properties), true
	}
	statusSHA := ""
	dirty := false
	branch := ""
	for _, line := range strings.Split(statusOutput, "\n") {
		switch {
		case strings.HasPrefix(line, "# branch.oid "):
			statusSHA = strings.TrimSpace(strings.TrimPrefix(line, "# branch.oid "))
		case detectBranch && strings.HasPrefix(line, "# branch.head "):
			branch = strings.TrimSpace(strings.TrimPrefix(line, "# branch.head "))
		case line != "" && !strings.HasPrefix(line, "# "):
			dirty = true
		}
	}
	// Only associate status-derived fields with the independently resolved
	// revision when both observations agree. A concurrent checkout is unknown,
	// never falsely clean.
	if !strings.EqualFold(statusSHA, sha) {
		return inferredAppGitSnapshot(properties), true
	}
	properties[appCommitDirtyProperty] = dirty
	if detectBranch && branch != "" && branch != "(detached)" {
		properties[appBranchProperty] = branch
	}
	return inferredAppGitSnapshot(properties), true
}

func runGitCommand(directory string, commandEnv []string, args ...string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), gitDiscoveryTimeout)
	defer cancel()
	cmdArgs := append([]string{"-C", directory}, args...)
	cmd := exec.CommandContext(ctx, "git", cmdArgs...)
	cmd.Env = commandEnv
	cmd.WaitDelay = gitCommandWaitDelay
	var output limitedBuffer
	output.limit = gitDiscoveryOutputMax
	cmd.Stdout = &output
	cmd.Stderr = &limitedBuffer{limit: 4096}
	if err := cmd.Run(); err != nil || output.exceeded || ctx.Err() != nil {
		return output.String(), false
	}
	return output.String(), true
}

func discoverCIAppGit(detectBranch bool) (appGitSnapshot, bool) {
	type source struct {
		enabled     bool
		sha, branch string
	}
	for _, candidate := range []source{
		{environmentTrue("GITHUB_ACTIONS"), os.Getenv("GITHUB_SHA"), os.Getenv("GITHUB_REF_NAME")},
		{environmentTrue("GITLAB_CI"), os.Getenv("CI_COMMIT_SHA"), os.Getenv("CI_COMMIT_BRANCH")},
		{environmentTrue("CIRCLECI"), os.Getenv("CIRCLE_SHA1"), os.Getenv("CIRCLE_BRANCH")},
		{environmentTrue("BUILDKITE"), os.Getenv("BUILDKITE_COMMIT"), os.Getenv("BUILDKITE_BRANCH")},
	} {
		if !candidate.enabled || !validAutomaticSHA(candidate.sha) {
			continue
		}
		properties := map[string]any{appCommitSHAProperty: strings.ToLower(candidate.sha)}
		if detectBranch && candidate.branch != "" {
			properties[appBranchProperty] = candidate.branch
		}
		return inferredAppGitSnapshot(properties), true
	}
	return emptyAppGitSnapshot(), false
}

func emptyAppGitSnapshot() appGitSnapshot {
	return appGitSnapshot{properties: map[string]any{}, inferred: map[string]bool{}, attributes: map[string]otlpAnyValue{}}
}

func inferredAppGitSnapshot(properties map[string]any) appGitSnapshot {
	inferred := make(map[string]bool, len(properties))
	for key := range properties {
		inferred[key] = true
	}
	return appGitSnapshot{properties: properties, inferred: inferred, attributes: map[string]otlpAnyValue{}}
}

func mergeAutomaticAndExplicit(automatic, explicit appGitSnapshot) appGitSnapshot {
	merged := cloneAppGitSnapshot(automatic)
	for key, value := range explicit.properties {
		merged.properties[key] = value
		delete(merged.inferred, key)
	}
	return merged
}

func appGitWithPropertyOverrides(base appGitSnapshot, properties map[string]any) appGitSnapshot {
	effective := cloneAppGitSnapshot(base)
	if _, overridesSHA := properties[appCommitSHAProperty]; overridesSHA {
		dropInferredRevisionContext(&effective)
	}
	for key, value := range properties {
		if !isAppGitProperty(key) {
			continue
		}
		effective.properties[key] = value
		delete(effective.inferred, key)
	}
	return effective
}

func appGitForSpan(base appGitSnapshot, properties map[string]any, attrs []Attribute, limit int) appGitSnapshot {
	effective := appGitWithPropertyOverrides(base, properties)
	if effective.attributes == nil {
		effective.attributes = map[string]otlpAnyValue{}
	}
	for key, value := range effective.properties {
		effective.attributes[key] = appGitOTLPValue(value, limit)
	}
	for _, attr := range attrs {
		if !isAppGitProperty(attr.Key) {
			continue
		}
		if attr.Key == appCommitSHAProperty {
			dropInferredRevisionContext(&effective)
		}
		delete(effective.properties, attr.Key)
		effective.attributes[attr.Key] = attr.Value
		delete(effective.inferred, attr.Key)
	}
	return effective
}

func appGitOTLPValue(value any, limit int) otlpAnyValue {
	attrs := toolPropertyAttributes(map[string]any{appCommitSHAProperty: value}, limit)
	if len(attrs) == 0 {
		return otlpAnyValue{}
	}
	return attrs[0].Value
}

func dropInferredRevisionContext(snapshot *appGitSnapshot) {
	for _, key := range []string{appCommitSHAProperty, appCommitDirtyProperty, appBranchProperty} {
		if snapshot.inferred[key] {
			delete(snapshot.properties, key)
			delete(snapshot.attributes, key)
			delete(snapshot.inferred, key)
		}
	}
}

func environmentTrue(key string) bool {
	value := strings.TrimSpace(os.Getenv(key))
	return strings.EqualFold(value, "true") || value == "1"
}

func validAutomaticSHA(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func parseBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

type limitedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	if w.exceeded {
		return len(p), nil
	}
	remaining := w.limit - w.buffer.Len()
	if remaining <= 0 {
		w.exceeded = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = w.buffer.Write(p[:remaining])
		w.exceeded = true
		return len(p), nil
	}
	_, _ = w.buffer.Write(p)
	return len(p), nil
}

func (w *limitedBuffer) String() string { return w.buffer.String() }
