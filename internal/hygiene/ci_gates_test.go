// Package hygiene holds tests that assert on repo wiring rather than on code.
//
// It exists because wiring is what rots. Reverting a pin to `@v6`, deleting the
// Dependabot entry, or dropping the format gate is a one-line change whose absence
// is completely silent — nothing fails, CI's supply chain just quietly goes back to
// being mutable and the tree starts drifting again. These tests make that fail.
// (#7, #8)
//
// There are no non-test files here by design; the assertions are about the repo.
//
// Everything below uses only the standard library, on purpose. cohort's whole
// value proposition is that the same unmodified core compiles anywhere, and
// doc.go / API.md §8 state its only dependency is golang.org/x/sync. Pulling in a
// YAML library just to read a 70-line config would put a second require line in a
// library that advertises having one — so the parsing here is deliberately narrow
// and hand-rolled rather than general.
package hygiene

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const workflows = "../../.github/workflows"

// TestCIGatesFormatting checks that the formatting gate is still wired into CI,
// and that it REPORTS drift rather than fixing it.
//
// The second half is the subtle one. `gofmt -w` rewrites files and exits 0, so a
// "gate" built on it can only ever report success — green on a dirty tree
// forever, indistinguishable from having no gate at all. Only `gofmt -l`/`-d` can
// fail. That is why four files sat unformatted on main: nothing checked, and
// `go vet` does not look at formatting. This repo has no Makefile, so the gate is
// inline in the workflow. (#8)
func TestCIGatesFormatting(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(workflows, "ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	step, ok := workflowStep(string(data), "Format gate")
	if !ok {
		t.Fatal("the CI workflow has no 'Format gate' step; without it nothing " +
			"prevents unformatted code from reaching main (#8)")
	}
	if !strings.Contains(step, "gofmt -l") {
		t.Error("the Format gate does not run 'gofmt -l'; it must LIST offenders to be able to fail")
	}
	// Commands only, not message text: the step's error message tells the reader
	// to "run 'gofmt -w' on them", which is advice, not an invocation.
	if strings.Contains(stripQuoted(step), "gofmt -w") {
		t.Error("the Format gate runs 'gofmt -w': that rewrites files and always exits 0, " +
			"so it reports success on a dirty tree. A gate must report, not fix.")
	}
	if !strings.Contains(step, "exit 1") {
		t.Error("the Format gate never exits non-zero, so CI can't fail on drift")
	}
}

// TestActionsArePinnedToSHAs: every `uses:` in a workflow must name a full 40-hex
// commit SHA, not a tag.
//
// A tag is mutable. `actions/checkout@v6` means "whatever v6 points at when the
// job runs", so the code executing in CI can change with no commit here. That is
// a supply-chain hole and it is not hypothetical: actions/checkout@v6 moved from
// df4cb1c (2026-06-02) to d23441a (2026-07-16), silently, exactly as tags are
// designed to.
//
// The trailing `# vX.Y.Z` comment is required too. A bare SHA is unreadable, and
// the version is what makes a bump reviewable — without it nobody can tell
// whether a pin is current or two years stale.
func TestActionsArePinnedToSHAs(t *testing.T) {
	// A local `uses: ./.github/...` is a path, not a registry ref — nothing to pin.
	pinned := regexp.MustCompile(`^[^@\s]+@[0-9a-f]{40}\s+#\s*v?\d`)
	found := 0
	for _, f := range workflowFiles(t) {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			ref, ok := usesRef(line)
			if !ok {
				continue
			}
			found++
			if !pinned.MatchString(ref) {
				t.Errorf("%s:%d: %q is not pinned to a full commit SHA with a version comment.\n"+
					"A tag is mutable, so the code CI runs can change without a commit here. Use:\n"+
					"    uses: owner/action@<40-hex-sha> # vX.Y.Z",
					filepath.Base(f), i+1, ref)
			}
		}
	}
	// Anti-vacuous: without this, a parser that stops matching passes forever.
	if found == 0 {
		t.Error("no `uses:` lines found in .github/workflows — this test is asserting nothing; " +
			"check the parser against the current workflow layout")
	}
}

// TestDependabotCoversEveryAction is the other half of pinning to SHAs.
//
// A pin closes the mutable-tag hole but opens a staleness one: a SHA never moves,
// including past a security fix, and unlike `@v6` nothing updates it for you. So
// pinning is only safe if something bumps the pins — here, Dependabot. Without it
// the workflow slowly freezes on old actions and nothing ever says so, since this
// repo has no security workflow to flag it either.
//
// The check that matters is coverage: a group whose patterns don't match an action
// leaves it outside the grouped PR, silently.
func TestDependabotCoversEveryAction(t *testing.T) {
	cfg := readDependabot(t)

	e, ok := cfg["github-actions"]
	if !ok {
		t.Fatalf("dependabot.yml has no `github-actions` entry, so the SHA-pinned "+
			"actions in .github/workflows are never bumped (entries found: %v)", ecosystems(cfg))
	}
	if e.directory != "/" {
		t.Errorf("the github-actions entry watches %q; workflows live in /.github/workflows, "+
			"which Dependabot finds via directory \"/\"", e.directory)
	}
	if len(e.patterns) == 0 {
		t.Fatal("the github-actions entry has no group patterns, so each action would " +
			"open its own PR outside any group")
	}

	for _, action := range workflowActions(t) {
		matched := false
		for _, p := range e.patterns {
			if globMatch(p, action) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("%s is not matched by any Dependabot group pattern %v, so it would "+
				"open its own PR outside the group (or be missed). Widen the pattern.",
				action, e.patterns)
		}
	}
}

// TestDependabotCoversGoModules: cohort has exactly one dependency
// (golang.org/x/sync) and an import discipline designed to keep it that way — but
// "small" is not "watched". There is no govulncheck and no Trivy in this repo, so
// if that one dependency grew an advisory, Dependabot is the only thing that would
// say so.
func TestDependabotCoversGoModules(t *testing.T) {
	cfg := readDependabot(t)
	e, ok := cfg["gomod"]
	if !ok {
		t.Fatalf("dependabot.yml has no `gomod` entry, so go.mod dependencies are never "+
			"updated and no security advisory against them would surface here — this repo "+
			"has no govulncheck workflow either (entries found: %v)", ecosystems(cfg))
	}
	if e.directory != "/" {
		t.Errorf("the gomod entry watches %q, but go.mod is at the repo root", e.directory)
	}
}

// --- helpers ---------------------------------------------------------------
//
// A hand-rolled, deliberately narrow reader for the one file shape this repo has:
// `dependabot.yml` as written here — a `version:` scalar and a flat `updates:`
// list of block mappings. It is not a YAML parser and is not trying to be. If the
// config ever grows anchors, flow mappings or nesting, these tests will fail
// loudly (a missing entry is a hard Fatal, never a silent pass) rather than
// quietly mis-parse, which is the failure mode that matters here.

// workflowStep returns the YAML block for the step named name: from its
// "- name:" line up to the next line at the same indentation starting a new list
// item. Scoping to one step matters — otherwise an assertion could be satisfied
// by an unrelated step elsewhere in the file.
func workflowStep(doc, name string) (string, bool) {
	lines := strings.Split(doc, "\n")
	start := -1
	var indent string
	for i, line := range lines {
		if strings.HasSuffix(strings.TrimSpace(line), "name: "+name) &&
			strings.HasPrefix(strings.TrimSpace(line), "- ") {
			start = i
			indent = line[:strings.Index(line, "- ")]
			break
		}
	}
	if start < 0 {
		return "", false
	}
	for i := start + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], indent+"- ") {
			return strings.Join(lines[start:i], "\n"), true
		}
	}
	return strings.Join(lines[start:], "\n"), true
}

// stripQuoted removes single- and double-quoted spans, leaving roughly the
// commands. Crude, but the question it answers is narrow: does the step RUN
// something, or merely mention it in a message? (Distinct from unquote below,
// which unwraps one fully-quoted YAML scalar.)
func stripQuoted(s string) string {
	var b strings.Builder
	var quote rune
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

type entry struct {
	directory string
	patterns  []string
}

// readDependabot returns the parsed entries keyed by package-ecosystem.
func readDependabot(t *testing.T) map[string]entry {
	t.Helper()
	const path = "../../.github/dependabot.yml"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v\nCI's actions are pinned to SHAs; without Dependabot "+
			"nothing ever bumps them, so a pinned SHA just freezes.", path, err)
	}

	// Strip comments and blank lines. This is safe only because no VALUE in this
	// file contains a '#' — the `# vX.Y.Z` version comments live in the workflows,
	// not here. Asserted rather than assumed: if a value ever does, naive stripping
	// would truncate it and the affected key would read as empty, which is the kind
	// of quiet mis-parse these tests exist to prevent.
	var lines []string
	for i, line := range strings.Split(string(data), "\n") {
		if h := strings.Index(line, "#"); h >= 0 {
			if _, value, isPair := strings.Cut(line[:h], ":"); isPair && strings.TrimSpace(value) != "" {
				t.Fatalf("%s:%d: a value on this line is followed by '#'; this reader strips "+
					"comments naively and would truncate it. Move the comment to its own line.",
					path, i+1)
			}
			line = line[:h]
		}
		if strings.TrimSpace(line) != "" {
			lines = append(lines, strings.TrimRight(line, " \t"))
		}
	}

	if v := scalar(lines, "version"); v != "2" {
		t.Errorf("dependabot.yml version = %q, want \"2\" (v1 is unsupported and ignored)", v)
	}

	out := map[string]entry{}
	var cur *entry
	var curName string
	inPatterns := false

	commit := func() {
		if cur != nil && curName != "" {
			out[curName] = *cur
		}
		cur, curName, inPatterns = nil, "", false
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// A new updates: list item starts a new entry.
		if strings.HasPrefix(trimmed, "- package-ecosystem:") {
			commit()
			cur = &entry{}
			curName = unquote(after(trimmed, "- package-ecosystem:"))
			continue
		}
		if cur == nil {
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "directory:"):
			cur.directory = unquote(after(trimmed, "directory:"))
			inPatterns = false
		case trimmed == "patterns:":
			inPatterns = true
		case inPatterns && strings.HasPrefix(trimmed, "- "):
			cur.patterns = append(cur.patterns, unquote(after(trimmed, "-")))
		case strings.HasSuffix(trimmed, ":"):
			// Any other key (schedule:, groups:, a group name, …) ends a patterns list.
			inPatterns = false
		}
	}
	commit()

	if len(out) == 0 {
		t.Fatal("parsed no entries out of dependabot.yml — this test would be asserting " +
			"nothing; check the reader against the current file (it handles only a flat " +
			"`updates:` list of block mappings)")
	}
	return out
}

// scalar returns the value of a top-level `key: value` line.
func scalar(lines []string, key string) string {
	for _, line := range lines {
		if strings.HasPrefix(line, key+":") {
			return unquote(after(line, key+":"))
		}
	}
	return ""
}

func after(s, prefix string) string { return strings.TrimSpace(strings.TrimPrefix(s, prefix)) }

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

func ecosystems(cfg map[string]entry) []string {
	out := make([]string, 0, len(cfg))
	for k := range cfg {
		out = append(out, k)
	}
	return out
}

// usesRef extracts the ref from a `uses:` line, skipping local `./` paths.
func usesRef(line string) (string, bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(line), "- ")
	if !strings.HasPrefix(trimmed, "uses:") {
		return "", false
	}
	ref := after(trimmed, "uses:")
	if ref == "" || strings.HasPrefix(ref, "./") {
		return "", false
	}
	return ref, true
}

func workflowFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(workflows)
	if err != nil {
		t.Fatalf("read workflows dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if n := e.Name(); strings.HasSuffix(n, ".yml") || strings.HasSuffix(n, ".yaml") {
			out = append(out, filepath.Join(workflows, n))
		}
	}
	if len(out) == 0 {
		t.Fatal("no workflow files found under " + workflows)
	}
	return out
}

// workflowActions returns the deduplicated owner/name of every registry action in
// use (local `./...` refs excluded — nothing to update).
func workflowActions(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	var out []string
	for _, f := range workflowFiles(t) {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			ref, ok := usesRef(line)
			if !ok {
				continue
			}
			name, _, _ := strings.Cut(ref, "@")
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("found no actions in .github/workflows — this test would assert nothing; " +
			"check the parser against the current workflow layout")
	}
	return out
}

// globMatch implements the only wildcard Dependabot patterns use: `*`, matching
// any run of characters (including `/`, so `*` alone matches everything).
func globMatch(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for _, p := range parts[1 : len(parts)-1] {
		i := strings.Index(s, p)
		if i < 0 {
			return false
		}
		s = s[i+len(p):]
	}
	return strings.HasSuffix(s, parts[len(parts)-1])
}
