# Changelog

All notable changes to **cohort** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **CI now fails on unformatted code, and four files that had drifted are
  reformatted (#8).** `entity.go`, `reconcile.go`, `reconcile_test.go` and
  `state.go` were not `gofmt`-clean on `main`, because nothing checked: `go vet`
  does not look at formatting, and the repo has no Makefile target that would.
  A `Format gate` step in `ci.yml` now runs `gofmt -l` and fails with a diff of
  the offenders. The distinction matters — a gate built on `gofmt -w` rewrites
  files and exits 0, so it can only ever report success, which is
  indistinguishable from having no gate at all. The reformatting is
  whitespace-only (`git diff --ignore-all-space` is empty). An
  `internal/hygiene` test asserts the step exists *and* that it lists rather
  than rewrites, so neither the gate nor its meaning can be quietly removed.
- **A pin's version comment can no longer silently misstate what CI runs (#11).**
  `TestActionsArePinnedToSHAs` required only that *some* `# vN` comment be present,
  never that it was true. A wrong label is worse than a missing one: it makes a
  major-version jump read as a routine same-line bump. Not hypothetical —
  Dependabot bumped nf-spawn's `checkout` pin to a **v7.0.1** SHA while leaving the
  comment reading `# v6`, and the identical regex passed it. Two
  complementary halves now: the test requires an exact `vX.Y.Z` (offline,
  stdlib-only, hermetic), and a new `scripts/verify-pins.sh` resolves each SHA
  against the tag its comment claims and fails if they disagree (needs the network,
  so it runs as its own CI step). Neither alone suffices — a bare label defeats the
  second, an exact-but-false one defeats the first.
  cohort's own two pins were already exact and true, so nothing needed relabelling
  here; the gate is what changed.

### Security
- **CI actions are pinned to commit SHAs, and Dependabot now bumps them (#7).**
  Both `uses:` refs in `ci.yml` were floating tags (`actions/checkout@v6`,
  `actions/setup-go@v6`). A tag is mutable, so the code running in CI could change
  with no commit here — not hypothetically: `actions/checkout@v6` moved from
  `df4cb1c` to `d23441a` with no signal to consumers. Both are now 40-hex SHAs
  with a `# v6` comment. Pinning alone would only trade a live hole for a slow one
  (a SHA never moves, including past a security fix), so `.github/dependabot.yml`
  arrives with it: `github-actions` to bump the pins and `gomod` to watch
  `golang.org/x/sync`, which nothing was watching — this repo has no govulncheck
  or Trivy workflow. A new `internal/hygiene` test package fails CI if a pin is
  reverted or an entry is dropped, since either is a silent one-line change. The
  tests are stdlib-only on purpose: cohort advertises exactly one dependency, and
  a YAML library pulled in to read a config would undercut that.

## [0.2.0] - 2026-06-21

### Added
- **Collective placement: an all-or-nothing cohort now places as a unit (#5).**
  A cohort with `MinViable == len(Members)` (e.g. `NewMPICohort`) shares ONE
  placement ladder: any member's capacity fault advances the **cohort's** rung
  and all members (re)launch on it together, draining anything already up on the
  abandoned rung. Previously each member advanced its own placement
  independently — which for MPI silently broke the cluster placement group (one
  node could fall back to a different AZ than its siblings). The AZ invariant now
  holds by construction: every member is always on the same rung. Partial-success
  cohorts (`MinViable < len`) keep per-entity placement, unchanged. The
  capacity-exhaustion failure still names a culprit (Terminal) with the rest
  CohortCancelled, preserving legibility. No new API — implied by the
  all-or-nothing shape. (Design: `docs/collective-placement-design.md`.)

### Fixed
- Enrollment no longer destroys an entity's observed address. `waitEnrolled`
  was assigning `Readiness.Detail` (a human-readable display string, e.g.
  "efa ok") into `Observation.Address` — the private IP the `Assembler` needs
  for MPI hostfile / PMIx wire-up. So a successful enrollment overwrote the
  address with display text, and the collective assembly phase received
  addressless members. `Address` now stays as the Observer reported it
  (infrastructure truth); `Readiness.Detail` is surfaced where it was always
  documented to go — `Record.EnrollDetail`, rendered by `Explain()`. Found by
  the first consumer (spawn-MPI) that actually reads `Address` through
  `Assemble`; cohort's own suite missed it because its assemblers were no-ops.
  Added a regression test that an Assembler receives the observed address, not
  the Detail string.

### Changed
- **BREAKING (#1): `EntityIntent.Rung` + `FallbackChain` replaced by an opaque
  `Placement` seam.** The EC2/capacity-market vocabulary (`InstanceType`,
  `AvailZone`, `CapacityModel`, …) was baked into the caller-facing input type,
  so a non-cloud consumer (e.g. an agent-transport reconciler) had to fabricate
  fake instance types to construct an intent. `Placement` is now an interface —
  parallel to `Classifier` being per-provider — that the core advances on a
  fallback-eligible fault but never inspects. The fallback-ladder *mechanism* is
  unchanged; only the field vocabulary moved behind the seam.
  - The AWS/MPI consumers migrate via the built-in `cohort.RungPlacement{Rung, Chain}`
    adapter (`Rung` and `CapacityModel` stay exported, unchanged):
    `Rung: r, FallbackChain: chain` → `Placement: cohort.RungPlacement{Rung: r, Chain: chain}`.
  - `NewEntityIntent`'s signature changes: `(…, rung, chain, token)` →
    `(…, placement, token)`.
  - `Record.Attempt.Rung` is now a provider-agnostic `PlacementRung{Name, Class,
    WarmStart}`, so `Explain()` renders a legible line for any provider.
  - Coordinated v0.2.0 release: queuezero/substrate (AWS) and Telos (transport)
    migrate against this tag. `placement_test.go` proves a non-`Rung` placement
    reconciles end-to-end in cohort's own suite.

### Added
- `scripts/guard-cohort.sh` + a `make guard` target + a required CI step that
  assert the core imports no cloud SDK and no scheduler (#2). API.md §8 calls
  this guard "not optional" — it's the invariant that lets every consumer trust
  the same unmodified core — but it didn't travel into the standalone repo's CI
  (which ran only build/vet/test). The dependency graph is clean today
  (`golang.org/x/sync` only); this locks that in.

## [0.1.0] - 2026-05-30

### Added
- Initial standalone release. `cohort` graduated verbatim from queuezero's
  `internal/cohort` once two independent domains (MPI and Slurm) compiled
  against an unmodified core.
- The reconciliation core: `Reconciler` (+ `NewReconciler`, `Reconcile`,
  `Drain`), the cohort/entity model (`Cohort`, `EntityIntent`, `EntityID`,
  `CohortID`, `Generation`, `Rung`, `CapacityModel`), lifecycle + phase types
  (`LifecycleState`, `Phase`, `PhaseBudget`, `StopMode`), fault classification
  (`Fault`, `FaultClass`), observation/readiness (`Observation`, `Readiness`),
  the structured `Outcome`/`Record` legibility surface, deterministic
  idempotency tokens (`Token`), and backoff (`BackoffPolicy`).
- The two seams as interface-only ports: provider (`Actuator`/`Observer`/
  `Classifier`/`RateLimiter`) and domain (`Enroller`/`Assembler`).
- `API.md` — the exported-surface review and keep/unexport rationale.

Only dependency: `golang.org/x/sync`.

[Unreleased]: https://github.com/spore-host/cohort/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/spore-host/cohort/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/spore-host/cohort/releases/tag/v0.1.0
