# Changelog

All notable changes to **cohort** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
