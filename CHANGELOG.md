# Changelog

All notable changes to **cohort** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/spore-host/cohort/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/spore-host/cohort/releases/tag/v0.1.0
