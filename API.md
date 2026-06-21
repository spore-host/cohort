# internal/cohort — API Surface Review

**Status:** provisional, v0.x pre-extraction. All interfaces are still
movable in coordinated multi-repo commits. v1.0 is earned by the co-proof
(§7), not declared here.

**Step 5.8 updates:** OQ-1..5 resolved (see §Open Questions at end).  
**Step 5.9 updates:** three follow-on items resolved; cohort-internal hardening complete.

---

## 1. Exported vs internal — keep/unexport verdicts

All names below are in `package cohort`. "External consumer" means spawn's
MPI transplant or queuezero's Slurm domain, once cohort is its own module.

### KEEP EXPORTED — consumer must name it

| Name | File | Reasoning |
|---|---|---|
| `Reconciler` | reconcile.go | Entry point; spawn and ASBX both construct one |
| `NewReconciler` | reconcile.go | Constructor added this commit (see §5) |
| `Reconciler.Reconcile` | reconcile.go | The single public method consumers call |
| `Reconciler.Drain` | reconcile.go | Called by ASBX suspend-sweeper and by emergency teardown paths outside the normal reconcile loop |
| `RateLimiter` | reconcile.go | Filed for account-shared throttle; substrate passes *Limiter through this |
| `Actuator` | ports.go | Provider seam — implemented per cloud |
| `Observer` | ports.go | Provider seam — implemented per cloud |
| `Classifier` | ports.go | Provider seam — implemented per cloud |
| `Enroller` | ports.go | Domain seam — implemented per domain |
| `Assembler` | ports.go | Domain seam — implemented per domain |
| `Cohort` | cohort.go | Caller-constructed input to Reconcile |
| `NewSerialCohort` / `NewMPICohort` / `NewPartialCohort` | cohort.go | Validating constructors (Step 5.8); default zero Budget to DefaultBudget() |
| `NewEntityIntent` | entity.go | Validates ID, Rung, chain; auto-generates deterministic token |
| `Token` | token.go | Deterministic idempotency-key derivation; provider-agnostic |
| `CohortID` | cohort.go | Used in Cohort and in Record; consumers match cohorts by ID |
| `PhaseBudget` / `DefaultBudget` | cohort.go | Caller-set per partition |
| `Outcome` | cohort.go | Returned by Reconcile |
| `EntityID` | entity.go | The identity-preserving name — passed everywhere |
| `Generation` | entity.go | Used in EntityIntent and in Record for reap-safety |
| `EntityIntent` | entity.go | Caller-constructed; one slot in Cohort.Members |
| `Rung` / `CapacityModel` / `CapacityOnDemand/Spot/Reserved` | entity.go | Fallback-chain building block |
| `Observation` | entity.go | Returned from Actuator/Observer; passed to Assembler |
| `Readiness` | entity.go | Returned from Enroller |
| `LifecycleState` / `State*` consts | state.go | Observer and test code must pattern-match state |
| `StopMode` / `StopWarm` / `StopHibernate` | state.go | Actuator.Stop argument |
| `FaultClass` / `Fault*` consts | state.go | Classifier returns Fault; consumers match class |
| `Fault` | state.go | Returned by Classifier; stored in Record.Terminal |
| `Phase` / `Phase*` consts | state.go | Record carries ReachedPhase; Slurm domain encodes it in scontrol reason |
| `Record` | explain.go | Read-only output; `q0 explain` renders it |
| `Attempt` | explain.go | Embedded in Record.Attempts |
| `CohortCancelInfo` / `ParentCancelInfo` | explain.go | Read from Record; consumers distinguish culprit from survivor |
| `BackoffPolicy` / `DefaultBackoffPolicy` | backoff.go | Provider-agnostic; substrate constructs its own instance for throttle path |

### ALREADY UNEXPORTED — correct as-is

| Name | File | Reasoning |
|---|---|---|
| `entityTracker` and all its methods | reconcile.go | Pure reconciler-internal bookkeeping; zero reason for a consumer to touch |
| `culpritInfo` | reconcile.go | Internal coordination struct between goroutines |
| `sleep` | reconcile.go | Utility; not an API concept |
| `maxConsistencyRetries` / `pollInterval` | reconcile.go | Tuning knobs, but internal ones — tuning is via PhaseBudget and BackoffPolicy |

### OPEN QUESTION — debatable, not changed

| Name | Issue |
|---|---|
| `Reconciler.Clock` | Exported field used only in tests. Idiomatic Go test injection uses an unexported field set via a test-scoped constructor (e.g. `newReconcilerWithClock`). Leaving exported for now because it is a non-breaking removal later and the test ergonomics are fine. Revisit before v1.0. |
| `Reconciler.Drain` | Could be unexported (it is called internally and is also a seam for external teardown). Keep exported until Phase 2 suspend-sweeper is wired; reassess then. |
| `Fault.Retryable` | Convenience bool; a consumer can derive it from `Class`. Not harmful to keep, but redundant. Flag for v1.0 cleanup. |

---

## 2. The five port interfaces — vocabulary check

The substitution test: read each signature with "Globus" mentally swapped for
"MPI" / "Slurm." If it only makes sense for one domain, the seam leaks.

### Actuator
```go
Launch(ctx, EntityIntent) (Observation, error)
Start(ctx, EntityID) (Observation, error)
Stop(ctx, EntityID, StopMode) error
Terminate(ctx, EntityID) error
```
**PASS.** All vocabulary is domain-neutral: *launch*, *start*, *stop*,
*terminate*, *EntityID*, *StopMode* (warm/hibernate). A Globus-domain Actuator
that creates and destroys DTN VMs fits this signature without any rename. Note
that `StopMode` (warm vs hibernate) is cloud-flavoured — a non-cloud domain
would always pass `StopWarm` — but this is not a leak: the interface is callable
from any domain, even if only one mode is meaningful.

### Observer
```go
Observe(ctx, []EntityID) ([]Observation, error)
```
**PASS.** `Observation` carries only cohort vocabulary: `ID`, `State`
(LifecycleState), `ProviderID`, `Rung`, `Address`, `ObservedAt`. No cloud type
leaks through. `ProviderID` is a plain string — an EC2 instance ID or a VM ID
or a Globus endpoint ID all fit.

### Classifier
```go
Classify(err error) Fault
```
**PASS.** Takes a raw error; returns a Fault with a `FaultClass` enum and a
verbatim string code. The doc notes it is the most provider-specific artifact and
explicitly NOT portable — the interface itself is clean, but every cloud needs its
own implementation. This is the correct design.

**One flag:** `FaultAmbiguous` is documented as "must not escape `substrate.Client`."
This is a protocol, not a type constraint. The interface permits `FaultAmbiguous`
to be returned by any Classifier; only the `Reconciler` enforces it via the loud
BUG path. Consider adding a doc comment on `Classifier` explicitly: "implementors
of this interface for use with `Reconciler` MUST NOT return `FaultAmbiguous`; use
idempotency tokens to resolve ambiguity before classification reaches here."
**Action: add the doc comment (in this commit).**

### Enroller
```go
IsEnrolled(ctx, EntityID) (Readiness, error)
```
**PASS (resolved in Step 5.8).** `Readiness.MountHealthy` was renamed to
`Readiness.Operational`. The concept is unchanged — "fully functional, not merely
running" — but the name now generalizes off HPC vocabulary. What "operational"
means is domain-defined: Slurm checks mounts, MPI checks EFA health, Globus checks
endpoint health. cohort defines only the slot, not the content.

### Assembler
```go
Assemble(ctx, []Observation) error
```
**PASS.** "Assemble" is domain-neutral: MPI PMIx wire-up, Globus mesh join, Slurm
hostfile publication — all read as assembly over a complete, simultaneously-live
set. `[]Observation` is cohort vocabulary. The Assembler receives only what it
needs and returns only pass/fail; the reconciler never inspects topology.

---

## 3. Struct fields as contract

### EntityIntent — caller-constructed

| Field | Zero-value trap? | Notes |
|---|---|---|
| `ID` | ~~Empty string silently creates an entity with no name.~~ **Resolved (OQ-2): `NewEntityIntent` rejects empty ID.** | |
| `Generation` | Empty string is valid (e.g. bootstrap generation). Fine. | |
| `Cohort` | Must match the parent Cohort.ID. Not validated. | Low risk; mismatch surfaces in Record, not silently wrong. |
| `Placement` | ~~Zero-value Rung (empty strings) would pass an empty InstanceType to Launch.~~ **Resolved (#1, v0.2.0): `Rung` + `FallbackChain` replaced by an opaque `Placement` seam. `NewEntityIntent` and the Cohort constructors call `validatePlacement` (nil check + non-empty `Current().Name` + the placement's optional `Validate()`).** | |
| `IdempotencyToken` | ~~Empty token means no idempotency.~~ **Resolved (OQ-3): `NewEntityIntent` auto-generates via `cohort.Token(cluster, entity, generation)` — deterministic SHA-256 hash. Random tokens are rejected by design; determinism is the idempotency guarantee.** | |

### Placement — the provider-specific placement seam (#1, v0.2.0)

`EntityIntent` no longer embeds the EC2-shaped `Rung`/`FallbackChain` directly.
It carries an opaque `Placement` — parallel to how `Classifier` is per-provider.
The reconciler advances the fallback ladder through `Placement.Advance()` on a
fallback-eligible fault and renders `Placement.Current()` (a `PlacementRung` of
`{Name, Class, WarmStart}`) into the `Record`; it never inspects provider fields.

- **AWS provider** supplies `cohort.RungPlacement{Rung, Chain}` — the built-in
  adapter wrapping today's `Rung` (unchanged, still exported). Migration is
  near-mechanical: `Rung: r, FallbackChain: chain` → `Placement: cohort.RungPlacement{Rung: r, Chain: chain}`.
- **Non-cloud providers** (agent transports, …) supply their own `Placement`
  with no instance type / AZ / capacity model — proven in `placement_test.go`,
  which reconciles a goroutine→session→instance ladder through the unmodified
  core. This is the third-consumer generalization the v1.0 thesis depends on.

`Rung` (instance type / AZ / `CapacityModel` / `WarmStart` / multi-account
`AccountID`) remains the AWS provider's vocabulary, carried into the core only
via `RungPlacement`. A non-AWS provider never constructs a `Rung`.

### Cohort — caller-constructed

| Field | Zero-value trap? | Notes |
|---|---|---|
| `ID` | Empty string allowed (1-entity workloads may not need a name). Low risk. | |
| `Members` | Nil/empty = reconcile succeeds immediately with no work. Surprising but not harmful. | |
| `Budget` | ~~Zero PhaseBudget = all timeouts fire instantly. **Critical trap.**~~ **Resolved (OQ-5): `NewSerialCohort`, `NewMPICohort`, and `NewPartialCohort` apply `DefaultBudget()` when the passed budget is fully zero.** | |
| `MinViable` | Zero = full membership (documented). **`NewPartialCohort` requires `minViable > 0`; `NewMPICohort` sets it to `len(members)`.** | |

### Record — returned to caller, never constructed externally

All fields are read-only from the caller's perspective. The inspection path via
`Succeeded()`, `WasCohortCancelled()`, `WasParentCancelled()`, `Summary()`,
`Explain()` is complete (see §4). The exported struct fields are accessible for
consumers who want to walk `Attempts` or format their own rendering.

---

## 4. Error / outcome inspection

Can a consumer answer every outcome question without reaching into unexported fields?

| Question | Method | Complete? |
|---|---|---|
| Did this entity succeed? | `rec.Succeeded()` | Yes |
| Did the cohort fast-fail around it? | `rec.WasCohortCancelled()` | Yes |
| Who was the culprit and why? | `rec.CohortCancelled.CulpritID`, `.CulpritFault.Code`, `.CulpritPhase` | Yes — all exported |
| Was it a parent-context cancel? | `rec.WasParentCancelled()` | Yes |
| What phase did it reach? | `rec.ReachedPhase` (Phase, exported) | Yes |
| What was the verbatim fault? | `rec.Terminal.Code`, `.Class` | Yes |
| What rungs were tried? | `rec.Attempts` ([]Attempt, all exported fields) | Yes |
| One-line reason for scontrol? | `rec.Summary()` | Yes |
| Full trace for q0 explain? | `rec.Explain()` | Yes |

**No inspection requires reaching into unexported fields.** The surface is complete.

One mild gap: `Outcome.Ready` is a bool. A consumer who wants to know *why* it is
not ready must iterate `Records`. This is by design (each entity has its own
reason) and is not a gap.

---

## 5. Construction — decision and implementation

**Step 5.7:** `NewReconciler` constructor added. Signature:
```go
func NewReconciler(act Actuator, obs Observer, clf Classifier,
    enr Enroller, asm Assembler, lim RateLimiter) *Reconciler
```
`Enroller`, `Assembler`, and `Limiter` may be nil (documented). `Clock` remains
exported for test injection, marked `TEST-ONLY`.

**Step 5.8:** Three validating Cohort constructors added:

```go
func NewSerialCohort(id CohortID, member EntityIntent, budget PhaseBudget) (Cohort, error)
func NewMPICohort(id CohortID, members []EntityIntent, budget PhaseBudget) (Cohort, error)
func NewPartialCohort(id CohortID, members []EntityIntent, budget PhaseBudget, minViable int, asm Assembler) (Cohort, error)
```

All three fill every individually-zero `PhaseBudget` field from `DefaultBudget()`
(field-by-field, not all-or-nothing — see Step 5.9). `NewMPICohort` sets
`MinViable = len(members)`. `NewPartialCohort` validates `0 < minViable ≤ len(members)`.

**Step 5.9.1:** `applyDefaultBudget` is now field-by-field: each zero duration
is filled independently. A partially-set budget (e.g. `Running` set, `CohortBarrier`
zero) no longer leaves the zero field as an instant deadline.

**Step 5.9.2:** `substrate.Token` implementation deleted; now delegates to
`cohort.Token`. One canonical implementation, zero drift window. guard-cohort
confirmed: cohort still imports nothing from substrate.

**Step 5.9.3:** `NewPartialCohort` accepts an `Assembler` parameter and rejects
non-nil with an explicit error: "partial cohorts do not support an assembly phase."
`Cohort.NoAssembly bool` field set by the constructor; `Reconcile` checks it and
returns `AssemblyDisallowed` fault if a Reconciler has a non-nil Assembler and the
cohort prohibits it. Defense-in-depth: catches callers who bypass the constructor.

`NewEntityIntent` added (token.go + entity.go):
```go
func NewEntityIntent(cluster string, id EntityID, gen Generation, cohortID CohortID,
    rung Rung, chain []Rung, token string) (EntityIntent, error)
```
Validates ID non-empty, Rung.InstanceType non-empty, all FallbackChain rungs valid.
Auto-generates a deterministic token via `cohort.Token(cluster, entity, generation)`
when token is empty (OQ-3). `cohort.Token` is SHA-256 of the three inputs — same
algorithm as the temporary `substrate.Token`, which substrate will retire in favor
of this once cohort is extracted.

---

## 6. BackoffPolicy placement

`BackoffPolicy` belongs in `package cohort`. It is:
- Provider-agnostic: computes durations, imports nothing scheduler/cloud specific.
- Used by the reconciler internally for `RetryableConsistency` retries.
- Exported so substrate can construct a SEPARATE instance with a longer cap for
  its throttle retry path.

`substrate/aws/client.go` constructs `throttleBackoff` as a package-level
`cohort.BackoffPolicy` literal (500ms base, 60s cap, 20% jitter) — does NOT use
`DefaultBackoffPolicy()`. This is correct: the throttle path needs a longer cap
than the consistency-lag path. The separation is working as designed.

**No change needed.**

---

## 7. Module path and versioning

**Intended import path (once extracted):** `github.com/spore-host/cohort`

Rationale: cohort is not a spawn sub-tool — it supersedes spawn's collective
path and is the conceptual heart of the spore.host suite. It should sit at the
same level as `spawn`, `truffle`, `lagotto`, `spored` — a top-level peer.

**Version:** starts at `v0.x`, explicitly pre-1.0. Interfaces are still movable
in coordinated commits across `github.com/spore-host/cohort`, `github.com/spore-host/spawn`,
and `queuezero/queuezero`.

**v1.0 is earned by the co-proof (ARCHITECTURE §15):** the same unmodified cohort
core must compile against both:
1. The MPI domain (spawn transplant — Step 6 of the Phase 1 build plan)
2. The Slurm domain (queuezero ASBX — Phase 2)

When both compile against an unmodified core AND all tests pass, the seams are
proven. That earns v1.0 — not a calendar date.

**Current home:** `internal/cohort` within `github.com/queuezero/queuezero`. The
`internal/` keyword makes the package uncallable from outside the module, keeping
the interfaces movable until the co-proof milestone.

---

## 8. The guard travels with cohort

`make guard-cohort` (scripts/guard-cohort.sh) enforces the import discipline:
`internal/cohort` must import no cloud SDK and no scheduler. Currently this
check lives in queuezero's Makefile.

**When cohort becomes its own repo, `guard-cohort` MUST move into cohort's own
CI.** It cannot depend on a consumer's build to enforce the rule that makes cohort
reusable. Concretely:

- `spore-host/cohort` CI must run `go list ./...` and assert no import from
  `github.com/aws/aws-sdk-go-v2/...`, `azure-sdk`, `cloud.google.com`, or any
  scheduler package.
- The check is not optional: it is the invariant that makes the extraction a
  `git mv` rather than an archaeology project, and it is what future domain
  consumers depend on.

---

## Open questions

| # | Item | Status | Risk | When to resolve |
|---|---|---|---|---|
| OQ-1 | `Readiness.MountHealthy` naming — HPC-specific | **RESOLVED (Step 5.8):** renamed to `Readiness.Operational`; doc states it is domain-defined | — | — |
| OQ-2 | `EntityIntent.ID` empty string not validated | **RESOLVED (Step 5.8):** `NewEntityIntent` rejects empty ID | — | — |
| OQ-3 | `EntityIntent.IdempotencyToken` empty silently disables idempotency | **RESOLVED (Step 5.8):** `NewEntityIntent` generates deterministic token via `cohort.Token()`; option (a) chosen — derivation lives in cohort, provider-agnostic | — | — |
| OQ-4 | `Rung` zero-value (empty InstanceType) not validated | **RESOLVED (Step 5.8):** `validateRung` called in `NewEntityIntent` and all Cohort constructors | — | — |
| OQ-5 | `PhaseBudget` zero-value trap (all deadlines fire instantly) | **RESOLVED (Step 5.8):** `applyDefaultBudget` in all Cohort constructors; behavioral test asserts phases actually run | — | — |
| OQ-6 | `Reconciler.Clock` exported — test-only field on a production struct | Open | Low | Before v1.0; change to unexported + test setter |
| OQ-7 | `Reconciler.Drain` — keep exported? | Open | Low | Reassess after Phase 2 suspend-sweeper is wired |
| OQ-8 | `Fault.Retryable` redundant (derivable from Class) | Open | Low | v1.0 cleanup |
| OQ-9 | `Classifier` interface doc — "MUST NOT return FaultAmbiguous" | **RESOLVED (Step 5.7):** doc comment added to interface | — | — |
| #1 | `EntityIntent.Rung`'s EC2 field vocabulary doesn't generalize to non-cloud providers | **RESOLVED (v0.2.0):** `Rung`+`FallbackChain` → opaque `Placement` seam; AWS uses built-in `RungPlacement`; non-Rung placement proven in `placement_test.go`. Third-consumer (Telos) report. | — | — |

---

*This document is the v0.x API surface review. Revisit after the co-proof
milestone (Step 6 MPI transplant + Phase 2 Slurm domain) for the v1.0 surface
decision.*
