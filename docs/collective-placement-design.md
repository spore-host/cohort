# Collective placement — design note (cohort #5)

**Status:** design, pre-implementation. Decision settled: a collective cohort
places **as a unit** (shared placement ladder), not per-entity — the structural
fix for the MPI placement-group-under-fallback gap (#5). API surface settled:
**implied by `IsCollective()`** — `NewMPICohort` gets shared-rung placement;
serial/partial cohorts keep today's per-entity behavior. No new API.

This note compares the two ways to *implement* shared-rung placement against the
current reconcile loop, so we choose with eyes open. No code yet.

---

## The invariant we're enforcing

A tightly-coupled cohort (MPI) has a constraint the core can't see: every member
must land on a placement consistent with its siblings (same AZ, so the cluster
placement group / EFA fabric forms). "Same AZ" is **MPI-on-AWS vocabulary** — it
must NOT enter the core. The core enforces only the general rule:

> In a collective cohort, all members share ONE placement. A capacity fault by
> any member advances the **cohort's** placement; the constraint is satisfied by
> construction because every member is always on the same rung.

The shared rung is provider-typed (`RungPlacement` for AWS); the core advances it
via `Placement.Advance()` and never knows it's an AZ.

---

## Current loop (what we're changing)

`Reconcile` launches N entities as independent goroutines under an errgroup
(`reconcile.go:190`). Each runs `reconcileEntity` → `doLaunch`, and **each owns
and advances its own `tr.intent.Placement`** on `FaultCapacityExhausted`
(`doLaunch`, reconcile.go:459-470). Coordination is only *after* failure: a
member going terminal may trip `fastFailCancel`, draining survivors.

So today: placement is per-entity; there is no shared rung and no notion that one
member's ICE should move another. That independence is exactly the bug for a
collective.

---

## Option 1 — Shared-rung + round restart (structural)

**Model.** A collective cohort's launch phase runs as *rounds* over a shared
placement `R`:

```
R := members' shared starting placement
loop:
  attempt to launch ALL N members on R (in parallel)
  if every member acked launch:        → proceed to running/barrier/assembly
  if any member hit CapacityExhausted:  → abandon the round:
        drain any members that DID launch on R   (PG can't span AZs — they must move)
        R, ok := R.Advance()
        if !ok: fast-fail the cohort (chain exhausted)   → drain, terminal
        else: restart the loop on the new R
  if any member hit a Terminal (non-capacity) fault:  → fast-fail immediately
```

**Why it's clean.** The invariant is *structural*: at any instant every member is
on the same `R`, so "same AZ" can never be violated — there's nothing to police.
It is the all-or-nothing thesis extended from outcome to placement: the cohort
launches as a unit, and if the unit can't fit on a rung, the whole unit moves.

**Cost.** An advance after a partial launch drains and re-launches the members
that already came up on the old rung. That's real EC2 churn — but it's *correct*:
a half-placed MPI cluster on a now-abandoned AZ is useless, which is exactly why
cohort already drains a fast-failed cohort. The cost is inherent to the AWS
constraint (a PG cannot span AZs), not to this design.

**Reconcile-loop impact.** Larger. The per-entity `doLaunch` capacity-advance
(459-470) is removed for collective cohorts; the launch *phase* becomes a
cohort-level round loop wrapping the existing per-entity launch. Phases 2–4
(running/enrolled/barrier/assembly) are unchanged — they already operate on the
member set as a whole. Serial/partial cohorts keep `doLaunch` exactly as-is.

**Concurrency.** Simple. Within a round, members launch in parallel (today's
errgroup), but the round *barrier* (all-acked vs any-ICE) is a single join point —
no cross-goroutine rung mutation, no races. The shared `R` only changes between
rounds, single-threaded.

**Failure legibility.** Each member's `Record.Attempts` shows the rungs tried in
order (round 1 → R0, round 2 → R1, …) — the same Attempt trail, now coherent
across the cohort (every member shows the same rung sequence, which is the truth).

---

## Option 2 — Shared-rung pointer + per-entity re-sync (reactive)

**Model.** Members still each run `doLaunch` in their own goroutine, but the
placement is a shared, mutex-guarded `*sharedPlacement` instead of `tr.intent`'s
own. On ICE, a member: locks, advances the shared rung (deduping concurrent
advances by generation), then *signals* all siblings to abandon their current
attempt and re-launch on the new rung.

**Why it's tempting.** Smaller diff to the orchestration — keeps the
N-goroutine structure; `doLaunch` reads `shared.Current()` instead of
`tr.intent.Placement`.

**Why it's worse (the wart we're trying to avoid).** The cross-goroutine signal
is the racy part:
- Two members can ICE near-simultaneously and both try to advance — must dedupe
  (advance is idempotent per generation, but a member mid-launch on R(n) when R
  becomes R(n+1) must be caught and unwound).
- A member that *already acked* a launch on R(n) when a sibling advances to
  R(n+1) must be detected and drained+relaunched — i.e. we still need the
  drain-and-restart of Option 1, but now triggered asynchronously mid-flight
  instead of at a clean round boundary.
- "Restart this goroutine's launch" has no clean primitive in the current loop;
  it means cancelling and re-entering `doLaunch`, which tangles with the existing
  `fastFailCancel`/budget contexts.

So Option 2 does **all the work of Option 1** (it still drains and relaunches on
advance) but pays for it with concurrent-mutation complexity and a fuzzier
failure story. It's the predicate option's race in a different coat.

---

## Recommendation

**Option 1 (shared-rung + round restart).** It's the larger reconcile-loop change
but the *simpler system*: the invariant is structural (can't be violated), the
concurrency is a clean per-round join (no shared-mutable-rung races), and the
failure trail stays coherent. Option 2 is a smaller diff that reintroduces exactly
the concurrency hazard we rejected the predicate approach to avoid.

Scope, concretely:
- `Cohort` (collective only): one shared starting `Placement` rather than
  per-member placements. (Per-member `EntityIntent.Placement` still exists; for a
  collective cohort the constructor sets them all to the same shared rung, and the
  round loop advances them together.)
- New: a `launchCollective` round loop used when `c.IsCollective()`, wrapping the
  existing per-entity launch + a drain-on-advance. Serial/partial unchanged.
- The per-entity capacity-advance in `doLaunch` (459-470) is bypassed for
  collective members (the round loop owns advancement); still used for
  serial/partial.
- Prove it on the spawn-MPI spike: an ICE on one node advances the whole cohort's
  AZ, the already-launched siblings drain and relaunch, and the assembled cohort
  is entirely on the new AZ (invariant preserved end-to-end).

Open sub-question for implementation: whether the shared placement is modeled as
a field on `Cohort` or derived from member[0]'s placement (all members identical
by construction). Leaning toward an explicit cohort-level shared placement to make
"the cohort is the unit of placement" legible in the type, not implicit.
