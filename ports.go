package cohort

import "context"

// ---------------------------------------------------------------------------
// Provider seam — supplied per cloud. AWS implementations live in
// internal/substrate/aws. None of these may leak provider types into cohort.
// ---------------------------------------------------------------------------

// Observer reports the actual, infrastructure-truth state of named entities.
//
// Implementations MUST tolerate eventual consistency: a Describe-miss on a
// freshly created entity is reported as StateUnknown, never StateAbsent. The
// reconciler decides what a miss means using the idempotency token as ground
// truth (see substrate.Client) — the Observer only reports what it sees.
type Observer interface {
	Observe(ctx context.Context, ids []EntityID) ([]Observation, error)
}

// Actuator drives a single entity toward a desired lifecycle state. It NEVER
// operates on counts; every call names exactly one entity. This is the
// non-negotiable consequence of "the named entity is the unit" — a count
// abstraction structurally cannot express partial-failure correctness.
type Actuator interface {
	// Launch creates a new entity. The Intent carries the deterministic
	// idempotency token; re-issuing Launch after an Ambiguous fault is safe.
	Launch(ctx context.Context, intent EntityIntent) (Observation, error)
	// Start resumes a Stopped or Hibernated entity. May itself fault to
	// CapacityExhausted — a warm entity is not reserved capacity.
	Start(ctx context.Context, id EntityID) (Observation, error)
	// Stop transitions a Running entity to Stopped or Hibernated per mode.
	Stop(ctx context.Context, id EntityID, mode StopMode) error
	// Terminate destroys an entity. Idempotent.
	Terminate(ctx context.Context, id EntityID) error
}

// Classifier maps a provider error into exactly one Fault class.
//
// This is the single most provider-specific artifact in queuezero and is NOT
// portable across clouds. Each provider supplies its own. See
// docs/ARCHITECTURE.md §5 and §13.
//
// Implementations used with Reconciler MUST NOT return FaultAmbiguous.
// FaultAmbiguous means "mutation status unknown" and must be resolved by the
// provider layer (via idempotency-token re-issue) before classification reaches
// the reconciler. Returning FaultAmbiguous here is a bug; Reconciler flags it
// loudly as a Terminal fault with code "AmbiguousReachedReconciler".
type Classifier interface {
	Classify(err error) Fault
}

// ---------------------------------------------------------------------------
// Domain seam — supplied per domain. Slurm/MPI implementations live in
// internal/slurm. A Globus domain (the future second consumer) would supply
// its own. The core invokes these; it never inspects what they do.
// ---------------------------------------------------------------------------

// Enroller is the domain probe for the late per-entity phase: "the entity has
// been accepted by whatever external authority the domain cares about."
// Slurm domain: slurmd has checked in. Globus domain: endpoint registered
// with the collection. The core knows the phase exists; not what it means.
type Enroller interface {
	IsEnrolled(ctx context.Context, id EntityID) (Readiness, error)
}

// Assembler is the cohort-scoped action that runs ONCE, after the collective
// barrier, over a complete and simultaneously-live cohort.
//
// Slurm/MPI domain: PMIx wire-up — publish the hostlist, exchange addresses.
// Globus domain: collection mesh join.
//
// The core invokes Assemble and learns only pass/fail. It never sees the
// address list, the topology, or the peer graph. Mechanism is the domain's;
// the phase-slot is the core's. This is the boundary that keeps cohort a
// reconciler and not a workflow orchestrator.
type Assembler interface {
	Assemble(ctx context.Context, members []Observation) error
}
