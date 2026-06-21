package cohort

import (
	"errors"
	"fmt"
	"time"
)

// EntityID is the stable, identity-preserving name of a single managed entity.
// In the Slurm domain this is the node name (e.g. "gpu-042"). It is NEVER a
// count, an index into an anonymous pool, or anything ASG-shaped.
type EntityID string

// Generation tags every entity with the spec revision that created it.
// Instances from a superseded partitions.yaml apply are unambiguously
// reapable; current-generation instances are protected from the suspend
// sweeper. See docs/ARCHITECTURE.md §11.
type Generation string

// EntityIntent is the desired specification for one entity. The reconciler's
// intent for a cohort is a set of these — a set of NAMED slots, never "N".
//
// Construct with NewEntityIntent to validate all fields and auto-generate an
// idempotency token if one is not supplied.
type EntityIntent struct {
	ID         EntityID
	Generation Generation
	Cohort     CohortID

	// Rung is the (instance type, AZ, capacity model) currently selected from
	// the approved fallback chain. The reconciler advances this on a
	// FaultCapacityExhausted; it never substitutes outside the chain.
	Rung Rung

	// FallbackChain is the ordered list of approved rungs from partitions.yaml.
	// The reconciler advances through this chain on FaultCapacityExhausted;
	// it NEVER substitutes a rung outside it. Empty means single-rung (no fallback).
	FallbackChain []Rung

	// IdempotencyToken is deterministic in (cluster, entity, generation).
	// It collapses FaultAmbiguous at the substrate layer — re-issuing a mutation
	// with the same token returns the existing resource rather than creating a
	// duplicate. MUST NOT be empty or random; use NewEntityIntent which generates
	// it via cohort.Token when not supplied by the caller.
	IdempotencyToken string
}

// NewEntityIntent constructs and validates an EntityIntent for one named entity.
//
// cluster is used only for token derivation; it need not appear in ID.
// If token is empty, one is generated deterministically via cohort.Token.
//
// Validation:
//   - ID must not be empty.
//   - Rung.InstanceType must not be empty (catches uninitialized Rung).
//   - Every rung in FallbackChain must have a non-empty InstanceType.
func NewEntityIntent(cluster string, id EntityID, gen Generation, cohortID CohortID, rung Rung, chain []Rung, token string) (EntityIntent, error) {
	if id == "" {
		return EntityIntent{}, errors.New("cohort: EntityIntent.ID must not be empty")
	}
	if err := validateRung(rung); err != nil {
		return EntityIntent{}, errors.New("cohort: EntityIntent.Rung: " + err.Error())
	}
	for i, r := range chain {
		if err := validateRung(r); err != nil {
			return EntityIntent{}, fmt.Errorf("cohort: EntityIntent.FallbackChain[%d]: %w", i, err)
		}
	}
	if token == "" {
		token = Token(cluster, string(id), string(gen))
	}
	return EntityIntent{
		ID:               id,
		Generation:       gen,
		Cohort:           cohortID,
		Rung:             rung,
		FallbackChain:    chain,
		IdempotencyToken: token,
	}, nil
}

// Rung is one option in a capacity fallback chain. There is no "safe
// baseline" — on-demand and spot are both rungs that can fault to capacity;
// they differ only in ICE probability and price. ODCR/capacity-block rungs
// are the one kind genuinely reserved against ICE.
type Rung struct {
	InstanceType  string
	AvailZone     string
	CapacityModel CapacityModel
	AccountID     string // execution account for this rung (multi-account, §3)
	               // Empty AccountID means single-account mode — the correct default.

	// WarmStart means resume a Stopped/Hibernated entity rather than cold-launch.
	// It is a RUNG property, not a pre-check: if warm-start ICEs, advanceRung
	// moves to the next rung in the chain exactly like any other ICE.
	WarmStart bool
}

// validateRung returns an error if rung has an empty InstanceType.
// An uninitialized Rung passes a zero InstanceType to Actuator.Launch, which
// would reach EC2 as garbage. Catch it at construction, not at launch time.
func validateRung(r Rung) error {
	if r.InstanceType == "" {
		return errors.New("InstanceType must not be empty")
	}
	return nil
}

type CapacityModel int

const (
	CapacityOnDemand CapacityModel = iota
	CapacitySpot
	CapacityReserved // ODCR / capacity block — should not ICE
)

// Observation is one entity's infrastructure-truth state as seen by an
// Observer. It is advisory: a StateUnknown is lag, and the idempotency token
// is consulted for ground truth.
type Observation struct {
	ID         EntityID
	Generation Generation
	State      LifecycleState
	ProviderID string // e.g. EC2 instance ID, once known
	Rung       Rung
	Address    string // private address, once Running — domain may need it
	ObservedAt time.Time
}

// Readiness is the result of a domain Enroller probe.
//
// Two fields beyond "enrolled?":
//   - Operational: the entity is fully functional, not merely running. What
//     "operational" means is domain-defined: a Slurm Enroller checks mount health;
//     an MPI Enroller checks EFA health; a Globus Enroller checks endpoint health.
//     cohort does not define WHAT operational means — only that the domain
//     reports it. A node can be running+idle with a dead mount; Operational=false
//     catches that case regardless of domain.
//   - Detail: human-readable, propagated to q0 explain via Record.
type Readiness struct {
	Enrolled    bool
	Operational bool   // domain-defined: fully functional, not merely running
	Detail      string // human-readable, surfaced by q0 explain
}

// OK reports whether the entity is both enrolled and fully operational.
func (r Readiness) OK() bool { return r.Enrolled && r.Operational }
