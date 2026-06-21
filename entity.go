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

	// Placement is the provider-specific placement payload, opaque to the core
	// (#1). It is the seam parallel to Classifier: the reconciler drives the
	// fallback ladder through it — Advance on a fallback-eligible fault — but
	// never inspects its fields. The AWS provider supplies a RungPlacement
	// (instance type / AZ / capacity model); an agent-transport provider
	// supplies its own (goroutine → session → instance) and never sees a
	// CapacityModel. The core learns only what Current() renders.
	Placement Placement

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
//   - placement must be non-nil and its Current().Name must be non-empty
//     (catches an uninitialized placement — the provider-agnostic equivalent of
//     the old "empty InstanceType" check).
func NewEntityIntent(cluster string, id EntityID, gen Generation, cohortID CohortID, placement Placement, token string) (EntityIntent, error) {
	if id == "" {
		return EntityIntent{}, errors.New("cohort: EntityIntent.ID must not be empty")
	}
	if err := validatePlacement(placement); err != nil {
		return EntityIntent{}, fmt.Errorf("cohort: EntityIntent.Placement: %w", err)
	}
	if token == "" {
		token = Token(cluster, string(id), string(gen))
	}
	return EntityIntent{
		ID:               id,
		Generation:       gen,
		Cohort:           cohortID,
		Placement:        placement,
		IdempotencyToken: token,
	}, nil
}

// Placement is the provider-specific placement payload and fallback ladder,
// opaque to the core (#1). It is the seam that keeps cohort provider-agnostic:
// the reconciler advances the ladder on a fallback-eligible fault but never
// inspects what a rung means. AWS supplies RungPlacement; an agent-transport
// provider supplies its own (goroutine → session → instance).
type Placement interface {
	// Current returns the legible identity of the rung currently selected —
	// what Record/Attempt/Explain render. It must have a non-empty Name.
	Current() PlacementRung
	// Advance returns the next placement in the approved fallback chain and true,
	// or false when the chain is exhausted. It must NEVER substitute a rung
	// outside the approved chain. The returned Placement is a new value; the
	// receiver is not mutated.
	Advance() (Placement, bool)
}

// PlacementRung is the core-visible, provider-agnostic view of one rung: a
// human-legible name, a provider-defined class, and whether selecting it means
// resuming a warm entity vs. a cold launch. This is all the core needs — the
// provider's real placement fields (instance type, AZ, capacity model, …) stay
// inside the provider's Placement implementation.
type PlacementRung struct {
	// Name is the legible identifier rendered in Record/Explain, e.g.
	// "p5.48xlarge/us-east-1a/spot" (AWS) or "a2a-session" / "goroutine" (Telos).
	Name string
	// Class is a provider-defined category, e.g. AWS capacity model ("spot") or
	// a transport rung kind. Free-form; the core only displays it.
	Class string
	// WarmStart means "resume a Stopped/Hibernated entity" rather than cold-launch
	// — the one placement property the core acts on (it picks Actuator.Start vs
	// Launch). A provider with no warm state simply always returns false.
	WarmStart bool
}

// Rung is one option in an AWS capacity fallback chain. There is no "safe
// baseline" — on-demand and spot are both rungs that can fault to capacity;
// they differ only in ICE probability and price. ODCR/capacity-block rungs
// are the one kind genuinely reserved against ICE.
//
// Rung is the AWS provider's placement vocabulary, not core vocabulary — it is
// carried into the core via RungPlacement, which adapts it to the Placement
// seam. A non-AWS provider never constructs a Rung.
type Rung struct {
	InstanceType  string
	AvailZone     string
	CapacityModel CapacityModel
	AccountID     string // execution account for this rung (multi-account, §3)
	               // Empty AccountID means single-account mode — the correct default.

	// WarmStart means resume a Stopped/Hibernated entity rather than cold-launch.
	// It is a RUNG property, not a pre-check: if warm-start ICEs, the chain
	// advances to the next rung exactly like any other ICE.
	WarmStart bool
}

// RungPlacement is the built-in Placement backed by an AWS Rung and its approved
// fallback chain — the adapter that lets the AWS/MPI consumers migrate to the
// Placement seam near-mechanically (`Placement: cohort.RungPlacement{Rung: r, Chain: chain}`).
// The chain is the ordered list of approved rungs; Advance walks it and never
// substitutes outside it. An empty chain means single-rung (no fallback).
type RungPlacement struct {
	Rung  Rung
	Chain []Rung
}

// Current renders the active rung for the core's Record/Explain.
func (p RungPlacement) Current() PlacementRung {
	return PlacementRung{
		Name:      fmt.Sprintf("%s/%s/%v", p.Rung.InstanceType, p.Rung.AvailZone, p.Rung.CapacityModel),
		Class:     fmt.Sprintf("%v", p.Rung.CapacityModel),
		WarmStart: p.Rung.WarmStart,
	}
}

// Advance returns the next approved rung from the chain, or false when exhausted.
// It mirrors the old advanceRung: find the current rung in the chain by equality,
// step to the next. A chain that doesn't contain the current rung (or a single-
// rung intent with no chain) is exhausted immediately.
func (p RungPlacement) Advance() (Placement, bool) {
	for i, r := range p.Chain {
		if r == p.Rung && i+1 < len(p.Chain) {
			return RungPlacement{Rung: p.Chain[i+1], Chain: p.Chain}, true
		}
	}
	return nil, false
}

// Validate is the AWS-provider self-check, preserving the old per-rung
// validation now that the core no longer knows what a Rung is: the active rung
// and every approved-chain rung must name an instance type. It satisfies the
// optional Validate() hook that validatePlacement calls (see below).
func (p RungPlacement) Validate() error {
	if p.Rung.InstanceType == "" {
		return errors.New("Rung.InstanceType must not be empty")
	}
	for i, r := range p.Chain {
		if r.InstanceType == "" {
			return fmt.Errorf("FallbackChain[%d].InstanceType must not be empty", i)
		}
	}
	return nil
}

// validatePlacement returns an error if placement is nil or unusable. Two
// layers: (1) a provider-agnostic floor — Current().Name must be non-empty, so
// an uninitialized placement can't reach Actuator.Launch as garbage; (2) an
// OPTIONAL deeper self-check — if the placement implements `Validate() error`
// (RungPlacement does, checking the whole fallback chain), that runs too. The
// optional hook keeps the required Placement interface at two methods while
// letting a provider validate its own richer payload.
func validatePlacement(p Placement) error {
	if p == nil {
		return errors.New("must not be nil")
	}
	if p.Current().Name == "" {
		return errors.New("Current().Name must not be empty (uninitialized placement?)")
	}
	if v, ok := p.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return err
		}
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
