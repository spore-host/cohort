package cohort

import (
	"errors"
	"fmt"
	"time"
)

// CohortID names a set of entities reconciled as a unit.
type CohortID string

// Cohort is a named set of identity-bearing entities that succeed, fail, and
// fast-fail together. A serial HPC job is the 1-cohort: cardinality one, a
// trivially-satisfied barrier, and a no-op Assembler. It is the SAME logic,
// not a special case — which is the evidence the model is the right shape.
//
// Construct with NewSerialCohort, NewMPICohort, or NewPartialCohort rather
// than a struct literal to avoid zero-value traps (zero Budget fires all
// deadlines instantly; MinViable==0 means full membership, not no-quorum).
type Cohort struct {
	ID      CohortID
	Members []EntityIntent

	// Budget bounds each phase with its own deadline, so a failure names the
	// phase it died in. Any individually-zero field fires that phase's deadline
	// at t=0 — always use DefaultBudget() or an explicit budget. The constructors
	// fill every zero field from DefaultBudget() automatically (field-by-field).
	Budget PhaseBudget

	// MinViable is the minimum number of enrolled entities required to satisfy
	// the cohort barrier.
	//
	// ZERO-VALUE CONTRACT: MinViable==0 means "full membership required" —
	// len(Members) is used. It does NOT mean "no quorum required." This is the
	// correct default for MPI (all-or-nothing). For partial success, use
	// NewPartialCohort with an explicit MinViable.
	MinViable int

	// NoAssembly is set by NewPartialCohort. When true, Reconcile refuses to
	// invoke the Assembler even if one is configured on the Reconciler.
	// Partial cohorts have undefined assembly semantics: if 8 of 10 satisfy
	// MinViable, does assembly run over 8 or wait for 10? Rather than guess,
	// partial cohorts explicitly prohibit the assembly phase.
	NoAssembly bool
}

// NewSerialCohort constructs the 1-cohort: a single named entity, no real barrier,
// no assembler needed. MinViable is 1. Budget defaults to DefaultBudget() if zero.
//
// Returns an error if the member has an invalid EntityIntent.
func NewSerialCohort(id CohortID, member EntityIntent, budget PhaseBudget) (Cohort, error) {
	if err := validateIntent(member); err != nil {
		return Cohort{}, err
	}
	return Cohort{
		ID:        id,
		Members:   []EntityIntent{member},
		Budget:    applyDefaultBudget(budget),
		MinViable: 1,
	}, nil
}

// NewMPICohort constructs an all-or-nothing cohort (MinViable = len(members)).
// Use this for MPI and any collective domain where partial membership is not viable.
// Budget defaults to DefaultBudget() if zero.
//
// Returns an error if members is empty or any member has an invalid EntityIntent.
func NewMPICohort(id CohortID, members []EntityIntent, budget PhaseBudget) (Cohort, error) {
	if len(members) == 0 {
		return Cohort{}, errors.New("cohort: NewMPICohort requires at least one member")
	}
	for _, m := range members {
		if err := validateIntent(m); err != nil {
			return Cohort{}, err
		}
	}
	return Cohort{
		ID:        id,
		Members:   members,
		Budget:    applyDefaultBudget(budget),
		MinViable: len(members),
	}, nil
}

// NewPartialCohort constructs a cohort where fewer than all members may succeed.
// Use this for embarrassingly-parallel sets where partial membership is acceptable.
// Budget defaults to DefaultBudget() if zero. MinViable must be > 0 and ≤ len(members).
//
// Partial cohorts do NOT support an assembly phase: assembly requires all members
// to be simultaneously live and enrolled, which is the all-or-nothing barrier of
// NewMPICohort. If the Reconciler's Assembler is non-nil when reconciling a partial
// cohort, Reconcile returns an error immediately. Pass asm to have this caught at
// construction time rather than at reconcile time.
//
// Returns an error if:
//   - members is empty
//   - minViable is out of range [1, len(members)]
//   - asm is non-nil (partial cohorts prohibit assembly)
//   - any member has an invalid EntityIntent
func NewPartialCohort(id CohortID, members []EntityIntent, budget PhaseBudget, minViable int, asm Assembler) (Cohort, error) {
	if len(members) == 0 {
		return Cohort{}, errors.New("cohort: NewPartialCohort requires at least one member")
	}
	if minViable <= 0 || minViable > len(members) {
		return Cohort{}, fmt.Errorf("cohort: NewPartialCohort minViable %d out of range [1, %d]", minViable, len(members))
	}
	if asm != nil {
		return Cohort{}, errors.New("cohort: NewPartialCohort does not support an assembly phase; " +
			"assembly requires full membership — use NewMPICohort for all-or-nothing collectives")
	}
	for _, m := range members {
		if err := validateIntent(m); err != nil {
			return Cohort{}, err
		}
	}
	return Cohort{
		ID:         id,
		Members:    members,
		Budget:     applyDefaultBudget(budget),
		MinViable:  minViable,
		NoAssembly: true,
	}, nil
}

// applyDefaultBudget fills any zero field in b from DefaultBudget().
// A zero duration for an individual phase fires that phase's deadline at t=0,
// which is never intentional. Callers who set some fields explicitly have those
// honored; any field left at zero is filled from the defaults.
func applyDefaultBudget(b PhaseBudget) PhaseBudget {
	d := DefaultBudget()
	if b.LaunchAcked == 0 {
		b.LaunchAcked = d.LaunchAcked
	}
	if b.Running == 0 {
		b.Running = d.Running
	}
	if b.Enrolled == 0 {
		b.Enrolled = d.Enrolled
	}
	if b.CohortBarrier == 0 {
		b.CohortBarrier = d.CohortBarrier
	}
	if b.CohortAssembly == 0 {
		b.CohortAssembly = d.CohortAssembly
	}
	return b
}

// validateIntent checks the mandatory fields of an EntityIntent.
func validateIntent(m EntityIntent) error {
	if m.ID == "" {
		return errors.New("cohort: EntityIntent.ID must not be empty")
	}
	if err := validatePlacement(m.Placement); err != nil {
		return fmt.Errorf("cohort: EntityIntent %q Placement: %w", m.ID, err)
	}
	return nil
}

// IsCollective reports whether this cohort has a real barrier. A 1-cohort is
// not collective; an MPI cohort is.
func (c Cohort) IsCollective() bool { return len(c.Members) > 1 }

// PhaseBudget is the deadline for each phase of reconciliation. Phase 1 is
// deliberately tight — blowing it means throttling or an API problem, never a
// capacity problem, and that distinction is load-bearing for legibility.
type PhaseBudget struct {
	LaunchAcked    time.Duration // ~10-15s
	Running        time.Duration // ~minutes; capacity faults surface here
	Enrolled       time.Duration // bootstrap + authority check-in + operational probe
	CohortBarrier  time.Duration // how long to wait for stragglers before fast-failing the set
	CohortAssembly time.Duration // domain wire-up
}

// DefaultBudget is a starting point; partitions.yaml may override per partition.
func DefaultBudget() PhaseBudget {
	return PhaseBudget{
		LaunchAcked:    15 * time.Second,
		Running:        5 * time.Minute,
		Enrolled:       3 * time.Minute,
		CohortBarrier:  90 * time.Second,
		CohortAssembly: 60 * time.Second,
	}
}

// Outcome is the result of reconciling one cohort. Whether success or failure,
// every member carries a Record (see explain.go) — "it didn't work" is never
// an acceptable answer; "ICE on p5.48xlarge in us-east-1a, chain exhausted at
// 14:32:07" is.
type Outcome struct {
	Cohort  CohortID
	Ready   bool
	Records map[EntityID]Record
}
