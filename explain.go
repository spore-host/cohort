package cohort

import (
	"fmt"
	"time"
)

// Record is the structured legibility artifact every reconciled entity
// carries. Legibility is a deep requirement, not polish: queuezero must be
// crystal clear about WHY when something fails, or fall back in a legible and
// approved manner. See docs/ARCHITECTURE.md §10.
//
// Exactly one of {Succeeded(), WasCohortCancelled(), WasParentCancelled(),
// Terminal != nil} is true for every completed Record. Never more than one.
type Record struct {
	Entity     EntityID
	Generation Generation
	Cohort     CohortID

	// ReachedPhase is the furthest phase the entity got to.
	ReachedPhase Phase

	// Attempts is every rung tried, in order.
	Attempts []Attempt

	// Terminal, if set, is the fault that ended reconciliation for THIS entity.
	// Nil on success, nil on CohortCancelled, nil on ParentCancelled.
	Terminal *Fault

	// CohortCancelled is set when this entity was healthy and in-flight but
	// the cohort fast-failed around it because ANOTHER entity became unsalvageable.
	// The entity did not fail; the cohort died around it.
	// Mutually exclusive with Terminal and ParentCancelled.
	//
	// q0 explain on a 64-node cohort must distinguish the ONE entity that
	// caused the fast-fail from the 63 cancelled because of it.
	CohortCancelled *CohortCancelInfo

	// ParentCancelled is set when the PARENT context was cancelled (operator
	// shutdown, q0 process exiting, an outer deadline on the whole reconcile).
	// There is no culprit entity — the reconcile was aborted externally.
	// Mutually exclusive with Terminal and CohortCancelled.
	//
	// This is distinct from CohortCancelled: no cohort-level cause was recorded.
	// Reading it as CohortCancelled-with-empty-culprit would be a fabricated story.
	ParentCancelled *ParentCancelInfo

	// EnrollDetail is the human-readable Readiness.Detail from a successful
	// enrollment (e.g. "efa ok"), surfaced in Explain(). It is a DISPLAY string
	// — distinct from Observation.Address, which carries the entity's actual
	// address for the Assembler.
	EnrollDetail string

	StartedAt  time.Time
	FinishedAt time.Time
}

// CohortCancelInfo describes why a healthy entity was cancelled by a cohort fast-fail.
type CohortCancelInfo struct {
	// CulpritID is the entity whose fault made the gate unsatisfiable.
	CulpritID EntityID
	// CulpritFault is the verbatim fault (class + provider code) the culprit hit.
	CulpritFault Fault
	// CulpritPhase is the phase the culprit was in when it failed.
	CulpritPhase Phase
	// At is when the fast-fail was triggered.
	At time.Time
	// SurvivorPhase is the phase THIS entity had reached when it was cancelled.
	// This is set per-survivor — not the cohort's phase, each entity's own phase
	// at the instant IT observed the cancellation.
	SurvivorPhase Phase
}

// ParentCancelInfo describes a reconcile aborted by the parent context.
// There is no culprit entity.
type ParentCancelInfo struct {
	// SurvivorPhase is the phase this entity had reached when cancelled.
	SurvivorPhase Phase
	// Cause is the parent context error string ("context canceled" or
	// "context deadline exceeded"). Preserved verbatim for legibility.
	Cause string
}

// Attempt records one rung tried for one entity. Rung is the core-visible,
// provider-agnostic view (PlacementRung) so Explain renders a legible line for
// any provider — AWS, agent-transport, or otherwise (#1).
type Attempt struct {
	Rung  PlacementRung
	Phase Phase  // phase reached on this rung before the fault (or PhaseReady)
	Fault *Fault // nil if this attempt succeeded
	At    time.Time
}

// Succeeded reports whether the entity reached PhaseReady.
func (r Record) Succeeded() bool {
	return r.Terminal == nil &&
		r.CohortCancelled == nil &&
		r.ParentCancelled == nil &&
		r.ReachedPhase == PhaseReady
}

// WasCohortCancelled reports whether this entity was cancelled by a cohort fast-fail.
// Use this to distinguish survivors (CohortCancelled) from the culprit (Terminal).
func (r Record) WasCohortCancelled() bool { return r.CohortCancelled != nil }

// WasParentCancelled reports whether this entity was cancelled by an external
// parent-context cancellation (not a cohort-internal fast-fail).
func (r Record) WasParentCancelled() bool { return r.ParentCancelled != nil }

// Summary is the one-line, scontrol-reason-shaped form.
func (r Record) Summary() string {
	if r.Succeeded() {
		return fmt.Sprintf("ready (%d attempt(s))", len(r.Attempts))
	}
	if r.CohortCancelled != nil {
		cc := r.CohortCancelled
		return fmt.Sprintf("cohort-cancelled at phase=%s — culprit=%s %s/%s at phase=%s",
			cc.SurvivorPhase, cc.CulpritID, cc.CulpritFault.Class, cc.CulpritFault.Code, cc.CulpritPhase)
	}
	if r.ParentCancelled != nil {
		pc := r.ParentCancelled
		return fmt.Sprintf("parent-cancelled at phase=%s (%s)", pc.SurvivorPhase, pc.Cause)
	}
	if r.Terminal != nil {
		return fmt.Sprintf("%s/%s at phase=%s after %d rung(s)",
			r.Terminal.Class, r.Terminal.Code, r.ReachedPhase, len(r.Attempts))
	}
	return fmt.Sprintf("incomplete at phase=%s", r.ReachedPhase)
}

// Explain renders the full multi-line trace for `q0 explain`.
func (r Record) Explain() string {
	out := fmt.Sprintf("entity %s  cohort=%s  generation=%s\n", r.Entity, r.Cohort, r.Generation)
	out += fmt.Sprintf("  outcome: %s\n", r.Summary())
	if r.CohortCancelled != nil {
		cc := r.CohortCancelled
		out += fmt.Sprintf("  cohort fast-failed at %s\n", cc.At.Format(time.RFC3339))
		out += fmt.Sprintf("  culprit: entity=%s fault=%s/%s phase=%s\n",
			cc.CulpritID, cc.CulpritFault.Class, cc.CulpritFault.Code, cc.CulpritPhase)
		out += fmt.Sprintf("  this entity was at phase=%s when cancelled\n", cc.SurvivorPhase)
		return out
	}
	if r.ParentCancelled != nil {
		pc := r.ParentCancelled
		out += fmt.Sprintf("  parent context cancelled (%s); this entity was at phase=%s\n",
			pc.Cause, pc.SurvivorPhase)
		return out
	}
	for i, a := range r.Attempts {
		rung := a.Rung.Name
		if a.Fault != nil {
			out += fmt.Sprintf("  [%d] %s  reached=%s  -> %s/%s  (%s)\n",
				i+1, rung, a.Phase, a.Fault.Class, a.Fault.Code, a.At.Format(time.RFC3339))
		} else {
			out += fmt.Sprintf("  [%d] %s  reached=%s  -> ok  (%s)\n",
				i+1, rung, a.Phase, a.At.Format(time.RFC3339))
		}
	}
	if r.EnrollDetail != "" {
		out += fmt.Sprintf("  enrollment: %s\n", r.EnrollDetail)
	}
	return out
}
