package cohort

// ---------------------------------------------------------------------------
// Lifecycle state — the per-entity state set. Note Stopped and Hibernated:
// queuezero does not always terminate-and-reprovision; a warm entity can be
// resumed in seconds with its mounts already present.
// ---------------------------------------------------------------------------

type LifecycleState int

const (
	StateUnknown   LifecycleState = iota // Observer could not determine — treat as lag, not absence
	StateAbsent                          // no entity exists for this ID
	StateLaunching                       // Launch/Start acknowledged, not yet Running
	StateRunning                         // provider reports running
	StateStopped                         // warm: EBS persists, instance-store gone
	StateHibernated                      // RAM frozen to EBS: mounts/processes/page-cache survive
	StateDraining                        // marked for teardown
	StateFailed                          // terminal for this generation
)

func (s LifecycleState) String() string {
	switch s {
	case StateAbsent:
		return "absent"
	case StateLaunching:
		return "launching"
	case StateRunning:
		return "running"
	case StateStopped:
		return "stopped"
	case StateHibernated:
		return "hibernated"
	case StateDraining:
		return "draining"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// StopMode selects warm-stop vs hibernate when suspending an entity.
type StopMode int

const (
	StopWarm StopMode = iota // EBS persists
	StopHibernate            // RAM frozen to EBS
)

// ---------------------------------------------------------------------------
// Fault classification — the taxonomy is the product. Every provider error
// maps to exactly one class via an explicit table in the provider Classifier.
// ---------------------------------------------------------------------------

type FaultClass int

const (
	// FaultRetryableConsistency: propagation lag, not failure. Bounded retry,
	// short backoff, TIGHT ceiling (single-digit seconds).
	FaultRetryableConsistency FaultClass = iota
	// FaultThrottle: rate-limited. Exponential backoff + jitter. The fix is
	// slowing the whole client, not waiting — hence distinct from consistency.
	FaultThrottle
	// FaultCapacityExhausted: ICE / no capacity. NEVER retry in place — advance
	// the fallback chain. Purchase-model-independent: on-demand ICEs too.
	FaultCapacityExhausted
	// FaultTerminal: auth, quota, bad parameter. Fail immediately and loud.
	FaultTerminal
	// FaultAmbiguous: timeout / reset / 5xx — mutation status unknown. This
	// class MUST be collapsed into FaultRetryableConsistency by idempotency
	// tokens before it reaches the reconciler. It should never be observed
	// downstream of substrate.Client.
	FaultAmbiguous
)

func (c FaultClass) String() string {
	switch c {
	case FaultRetryableConsistency:
		return "retryable-consistency"
	case FaultThrottle:
		return "throttle"
	case FaultCapacityExhausted:
		return "capacity-exhausted"
	case FaultTerminal:
		return "terminal"
	case FaultAmbiguous:
		return "ambiguous"
	default:
		return "unclassified"
	}
}

// Fault is the classified form of a provider error: the class plus the
// verbatim provider code, preserved for legibility (q0 explain).
type Fault struct {
	Class     FaultClass
	Code      string // verbatim provider error code, never paraphrased
	Retryable bool   // convenience: Class is RetryableConsistency or Throttle
	Message   string
}

// ---------------------------------------------------------------------------
// Phases — each carries its own deadline and its own failure reason, so a
// failure names WHICH phase it died in. See docs/ARCHITECTURE.md §7.
// ---------------------------------------------------------------------------

type Phase int

const (
	PhaseLaunchAcked    Phase = iota // 1: Launch/Start returned. Blowing it => throttle/API, NOT capacity.
	PhaseRunning                     // 2: provider reports running. Capacity faults surface here or in 1.
	PhaseEnrolled                    // 3: entity accepted by its authority; readiness probe (incl. mount) passes.
	PhaseCohortBarrier               // 4: ALL members reached PhaseEnrolled, or gate unsatisfiable => fast-fail set.
	PhaseCohortAssembly              // 5: domain Assembler succeeded over the complete cohort.
	PhaseReady                       // terminal-success: cohort is usable.
)

func (p Phase) String() string {
	switch p {
	case PhaseLaunchAcked:
		return "launch-acked"
	case PhaseRunning:
		return "running"
	case PhaseEnrolled:
		return "enrolled"
	case PhaseCohortBarrier:
		return "cohort-barrier"
	case PhaseCohortAssembly:
		return "cohort-assembly"
	case PhaseReady:
		return "ready"
	default:
		return "unknown-phase"
	}
}
