package cohort

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Reconciler converges one Cohort against eventually-consistent infrastructure.
//
// It is the core loop: declare intent (a set of named entities), observe
// actual state tolerating consistency gaps, diff PER-ENTITY, correct, repeat
// until the cohort converges or a phase budget runs out. This is ASG done
// right and visible — and its unit is the named entity, never a count.
//
// The Reconciler holds NO provider or domain knowledge. Everything it needs
// arrives through the ports interfaces.
//
// Construct with NewReconciler rather than a struct literal; the field set
// may grow in v0.x without notice.
type Reconciler struct {
	Actuator   Actuator
	Observer   Observer
	Classifier Classifier
	Enroller   Enroller
	Assembler  Assembler

	// Limiter gates outbound provider mutations. It is account-shared: all
	// cohorts reconciling against one account share one budget, because
	// throttling is a property of the account, not the call site.
	Limiter RateLimiter

	// Clock overrides time.Now for deterministic tests. Nil uses time.Now.
	// TEST-ONLY: do not set in production code.
	Clock func() time.Time
}

// NewReconciler constructs a Reconciler with the required ports. Enroller and
// Assembler may be nil: a nil Enroller trivially enrolls every entity (the
// 1-cohort / no-domain case); a nil Assembler skips the collective assembly
// phase. Limiter may be nil; if nil, mutations are rate-unlimited (use only in
// tests or single-call tooling — never in production multi-cohort workloads).
func NewReconciler(act Actuator, obs Observer, clf Classifier, enr Enroller, asm Assembler, lim RateLimiter) *Reconciler {
	return &Reconciler{
		Actuator:   act,
		Observer:   obs,
		Classifier: clf,
		Enroller:   enr,
		Assembler:  asm,
		Limiter:    lim,
	}
}

// RateLimiter is the account-shared client-side throttle. On a FaultThrottle
// the whole limiter backs off, not just one caller.
type RateLimiter interface {
	Acquire(ctx context.Context) error
	Backoff(d time.Duration)
}

// entityTracker holds per-entity reconciliation state.
type entityTracker struct {
	mu              sync.Mutex
	intent          EntityIntent
	phase           Phase
	enrolled        bool             // true only when waitEnrolled returned success
	attempts        []Attempt
	terminal        *Fault           // set when this entity cannot recover
	cohortCancelled *CohortCancelInfo // cohort fast-failed around this healthy entity
	parentCancelled *ParentCancelInfo // parent context cancelled the whole reconcile
	obs             Observation
	startedAt       time.Time
}

func (t *entityTracker) addAttempt(rung PlacementRung, phase Phase, f *Fault) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts = append(t.attempts, Attempt{Rung: rung, Phase: phase, Fault: f, At: time.Now()})
}

func (t *entityTracker) setTerminal(phase Phase, f Fault) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phase = phase
	cp := f
	t.terminal = &cp
}

func (t *entityTracker) setPhase(p Phase) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phase = p
}

func (t *entityTracker) getPhase() Phase {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phase
}

func (t *entityTracker) isTerminal() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.terminal != nil
}

func (t *entityTracker) isEnrolled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.enrolled
}

func (t *entityTracker) setEnrolled() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enrolled = true
}

// hasFinalOutcome returns true when the entity has any resolved outcome —
// terminal failure, cohort-cancel, or parent-cancel. Used after Wait to
// check what is still genuinely in-flight (should only be healthy entities).
func (t *entityTracker) hasFinalOutcome() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.terminal != nil || t.cohortCancelled != nil || t.parentCancelled != nil
}

func (t *entityTracker) setCohortCancelled(info CohortCancelInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := info
	cp.SurvivorPhase = t.phase
	t.cohortCancelled = &cp
}

func (t *entityTracker) setParentCancelled(cause string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parentCancelled = &ParentCancelInfo{
		SurvivorPhase: t.phase,
		Cause:         cause,
	}
}

// Reconcile drives a cohort to PhaseReady or returns an Outcome explaining,
// per entity, exactly where and why it stopped.
func (r *Reconciler) Reconcile(ctx context.Context, c Cohort) (Outcome, error) {
	now := r.clock()
	minViable := c.MinViable
	if minViable == 0 {
		minViable = len(c.Members)
	}

	// Build per-entity trackers.
	trackers := make([]*entityTracker, len(c.Members))
	for i, m := range c.Members {
		trackers[i] = &entityTracker{
			intent:    m,
			phase:     PhaseLaunchAcked,
			startedAt: now,
		}
	}

	// Phase 1–3: per-entity, concurrent.
	//
	// Two causes of cancellation must be distinguished:
	//   1. fastFailCancel() — gate became unsatisfiable; culprit is RECORDED below
	//      before the cancel fires. Survivors → CohortCancelled.
	//   2. Parent ctx cancelled — external shutdown/deadline, no culprit recorded.
	//      Survivors → ParentCancelled.
	//
	// isFastFailCancel(context.Canceled) is NOT used here. Instead: after Wait,
	// if culprit.id is set, use CohortCancelled; otherwise ParentCancelled.
	// This is a recorded fact, not a sentinel inference.
	fastFailCtx, fastFailCancel := context.WithCancel(ctx)
	defer fastFailCancel()

	var mu sync.Mutex
	failedCount := 0
	// culprit is written BEFORE fastFailCancel() fires. Any survivor that wakes
	// up after cancel can read it (under mu or after Wait returns) and knows
	// exactly who caused the fast-fail.
	var culprit culpritInfo

	eg, egCtx := errgroup.WithContext(fastFailCtx)
	for _, tr := range trackers {
		tr := tr
		eg.Go(func() error {
			r.reconcileEntity(egCtx, tr, c.Budget)
			if tr.isTerminal() {
				mu.Lock()
				failedCount++
				satisfiable := (len(trackers) - failedCount) >= minViable
				if !satisfiable && culprit.id == "" {
					// Record the culprit BEFORE calling fastFailCancel so any
					// survivor that reads it post-cancel sees a populated value.
					tr.mu.Lock()
					culprit = culpritInfo{
						id:    tr.intent.ID,
						fault: *tr.terminal,
						phase: tr.phase,
						at:    time.Now(),
					}
					tr.mu.Unlock()
					// fastFailCancel fires AFTER culprit is written.
					fastFailCancel()
				}
				mu.Unlock()
			}
			return nil
		})
	}
	_ = eg.Wait()

	outcome := Outcome{
		Cohort:  c.ID,
		Records: make(map[EntityID]Record, len(trackers)),
	}

	// S5.6b.1: check the parent context FIRST, before evaluating the gate and
	// before invoking the assembler. A parent cancel means the whole reconcile
	// is being torn down; the gate and assembly must not run on a dead context.
	if ctx.Err() != nil {
		cause := ctx.Err().Error()
		var surviving []EntityID
		for _, tr := range trackers {
			if !tr.isTerminal() && tr.obs.ProviderID != "" {
				surviving = append(surviving, tr.intent.ID)
			}
		}
		if len(surviving) > 0 {
			_ = r.Drain(context.Background(), surviving)
		}
		for _, tr := range trackers {
			if tr.isTerminal() {
				continue
			}
			// culprit.id may be set if a fast-fail AND a parent cancel raced.
			// Parent cancel takes priority in labelling: the operator-visible cause.
			tr.setParentCancelled(cause)
		}
		for _, tr := range trackers {
			outcome.Records[tr.intent.ID] = r.buildRecord(tr, c.ID)
		}
		outcome.Ready = false
		return outcome, nil
	}

	// S5.6b.2: gateSatisfied counts entities that COMPLETED ENROLLMENT — not
	// merely non-terminal entities. A Canceled entity is non-terminal but was
	// not enrolled (it exited a phase loop clean without completing).
	enrolledCount := 0
	for _, tr := range trackers {
		if tr.isEnrolled() {
			enrolledCount++
		}
	}
	gateSatisfied := enrolledCount >= minViable

	if !gateSatisfied {
		// Drain surviving instances so nothing idles and bills.
		var surviving []EntityID
		for _, tr := range trackers {
			if !tr.isTerminal() && tr.obs.ProviderID != "" {
				surviving = append(surviving, tr.intent.ID)
			}
		}
		if len(surviving) > 0 {
			_ = r.Drain(context.Background(), surviving)
		}

		// Classify non-terminal entities by recorded fact:
		// culprit.id set → CohortCancelled; empty → unreachable here (parent
		// cancel was handled above and is the only way culprit.id stays empty
		// while gate fails — defensive fallback to ParentCancelled just in case).
		for _, tr := range trackers {
			if tr.isTerminal() {
				continue
			}
			if culprit.id != "" {
				tr.setCohortCancelled(CohortCancelInfo{
					CulpritID:    culprit.id,
					CulpritFault: culprit.fault,
					CulpritPhase: culprit.phase,
					At:           culprit.at,
				})
			} else {
				tr.setParentCancelled("context canceled")
			}
		}

		for _, tr := range trackers {
			outcome.Records[tr.intent.ID] = r.buildRecord(tr, c.ID)
		}
		outcome.Ready = false
		return outcome, nil
	}

	// Phase 4 passed: barrier satisfied, parent context live. Run assembly.
	// NoAssembly is set by NewPartialCohort; partial cohorts must not assemble.
	if c.NoAssembly && r.Assembler != nil {
		f := Fault{
			Class:   FaultTerminal,
			Code:    "AssemblyDisallowed",
			Message: "partial cohort has NoAssembly=true but Reconciler has a non-nil Assembler; use NewMPICohort for assembly",
		}
		for _, tr := range trackers {
			if !tr.isTerminal() {
				tr.setTerminal(PhaseCohortBarrier, f)
			}
		}
		for _, tr := range trackers {
			outcome.Records[tr.intent.ID] = r.buildRecord(tr, c.ID)
		}
		outcome.Ready = false
		return outcome, nil
	}
	if c.IsCollective() && r.Assembler != nil {
		var members []Observation
		for _, tr := range trackers {
			if tr.isEnrolled() {
				tr.mu.Lock()
				obs := tr.obs
				tr.mu.Unlock()
				members = append(members, obs)
			}
		}
		assembleCtx, assembleCancel := context.WithTimeout(ctx, c.Budget.CohortAssembly)
		defer assembleCancel()
		if err := r.Assembler.Assemble(assembleCtx, members); err != nil {
			f := Fault{
				Class:   FaultTerminal,
				Code:    "AssemblyFailed",
				Message: err.Error(),
			}
			for _, tr := range trackers {
				if !tr.isTerminal() {
					tr.setTerminal(PhaseCohortAssembly, f)
				}
			}
			for _, tr := range trackers {
				outcome.Records[tr.intent.ID] = r.buildRecord(tr, c.ID)
			}
			outcome.Ready = false
			return outcome, nil
		}
	}

	// All phases complete.
	for _, tr := range trackers {
		if !tr.isTerminal() {
			tr.setPhase(PhaseReady)
		}
		outcome.Records[tr.intent.ID] = r.buildRecord(tr, c.ID)
	}
	outcome.Ready = true
	return outcome, nil
}

// reconcileEntity drives one entity through phases 1–3 within budget.
// ctx is egCtx (derived from fastFailCtx); phase-scoped sub-contexts add deadlines.
//
// Phase-loop cancellation policy:
//   - Own phase deadline (DeadlineExceeded on the phase-scoped context): real
//     failure — call recordDeadline, set Terminal.
//   - Any Canceled (fast-fail OR parent): leave non-terminal. The classification
//     into CohortCancelled vs ParentCancelled is done POST-Wait in Reconcile,
//     by checking whether culprit.id was populated before fastFailCancel() fired.
//     Inside reconcileEntity we cannot tell which cancel it was — and we don't
//     need to: the phase loop's job is only to stop cleanly.
func (r *Reconciler) reconcileEntity(ctx context.Context, tr *entityTracker, budget PhaseBudget) {
	// Phase 1: launch-acked.
	phase1Ctx, cancel1 := context.WithTimeout(ctx, budget.LaunchAcked)
	defer cancel1()

	if !r.doLaunch(phase1Ctx, tr) {
		// If terminal was set, this is a real failure. Otherwise the entity was
		// cancelled by fastFailCancel — leave it non-terminal.
		return
	}
	tr.setPhase(PhaseRunning)

	// Phase 2: running.
	phase2Ctx, cancel2 := context.WithTimeout(ctx, budget.Running)
	defer cancel2()

	if !r.waitRunning(phase2Ctx, tr) {
		return
	}
	tr.setPhase(PhaseEnrolled)

	// Phase 3: enrolled.
	phase3Ctx, cancel3 := context.WithTimeout(ctx, budget.Enrolled)
	defer cancel3()

	r.waitEnrolled(phase3Ctx, tr)
	// Mark enrolled only when waitEnrolled returned without setting terminal.
	// A cancelled or deadline entity is non-terminal but NOT enrolled.
	if !tr.isTerminal() {
		tr.setEnrolled()
	}
}

// doLaunch issues Launch (or Start if PreferWarm) for one entity,
// retrying on RetryableConsistency and advancing the chain on CapacityExhausted.
// Returns true if launch was acknowledged.
func (r *Reconciler) doLaunch(ctx context.Context, tr *entityTracker) bool {
	consistencyAttempt := 0
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.Canceled) {
				return false // fast-fail or parent cancel; classification done post-Wait
			}
			r.recordDeadline(tr, PhaseLaunchAcked)
			return false
		}

		var obs Observation
		var err error

		if tr.intent.Placement.Current().WarmStart {
			obs, err = r.Actuator.Start(ctx, tr.intent.ID)
		} else {
			obs, err = r.Actuator.Launch(ctx, tr.intent)
		}

		if err == nil {
			tr.mu.Lock()
			tr.obs = obs
			tr.mu.Unlock()
			tr.addAttempt(tr.intent.Placement.Current(), PhaseLaunchAcked, nil)
			return true
		}

		f := r.Classifier.Classify(err)

		switch f.Class {
		case FaultRetryableConsistency:
			consistencyAttempt++
			if consistencyAttempt > maxConsistencyRetries {
				tr.addAttempt(tr.intent.Placement.Current(), PhaseLaunchAcked, &f)
				tr.setTerminal(PhaseLaunchAcked, f)
				return false
			}
			sleep(ctx, DefaultBackoffPolicy().Duration(consistencyAttempt))

		case FaultThrottle:
			d := DefaultBackoffPolicy().Duration(0)
			if r.Limiter != nil {
				r.Limiter.Backoff(d)
			}
			sleep(ctx, d)

		case FaultCapacityExhausted:
			tr.addAttempt(tr.intent.Placement.Current(), PhaseLaunchAcked, &f)
			// Advance the fallback ladder through the opaque Placement seam. The
			// trigger fault stays capacity-named — it's the AWS provider's signal;
			// a transport provider simply never emits it (#1).
			next, ok := tr.intent.Placement.Advance()
			if !ok {
				tr.setTerminal(PhaseLaunchAcked, f)
				return false
			}
			tr.intent.Placement = next
			consistencyAttempt = 0

		case FaultAmbiguous:
			// Must not reach here — Step 3 Client consumed Ambiguous.
			// Treat as terminal with a loud message to surface Step 3 regressions.
			f.Code = "AmbiguousReachedReconciler"
			f.Message = "BUG: FaultAmbiguous escaped substrate.Client — Step 3 regression"
			tr.addAttempt(tr.intent.Placement.Current(), PhaseLaunchAcked, &f)
			tr.setTerminal(PhaseLaunchAcked, f)
			return false

		default: // Terminal
			tr.addAttempt(tr.intent.Placement.Current(), PhaseLaunchAcked, &f)
			tr.setTerminal(PhaseLaunchAcked, f)
			return false
		}
	}
}

// waitRunning polls Observer until the entity reaches StateRunning or the budget expires.
func (r *Reconciler) waitRunning(ctx context.Context, tr *entityTracker) bool {
	consistencyAttempt := 0
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.Canceled) {
				return false
			}
			r.recordDeadline(tr, PhaseRunning)
			return false
		}

		obs, err := r.Observer.Observe(ctx, []EntityID{tr.intent.ID})
		if err != nil {
			f := r.Classifier.Classify(err)
			if f.Class == FaultTerminal {
				tr.addAttempt(tr.intent.Placement.Current(), PhaseRunning, &f)
				tr.setTerminal(PhaseRunning, f)
				return false
			}
			sleep(ctx, DefaultBackoffPolicy().Duration(consistencyAttempt))
			consistencyAttempt++
			continue
		}
		if len(obs) > 0 {
			tr.mu.Lock()
			tr.obs = obs[0]
			tr.mu.Unlock()

			switch obs[0].State {
			case StateRunning:
				return true
			case StateFailed, StateDraining:
				f := Fault{Class: FaultTerminal, Code: "InstanceFailed",
					Message: fmt.Sprintf("instance entered state %s during phase 2", obs[0].State)}
				tr.addAttempt(tr.intent.Placement.Current(), PhaseRunning, &f)
				tr.setTerminal(PhaseRunning, f)
				return false
			}
		}
		sleep(ctx, pollInterval)
	}
}

// waitEnrolled polls Enroller until the entity is enrolled or budget expires.
func (r *Reconciler) waitEnrolled(ctx context.Context, tr *entityTracker) {
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.Canceled) {
				return
			}
			r.recordDeadline(tr, PhaseEnrolled)
			return
		}

		if r.Enroller != nil {
			readiness, err := r.Enroller.IsEnrolled(ctx, tr.intent.ID)
			if err == nil && readiness.OK() {
				tr.mu.Lock()
				tr.obs.Address = readiness.Detail
				tr.mu.Unlock()
				return // enrolled successfully; caller sets PhaseEnrolled
			}
		} else {
			// No Enroller — the 1-cohort / no-domain case; trivially enrolled.
			return
		}
		sleep(ctx, pollInterval)
	}
}

// Fallback-ladder advancement now lives behind the Placement seam
// (Placement.Advance); the AWS implementation is RungPlacement.Advance in
// entity.go. The reconciler no longer knows what a rung is (#1).

// Drain marks the given entities for teardown after a cohort failure,
// so no member is left Running-but-useless and billing.
func (r *Reconciler) Drain(ctx context.Context, ids []EntityID) error {
	var lastErr error
	for _, id := range ids {
		if err := r.Actuator.Terminate(ctx, id); err != nil {
			lastErr = err // best-effort: continue draining others
		}
	}
	return lastErr
}

// culpritInfo captures the first entity that made the cohort gate unsatisfiable.
type culpritInfo struct {
	id    EntityID
	fault Fault
	phase Phase
	at    time.Time
}

func (r *Reconciler) recordDeadline(tr *entityTracker, phase Phase) {
	f := Fault{
		Class:   FaultTerminal,
		Code:    "PhaseBudgetExceeded",
		Message: fmt.Sprintf("phase %s budget exceeded", phase),
	}
	tr.addAttempt(tr.intent.Placement.Current(), phase, &f)
	tr.setTerminal(phase, f)
}

func (r *Reconciler) buildRecord(tr *entityTracker, cohortID CohortID) Record {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return Record{
		Entity:          tr.intent.ID,
		Generation:      tr.intent.Generation,
		Cohort:          cohortID,
		ReachedPhase:    tr.phase,
		Attempts:        append([]Attempt(nil), tr.attempts...),
		Terminal:        tr.terminal,
		CohortCancelled: tr.cohortCancelled,
		ParentCancelled: tr.parentCancelled,
		StartedAt:       tr.startedAt,
		FinishedAt:      time.Now(),
	}
}

func (r *Reconciler) clock() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

// sleep sleeps for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

const (
	maxConsistencyRetries = 5
	pollInterval          = 100 * time.Millisecond
)
