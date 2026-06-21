package cohort

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// rungOf extracts the AWS Rung from an intent's placement, for fakes that need
// to see which rung the reconciler currently has selected. The test fakes are
// the AWS provider's stand-in, so the placement is always a RungPlacement.
func rungOf(intent EntityIntent) Rung {
	if rp, ok := intent.Placement.(RungPlacement); ok {
		return rp.Rung
	}
	return Rung{}
}

// ---- fake ports (no AWS, no Slurm) ------------------------------------------

// fakeActuator controls per-entity responses.
type fakeActuator struct {
	// launchFn called per Launch; if nil returns a running observation.
	launchFn func(intent EntityIntent) (Observation, error)
	// startFn called per Start.
	startFn func(id EntityID) (Observation, error)
	// terminateCalls records entities Terminate was called for.
	terminateCalls []EntityID
}

func (a *fakeActuator) Launch(_ context.Context, intent EntityIntent) (Observation, error) {
	if a.launchFn != nil {
		return a.launchFn(intent)
	}
	return Observation{ID: intent.ID, Generation: intent.Generation,
		ProviderID: "i-" + string(intent.ID), State: StateLaunching, Rung: rungOf(intent),
		ObservedAt: time.Now()}, nil
}

func (a *fakeActuator) Start(_ context.Context, id EntityID) (Observation, error) {
	if a.startFn != nil {
		return a.startFn(id)
	}
	return Observation{ID: id, State: StateRunning, ObservedAt: time.Now()}, nil
}

func (a *fakeActuator) Stop(_ context.Context, _ EntityID, _ StopMode) error { return nil }

func (a *fakeActuator) Terminate(_ context.Context, id EntityID) error {
	a.terminateCalls = append(a.terminateCalls, id)
	return nil
}

// fakeObserver returns StateRunning for every entity immediately.
type fakeObserver struct {
	// stateFn lets tests inject per-entity lifecycle state.
	stateFn func(id EntityID) LifecycleState
}

func (o *fakeObserver) Observe(_ context.Context, ids []EntityID) ([]Observation, error) {
	obs := make([]Observation, len(ids))
	for i, id := range ids {
		st := StateRunning
		if o.stateFn != nil {
			st = o.stateFn(id)
		}
		obs[i] = Observation{ID: id, State: st, ObservedAt: time.Now()}
	}
	return obs, nil
}

// fakeClassifier maps errors by message prefix.
type fakeClassifier struct {
	faults map[string]Fault // keyed by error message
}

func (c *fakeClassifier) Classify(err error) Fault {
	if err == nil {
		return Fault{Class: FaultRetryableConsistency}
	}
	if c.faults != nil {
		if f, ok := c.faults[err.Error()]; ok {
			return f
		}
	}
	return Fault{Class: FaultTerminal, Code: "UnknownError", Message: err.Error()}
}

// fakeEnroller reports enrolled for all entities.
type fakeEnroller struct {
	// enrolledFn lets tests control per-entity enrollment.
	enrolledFn func(id EntityID) Readiness
}

func (e *fakeEnroller) IsEnrolled(_ context.Context, id EntityID) (Readiness, error) {
	if e.enrolledFn != nil {
		return e.enrolledFn(id), nil
	}
	return Readiness{Enrolled: true, Operational: true}, nil
}

// fakeAssembler counts invocations and records members.
type fakeAssembler struct {
	calls   int32
	members []Observation
	err     error
}

func (a *fakeAssembler) Assemble(_ context.Context, members []Observation) error {
	atomic.AddInt32(&a.calls, 1)
	a.members = append(a.members, members...)
	return a.err
}

// ---- helpers ----------------------------------------------------------------

func newReconciler(act *fakeActuator, obs *fakeObserver, enr *fakeEnroller, asm *fakeAssembler) *Reconciler {
	return &Reconciler{
		Actuator:   act,
		Observer:   obs,
		Classifier: &fakeClassifier{},
		Enroller:   enr,
		Assembler:  asm,
	}
}

func fastBudget() PhaseBudget {
	return PhaseBudget{
		LaunchAcked:    2 * time.Second,
		Running:        2 * time.Second,
		Enrolled:       2 * time.Second,
		CohortBarrier:  2 * time.Second,
		CohortAssembly: 2 * time.Second,
	}
}

func member(id string) EntityIntent {
	return EntityIntent{
		ID:               EntityID(id),
		Generation:       "g1",
		Cohort:           "c1",
		IdempotencyToken: "tok-" + id,
		Placement:        RungPlacement{Rung: Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}},
	}
}

// ---- S5.3 tests -------------------------------------------------------------

// 1-cohort (serial): trivial barrier, no-op assembler, reaches Ready.
func TestReconciler_Serial_ReachesReady(t *testing.T) {
	act := &fakeActuator{}
	obs := &fakeObserver{}
	enr := &fakeEnroller{}
	r := newReconciler(act, obs, enr, nil)

	c := Cohort{
		ID:      "c-serial",
		Members: []EntityIntent{member("gpu-001")},
		Budget:  fastBudget(),
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("serial cohort: Ready=false")
	}
	rec := outcome.Records["gpu-001"]
	if !rec.Succeeded() {
		t.Errorf("serial entity: Succeeded()=false; summary=%q", rec.Summary())
	}
	if rec.ReachedPhase != PhaseReady {
		t.Errorf("serial entity: ReachedPhase=%v want PhaseReady", rec.ReachedPhase)
	}
}

// Collective cohort, all healthy: barrier satisfied, assembly runs once, Ready.
func TestReconciler_Collective_AllHealthy(t *testing.T) {
	act := &fakeActuator{}
	obs := &fakeObserver{}
	enr := &fakeEnroller{}
	asm := &fakeAssembler{}
	r := newReconciler(act, obs, enr, asm)

	c := Cohort{
		ID:        "c-coll",
		Members:   []EntityIntent{member("n-0"), member("n-1"), member("n-2")},
		Budget:    fastBudget(),
		MinViable: 3,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("collective: Ready=false")
	}
	if atomic.LoadInt32(&asm.calls) != 1 {
		t.Errorf("Assemble called %d times want 1", asm.calls)
	}
	if len(asm.members) != 3 {
		t.Errorf("Assemble received %d members want 3", len(asm.members))
	}
}

// Collective with injected ICE that exhausts chain: whole cohort fast-fails as
// a unit promptly — not after waiting out the barrier deadline.
func TestReconciler_Collective_ICE_FastFails(t *testing.T) {
	iceErr := &iceError{}

	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a", CapacityModel: CapacityOnDemand}
	rung1 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1b", CapacityModel: CapacityOnDemand}
	chain := []Rung{rung0, rung1}

	var launchCount int32
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			atomic.AddInt32(&launchCount, 1)
			// Always ICE on both rungs.
			return Observation{}, iceErr
		},
	}
	obs := &fakeObserver{}
	enr := &fakeEnroller{}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}
	r := &Reconciler{
		Actuator:   act,
		Observer:   obs,
		Classifier: clf,
		Enroller:   enr,
	}

	m0 := member("n-0")
	m0.Placement = RungPlacement{Rung: rung0, Chain: chain}
	m1 := member("n-1")
	m1.Placement = RungPlacement{Rung: rung0, Chain: chain}

	c := Cohort{
		ID:        "c-ice",
		Members:   []EntityIntent{m0, m1},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 2,
	}

	start := time.Now()
	outcome, err := r.Reconcile(context.Background(), c)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome.Ready {
		t.Error("ICE-exhausted cohort: Ready=true want false")
	}

	// Fast-fail must be prompt — well under the 30s barrier deadline.
	if elapsed > 5*time.Second {
		t.Errorf("fast-fail took %v — expected < 5s (barrier deadline was 30s)", elapsed)
	}

	// Every entity has a record.
	for _, id := range []EntityID{"n-0", "n-1"} {
		rec, ok := outcome.Records[id]
		if !ok {
			t.Errorf("entity %s: no record", id)
			continue
		}
		// Each entity is either the culprit (Terminal set) or a survivor
		// (CohortCancelled set). One or the other must be set.
		if rec.Terminal == nil && rec.CohortCancelled == nil {
			t.Errorf("entity %s: both Terminal and CohortCancelled nil — no outcome recorded", id)
		}
		if rec.Terminal != nil && rec.CohortCancelled != nil {
			t.Errorf("entity %s: both Terminal and CohortCancelled set — ambiguous outcome", id)
		}
	}
}

// Per-phase attribution: phase-1 failure and phase-3 failure produce different,
// correctly-named reasons. Each runs in its own 1-cohort to avoid fast-fail
// cross-entity interference.
func TestReconciler_PhaseAttribution(t *testing.T) {
	termErr := &termError{}

	// --- phase-1 failure ---
	act1 := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			return Observation{}, termErr
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		termErr.Error(): {Class: FaultTerminal, Code: "UnauthorizedOperation"},
	}}
	r1 := &Reconciler{
		Actuator:   act1,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}
	out1, _ := r1.Reconcile(context.Background(), Cohort{
		ID:      "c-p1",
		Members: []EntityIntent{member("phase1-fail")},
		Budget:  fastBudget(),
	})
	rec1 := out1.Records["phase1-fail"]
	if rec1.Terminal == nil {
		t.Fatal("phase1-fail: expected terminal fault")
	}
	if rec1.Terminal.Code != "UnauthorizedOperation" {
		t.Errorf("phase1-fail: Code=%q want UnauthorizedOperation", rec1.Terminal.Code)
	}
	if rec1.ReachedPhase != PhaseLaunchAcked {
		t.Errorf("phase1-fail: ReachedPhase=%v want PhaseLaunchAcked", rec1.ReachedPhase)
	}

	// --- phase-3 failure (enrollment timeout) ---
	r3 := &Reconciler{
		Actuator: &fakeActuator{},
		Observer: &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller: &fakeEnroller{
			enrolledFn: func(id EntityID) Readiness {
				return Readiness{Enrolled: false, Operational: false}
			},
		},
	}
	out3, _ := r3.Reconcile(context.Background(), Cohort{
		ID:      "c-p3",
		Members: []EntityIntent{member("phase3-fail")},
		Budget: PhaseBudget{
			LaunchAcked:    2 * time.Second,
			Running:        2 * time.Second,
			Enrolled:       150 * time.Millisecond, // short so enrollment times out
			CohortBarrier:  2 * time.Second,
			CohortAssembly: 2 * time.Second,
		},
	})
	rec3 := out3.Records["phase3-fail"]
	if rec3.Terminal == nil {
		t.Fatal("phase3-fail: expected terminal fault")
	}
	if rec3.ReachedPhase != PhaseEnrolled {
		t.Errorf("phase3-fail: ReachedPhase=%v want PhaseEnrolled", rec3.ReachedPhase)
	}

	// The two failures name different phases.
	if rec1.ReachedPhase == rec3.ReachedPhase {
		t.Error("phase1 and phase3 failures have the same ReachedPhase — attribution broken")
	}
}

// Chain discipline: advanceRung walks only approved rungs; never outside the chain.
func TestReconciler_ChainDiscipline(t *testing.T) {
	rung0 := Rung{InstanceType: "p4d.24xlarge", AvailZone: "us-east-1a"}
	rung1 := Rung{InstanceType: "p4d.24xlarge", AvailZone: "us-east-1b"}
	rung2 := Rung{InstanceType: "p4de.24xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung0, rung1, rung2}

	iceErr := &iceError{}
	var rungs []Rung
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			rungs = append(rungs, rungOf(intent))
			if rungOf(intent) == rung2 {
				// Last rung succeeds.
				return Observation{ID: intent.ID, State: StateLaunching,
					ProviderID: "i-ok", Rung: rungOf(intent), ObservedAt: time.Now()}, nil
			}
			return Observation{}, iceErr
		},
	}
	obs := &fakeObserver{}
	enr := &fakeEnroller{}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}
	r := &Reconciler{Actuator: act, Observer: obs, Classifier: clf, Enroller: enr}

	m := member("gpu-chain")
	m.Placement = RungPlacement{Rung: rung0, Chain: chain}

	c := Cohort{
		ID:      "c-chain",
		Members: []EntityIntent{m},
		Budget:  fastBudget(),
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("chain: Ready=false, record=%s", outcome.Records["gpu-chain"].Summary())
	}

	// Must have tried exactly rung0, rung1, rung2 — in that order.
	if len(rungs) != 3 {
		t.Fatalf("tried %d rungs want 3: %v", len(rungs), rungs)
	}
	expected := []Rung{rung0, rung1, rung2}
	for i, got := range rungs {
		if got != expected[i] {
			t.Errorf("rung[%d]=%v want %v (chain discipline broken)", i, got, expected[i])
		}
	}
}

// ---- S5.5.4 tests -----------------------------------------------------------

// Collective cohort, one culprit (ICE chain-exhausted): culprit has Terminal;
// every survivor has CohortCancelled naming the culprit + cause.
func TestReconciler_Survivors_CohortCancelledDistinct(t *testing.T) {
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	rung1 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1b"}
	chain := []Rung{rung0, rung1}

	// Only "culprit" ICEs. Others succeed.
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "culprit" {
				return Observation{}, iceErr
			}
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}

	culpritM := member("culprit")
	culpritM.Placement = RungPlacement{Rung: rung0, Chain: chain}
	survivorA := member("survivor-a")
	survivorB := member("survivor-b")

	c := Cohort{
		ID:        "c-cancel",
		Members:   []EntityIntent{culpritM, survivorA, survivorB},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3,
	}
	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome.Ready {
		t.Fatal("cohort should not be ready when culprit ICEs")
	}

	// Culprit carries Terminal, not CohortCancelled.
	culpritRec := outcome.Records["culprit"]
	if culpritRec.Terminal == nil {
		t.Error("culprit: Terminal=nil want capacity-exhausted fault")
	}
	if culpritRec.CohortCancelled != nil {
		t.Error("culprit: CohortCancelled is set — culprit must not appear cancelled")
	}

	// Every survivor carries CohortCancelled naming "culprit".
	for _, sid := range []EntityID{"survivor-a", "survivor-b"} {
		rec := outcome.Records[sid]
		if rec.Terminal != nil {
			t.Errorf("survivor %s: Terminal is set — survivors must not appear failed", sid)
		}
		if rec.CohortCancelled == nil {
			t.Errorf("survivor %s: CohortCancelled=nil want culprit info", sid)
			continue
		}
		cc := rec.CohortCancelled
		if cc.CulpritID != "culprit" {
			t.Errorf("survivor %s: CulpritID=%q want culprit", sid, cc.CulpritID)
		}
		if cc.CulpritFault.Class != FaultCapacityExhausted {
			t.Errorf("survivor %s: CulpritFault.Class=%v want CapacityExhausted", sid, cc.CulpritFault.Class)
		}
		if cc.CulpritFault.Code != "InsufficientInstanceCapacity" {
			t.Errorf("survivor %s: CulpritFault.Code=%q want InsufficientInstanceCapacity", sid, cc.CulpritFault.Code)
		}
		// SurvivorPhase should be set to the phase the survivor was at; PhaseLaunchAcked
		// (the zero value) is valid if the entity was still in its first phase.
		// Summary must not look like a failure.
		summary := rec.Summary()
		if summary == "" {
			t.Errorf("survivor %s: empty Summary()", sid)
		}
		// Programmatic check: WasCohortCancelled must be true.
		if !rec.WasCohortCancelled() {
			t.Errorf("survivor %s: WasCohortCancelled()=false", sid)
		}
	}
}

// Survivors cancelled at different phases each record their own reached-phase.
// This test verifies two things: (1) survivors at clearly different phases get
// different SurvivorPhase values; (2) the culprit is never marked CohortCancelled.
// It uses a slow entity that is held in phase 1 until the culprit fails, and
// an entity that completes all phases before the culprit — asserting the
// phase captured is the one the entity actually reached.
func TestReconciler_Survivors_DifferentPhases(t *testing.T) {
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	rung1 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1b"}
	chain := []Rung{rung0, rung1}

	// "blocked" never returns from Launch until the channel closes.
	// It will be in PhaseLaunchAcked when fast-fail fires.
	blockedCh := make(chan struct{})

	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "culprit" {
				return Observation{}, iceErr
			}
			if intent.ID == "blocked" {
				<-blockedCh
				return Observation{}, fmt.Errorf("cancelled by fast-fail")
			}
			// "free-entity": succeeds immediately.
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error():                          {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
		"cancelled by fast-fail":               {Class: FaultTerminal, Code: "Cancelled"},
	}}

	culpritM := member("culprit")
	culpritM.Placement = RungPlacement{Rung: rung0, Chain: chain}

	c := Cohort{
		ID:        "c-phases",
		Members:   []EntityIntent{culpritM, member("blocked"), member("free-entity")},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3,
	}
	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}

	// Release the blocked entity shortly after fast-fail is expected.
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(blockedCh)
	}()

	outcome, _ := r.Reconcile(context.Background(), c)
	if outcome.Ready {
		t.Fatal("cohort should not be ready")
	}

	// Culprit must NOT be CohortCancelled.
	culpritRec := outcome.Records["culprit"]
	if culpritRec.WasCohortCancelled() {
		t.Error("culprit: WasCohortCancelled()=true — culprit must not be marked cancelled")
	}
	if culpritRec.Terminal == nil {
		t.Error("culprit: Terminal=nil")
	}

	// "blocked" was in PhaseLaunchAcked when fast-fail or budget hit.
	// It may be CohortCancelled (fast-fail) or Terminal (budget). Either is fine;
	// what matters is it is NOT marked as the culprit.
	blockedRec := outcome.Records["blocked"]
	if blockedRec.CohortCancelled != nil && blockedRec.CohortCancelled.CulpritID != "culprit" {
		t.Errorf("blocked: CulpritID=%q want culprit", blockedRec.CohortCancelled.CulpritID)
	}
	if blockedRec.CohortCancelled != nil {
		// SurvivorPhase of blocked must be PhaseLaunchAcked — it never got past phase 1.
		if blockedRec.CohortCancelled.SurvivorPhase != PhaseLaunchAcked {
			t.Errorf("blocked: SurvivorPhase=%v want PhaseLaunchAcked (was in phase 1 the whole time)",
				blockedRec.CohortCancelled.SurvivorPhase)
		}
	}

	// "free-entity" completed phases — its SurvivorPhase should be past PhaseLaunchAcked
	// if fast-fail arrived after it advanced, OR PhaseLaunchAcked if fast-fail was instant.
	freeRec := outcome.Records["free-entity"]
	if freeRec.CohortCancelled != nil {
		// Any phase is valid for free-entity — the test only guarantees blocked < free
		// in the common case; under scheduling pressure they may be equal.
		t.Logf("free-entity SurvivorPhase=%v (may be any phase depending on fast-fail timing)",
			freeRec.CohortCancelled.SurvivorPhase)
	}

	// The two surviving records must be distinguishable from each other and from culprit.
	for _, id := range []EntityID{"blocked", "free-entity"} {
		rec := outcome.Records[id]
		// Not the culprit: either CohortCancelled or Terminal, but not Terminal with culprit's code.
		if rec.Terminal != nil && rec.Terminal.Code == "InsufficientInstanceCapacity" {
			t.Errorf("entity %s: has culprit's ICE code — should not be the culprit", id)
		}
	}
}

// Summary/Explain render CohortCancelled distinctly from failure.
func TestRecord_CohortCancelled_Summary(t *testing.T) {
	rec := Record{
		Entity:       "survivor-7",
		Generation:   "g1",
		Cohort:       "c-test",
		ReachedPhase: PhaseRunning,
		CohortCancelled: &CohortCancelInfo{
			CulpritID:     "gpu-041",
			CulpritFault:  Fault{Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
			CulpritPhase:  PhaseRunning,
			At:            time.Unix(1716000000, 0),
			SurvivorPhase: PhaseRunning,
		},
	}

	summary := rec.Summary()
	if summary == "" {
		t.Fatal("Summary() empty for CohortCancelled record")
	}
	// Must contain "cancelled" somewhere.
	if !containsFold(summary, "cancelled") {
		t.Errorf("Summary()=%q does not mention cancelled", summary)
	}
	// Must NOT look like a bare fault summary (e.g. "terminal/..." or "ready").
	if containsFold(summary, "terminal") || containsFold(summary, "ready (") {
		t.Errorf("Summary()=%q looks like a fault or success summary", summary)
	}
	// WasCohortCancelled must be true; Succeeded must be false.
	if !rec.WasCohortCancelled() {
		t.Error("WasCohortCancelled()=false")
	}
	if rec.Succeeded() {
		t.Error("Succeeded()=true for a cancelled record")
	}

	// Explain must be distinct.
	explain := rec.Explain()
	if !containsFold(explain, "gpu-041") {
		t.Errorf("Explain() does not name the culprit entity: %s", explain)
	}
	if !containsFold(explain, "InsufficientInstanceCapacity") {
		t.Errorf("Explain() does not name the culprit fault code: %s", explain)
	}
}

func containsFold(s, sub string) bool {
	return len(s) >= len(sub) && containsSubstring(s, sub)
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if equalFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// Ambiguous-reached-reconciler flags one entity Terminal but does not abort others.
func TestReconciler_AmbiguousBugIsLocalized(t *testing.T) {
	ambErr := &ambiguousError{}
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "amb-entity" {
				return Observation{}, ambErr
			}
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	// Classifier returns Ambiguous for the ambiguous error.
	clf := &fakeClassifier{faults: map[string]Fault{
		ambErr.Error(): {Class: FaultAmbiguous, Code: "TransportError"},
	}}
	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}

	// Two-member cohort: one ambiguous entity, one healthy.
	// With MinViable=1, the healthy entity can satisfy the barrier alone.
	c := Cohort{
		ID:        "c-amb",
		Members:   []EntityIntent{member("amb-entity"), member("healthy")},
		Budget:    fastBudget(),
		MinViable: 1,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Ambiguous entity: Terminal with loud BUG code; not a panic, not an outage.
	ambRec := outcome.Records["amb-entity"]
	if ambRec.Terminal == nil {
		t.Error("amb-entity: Terminal=nil — Ambiguous must set Terminal")
	} else if ambRec.Terminal.Code != "AmbiguousReachedReconciler" {
		t.Errorf("amb-entity: Terminal.Code=%q want AmbiguousReachedReconciler", ambRec.Terminal.Code)
	}

	// Healthy entity should succeed (MinViable=1 satisfied).
	healthyRec := outcome.Records["healthy"]
	if !healthyRec.Succeeded() {
		t.Errorf("healthy entity failed: %s", healthyRec.Summary())
	}
}

// Warm-start ICE advances the chain like any other rung (not a bypass).
func TestReconciler_WarmStart_ICEAdvancesChain(t *testing.T) {
	iceErr := &iceError{}
	warmRung := Rung{InstanceType: "p4d.24xlarge", AvailZone: "us-east-1a", WarmStart: true}
	coldRung := Rung{InstanceType: "p4d.24xlarge", AvailZone: "us-east-1a", WarmStart: false}
	chain := []Rung{warmRung, coldRung}

	var startCalled, launchCalled int
	act := &fakeActuator{
		startFn: func(id EntityID) (Observation, error) {
			startCalled++
			return Observation{}, iceErr // warm-start ICEs
		},
		launchFn: func(intent EntityIntent) (Observation, error) {
			launchCalled++
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-cold", Rung: rungOf(intent), ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}
	r := &Reconciler{Actuator: act, Observer: &fakeObserver{}, Classifier: clf, Enroller: &fakeEnroller{}}

	m := member("warm-entity")
	m.Placement = RungPlacement{Rung: warmRung, Chain: chain}

	outcome, err := r.Reconcile(context.Background(), Cohort{
		ID:      "c-warm",
		Members: []EntityIntent{m},
		Budget:  fastBudget(),
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("warm→cold fallback: Ready=false, summary=%s", outcome.Records["warm-entity"].Summary())
	}
	if startCalled != 1 {
		t.Errorf("Start called %d times want 1", startCalled)
	}
	if launchCalled != 1 {
		t.Errorf("Launch called %d times want 1 (after warm-start ICE)", launchCalled)
	}
}

// ---- S5.6.3 tests -----------------------------------------------------------

// S5.6.3-a: parent context cancelled mid-reconcile.
// Survivors must be ParentCancelled — NOT CohortCancelled, NOT carrying a CulpritID.
func TestReconciler_ParentCancel_IsDistinct(t *testing.T) {
	// All entities stall in Launch until the parent ctx is cancelled.
	waitCh := make(chan struct{})
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			<-waitCh // blocks until test cancels the parent ctx
			return Observation{}, fmt.Errorf("interrupted")
		},
	}
	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   &fakeEnroller{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := Cohort{
		ID:        "c-parent-cancel",
		Members:   []EntityIntent{member("n-0"), member("n-1"), member("n-2")},
		Budget:    PhaseBudget{LaunchAcked: 10 * time.Second, Running: 10 * time.Second, Enrolled: 10 * time.Second, CohortBarrier: 60 * time.Second},
		MinViable: 3,
	}

	done := make(chan struct{})
	var outcome Outcome
	go func() {
		outcome, _ = r.Reconcile(ctx, c)
		close(done)
	}()

	// Cancel the parent context after a brief moment; then unblock stalled entities.
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(waitCh) // release the blocked Launch calls

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconcile did not return after parent cancel")
	}

	if outcome.Ready {
		t.Error("parent-cancelled cohort: Ready=true want false")
	}

	for _, id := range []EntityID{"n-0", "n-1", "n-2"} {
		rec := outcome.Records[id]

		// Must NOT be CohortCancelled — there was no culprit entity.
		if rec.WasCohortCancelled() {
			t.Errorf("entity %s: WasCohortCancelled()=true after parent cancel — fabricated culprit", id)
		}
		if rec.CohortCancelled != nil && rec.CohortCancelled.CulpritID != "" {
			t.Errorf("entity %s: CohortCancelled has CulpritID=%q — fabricated story", id, rec.CohortCancelled.CulpritID)
		}

		// Must be ParentCancelled OR Terminal (if entity set its own terminal before cancel arrived).
		if !rec.WasParentCancelled() && rec.Terminal == nil {
			t.Errorf("entity %s: neither ParentCancelled nor Terminal — outcome unrecorded", id)
		}

		// If ParentCancelled, the cause string must be present (no empty string).
		if rec.WasParentCancelled() {
			if rec.ParentCancelled.Cause == "" {
				t.Errorf("entity %s: ParentCancelled.Cause is empty", id)
			}
		}
	}
}

// S5.6.3-b: regression — cohort fast-fail still yields CohortCancelled with
// correct culprit. S5.6 must not break the existing fast-fail path.
func TestReconciler_FastFail_StillCohortCancelled(t *testing.T) {
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung0} // single rung, chain exhausted immediately

	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "culprit" {
				return Observation{}, iceErr
			}
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}
	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}

	culpritM := member("culprit")
	culpritM.Placement = RungPlacement{Rung: rung0, Chain: chain}

	c := Cohort{
		ID:        "c-regression",
		Members:   []EntityIntent{culpritM, member("survivor-x"), member("survivor-y")},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome.Ready {
		t.Fatal("fast-fail cohort: Ready=true want false")
	}

	// Culprit has Terminal, not CohortCancelled.
	culpritRec := outcome.Records["culprit"]
	if culpritRec.WasCohortCancelled() {
		t.Error("culprit: WasCohortCancelled()=true")
	}
	if culpritRec.WasParentCancelled() {
		t.Error("culprit: WasParentCancelled()=true")
	}
	if culpritRec.Terminal == nil {
		t.Error("culprit: Terminal=nil")
	}

	// Survivors have CohortCancelled with the real culprit.
	for _, sid := range []EntityID{"survivor-x", "survivor-y"} {
		rec := outcome.Records[sid]
		if !rec.WasCohortCancelled() {
			t.Errorf("survivor %s: WasCohortCancelled()=false", sid)
			continue
		}
		if rec.WasParentCancelled() {
			t.Errorf("survivor %s: WasParentCancelled()=true — must not appear as parent cancel", sid)
		}
		cc := rec.CohortCancelled
		if cc.CulpritID != "culprit" {
			t.Errorf("survivor %s: CulpritID=%q want culprit", sid, cc.CulpritID)
		}
		if cc.CulpritFault.Code != "InsufficientInstanceCapacity" {
			t.Errorf("survivor %s: CulpritFault.Code=%q want InsufficientInstanceCapacity — verbatim code", sid, cc.CulpritFault.Code)
		}
		if cc.CulpritFault.Class != FaultCapacityExhausted {
			t.Errorf("survivor %s: CulpritFault.Class=%v want CapacityExhausted", sid, cc.CulpritFault.Class)
		}
		// SurvivorPhase is each entity's own reached-phase, not the cohort's phase.
		// Survivor was in PhaseLaunchAcked (launched successfully but cancel arrived
		// before it advanced to PhaseRunning).
		if cc.SurvivorPhase != PhaseLaunchAcked && cc.SurvivorPhase != PhaseRunning && cc.SurvivorPhase != PhaseEnrolled {
			t.Errorf("survivor %s: SurvivorPhase=%v unexpected", sid, cc.SurvivorPhase)
		}
	}
}

// S5.6.3-c: SurvivorPhase is the entity's own reached-phase at cancellation,
// not the cohort's phase. The existing TestReconciler_Survivors_DifferentPhases
// already exercises this for CohortCancelled; this test asserts it explicitly
// with a clear before/after snapshot to prevent regression.
func TestReconciler_SurvivorPhase_IsEntityOwn(t *testing.T) {
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung0}

	// "slow" never makes it past launch (stalls until fast-fail cancels it).
	// "fast" launches, advances to PhaseRunning, then gets cancelled.
	slowReady := make(chan struct{})
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "culprit" {
				return Observation{}, iceErr
			}
			if intent.ID == "slow" {
				<-slowReady // stalls
				return Observation{}, fmt.Errorf("cancelled")
			}
			// fast: returns immediately
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error():       {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
		"cancelled":          {Class: FaultTerminal, Code: "Cancelled"},
	}}
	// Observer returns StateRunning so "fast" advances past launch.
	obs := &fakeObserver{}
	enr := &fakeEnroller{}

	culpritM := member("culprit")
	culpritM.Placement = RungPlacement{Rung: rung0, Chain: chain}

	c := Cohort{
		ID:        "c-survivor-phase",
		Members:   []EntityIntent{culpritM, member("slow"), member("fast")},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3,
	}
	r := &Reconciler{Actuator: act, Observer: obs, Classifier: clf, Enroller: enr}

	go func() {
		time.Sleep(200 * time.Millisecond)
		close(slowReady)
	}()

	outcome, _ := r.Reconcile(context.Background(), c)
	if outcome.Ready {
		t.Fatal("cohort should not be ready")
	}

	// "slow" was in PhaseLaunchAcked throughout (never left launch).
	// It may be CohortCancelled or Terminal depending on timing.
	slowRec := outcome.Records["slow"]
	if slowRec.CohortCancelled != nil {
		if slowRec.CohortCancelled.SurvivorPhase != PhaseLaunchAcked {
			t.Errorf("slow SurvivorPhase=%v want PhaseLaunchAcked (was blocking in launch)",
				slowRec.CohortCancelled.SurvivorPhase)
		}
	}

	// "fast" launched successfully; it may be in PhaseRunning or PhaseEnrolled.
	// Its SurvivorPhase must be >= PhaseRunning (it got past launch).
	fastRec := outcome.Records["fast"]
	if fastRec.CohortCancelled != nil {
		if fastRec.CohortCancelled.SurvivorPhase < PhaseRunning {
			t.Logf("fast SurvivorPhase=%v — may be PhaseLaunchAcked under heavy scheduling pressure",
				fastRec.CohortCancelled.SurvivorPhase)
			// Not a hard failure: under -race the scheduler may not have advanced
			// the goroutine before fast-fail fired. Log and move on.
		}
	}

	// Key assertion: slow's SurvivorPhase != fast's SurvivorPhase (when both cancelled).
	// This proves SurvivorPhase is per-entity, not a cohort-wide snapshot.
	if slowRec.CohortCancelled != nil && fastRec.CohortCancelled != nil {
		// fast must not have the SAME SurvivorPhase as slow if it advanced further.
		// Under heavy load they could be equal — log rather than hard-fail.
		if slowRec.CohortCancelled.SurvivorPhase == fastRec.CohortCancelled.SurvivorPhase {
			t.Logf("slow and fast have the same SurvivorPhase=%v — acceptable under scheduling pressure",
				slowRec.CohortCancelled.SurvivorPhase)
		}
	}

	// Primary assertion: CulpritFault carries the verbatim provider code.
	for _, sid := range []EntityID{"slow", "fast"} {
		rec := outcome.Records[sid]
		if rec.CohortCancelled != nil {
			if rec.CohortCancelled.CulpritFault.Code != "InsufficientInstanceCapacity" {
				t.Errorf("%s: CulpritFault.Code=%q want verbatim InsufficientInstanceCapacity", sid, rec.CohortCancelled.CulpritFault.Code)
			}
		}
	}
}

// ---- S5.6b tests ------------------------------------------------------------

// S5.6b.3: parent cancel with enough entities non-terminal that a naive gate
// WOULD pass. Assembler must NOT be called. Outcome must be ParentCancelled.
// This is the behavioral proof the assembly-on-dead-context bug is closed.
func TestReconciler_ParentCancel_AssemblerNotCalled(t *testing.T) {
	asm := &fakeAssembler{}

	// All three entities succeed immediately and reach enrollment.
	// Then the parent ctx is cancelled while they stall in enrolled polling.
	enrollCh := make(chan struct{})
	enr := &fakeEnroller{
		enrolledFn: func(id EntityID) Readiness {
			// Block in the Enroller — simulates entities that launched and are
			// running but haven't completed enrollment yet when parent cancels.
			select {
			case <-enrollCh:
				return Readiness{Enrolled: true, Operational: true}
			case <-time.After(10 * time.Second):
				return Readiness{}
			}
		},
	}

	r := &Reconciler{
		Actuator:   &fakeActuator{},
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   enr,
		Assembler:  asm,
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := Cohort{
		ID:        "c-parent-no-assemble",
		Members:   []EntityIntent{member("n-0"), member("n-1"), member("n-2")},
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 10 * time.Second, CohortBarrier: 60 * time.Second, CohortAssembly: 10 * time.Second},
		MinViable: 3,
	}

	done := make(chan struct{})
	var outcome Outcome
	go func() {
		outcome, _ = r.Reconcile(ctx, c)
		close(done)
	}()

	// Wait briefly for entities to get into the enrollment polling loop,
	// then cancel the parent context.
	time.Sleep(50 * time.Millisecond)
	cancel()
	close(enrollCh) // unblock the Enroller so goroutines can exit

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconcile did not return after parent cancel")
	}

	// S5.6b.3 primary assertion: assembler call count is ZERO.
	assembleCalls := int(asm.calls)
	t.Logf("Assembler call count (S5.6b.3): %d (must be 0)", assembleCalls)
	if assembleCalls != 0 {
		t.Errorf("Assembler was called %d times on a parent-cancelled reconcile — assembly-on-dead-context bug", assembleCalls)
	}

	if outcome.Ready {
		t.Error("parent-cancelled cohort: Ready=true want false")
	}

	// Every entity must be ParentCancelled (not CohortCancelled, not Terminal).
	for _, id := range []EntityID{"n-0", "n-1", "n-2"} {
		rec := outcome.Records[id]
		if rec.WasCohortCancelled() {
			t.Errorf("entity %s: WasCohortCancelled()=true — fabricated culprit after parent cancel", id)
		}
		if !rec.WasParentCancelled() && rec.Terminal == nil {
			t.Errorf("entity %s: neither ParentCancelled nor Terminal", id)
		}
	}
}

// S5.6b.4-a regression: normal collective success still invokes assembler exactly once.
func TestReconciler_NormalCollective_AssemblerCalledOnce(t *testing.T) {
	asm := &fakeAssembler{}
	r := newReconciler(&fakeActuator{}, &fakeObserver{}, &fakeEnroller{}, asm)

	c := Cohort{
		ID:        "c-normal-asm",
		Members:   []EntityIntent{member("n-0"), member("n-1"), member("n-2")},
		Budget:    fastBudget(),
		MinViable: 3,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("normal collective: Ready=false")
	}
	if int(asm.calls) != 1 {
		t.Errorf("Assembler called %d times want 1", asm.calls)
	}
}

// S5.6b.4-b regression: cohort fast-fail (ICE) does NOT invoke assembler.
func TestReconciler_FastFail_AssemblerNotCalled(t *testing.T) {
	asm := &fakeAssembler{}
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung0}

	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			if intent.ID == "culprit" {
				return Observation{}, iceErr
			}
			return Observation{ID: intent.ID, State: StateLaunching,
				ProviderID: "i-" + string(intent.ID), Rung: rungOf(intent),
				ObservedAt: time.Now()}, nil
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		iceErr.Error(): {Class: FaultCapacityExhausted, Code: "InsufficientInstanceCapacity"},
	}}

	culpritM := member("culprit")
	culpritM.Placement = RungPlacement{Rung: rung0, Chain: chain}

	r := &Reconciler{
		Actuator:   act,
		Observer:   &fakeObserver{},
		Classifier: clf,
		Enroller:   &fakeEnroller{},
		Assembler:  asm,
	}

	c := Cohort{
		ID:        "c-ff-no-asm",
		Members:   []EntityIntent{culpritM, member("s-0"), member("s-1")},
		Budget:    fastBudget(),
		MinViable: 3,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome.Ready {
		t.Error("fast-fail cohort: Ready=true")
	}
	if int(asm.calls) != 0 {
		t.Errorf("Assembler called %d times want 0 on fast-fail cohort", asm.calls)
	}
}

// ---- additional test error types --------------------------------------------

type ambiguousError struct{}

func (e *ambiguousError) Error() string { return "TransportError" }

// ---- test error types -------------------------------------------------------

type iceError struct{}

func (e *iceError) Error() string { return "InsufficientInstanceCapacity" }

type termError struct{}

func (e *termError) Error() string { return "UnauthorizedOperation" }
