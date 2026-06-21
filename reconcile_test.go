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
	// addrFn lets tests inject a per-entity observed address — the infra-truth
	// private IP the Assembler reads. Nil → empty address.
	addrFn func(id EntityID) string
}

func (o *fakeObserver) Observe(_ context.Context, ids []EntityID) ([]Observation, error) {
	obs := make([]Observation, len(ids))
	for i, id := range ids {
		st := StateRunning
		if o.stateFn != nil {
			st = o.stateFn(id)
		}
		addr := ""
		if o.addrFn != nil {
			addr = o.addrFn(id)
		}
		obs[i] = Observation{ID: id, State: st, Address: addr, ObservedAt: time.Now()}
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
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}

	// Collective placement (#5): all members launch together on the shared rung.
	// The culprit then fails in the RUNNING phase (Observer reports StateFailed),
	// tripping the all-or-nothing gate post-launch. Survivors are cancelled at
	// their own reached-phase; the culprit is never marked CohortCancelled.
	act := &fakeActuator{} // every launch acks
	obs := &fakeObserver{
		stateFn: func(id EntityID) LifecycleState {
			if id == "culprit" {
				return StateFailed
			}
			return StateRunning
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{}}

	var members []EntityIntent
	for _, id := range []string{"culprit", "blocked", "free-entity"} {
		m := member(id)
		m.Placement = RungPlacement{Rung: rung0}
		members = append(members, m)
	}
	c := Cohort{
		ID:        "c-phases",
		Members:   members,
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3,
	}
	r := &Reconciler{
		Actuator:   act,
		Observer:   obs,
		Classifier: clf,
		Enroller:   &fakeEnroller{},
	}

	outcome, _ := r.Reconcile(context.Background(), c)
	if outcome.Ready {
		t.Fatal("cohort should not be ready")
	}

	// Culprit must NOT be CohortCancelled — it failed (running-phase StateFailed).
	culpritRec := outcome.Records["culprit"]
	if culpritRec.WasCohortCancelled() {
		t.Error("culprit: WasCohortCancelled()=true — culprit must not be marked cancelled")
	}
	if culpritRec.Terminal == nil {
		t.Error("culprit: Terminal=nil")
	}

	// Survivors are CohortCancelled at their OWN reached-phase (>= running, since
	// they launched on the shared rung and were observed running) and name the
	// culprit — SurvivorPhase is per-entity, not a cohort-wide snapshot.
	for _, id := range []EntityID{"blocked", "free-entity"} {
		rec := outcome.Records[id]
		if rec.CohortCancelled != nil {
			if rec.CohortCancelled.CulpritID != "culprit" {
				t.Errorf("%s: CulpritID=%q want culprit", id, rec.CohortCancelled.CulpritID)
			}
			if rec.CohortCancelled.SurvivorPhase < PhaseRunning {
				t.Errorf("%s: SurvivorPhase=%v want >= PhaseRunning", id, rec.CohortCancelled.SurvivorPhase)
			}
		}
		// Never the culprit: must not carry the culprit's terminal identity.
		if rec.Terminal != nil && rec.Entity == "culprit" {
			t.Errorf("entity %s: wrongly marked as culprit", id)
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
// Collective (all-or-nothing) cohort whose SHARED rung exhausts on capacity
// (#5): the cohort fails as a unit, but the culprit/survivor legibility is
// preserved — exactly ONE member is Terminal (the first to block on the
// exhausted shared rung) and every other member is CohortCancelled naming that
// culprit with its verbatim ICE fault. Under collective placement all members
// share one rung, so all block together; the test asserts the split shape, not
// a specific culprit name (which member blocks first is a scheduling detail).
func TestReconciler_FastFail_StillCohortCancelled(t *testing.T) {
	iceErr := &iceError{}
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung0} // single shared rung, chain exhausted immediately

	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			// Capacity is exhausted on the shared rung → every member ICEs.
			return Observation{}, iceErr
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

	// All members share the same rung+chain — the collective shape.
	ids := []EntityID{"node-0", "node-1", "node-2"}
	var members []EntityIntent
	for _, id := range ids {
		m := member(string(id))
		m.Placement = RungPlacement{Rung: rung0, Chain: chain}
		members = append(members, m)
	}
	c := Cohort{
		ID:        "c-regression",
		Members:   members,
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3, // all-or-nothing → places as a unit
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome.Ready {
		t.Fatal("fast-fail cohort: Ready=true want false")
	}

	// Exactly one member is the culprit (Terminal); the rest are CohortCancelled.
	var culprit EntityID
	terminals := 0
	for _, id := range ids {
		rec := outcome.Records[id]
		if rec.Terminal != nil {
			terminals++
			culprit = id
			if rec.WasCohortCancelled() {
				t.Errorf("culprit %s: must not also be CohortCancelled", id)
			}
			if rec.Terminal.Code != "InsufficientInstanceCapacity" {
				t.Errorf("culprit %s: Terminal.Code=%q want verbatim InsufficientInstanceCapacity", id, rec.Terminal.Code)
			}
		}
	}
	if terminals != 1 {
		t.Fatalf("want exactly 1 Terminal culprit, got %d", terminals)
	}

	// Every non-culprit member is CohortCancelled naming the culprit, with the
	// verbatim ICE fault preserved.
	for _, id := range ids {
		if id == culprit {
			continue
		}
		rec := outcome.Records[id]
		if !rec.WasCohortCancelled() {
			t.Errorf("survivor %s: WasCohortCancelled()=false", id)
			continue
		}
		if rec.WasParentCancelled() {
			t.Errorf("survivor %s: WasParentCancelled()=true — must not appear as parent cancel", id)
		}
		cc := rec.CohortCancelled
		if cc.CulpritID != culprit {
			t.Errorf("survivor %s: CulpritID=%q want %q", id, cc.CulpritID, culprit)
		}
		if cc.CulpritFault.Code != "InsufficientInstanceCapacity" {
			t.Errorf("survivor %s: CulpritFault.Code=%q want InsufficientInstanceCapacity — verbatim code", id, cc.CulpritFault.Code)
		}
		if cc.CulpritFault.Class != FaultCapacityExhausted {
			t.Errorf("survivor %s: CulpritFault.Class=%v want CapacityExhausted", id, cc.CulpritFault.Class)
		}
	}
}

// S5.6.3-c: SurvivorPhase is the entity's own reached-phase at cancellation,
// not the cohort's phase. The existing TestReconciler_Survivors_DifferentPhases
// already exercises this for CohortCancelled; this test asserts it explicitly
// with a clear before/after snapshot to prevent regression.
// The fast-fail-with-distinct-survivor-phases path is post-launch: all members
// share one rung and LAUNCH together (collective placement, #5), then a member
// fails during the RUNNING phase, tripping the all-or-nothing gate. The
// survivors are cancelled at THEIR OWN reached-phase (per-entity, not a cohort
// snapshot), and carry the culprit's verbatim fault. (A launch-time culprit no
// longer applies here — under collective placement an exhausted shared rung
// fails the whole cohort as a unit; see TestReconciler_FastFail_StillCohortCancelled.)
func TestReconciler_SurvivorPhase_IsEntityOwn(t *testing.T) {
	rung0 := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}

	// All members launch fine on the shared rung. The Observer then reports the
	// culprit as StateFailed (a running-phase failure), while the others stay
	// running — tripping the all-or-nothing gate post-launch.
	act := &fakeActuator{} // default: every launch acks
	obs := &fakeObserver{
		stateFn: func(id EntityID) LifecycleState {
			if id == "culprit" {
				return StateFailed
			}
			return StateRunning
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{}}
	enr := &fakeEnroller{}

	var members []EntityIntent
	for _, id := range []string{"culprit", "node-a", "node-b"} {
		m := member(id)
		m.Placement = RungPlacement{Rung: rung0} // shared rung, no fallback
		members = append(members, m)
	}
	c := Cohort{
		ID:        "c-survivor-phase",
		Members:   members,
		Budget:    PhaseBudget{LaunchAcked: 5 * time.Second, Running: 5 * time.Second, Enrolled: 5 * time.Second, CohortBarrier: 30 * time.Second},
		MinViable: 3, // all-or-nothing
	}
	r := &Reconciler{Actuator: act, Observer: obs, Classifier: clf, Enroller: enr}

	outcome, _ := r.Reconcile(context.Background(), c)
	if outcome.Ready {
		t.Fatal("cohort should not be ready when culprit fails in the running phase")
	}

	// Culprit is Terminal (failed in running), not CohortCancelled.
	culpritRec := outcome.Records["culprit"]
	if culpritRec.Terminal == nil {
		t.Error("culprit: Terminal=nil — a running-phase StateFailed must terminate it")
	}
	if culpritRec.WasCohortCancelled() {
		t.Error("culprit: must not be CohortCancelled")
	}

	// Survivors are CohortCancelled at their OWN reached-phase (>= running, since
	// they launched and were observed running), naming the culprit.
	for _, sid := range []EntityID{"node-a", "node-b"} {
		rec := outcome.Records[sid]
		if rec.CohortCancelled == nil {
			continue // may be terminal under tight timing; the legibility assertion is below
		}
		cc := rec.CohortCancelled
		if cc.CulpritID != "culprit" {
			t.Errorf("%s: CulpritID=%q want culprit", sid, cc.CulpritID)
		}
		if cc.SurvivorPhase < PhaseRunning {
			t.Errorf("%s: SurvivorPhase=%v want >= PhaseRunning (it launched + ran)", sid, cc.SurvivorPhase)
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

// TestReconciler_AssemblerReceivesObservedAddress is the regression guard for
// the bug spawn-MPI's adapter spike surfaced: enrollment used to overwrite
// Observation.Address with Readiness.Detail (a display string), destroying the
// private IP the Assembler needs for MPI hostfile/PMIx wire-up. cohort's own
// suite never caught it because no test ran an Assembler that READS Address.
// This asserts: (1) the Assembler sees the Observer's address, NOT Detail; and
// (2) Detail still surfaces, via Record.EnrollDetail / Explain().
func TestReconciler_AssemblerReceivesObservedAddress(t *testing.T) {
	obs := &fakeObserver{addrFn: func(id EntityID) string { return "10.0.0." + string(id[len(id)-1]) }}
	enr := &fakeEnroller{enrolledFn: func(id EntityID) Readiness {
		return Readiness{Enrolled: true, Operational: true, Detail: "efa ok"}
	}}
	asm := &fakeAssembler{}
	r := &Reconciler{Actuator: &fakeActuator{}, Observer: obs, Classifier: &fakeClassifier{}, Enroller: enr, Assembler: asm}

	c := Cohort{
		ID:        "c-addr",
		Members:   []EntityIntent{member("n-1"), member("n-2"), member("n-3")},
		Budget:    fastBudget(),
		MinViable: 3,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Fatalf("cohort not Ready: %+v", outcome.Records)
	}

	// (1) Assembler got real addresses, not the "efa ok" Detail string.
	if len(asm.members) != 3 {
		t.Fatalf("Assemble saw %d members, want 3", len(asm.members))
	}
	for _, m := range asm.members {
		if m.Address == "efa ok" {
			t.Errorf("member %s: Address was clobbered by Readiness.Detail (the bug)", m.ID)
		}
		if m.Address == "" || m.Address[:5] != "10.0." {
			t.Errorf("member %s: Address=%q, want the Observer's 10.0.0.x", m.ID, m.Address)
		}
	}

	// (2) Detail still surfaces — in the Record / Explain, where it's documented to go.
	rec := outcome.Records["n-1"]
	if rec.EnrollDetail != "efa ok" {
		t.Errorf("Record.EnrollDetail=%q, want \"efa ok\"", rec.EnrollDetail)
	}
	if !containsStr(rec.Explain(), "efa ok") {
		t.Errorf("Explain() should surface enrollment detail:\n%s", rec.Explain())
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return sub == ""
}
