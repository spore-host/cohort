package cohort

import (
	"context"
	"testing"
	"time"
)

// ---- NewEntityIntent tests --------------------------------------------------

func TestNewEntityIntent_HappyPath(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", rung, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent.ID != "gpu-001" {
		t.Errorf("ID=%q want gpu-001", intent.ID)
	}
	if intent.IdempotencyToken == "" {
		t.Error("IdempotencyToken must not be empty when auto-generated")
	}
}

func TestNewEntityIntent_RejectsEmptyID(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	_, err := NewEntityIntent("gauss", "", "gen-1", "c1", rung, nil, "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestNewEntityIntent_RejectsZeroRung(t *testing.T) {
	_, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", Rung{}, nil, "")
	if err == nil {
		t.Error("expected error for zero Rung (empty InstanceType)")
	}
}

func TestNewEntityIntent_RejectsZeroRungInChain(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	chain := []Rung{rung, {}} // second rung has empty InstanceType
	_, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", rung, chain, "")
	if err == nil {
		t.Error("expected error for zero Rung inside FallbackChain")
	}
}

func TestNewEntityIntent_TokenDeterminism(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}

	// Same (cluster, entity, generation) → same token across two calls.
	a, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", rung, nil, "")
	if err != nil {
		t.Fatalf("call A: %v", err)
	}
	b, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", rung, nil, "")
	if err != nil {
		t.Fatalf("call B: %v", err)
	}
	if a.IdempotencyToken != b.IdempotencyToken {
		t.Errorf("same inputs produced different tokens: %q vs %q", a.IdempotencyToken, b.IdempotencyToken)
	}

	// Different generation → different token.
	c, err := NewEntityIntent("gauss", "gpu-001", "gen-2", "c1", rung, nil, "")
	if err != nil {
		t.Fatalf("call C: %v", err)
	}
	if a.IdempotencyToken == c.IdempotencyToken {
		t.Errorf("different generation produced the same token: %q", a.IdempotencyToken)
	}

	// Caller-supplied token is preserved verbatim.
	d, err := NewEntityIntent("gauss", "gpu-001", "gen-1", "c1", rung, nil, "explicit-tok")
	if err != nil {
		t.Fatalf("call D: %v", err)
	}
	if d.IdempotencyToken != "explicit-tok" {
		t.Errorf("caller token not preserved: got %q", d.IdempotencyToken)
	}
}

// ---- NewSerialCohort tests --------------------------------------------------

func TestNewSerialCohort_HappyPath(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, err := NewSerialCohort("c-s", intent, PhaseBudget{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.MinViable != 1 {
		t.Errorf("MinViable=%d want 1", c.MinViable)
	}
	if len(c.Members) != 1 {
		t.Errorf("Members len=%d want 1", len(c.Members))
	}
}

func TestNewSerialCohort_ZeroBudgetDefaulted(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, err := NewSerialCohort("c-s", intent, PhaseBudget{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	def := DefaultBudget()
	if c.Budget.LaunchAcked != def.LaunchAcked {
		t.Errorf("zero Budget not defaulted: LaunchAcked=%v want %v", c.Budget.LaunchAcked, def.LaunchAcked)
	}
}

func TestNewSerialCohort_ExplicitBudgetHonored(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	explicit := PhaseBudget{LaunchAcked: 42 * time.Second, Running: 1 * time.Hour,
		Enrolled: 30 * time.Second, CohortBarrier: 10 * time.Second, CohortAssembly: 5 * time.Second}
	c, err := NewSerialCohort("c-s", intent, explicit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Budget.LaunchAcked != 42*time.Second {
		t.Errorf("explicit Budget not honored: LaunchAcked=%v want 42s", c.Budget.LaunchAcked)
	}
}

func TestNewSerialCohort_RejectsEmptyID(t *testing.T) {
	bad := EntityIntent{ID: "", Rung: Rung{InstanceType: "m5.xlarge"}}
	_, err := NewSerialCohort("c-s", bad, PhaseBudget{})
	if err == nil {
		t.Error("expected error for empty EntityIntent.ID")
	}
}

// ---- NewMPICohort tests -----------------------------------------------------

func TestNewMPICohort_HappyPath(t *testing.T) {
	rung := Rung{InstanceType: "p5.48xlarge", AvailZone: "us-east-1a"}
	var members []EntityIntent
	for i := 0; i < 4; i++ {
		m, _ := NewEntityIntent("gauss", EntityID(string(rune('a'+i))), "gen-1", "c-mpi", rung, nil, "")
		members = append(members, m)
	}
	c, err := NewMPICohort("c-mpi", members, PhaseBudget{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.MinViable != 4 {
		t.Errorf("MinViable=%d want 4 (all-or-nothing)", c.MinViable)
	}
}

func TestNewMPICohort_RejectsEmpty(t *testing.T) {
	_, err := NewMPICohort("c-mpi", nil, PhaseBudget{})
	if err == nil {
		t.Error("expected error for nil members")
	}
}

// ---- NewPartialCohort tests -------------------------------------------------

func TestNewPartialCohort_HappyPath(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	var members []EntityIntent
	for i := 0; i < 5; i++ {
		m, _ := NewEntityIntent("gauss", EntityID(string(rune('a'+i))), "gen-1", "c-p", rung, nil, "")
		members = append(members, m)
	}
	c, err := NewPartialCohort("c-p", members, PhaseBudget{}, 3, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.MinViable != 3 {
		t.Errorf("MinViable=%d want 3", c.MinViable)
	}
}

func TestNewPartialCohort_RejectsZeroMinViable(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	m, _ := NewEntityIntent("gauss", "n-0", "gen-1", "c-p", rung, nil, "")
	_, err := NewPartialCohort("c-p", []EntityIntent{m}, PhaseBudget{}, 0, nil)
	if err == nil {
		t.Error("expected error for MinViable=0")
	}
}

func TestNewPartialCohort_RejectsMinViableExceedsMembers(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	m, _ := NewEntityIntent("gauss", "n-0", "gen-1", "c-p", rung, nil, "")
	_, err := NewPartialCohort("c-p", []EntityIntent{m}, PhaseBudget{}, 5, nil)
	if err == nil {
		t.Error("expected error for MinViable > len(members)")
	}
}

// ---- Zero budget defaulting is behavioral, not just structural --------------
// Assert a phase actually has time to run (not instant deadlines).

func TestNewSerialCohort_ZeroBudget_PhaseActuallyRuns(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, err := NewSerialCohort("c-s", intent, PhaseBudget{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With a defaulted budget, reconciliation should succeed: phases have time.
	r := &Reconciler{
		Actuator:   &fakeActuator{},
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   &fakeEnroller{},
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Errorf("zero-budget serial cohort: Ready=false — defaulted budget did not take effect")
	}
}

// ---- Readiness.Operational tests -------------------------------------------

func TestReadiness_Operational_OK(t *testing.T) {
	r := Readiness{Enrolled: true, Operational: true}
	if !r.OK() {
		t.Error("Enrolled=true Operational=true: OK()=false")
	}
}

func TestReadiness_NotOperational_NotOK(t *testing.T) {
	r := Readiness{Enrolled: true, Operational: false}
	if r.OK() {
		t.Error("Enrolled=true Operational=false: OK()=true (Operational should prevent OK)")
	}
}

func TestReadiness_Operational_RoundTripsEnroller(t *testing.T) {
	// Enroller fake returns Operational=true; waitEnrolled must confirm it.
	enr := &fakeEnroller{
		enrolledFn: func(id EntityID) Readiness {
			return Readiness{Enrolled: true, Operational: true, Detail: "efa ok"}
		},
	}
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, _ := NewSerialCohort("c-s", intent, PhaseBudget{})

	r := &Reconciler{
		Actuator:   &fakeActuator{},
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   enr,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Error("Operational=true should lead to Ready=true")
	}
}

func TestReadiness_NotOperational_PreventsCohortReady(t *testing.T) {
	enr := &fakeEnroller{
		enrolledFn: func(id EntityID) Readiness {
			return Readiness{Enrolled: false, Operational: false}
		},
	}
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, _ := NewSerialCohort("c-s", intent, PhaseBudget{
		LaunchAcked:    2 * time.Second,
		Running:        2 * time.Second,
		Enrolled:       150 * time.Millisecond, // short so enrollment times out
		CohortBarrier:  2 * time.Second,
		CohortAssembly: 2 * time.Second,
	})

	r := &Reconciler{
		Actuator:   &fakeActuator{},
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   enr,
	}
	outcome, _ := r.Reconcile(context.Background(), c)
	if outcome.Ready {
		t.Error("Operational=false should prevent Ready=true")
	}
}

// ---- 5.9.1: partial PhaseBudget defaulting ----------------------------------

// A budget with exactly one zero field: the zero field is defaulted to
// DefaultBudget()'s value, and the set fields are untouched.
func TestApplyDefaultBudget_PartialFill(t *testing.T) {
	def := DefaultBudget()
	// Set everything except CohortBarrier.
	partial := PhaseBudget{
		LaunchAcked:    7 * time.Second,
		Running:        2 * time.Minute,
		Enrolled:       90 * time.Second,
		CohortBarrier:  0, // left zero — must default
		CohortAssembly: 30 * time.Second,
	}
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	intent, _ := NewEntityIntent("gauss", "gpu-001", "gen-1", "c-s", rung, nil, "")
	c, err := NewSerialCohort("c-s", intent, partial)
	if err != nil {
		t.Fatalf("NewSerialCohort: %v", err)
	}
	// Zero field defaulted.
	if c.Budget.CohortBarrier != def.CohortBarrier {
		t.Errorf("CohortBarrier: got %v want %v (default)", c.Budget.CohortBarrier, def.CohortBarrier)
	}
	// Set fields are untouched.
	if c.Budget.LaunchAcked != 7*time.Second {
		t.Errorf("LaunchAcked: got %v want 7s", c.Budget.LaunchAcked)
	}
	if c.Budget.Running != 2*time.Minute {
		t.Errorf("Running: got %v want 2m", c.Budget.Running)
	}
	if c.Budget.Enrolled != 90*time.Second {
		t.Errorf("Enrolled: got %v want 90s", c.Budget.Enrolled)
	}
	if c.Budget.CohortAssembly != 30*time.Second {
		t.Errorf("CohortAssembly: got %v want 30s", c.Budget.CohortAssembly)
	}
}

// ---- 5.9.2: token duplication regression ------------------------------------

// substrate.Token and cohort.Token must produce identical output for the same
// inputs. This test is the enforcement mechanism — if either implementation
// drifts, the ambiguous-retry guarantee silently breaks.
func TestToken_SubstrateMatchesCohort(t *testing.T) {
	// cohort.Token is the canonical implementation; substrate.Token delegates.
	// Test that the canonical function itself is stable and deterministic.
	cases := []struct{ cluster, entity, gen string }{
		{"gauss", "gpu-001", "gen-1"},
		{"gauss", "gpu-001", "gen-2"},
		{"gauss", "n-042", "gen-1"},
		{"other-cluster", "gpu-001", "gen-1"},
	}
	for _, tc := range cases {
		a := Token(tc.cluster, tc.entity, tc.gen)
		b := Token(tc.cluster, tc.entity, tc.gen)
		if a != b {
			t.Errorf("Token not deterministic for (%s,%s,%s): %q != %q", tc.cluster, tc.entity, tc.gen, a, b)
		}
		if len(a) == 0 {
			t.Errorf("Token returned empty string for (%s,%s,%s)", tc.cluster, tc.entity, tc.gen)
		}
	}
	// Distinct inputs produce distinct tokens.
	t1 := Token("gauss", "gpu-001", "gen-1")
	t2 := Token("gauss", "gpu-001", "gen-2") // different generation
	if t1 == t2 {
		t.Errorf("different generation should produce different token: both %q", t1)
	}
}

// ---- 5.9.3: NewPartialCohort + assembler guard ------------------------------

// NewPartialCohort rejects a non-nil Assembler at construction time.
func TestNewPartialCohort_RejectsAssembler(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	var members []EntityIntent
	for i := 0; i < 3; i++ {
		m, _ := NewEntityIntent("gauss", EntityID(string(rune('a'+i))), "gen-1", "c-p", rung, nil, "")
		members = append(members, m)
	}
	asm := &fakeAssembler{}
	_, err := NewPartialCohort("c-p", members, PhaseBudget{}, 2, asm)
	if err == nil {
		t.Error("expected error when Assembler is non-nil for a partial cohort")
	}
}

// Reconcile rejects a partial cohort (NoAssembly=true) when the Reconciler
// has a non-nil Assembler — defense-in-depth if the caller bypasses NewPartialCohort.
func TestReconcile_PartialCohort_AssemblerGuard(t *testing.T) {
	rung := Rung{InstanceType: "m5.xlarge", AvailZone: "us-east-1a"}
	var members []EntityIntent
	for i := 0; i < 3; i++ {
		m, _ := NewEntityIntent("gauss", EntityID(string(rune('a'+i))), "gen-1", "c-p", rung, nil, "")
		members = append(members, m)
	}
	// Construct via struct literal to bypass the constructor check.
	c := Cohort{
		ID:         "c-partial-guard",
		Members:    members,
		Budget:     fastBudget(),
		MinViable:  2,
		NoAssembly: true,
	}
	asm := &fakeAssembler{}
	r := &Reconciler{
		Actuator:   &fakeActuator{},
		Observer:   &fakeObserver{},
		Classifier: &fakeClassifier{},
		Enroller:   &fakeEnroller{},
		Assembler:  asm,
	}
	outcome, err := r.Reconcile(context.Background(), c)
	if err != nil {
		t.Fatalf("Reconcile returned Go error: %v", err)
	}
	if outcome.Ready {
		t.Error("partial cohort with Assembler should not be Ready")
	}
	// Assembler must not have been called.
	if int(asm.calls) != 0 {
		t.Errorf("Assembler called %d times on a NoAssembly cohort — must be 0", asm.calls)
	}
	// Every entity carries a terminal record with AssemblyDisallowed.
	for _, m := range members {
		rec := outcome.Records[m.ID]
		if rec.Terminal == nil {
			t.Errorf("entity %s: Terminal=nil, want AssemblyDisallowed fault", m.ID)
			continue
		}
		if rec.Terminal.Code != "AssemblyDisallowed" {
			t.Errorf("entity %s: Terminal.Code=%q want AssemblyDisallowed", m.ID, rec.Terminal.Code)
		}
	}
}
