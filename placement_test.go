package cohort

import (
	"context"
	"testing"
	"time"
)

// This file proves the #1 thesis IN COHORT'S OWN SUITE: the core reconciles a
// NON-Rung placement — a fake agent-transport ladder (goroutine → a2a-session →
// instance) with no instance type, no AZ, no capacity model — through the same
// unmodified Placement seam the AWS RungPlacement uses. If this compiles and
// reconciles, the EC2 vocabulary is genuinely out of the core's caller-facing
// contract.

// transportPlacement is a minimal non-AWS Placement: a named ladder of transport
// rungs. It never references a Rung, CapacityModel, instance type, or AZ.
type transportPlacement struct {
	chain []string // e.g. ["goroutine", "a2a-session", "instance"]
	idx   int
}

func (p transportPlacement) Current() PlacementRung {
	return PlacementRung{
		Name:  p.chain[p.idx],
		Class: "transport",
		// A transport has no warm-resume concept — always a cold start.
		WarmStart: false,
	}
}

func (p transportPlacement) Advance() (Placement, bool) {
	if p.idx+1 >= len(p.chain) {
		return nil, false
	}
	return transportPlacement{chain: p.chain, idx: p.idx + 1}, true
}

// transportFault is the non-capacity fault a transport provider would emit when
// a rung is unavailable (e.g. the goroutine pool is saturated). It maps to the
// SAME fallback-eligible class the ladder advances on — proving the *mechanism*
// is general even though "capacity exhausted" is AWS vocabulary.
type transportUnavailable struct{}

func (transportUnavailable) Error() string { return "transport rung unavailable" }

func TestReconciler_NonRungPlacement_GeneralizesTheSeam(t *testing.T) {
	// The first two transport rungs are "unavailable"; the third succeeds —
	// exercising the ladder advance through a placement the core knows nothing
	// about.
	unavail := transportUnavailable{}
	var tried []string
	act := &fakeActuator{
		launchFn: func(intent EntityIntent) (Observation, error) {
			name := intent.Placement.Current().Name
			tried = append(tried, name)
			if name == "instance" { // last rung succeeds
				return Observation{ID: intent.ID, State: StateLaunching,
					ProviderID: "transport-" + name, ObservedAt: time.Now()}, nil
			}
			return Observation{}, unavail
		},
	}
	clf := &fakeClassifier{faults: map[string]Fault{
		unavail.Error(): {Class: FaultCapacityExhausted, Code: "TransportRungUnavailable"},
	}}
	r := &Reconciler{Actuator: act, Observer: &fakeObserver{}, Classifier: clf, Enroller: &fakeEnroller{}}

	intent := EntityIntent{
		ID:               "agent-node-1",
		Generation:       "g1",
		Cohort:           "c-telos",
		IdempotencyToken: "tok-agent-1",
		Placement:        transportPlacement{chain: []string{"goroutine", "a2a-session", "instance"}},
	}

	outcome, err := r.Reconcile(context.Background(), Cohort{
		ID:      "c-telos",
		Members: []EntityIntent{intent},
		Budget:  fastBudget(),
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !outcome.Ready {
		t.Fatalf("non-Rung placement: Ready=false, summary=%s", outcome.Records["agent-node-1"].Summary())
	}

	// The ladder advanced through all three transport rungs, in order — chain
	// discipline holds for a provider the core has never heard of.
	want := []string{"goroutine", "a2a-session", "instance"}
	if len(tried) != len(want) {
		t.Fatalf("tried %v, want %v", tried, want)
	}
	for i := range want {
		if tried[i] != want[i] {
			t.Errorf("rung[%d]=%q want %q", i, tried[i], want[i])
		}
	}

	// Explain() renders the transport rung names — legibility holds with no
	// EC2 vocabulary anywhere.
	rec := outcome.Records["agent-node-1"]
	explain := rec.Explain()
	if !contains(explain, "goroutine") || !contains(explain, "instance") {
		t.Errorf("Explain() lost the transport rung names:\n%s", explain)
	}
}

// TestNewEntityIntent_AcceptsNonRungPlacement confirms the constructor validates
// a non-Rung placement via the generic Name floor (no Validate() hook needed).
func TestNewEntityIntent_AcceptsNonRungPlacement(t *testing.T) {
	p := transportPlacement{chain: []string{"goroutine"}}
	intent, err := NewEntityIntent("telos", "agent-1", "g1", "c1", p, "")
	if err != nil {
		t.Fatalf("non-Rung placement rejected: %v", err)
	}
	if intent.Placement.Current().Name != "goroutine" {
		t.Errorf("placement not preserved: %q", intent.Placement.Current().Name)
	}
}

// contains is a tiny strings.Contains shim to avoid an import here.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}
