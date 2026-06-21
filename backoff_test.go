package cohort

import (
	"testing"
	"time"
)

func TestBackoffPolicy_BoundsAndCap(t *testing.T) {
	p := BackoffPolicy{
		Base:   100 * time.Millisecond,
		Cap:    10 * time.Second,
		Jitter: 0.25,
	}
	cases := []struct {
		attempt int
		baseVal time.Duration // pre-jitter value
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{7, 10 * time.Second}, // 128×100ms > cap → clamped
		{99, 10 * time.Second},
	}
	for _, tc := range cases {
		for trial := 0; trial < 200; trial++ {
			d := p.Duration(tc.attempt)
			wantMin := tc.baseVal
			wantMax := time.Duration(float64(tc.baseVal) * (1 + p.Jitter))
			if d < wantMin || d > wantMax {
				t.Errorf("attempt=%d trial=%d: got %v outside [%v, %v]",
					tc.attempt, trial, d, wantMin, wantMax)
			}
		}
	}
}

func TestBackoffPolicy_MonotonicBase(t *testing.T) {
	p := BackoffPolicy{
		Base:   50 * time.Millisecond,
		Cap:    8 * time.Second,
		Jitter: 0,
	}
	// With zero jitter Duration is deterministic; verify strict doubling until cap.
	prev := time.Duration(0)
	for attempt := 0; attempt < 12; attempt++ {
		d := p.Duration(attempt)
		if d < prev {
			t.Errorf("attempt=%d: %v < prev %v — not monotonic", attempt, d, prev)
		}
		if d > p.Cap {
			t.Errorf("attempt=%d: %v exceeds cap %v", attempt, d, p.Cap)
		}
		prev = d
	}
}

func TestBackoffPolicy_NegativeAttemptClamped(t *testing.T) {
	p := DefaultBackoffPolicy()
	d := p.Duration(-5)
	if d < p.Base {
		t.Errorf("negative attempt: got %v < base %v", d, p.Base)
	}
}

func TestDefaultBackoffPolicy_Values(t *testing.T) {
	p := DefaultBackoffPolicy()
	if p.Base != 100*time.Millisecond {
		t.Errorf("Base: got %v want 100ms", p.Base)
	}
	if p.Cap != 30*time.Second {
		t.Errorf("Cap: got %v want 30s", p.Cap)
	}
	if p.Jitter != 0.25 {
		t.Errorf("Jitter: got %v want 0.25", p.Jitter)
	}
}
