package monitor

import (
	"testing"
	"time"
)

func TestWithinCooldown(t *testing.T) {
	last := time.Now().Add(-5 * time.Second)
	if !WithinCooldown(last, 10) {
		t.Fatalf("expected within cooldown")
	}
}
