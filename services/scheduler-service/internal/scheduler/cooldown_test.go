package scheduler

import (
	"testing"
	"time"

	"predixaai-backend/services/scheduler-service/internal/monitor"
)

func TestWithinCooldown(t *testing.T) {
	last := time.Now().Add(-5 * time.Second)
	if !monitor.WithinCooldown(last, 10) {
		t.Fatalf("expected within cooldown")
	}
}
