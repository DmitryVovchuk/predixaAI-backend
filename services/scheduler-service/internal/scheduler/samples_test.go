package scheduler

import (
	"testing"
	"time"
)

func TestHasConsecutiveTimestampsEqual(t *testing.T) {
	now := time.Now().UTC()
	samples := []Sample{{TS: now, Value: 1}, {TS: now, Value: 2}}
	if hasConsecutiveTimestamps(samples) {
		t.Fatalf("expected false for equal timestamps")
	}
}

func TestHasConsecutiveTimestampsGap(t *testing.T) {
	now := time.Now().UTC()
	samples := []Sample{
		{TS: now, Value: 1},
		{TS: now.Add(10 * time.Second), Value: 2},
		{TS: now.Add(20 * time.Second), Value: 3},
		{TS: now.Add(80 * time.Second), Value: 4},
	}
	if hasConsecutiveTimestamps(samples) {
		t.Fatalf("expected false for large timestamp gap")
	}
}

func TestHasConsecutiveTimestampsOK(t *testing.T) {
	now := time.Now().UTC()
	samples := []Sample{
		{TS: now, Value: 1},
		{TS: now.Add(10 * time.Second), Value: 2},
		{TS: now.Add(20 * time.Second), Value: 3},
	}
	if !hasConsecutiveTimestamps(samples) {
		t.Fatalf("expected true for consistent timestamps")
	}
}
