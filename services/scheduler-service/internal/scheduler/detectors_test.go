package scheduler

import (
	"math"
	"testing"
	"time"
)

func TestMedianAndMAD(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	median := Median(values)
	if median != 3 {
		t.Fatalf("expected median 3 got %v", median)
	}
	if mad := MAD(values, median); mad != 1 {
		t.Fatalf("expected mad 1 got %v", mad)
	}
}

func TestEvaluateRobustZ(t *testing.T) {
	samples := []float64{10, 11, 10, 12, 11, 10, 11, 12, 11, 10, 11, 12, 11, 10, 11, 12, 11, 10, 11, 12}
	result := EvaluateRobustZ(samples, 20, 3, 5)
	if !result.Hit {
		t.Fatalf("expected anomaly")
	}
	if result.Severity == "" {
		t.Fatalf("expected severity")
	}
	if result.AnomalyScore == nil || math.Abs(*result.AnomalyScore) < 1 {
		t.Fatalf("expected anomaly score")
	}
}

func TestEvaluateMissingData(t *testing.T) {
	now := time.Now().UTC()
	latest := now.Add(-10 * time.Second)
	result := EvaluateMissingData(latest, 5, now)
	if !result.Hit {
		t.Fatalf("expected missing data alert")
	}
}
