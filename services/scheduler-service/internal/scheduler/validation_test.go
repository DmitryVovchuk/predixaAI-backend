package scheduler

import "testing"

func TestEvaluateCondition(t *testing.T) {
	hit, observed, expr := EvaluateCondition(ConditionSpec{Op: ">", Value: 5}, 8)
	if !hit {
		t.Fatalf("expected condition to hit")
	}
	if observed == "" || expr == "" {
		t.Fatalf("expected observed and expr values")
	}
}
