package scheduler

import "testing"

func TestEvaluateConditionBetween(t *testing.T) {
	min := 10.0
	max := 20.0
	hit, _, _ := EvaluateCondition(ConditionSpec{Op: "between", Min: &min, Max: &max}, 15)
	if !hit {
		t.Fatalf("expected hit")
	}
}

func TestEvaluateConditionGreater(t *testing.T) {
	hit, _, _ := EvaluateCondition(ConditionSpec{Op: ">", Value: 80}, 90)
	if !hit {
		t.Fatalf("expected hit")
	}
}
