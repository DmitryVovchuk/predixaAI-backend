package rules

import "testing"

func TestValidateRuleSpec(t *testing.T) {
	window := 60
	spec := RuleSpec{
		ConnectionRef: "conn-1",
		Source: SourceSpec{Table: "telemetry", ValueColumn: "temp", TimestampColumn: "ts"},
		ParameterName: "temp",
		Aggregation:   "avg",
		WindowSeconds: &window,
		Condition:     ConditionSpec{Op: ">", Value: 80},
		PollIntervalSeconds: 30,
		Enabled:             true,
	}
	if err := ValidateRuleSpec(spec, 5, 3600); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRuleSpecInvalidBetween(t *testing.T) {
	min := 10.0
	max := 5.0
	spec := RuleSpec{
		Source: SourceSpec{Table: "telemetry", ValueColumn: "temp", TimestampColumn: "ts"},
		Condition: ConditionSpec{Op: "between", Min: &min, Max: &max},
		PollIntervalSeconds: 10,
		Aggregation:         "latest",
	}
	if err := ValidateRuleSpec(spec, 5, 3600); err == nil {
		t.Fatalf("expected validation error")
	}
}
