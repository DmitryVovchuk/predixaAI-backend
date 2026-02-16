package rules

import "testing"

func TestValidateRuleSpec(t *testing.T) {
	spec := RuleSpec{
		ConnectionRef: "conn-1",
		Source:        SourceSpec{Table: "telemetry", TimestampColumn: "ts"},
		Parameters: []ParameterSpec{{
			ParameterName: "temp",
			ValueColumn:   "temp",
			Detector: DetectorSpec{
				Type:      "threshold",
				Threshold: &ThresholdSpec{Op: ">", Value: 80},
			},
		}},
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
		Source: SourceSpec{Table: "telemetry", TimestampColumn: "ts"},
		Parameters: []ParameterSpec{{
			ParameterName: "temp",
			ValueColumn:   "temp",
			Detector: DetectorSpec{
				Type:      "threshold",
				Threshold: &ThresholdSpec{Op: "between", Min: &min, Max: &max},
			},
		}},
		PollIntervalSeconds: 10,
	}
	if err := ValidateRuleSpec(spec, 5, 3600); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestValidateRuleSpecRobustZInvalid(t *testing.T) {
	spec := RuleSpec{
		Source: SourceSpec{Table: "telemetry", TimestampColumn: "ts"},
		Parameters: []ParameterSpec{{
			ParameterName: "temp",
			ValueColumn:   "temp",
			Detector: DetectorSpec{
				Type: "robust_zscore",
				RobustZ: &RobustZSpec{
					BaselineWindowSeconds: 60,
					EvalWindowSeconds:     120,
					ZWarn:                 3,
					ZCrit:                 5,
					MinSamples:            10,
				},
			},
		}},
		PollIntervalSeconds: 10,
	}
	if err := ValidateRuleSpec(spec, 5, 3600); err == nil {
		t.Fatalf("expected validation error")
	}
}
