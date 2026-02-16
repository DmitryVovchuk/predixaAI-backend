package rules

import "testing"

func TestParsePromptAmbiguous(t *testing.T) {
	_, err := ParsePrompt("check temperature above 80", "conn-1")
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if err.Code != "RULE_AMBIGUOUS" {
		t.Fatalf("unexpected code: %s", err.Code)
	}
}

func TestParsePromptSuccess(t *testing.T) {
	spec, err := ParsePrompt("table telemetry column temp timestamp ts above 80 every 10s", "conn-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Source.Table != "telemetry" {
		t.Fatalf("unexpected table")
	}
	if spec.PollIntervalSeconds != 10 {
		t.Fatalf("unexpected poll interval")
	}
	if len(spec.Parameters) != 1 {
		t.Fatalf("expected parameter")
	}
	if spec.Parameters[0].Detector.Type != "threshold" {
		t.Fatalf("unexpected detector")
	}
}

func TestParsePromptRangeFrom(t *testing.T) {
	spec, err := ParsePrompt("table telemetry column temp timestamp ts in range from 20 up to 40", "conn-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(spec.Parameters) != 1 {
		t.Fatalf("expected parameter")
	}
	if spec.Parameters[0].Detector.Type != "threshold" {
		t.Fatalf("unexpected detector")
	}
	if spec.Parameters[0].Detector.Threshold == nil || spec.Parameters[0].Detector.Threshold.Min == nil || spec.Parameters[0].Detector.Threshold.Max == nil {
		t.Fatalf("expected range bounds")
	}
}

func TestParsePromptBetweenTo(t *testing.T) {
	spec, err := ParsePrompt("table telemetry column temp timestamp ts between 20 to 40", "conn-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Parameters[0].Detector.Type != "threshold" {
		t.Fatalf("unexpected detector")
	}
	if spec.Parameters[0].Detector.Threshold == nil || spec.Parameters[0].Detector.Threshold.Min == nil || spec.Parameters[0].Detector.Threshold.Max == nil {
		t.Fatalf("expected range bounds")
	}
}

func TestParsePromptAbnormal(t *testing.T) {
	spec, err := ParsePrompt("table telemetry column temp timestamp ts abnormal", "conn-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Parameters[0].Detector.Type != "robust_zscore" {
		t.Fatalf("unexpected detector type: %s", spec.Parameters[0].Detector.Type)
	}
}

func TestParsePromptMissingData(t *testing.T) {
	spec, err := ParsePrompt("table telemetry column temp timestamp ts missing", "conn-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Parameters[0].Detector.Type != "missing_data" {
		t.Fatalf("unexpected detector type: %s", spec.Parameters[0].Detector.Type)
	}
}

func TestParsePromptWithDraft(t *testing.T) {
	draft := &RuleDraft{
		Table:           "telemetry",
		TimestampColumn: "ts",
		Parameters: []ParameterSpec{{
			ParameterName: "temp",
			ValueColumn:   "temp",
		}},
	}
	spec, err := ParsePromptWithDraft("above 80", "conn-1", draft)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Source.Table != "telemetry" {
		t.Fatalf("unexpected table")
	}
}
