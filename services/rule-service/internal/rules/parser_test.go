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
}
