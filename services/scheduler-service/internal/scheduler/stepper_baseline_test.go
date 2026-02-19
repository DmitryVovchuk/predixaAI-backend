package scheduler

import (
	"context"
	"testing"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/security"
)

func TestStepperBaselineInsufficient(t *testing.T) {
	adapter := &mcp.MockAdapter{
		Tables: []string{"telemetry"},
		Columns: map[string][]mcp.Column{
			"telemetry": {{Name: "value", Type: "float"}, {Name: "ts", Type: "timestamp"}},
		},
		RecentRows: mcp.FetchRecentRowsResult{Rows: []mcp.Row{
			{"value": 1.0, "ts": time.Now().Add(-2 * time.Minute).Format(time.RFC3339)},
			{"value": 2.0, "ts": time.Now().Add(-1 * time.Minute).Format(time.RFC3339)},
		}},
	}
	allowlist := security.Allowlist{Tables: []string{"telemetry"}}
	limits := security.DefaultLimits()
	resp, err := StepperBaselineCheck(context.Background(), adapter, allowlist, limits, StepperBaselineRequest{
		ConnectionRef:   "conn",
		Table:           "telemetry",
		TimestampColumn: "ts",
		ValueColumn:     "value",
		RuleType:        "SHEWHART_3SIGMA",
		BaselineSelector: selectorSpec{Kind: "lastN", Value: 5},
	})
	if err != nil {
		t.Fatalf("baseline check failed: %v", err)
	}
	if resp.Status != statusInsufficient {
		t.Fatalf("expected insufficient status")
	}
}

func TestStepperBaselineOK(t *testing.T) {
	rows := []mcp.Row{}
	for i := 0; i < 25; i++ {
		rows = append(rows, mcp.Row{"value": float64(i), "ts": time.Now().Add(time.Duration(-i) * time.Minute).Format(time.RFC3339)})
	}
	adapter := &mcp.MockAdapter{
		Tables: []string{"telemetry"},
		Columns: map[string][]mcp.Column{
			"telemetry": {{Name: "value", Type: "float"}, {Name: "ts", Type: "timestamp"}},
		},
		RecentRows: mcp.FetchRecentRowsResult{Rows: rows},
	}
	allowlist := security.Allowlist{Tables: []string{"telemetry"}}
	limits := security.DefaultLimits()
	resp, err := StepperBaselineCheck(context.Background(), adapter, allowlist, limits, StepperBaselineRequest{
		ConnectionRef:   "conn",
		Table:           "telemetry",
		TimestampColumn: "ts",
		ValueColumn:     "value",
		RuleType:        "SHEWHART_3SIGMA",
		BaselineSelector: selectorSpec{Kind: "lastN", Value: 25},
	})
	if err != nil {
		t.Fatalf("baseline check failed: %v", err)
	}
	if resp.Status != statusOK {
		t.Fatalf("expected ok status")
	}
}
