package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/security"
)

func TestStepperPreviewContract(t *testing.T) {
	adapter := &mcp.MockAdapter{
		Tables: []string{"telemetry"},
		Columns: map[string][]mcp.Column{
			"telemetry": {{Name: "value", Type: "float"}, {Name: "ts", Type: "timestamp"}},
		},
		RecentRows: mcp.FetchRecentRowsResult{Rows: []mcp.Row{
			{"value": 10.0, "ts": time.Now().Format(time.RFC3339)},
			{"value": 3.0, "ts": time.Now().Add(-1 * time.Minute).Format(time.RFC3339)},
			{"value": 2.0, "ts": time.Now().Add(-2 * time.Minute).Format(time.RFC3339)},
			{"value": 1.0, "ts": time.Now().Add(-3 * time.Minute).Format(time.RFC3339)},
		}},
	}
	allowlist := security.Allowlist{Tables: []string{"telemetry"}}
	limits := security.DefaultLimits()
	config, _ := json.Marshal(map[string]any{"mode": "spec", "specLimits": map[string]any{"usl": 5.0}})
	resp, err := StepperPreview(context.Background(), adapter, allowlist, limits, StepperPreviewRequest{
		ConnectionRef:   "conn",
		Table:           "telemetry",
		TimestampColumn: "ts",
		ValueColumn:     "value",
		RuleType:        "SPEC_LIMIT_VIOLATION",
		Config:          config,
	})
	if err != nil {
		t.Fatalf("preview failed: %v", err)
	}
	if resp.Status == "" || resp.Computed == nil || resp.Window == nil {
		t.Fatalf("expected preview contract fields")
	}
	if len(resp.Violations) == 0 {
		t.Fatalf("expected violations")
	}
}

func TestStepperPreviewRangeChart(t *testing.T) {
	rows := []mcp.Row{}
	now := time.Now()
	for i := 0; i < 9; i++ {
		rows = append(rows, mcp.Row{"value": 1.0, "ts": now.Add(time.Duration(-20-i*2) * time.Minute).Format(time.RFC3339), "batch_id": fmt.Sprintf("B%d", i)})
		rows = append(rows, mcp.Row{"value": 2.0, "ts": now.Add(time.Duration(-19-i*2) * time.Minute).Format(time.RFC3339), "batch_id": fmt.Sprintf("B%d", i)})
	}
	rows = append(rows, mcp.Row{"value": 1.0, "ts": now.Add(-2 * time.Minute).Format(time.RFC3339), "batch_id": "B9"})
	rows = append(rows, mcp.Row{"value": 25.0, "ts": now.Add(-1 * time.Minute).Format(time.RFC3339), "batch_id": "B9"})
	adapter := &mcp.MockAdapter{
		Tables: []string{"telemetry"},
		Columns: map[string][]mcp.Column{
			"telemetry": {{Name: "value", Type: "float"}, {Name: "ts", Type: "timestamp"}, {Name: "batch_id", Type: "text"}},
		},
		RecentRows: mcp.FetchRecentRowsResult{Rows: rows},
	}
	allowlist := security.Allowlist{Tables: []string{"telemetry"}}
	limits := security.DefaultLimits()
	config, _ := json.Marshal(map[string]any{"subgroupSize": 2, "minBaselineSubgroups": 2})
	resp, err := StepperPreview(context.Background(), adapter, allowlist, limits, StepperPreviewRequest{
		ConnectionRef:   "conn",
		Table:           "telemetry",
		TimestampColumn: "ts",
		ValueColumn:     "value",
		RuleType:        "RANGE_CHART_R",
		Config:          config,
		Subgrouping:     &subgroupSpec{Kind: "column", Column: "batch_id", SubgroupSize: 2},
	})
	if err != nil {
		t.Fatalf("preview failed: %v", err)
	}
	if resp.Status == "" || resp.Computed == nil {
		t.Fatalf("expected preview response")
	}
	if len(resp.Violations) == 0 {
		t.Fatalf("expected violations")
	}
}
