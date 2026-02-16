package validation

import (
	"context"
	"testing"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/scheduler"
	"predixaai-backend/services/scheduler-service/internal/security"
)

func TestRuntimeValidateRule(t *testing.T) {
	adapter := &mcp.MockAdapter{
		Tables: []string{"metrics"},
		Columns: map[string][]mcp.Column{
			"metrics": []mcp.Column{{Name: "value", Type: "float"}, {Name: "ts", Type: "time"}},
		},
	}
	spec := scheduler.RuleSpec{
		ConnectionRef: "conn-1",
		Source:        scheduler.SourceSpec{Table: "metrics", TimestampColumn: "ts"},
		Parameters: []scheduler.ParameterSpec{{
			ParameterName: "value",
			ValueColumn:   "value",
			Detector: scheduler.DetectorSpec{
				Type:      "threshold",
				Threshold: &scheduler.ThresholdSpec{Op: ">", Value: 5},
			},
		}},
		PollIntervalSeconds: 10,
	}
	allowlist := security.Allowlist{Tables: []string{"metrics"}}
	limits := security.DefaultLimits()
	if err := RuntimeValidateRule(context.Background(), adapter, spec, allowlist, limits); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
