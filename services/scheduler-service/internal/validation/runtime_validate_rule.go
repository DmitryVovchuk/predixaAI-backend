package validation

import (
	"context"
	"errors"
	"strings"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/scheduler"
	"predixaai-backend/services/scheduler-service/internal/security"
)

func RuntimeValidateRule(ctx context.Context, adapter mcp.DbMcpAdapter, spec scheduler.RuleSpec, allowlist security.Allowlist, limits security.Limits) error {
	if spec.PollIntervalSeconds < limits.MinPollSeconds || spec.PollIntervalSeconds > limits.MaxPollSeconds {
		return errors.New("poll interval out of bounds")
	}
	if spec.Aggregation != "" && spec.Aggregation != "latest" {
		if spec.WindowSeconds == nil || *spec.WindowSeconds <= 0 {
			return errors.New("windowSeconds required")
		}
		if *spec.WindowSeconds > limits.MaxWindowSeconds {
			return errors.New("windowSeconds exceeds limit")
		}
	}
	if !security.IsSafeIdentifier(spec.Source.Table) || !security.IsSafeIdentifier(spec.Source.TimestampColumn) {
		return errors.New("unsafe identifier")
	}
	params := normalizeParameters(spec)
	if len(params) == 0 {
		return errors.New("parameters required")
	}
	for _, param := range params {
		if !security.IsSafeIdentifier(param.ValueColumn) {
			return errors.New("unsafe value column")
		}
	}
	if spec.Source.Where != nil {
		for _, clause := range spec.Source.Where.Clauses {
			if !security.IsSafeIdentifier(clause.Column) {
				return errors.New("unsafe where identifier")
			}
		}
	}
	if !allowlist.AllowsTable(spec.Source.Table) {
		return errors.New("table not allowlisted")
	}
	if !adapter.Capabilities().SupportsIntrospection {
		return errors.New("adapter does not support introspection")
	}
	listCtx, cancel := context.WithTimeout(ctx, limits.MaxQueryDuration)
	defer cancel()
	tables, err := adapter.ListTables(listCtx, spec.ConnectionRef)
	if err != nil {
		return err
	}
	found := false
	for _, t := range tables {
		if t == spec.Source.Table {
			found = true
			break
		}
	}
	if !found {
		return errors.New("table not found")
	}
	colsCtx, cancelCols := context.WithTimeout(ctx, limits.MaxQueryDuration)
	defer cancelCols()
	cols, err := adapter.ListColumns(colsCtx, spec.ConnectionRef, spec.Source.Table)
	if err != nil {
		return err
	}
	colSet := map[string]struct{}{}
	colTypes := map[string]string{}
	for _, c := range cols {
		colSet[c.Name] = struct{}{}
		colTypes[c.Name] = c.Type
	}
	if _, ok := colSet[spec.Source.TimestampColumn]; !ok {
		return errors.New("timestamp column not found")
	}
	for _, param := range params {
		if _, ok := colSet[param.ValueColumn]; !ok {
			return errors.New("value column not found")
		}
		if param.Detector.Type == "robust_zscore" {
			if !isNumericType(colTypes[param.ValueColumn]) {
				return errors.New("non-numeric column for robust_zscore")
			}
			queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
			_, err = adapter.FetchRecentRows(queryCtx, mcp.FetchRecentRowsRequest{
				ConnectionRef:   spec.ConnectionRef,
				Table:           spec.Source.Table,
				Columns:         []string{param.ValueColumn, spec.Source.TimestampColumn},
				TimestampColumn: spec.Source.TimestampColumn,
				Where:           toWhere(spec.Source.Where),
				Since:           time.Now().Add(-time.Duration(param.Detector.RobustZ.BaselineWindowSeconds) * time.Second).Format(time.RFC3339),
				Limit:           limits.MaxSampleRows,
			})
			cancelQuery()
			if err != nil {
				return err
			}
			continue
		}
		if param.Detector.Type == "range_chart" {
			if param.Detector.RangeChart == nil {
				return errors.New("range_chart config missing")
			}
			if !isSupportedRangeChartSize(param.Detector.RangeChart.SubgroupSize) {
				return errors.New("unsupported subgroup size")
			}
			if !isNumericType(colTypes[param.ValueColumn]) {
				return errors.New("non-numeric column for range_chart")
			}
			mode := param.Detector.RangeChart.Subgrouping.Mode
			if mode == "column" {
				if param.Detector.RangeChart.Subgrouping.Column == "" {
					return errors.New("subgrouping column required")
				}
				if _, ok := colSet[param.Detector.RangeChart.Subgrouping.Column]; !ok {
					return errors.New("subgrouping column not found")
				}
			}
			queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
			_, err = adapter.FetchRecentRows(queryCtx, mcp.FetchRecentRowsRequest{
				ConnectionRef:   spec.ConnectionRef,
				Table:           spec.Source.Table,
				Columns:         []string{param.ValueColumn, spec.Source.TimestampColumn},
				TimestampColumn: spec.Source.TimestampColumn,
				Where:           toWhere(spec.Source.Where),
				Since:           time.Now().Add(-time.Duration(limits.MaxWindowSeconds) * time.Second).Format(time.RFC3339),
				Limit:           limits.MaxSampleRows,
			})
			cancelQuery()
			if err != nil {
				return err
			}
			continue
		}
		if param.Detector.Type == "shewhart" || param.Detector.Type == "trend" || param.Detector.Type == "tpa" || param.Detector.Type == "spec_limit" {
			if !isNumericType(colTypes[param.ValueColumn]) {
				return errors.New("non-numeric column for detector")
			}
		}
		if param.Detector.Type == "threshold" && spec.Aggregation != "" && spec.Aggregation != "latest" && spec.WindowSeconds != nil {
			queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
			_, err = adapter.QueryAggregate(queryCtx, mcp.AggregateRequest{
				ConnectionRef:   spec.ConnectionRef,
				Table:           spec.Source.Table,
				ValueColumn:     param.ValueColumn,
				TimestampColumn: spec.Source.TimestampColumn,
				Where:           toWhere(spec.Source.Where),
				Agg:             spec.Aggregation,
				WindowSeconds:   *spec.WindowSeconds,
			})
			cancelQuery()
			if err != nil {
				return err
			}
			continue
		}
		queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
		_, err = adapter.QueryLatestValue(queryCtx, mcp.LatestValueRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			ValueColumn:     param.ValueColumn,
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
		})
		cancelQuery()
		if err != nil {
			return err
		}
	}
	return nil
}

func toWhere(spec *scheduler.WhereSpec) *mcp.WhereSpec {
	if spec == nil {
		return nil
	}
	clauses := make([]mcp.WhereClause, 0, len(spec.Clauses))
	for _, c := range spec.Clauses {
		clauses = append(clauses, mcp.WhereClause{Column: c.Column, Op: c.Op, Value: c.Value})
	}
	return &mcp.WhereSpec{Type: spec.Type, Clauses: clauses}
}

func normalizeParameters(spec scheduler.RuleSpec) []scheduler.ParameterSpec {
	if len(spec.Parameters) > 0 {
		return spec.Parameters
	}
	if spec.Source.ValueColumn == "" {
		return nil
	}
	return []scheduler.ParameterSpec{{
		ParameterName: spec.ParameterName,
		ValueColumn:   spec.Source.ValueColumn,
		Detector: scheduler.DetectorSpec{
			Type: "threshold",
			Threshold: &scheduler.ThresholdSpec{
				Op:    spec.Condition.Op,
				Value: spec.Condition.Value,
				Min:   spec.Condition.Min,
				Max:   spec.Condition.Max,
			},
		},
	}}
}

func isNumericType(colType string) bool {
	value := strings.ToLower(colType)
	return strings.Contains(value, "int") || strings.Contains(value, "decimal") || strings.Contains(value, "numeric") || strings.Contains(value, "float") || strings.Contains(value, "double") || strings.Contains(value, "real")
}

func isSupportedRangeChartSize(size int) bool {
	switch size {
	case 2, 3, 4, 5, 6, 7, 8, 9, 10:
		return true
	default:
		return false
	}
}
