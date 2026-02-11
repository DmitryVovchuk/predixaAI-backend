package validation

import (
	"context"
	"errors"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/security"
	"predixaai-backend/services/scheduler-service/internal/scheduler"
)

func RuntimeValidateRule(ctx context.Context, adapter mcp.DbMcpAdapter, spec scheduler.RuleSpec, allowlist security.Allowlist, limits security.Limits) error {
	if spec.PollIntervalSeconds < limits.MinPollSeconds || spec.PollIntervalSeconds > limits.MaxPollSeconds {
		return errors.New("poll interval out of bounds")
	}
	if spec.WindowSeconds != nil {
		if *spec.WindowSeconds <= 0 {
			return errors.New("windowSeconds must be positive")
		}
		if *spec.WindowSeconds > limits.MaxWindowSeconds {
			return errors.New("windowSeconds exceeds limit")
		}
	}
	if !security.IsSafeIdentifier(spec.Source.Table) || !security.IsSafeIdentifier(spec.Source.ValueColumn) || !security.IsSafeIdentifier(spec.Source.TimestampColumn) {
		return errors.New("unsafe identifier")
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
	for _, c := range cols {
		colSet[c.Name] = struct{}{}
	}
	if _, ok := colSet[spec.Source.ValueColumn]; !ok {
		return errors.New("value column not found")
	}
	if _, ok := colSet[spec.Source.TimestampColumn]; !ok {
		return errors.New("timestamp column not found")
	}
	if spec.Aggregation == "latest" {
		queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
		defer cancelQuery()
		_, err = adapter.QueryLatestValue(queryCtx, mcp.LatestValueRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			ValueColumn:     spec.Source.ValueColumn,
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
		})
		return err
	}
	if !adapter.Capabilities().SupportsAggregate {
		return errors.New("adapter does not support aggregates")
	}
	if spec.WindowSeconds == nil {
		return errors.New("windowSeconds required")
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, limits.MaxQueryDuration)
	defer cancelQuery()
	_, err = adapter.QueryAggregate(queryCtx, mcp.AggregateRequest{
		ConnectionRef:   spec.ConnectionRef,
		Table:           spec.Source.Table,
		ValueColumn:     spec.Source.ValueColumn,
		TimestampColumn: spec.Source.TimestampColumn,
		Where:           toWhere(spec.Source.Where),
		Agg:             spec.Aggregation,
		WindowSeconds:   *spec.WindowSeconds,
	})
	return err
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
