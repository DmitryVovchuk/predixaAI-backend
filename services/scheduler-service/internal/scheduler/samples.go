package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
)

type Sample struct {
	TS       time.Time
	Value    float64
	Subgroup string
}

func fetchSamples(ctx context.Context, adapter mcp.DbMcpAdapter, spec RuleSpec, param ParameterSpec, columns []string, since time.Time, limit int, subgroupColumn string) ([]Sample, error) {
	if adapter == nil {
		return nil, errors.New("adapter not configured")
	}
	cols := append([]string{param.ValueColumn, spec.Source.TimestampColumn}, columns...)
	if subgroupColumn != "" {
		cols = append(cols, subgroupColumn)
	}
	rows, err := adapter.FetchRecentRows(ctx, mcp.FetchRecentRowsRequest{
		ConnectionRef:   spec.ConnectionRef,
		Table:           spec.Source.Table,
		Columns:         cols,
		TimestampColumn: spec.Source.TimestampColumn,
		Where:           toWhere(spec.Source.Where),
		Since:           since.UTC().Format(time.RFC3339),
		Limit:           limit,
	})
	if err != nil {
		return nil, err
	}
	samples := make([]Sample, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		val, ok := row[param.ValueColumn]
		if !ok {
			continue
		}
		floatVal, err := toFloat(val)
		if err != nil || math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
			continue
		}
		tsValue, ok := row[spec.Source.TimestampColumn]
		if !ok {
			continue
		}
		ts, err := parseTimeValue(tsValue)
		if err != nil {
			continue
		}
		sample := Sample{TS: ts, Value: floatVal}
		if subgroupColumn != "" {
			if subgroupVal, ok := row[subgroupColumn]; ok {
				sample.Subgroup = fmt.Sprint(subgroupVal)
			}
		}
		samples = append(samples, sample)
	}
	if len(samples) == 0 {
		return samples, nil
	}
	// rows are returned in DESC order, reverse to ASC
	for i, j := 0, len(samples)-1; i < j; i, j = i+1, j-1 {
		samples[i], samples[j] = samples[j], samples[i]
	}
	return samples, nil
}

func filterSamplesByRange(samples []Sample, start *time.Time, end *time.Time) []Sample {
	if start == nil && end == nil {
		return samples
	}
	filtered := make([]Sample, 0, len(samples))
	for _, sample := range samples {
		if start != nil && sample.TS.Before(*start) {
			continue
		}
		if end != nil && sample.TS.After(*end) {
			continue
		}
		filtered = append(filtered, sample)
	}
	return filtered
}

func groupConsecutive(samples []Sample, size int) [][]Sample {
	if size <= 0 {
		return nil
	}
	groups := [][]Sample{}
	for i := 0; i+size <= len(samples); i += size {
		group := make([]Sample, size)
		copy(group, samples[i:i+size])
		groups = append(groups, group)
	}
	return groups
}

func groupBySubgroup(samples []Sample, size int) [][]Sample {
	grouped := map[string][]Sample{}
	order := []string{}
	for _, sample := range samples {
		if sample.Subgroup == "" {
			continue
		}
		if _, ok := grouped[sample.Subgroup]; !ok {
			order = append(order, sample.Subgroup)
		}
		grouped[sample.Subgroup] = append(grouped[sample.Subgroup], sample)
	}
	sort.Strings(order)
	groups := [][]Sample{}
	for _, key := range order {
		values := grouped[key]
		if len(values) < size {
			continue
		}
		groups = append(groups, values[:size])
	}
	return groups
}

func hasConsecutiveTimestamps(samples []Sample) bool {
	if len(samples) < 2 {
		return true
	}
	deltas := make([]float64, 0, len(samples)-1)
	for i := 1; i < len(samples); i++ {
		delta := samples[i].TS.Sub(samples[i-1].TS).Seconds()
		if delta <= 0 {
			return false
		}
		deltas = append(deltas, delta)
	}
	median := Median(deltas)
	if median == 0 {
		return false
	}
	maxGap := median * 2
	for _, delta := range deltas {
		if delta > maxGap {
			return false
		}
	}
	return true
}
