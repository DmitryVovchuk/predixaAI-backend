package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/security"
)

type StepperBaselineRequest struct {
	ConnectionRef    string      `json:"connectionRef"`
	Table            string      `json:"table"`
	TimestampColumn  string      `json:"timestampColumn"`
	ValueColumn      string      `json:"valueColumn"`
	RuleType         string      `json:"ruleType"`
	BaselineSelector selectorSpec `json:"baselineSelector"`
	Subgrouping      *subgroupSpec `json:"subgrouping,omitempty"`
}

type StepperPreviewRequest struct {
	ConnectionRef    string          `json:"connectionRef"`
	Table            string          `json:"table"`
	TimestampColumn  string          `json:"timestampColumn"`
	ValueColumn      string          `json:"valueColumn"`
	RuleType         string          `json:"ruleType"`
	Config           json.RawMessage `json:"config"`
	BaselineSelector *selectorSpec   `json:"baselineSelector,omitempty"`
	EvalSelector     *selectorSpec   `json:"evalSelector,omitempty"`
	Subgrouping      *subgroupSpec   `json:"subgrouping,omitempty"`
}

type StepperBaselineResponse struct {
	Status     string           `json:"status"`
	Available  map[string]int   `json:"available"`
	Required   map[string]int   `json:"required"`
	Continuity continuitySummary `json:"continuity"`
	Messages   []string         `json:"messages"`
}

type StepperPreviewResponse struct {
	Status     string                 `json:"status"`
	Window     map[string]string      `json:"window"`
	Baseline   map[string]interface{} `json:"baseline"`
	Computed   map[string]interface{} `json:"computed"`
	Violations []map[string]interface{} `json:"violations"`
	Explain    string                 `json:"explain"`
}

type selectorSpec struct {
	Kind  string `json:"kind"`
	Value int    `json:"value,omitempty"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

type subgroupSpec struct {
	Kind         string `json:"kind"`
	Column       string `json:"column,omitempty"`
	SubgroupSize int    `json:"subgroupSize,omitempty"`
}

type continuitySummary struct {
	GapsDetected      bool    `json:"gapsDetected"`
	LargestGapSeconds float64 `json:"largestGapSeconds"`
}

func StepperBaselineCheck(ctx context.Context, adapter mcp.DbMcpAdapter, allowlist security.Allowlist, limits security.Limits, req StepperBaselineRequest) (StepperBaselineResponse, error) {
	spec, err := buildRuleSpec(req.ConnectionRef, req.Table, req.TimestampColumn, req.ValueColumn, req.RuleType, nil)
	if err != nil {
		return StepperBaselineResponse{}, err
	}
	if err := validateStepperMetadata(ctx, adapter, allowlist, spec, req.Subgrouping); err != nil {
		return StepperBaselineResponse{Status: statusInvalidConfig, Messages: []string{err.Error()}, Available: map[string]int{}, Required: map[string]int{}}, nil
	}
	baselineSamples, err := fetchForSelector(ctx, adapter, spec, req.BaselineSelector, req.Subgrouping, limits)
	if err != nil {
		return StepperBaselineResponse{}, err
	}
	available := map[string]int{"samples": len(baselineSamples)}
	required := map[string]int{"minBaselineSamples": 0, "minBaselineSubgroups": 0}
	if req.RuleType == "RANGE_CHART_R" {
		required["minBaselineSubgroups"] = defaultBaselineSubgroups
	}
	if req.RuleType == "SHEWHART_2SIGMA" || req.RuleType == "SHEWHART_3SIGMA" {
		required["minBaselineSamples"] = defaultBaselineMinN
	}
	continuity := continuitySummary{GapsDetected: false, LargestGapSeconds: 0}
	if len(baselineSamples) > 1 {
		gapsDetected, largestGap := computeTimestampContinuity(baselineSamples)
		continuity.GapsDetected = gapsDetected
		continuity.LargestGapSeconds = largestGap
	}
	status := statusOK
	if required["minBaselineSamples"] > 0 && len(baselineSamples) < required["minBaselineSamples"] {
		status = statusInsufficient
	}
	if req.RuleType == "RANGE_CHART_R" {
		groups := buildGroups(baselineSamples, req.Subgrouping)
		available["subgroups"] = len(groups)
		if len(groups) < required["minBaselineSubgroups"] {
			status = statusInsufficient
		}
	}
	return StepperBaselineResponse{Status: status, Available: available, Required: required, Continuity: continuity, Messages: []string{}}, nil
}

func StepperPreview(ctx context.Context, adapter mcp.DbMcpAdapter, allowlist security.Allowlist, limits security.Limits, req StepperPreviewRequest) (StepperPreviewResponse, error) {
	spec, err := buildRuleSpec(req.ConnectionRef, req.Table, req.TimestampColumn, req.ValueColumn, req.RuleType, req.Config)
	if err != nil {
		return StepperPreviewResponse{}, err
	}
	if err := validateStepperMetadata(ctx, adapter, allowlist, spec, req.Subgrouping); err != nil {
		return StepperPreviewResponse{Status: statusInvalidConfig, Explain: err.Error()}, nil
	}
	baselineSelector := req.BaselineSelector
	if baselineSelector == nil {
		baselineSelector = &selectorSpec{Kind: "lastN", Value: defaultBaselineLastN}
	}
	baselineSamples, err := fetchForSelector(ctx, adapter, spec, *baselineSelector, req.Subgrouping, limits)
	if err != nil {
		return StepperPreviewResponse{}, err
	}
	evalSelector := req.EvalSelector
	if evalSelector == nil {
		evalSelector = &selectorSpec{Kind: "lastN", Value: 50}
	}
	evalSamples, err := fetchForSelector(ctx, adapter, spec, *evalSelector, req.Subgrouping, limits)
	if err != nil {
		return StepperPreviewResponse{}, err
	}
	result := DetectorResult{Status: statusInvalidConfig}
	switch req.RuleType {
	case "SPEC_LIMIT_VIOLATION":
		if len(evalSamples) == 0 {
			result = insufficientData("not enough samples")
			break
		}
		result = EvaluateSpecLimit(evalSamples[len(evalSamples)-1], *spec.Parameters[0].Detector.SpecLimit)
	case "SHEWHART_3SIGMA", "SHEWHART_2SIGMA":
		result = EvaluateShewhart(baselineSamples, *spec.Parameters[0].Detector.Shewhart, spec.Parameters[0].Detector.Shewhart.SigmaMultiplier)
	case "RANGE_CHART_R":
		groups := buildGroups(baselineSamples, req.Subgrouping)
		result = EvaluateRangeChart(groups, *spec.Parameters[0].Detector.RangeChart)
	case "TREND_6_POINTS":
		result = EvaluateTrend6(evalSamples, *spec.Parameters[0].Detector.Trend)
	case "TPA":
		result = EvaluateTPA(evalSamples, *spec.Parameters[0].Detector.TPA)
	default:
		return StepperPreviewResponse{}, errors.New("unsupported rule type")
	}
	applyWindowAndBaseline(&result, evalSamples, nil, nil, req.RuleType == "SHEWHART_3SIGMA" || req.RuleType == "SHEWHART_2SIGMA" || req.RuleType == "RANGE_CHART_R")
	computed := map[string]interface{}{}
	for k, v := range result.Metadata {
		computed[k] = v
	}
	violations := []map[string]interface{}{}
	for _, v := range result.Violations {
		item := map[string]interface{}{
			"kind":      "point",
			"value":     v.Value,
			"reason":    v.Reason,
			"limitName": v.LimitName,
			"limitValue": v.LimitValue,
			"delta":     v.Delta,
		}
		if v.Timestamp != nil {
			item["timestamp"] = v.Timestamp.Format(time.RFC3339)
		}
		if v.Index != nil {
			item["index"] = *v.Index
		}
		violations = append(violations, item)
	}
	return StepperPreviewResponse{
		Status: result.Status,
		Window: map[string]string{
			"start": formatTime(result.WindowStart),
			"end":   formatTime(result.WindowEnd),
		},
		Baseline: map[string]interface{}{
			"start": formatTime(result.BaselineStart),
			"end":   formatTime(result.BaselineEnd),
			"count": len(baselineSamples),
		},
		Computed:  computed,
		Violations: violations,
		Explain:   buildExplain(result, spec.Parameters[0]),
	}, nil
}

func buildRuleSpec(connectionRef, table, timestampColumn, valueColumn, ruleType string, config json.RawMessage) (RuleSpec, error) {
	detector, err := buildDetector(ruleType, config)
	if err != nil {
		return RuleSpec{}, err
	}
	spec := RuleSpec{
		ConnectionRef: connectionRef,
		Source: SourceSpec{Table: table, TimestampColumn: timestampColumn},
		Parameters: []ParameterSpec{{
			ParameterName: valueColumn,
			ValueColumn:   valueColumn,
			Detector:      detector,
		}},
		PollIntervalSeconds: 10,
		Enabled:             true,
	}
	return spec, nil
}

func buildDetector(ruleType string, config json.RawMessage) (DetectorSpec, error) {
	switch ruleType {
	case "SPEC_LIMIT_VIOLATION":
		var spec SpecLimitSpec
		_ = json.Unmarshal(config, &spec)
		return DetectorSpec{Type: "spec_limit", SpecLimit: &spec}, nil
	case "SHEWHART_3SIGMA", "SHEWHART_2SIGMA":
		var spec ShewhartSpec
		_ = json.Unmarshal(config, &spec)
		sigma := 3.0
		if ruleType == "SHEWHART_2SIGMA" {
			sigma = 2
		}
		spec.SigmaMultiplier = sigma
		return DetectorSpec{Type: "shewhart", Shewhart: &spec}, nil
	case "RANGE_CHART_R":
		var spec RangeChartSpec
		_ = json.Unmarshal(config, &spec)
		return DetectorSpec{Type: "range_chart", RangeChart: &spec}, nil
	case "TREND_6_POINTS":
		var spec TrendSpec
		_ = json.Unmarshal(config, &spec)
		if spec.WindowSize == 0 {
			spec.WindowSize = 6
		}
		return DetectorSpec{Type: "trend", Trend: &spec}, nil
	case "TPA":
		var spec TPASpec
		_ = json.Unmarshal(config, &spec)
		return DetectorSpec{Type: "tpa", TPA: &spec}, nil
	default:
		return DetectorSpec{}, errors.New("unsupported rule type")
	}
}

func fetchForSelector(ctx context.Context, adapter mcp.DbMcpAdapter, spec RuleSpec, selector selectorSpec, subgroup *subgroupSpec, limits security.Limits) ([]Sample, error) {
	since := time.Now().Add(-time.Hour * 24 * 365)
	limit := limits.MaxSampleRows
	start := (*time.Time)(nil)
	end := (*time.Time)(nil)
	switch selector.Kind {
	case "lastN":
		if selector.Value > 0 {
			limit = clampLimit(selector.Value, limits.MaxSampleRows)
		}
	case "timeRange":
		parsedStart, parsedEnd, err := parseTimeRange(TimeRangeSpec{Start: selector.Start, End: selector.End})
		if err != nil {
			return nil, err
		}
		since = parsedStart
		start = &parsedStart
		end = &parsedEnd
	default:
		return nil, errors.New("invalid selector kind")
	}
	subgroupColumn := ""
	if subgroup != nil && subgroup.Kind == "column" {
		subgroupColumn = subgroup.Column
	}
	samples, err := fetchSamples(ctx, adapter, spec, spec.Parameters[0], nil, since, limit, subgroupColumn)
	if err != nil {
		return nil, err
	}
	if start != nil || end != nil {
		samples = filterSamplesByRange(samples, start, end)
	}
	return samples, nil
}

func buildGroups(samples []Sample, subgroup *subgroupSpec) [][]Sample {
	size := 0
	if subgroup != nil {
		size = subgroup.SubgroupSize
	}
	if size <= 0 {
		size = 5
	}
	if subgroup != nil && subgroup.Kind == "column" {
		return groupBySubgroup(samples, size)
	}
	return groupConsecutive(samples, size)
}

func computeTimestampContinuity(samples []Sample) (bool, float64) {
	if len(samples) < 2 {
		return false, 0
	}
	largest := 0.0
	gapsDetected := false
	for i := 1; i < len(samples); i++ {
		delta := samples[i].TS.Sub(samples[i-1].TS).Seconds()
		if delta > largest {
			largest = delta
		}
	}
	if !hasConsecutiveTimestamps(samples) {
		gapsDetected = true
	}
	return gapsDetected, largest
}

func formatTime(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func validateStepperMetadata(ctx context.Context, adapter mcp.DbMcpAdapter, allowlist security.Allowlist, spec RuleSpec, subgroup *subgroupSpec) error {
	if !allowlist.AllowsTable(spec.Source.Table) {
		return errors.New("table not allowlisted")
	}
	tables, err := adapter.ListTables(ctx, spec.ConnectionRef)
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
	cols, err := adapter.ListColumns(ctx, spec.ConnectionRef, spec.Source.Table)
	if err != nil {
		return err
	}
	colTypes := map[string]string{}
	for _, col := range cols {
		colTypes[col.Name] = col.Type
	}
	if _, ok := colTypes[spec.Source.TimestampColumn]; !ok {
		return errors.New("timestamp column not found")
	}
	param := spec.Parameters[0]
	valueType := colTypes[param.ValueColumn]
	if valueType == "" {
		return errors.New("value column not found")
	}
	if needsNumeric(param.Detector.Type) && !isNumericType(valueType) {
		return errors.New("value column must be numeric")
	}
	if subgroup != nil && subgroup.Kind == "column" {
		if _, ok := colTypes[subgroup.Column]; !ok {
			return errors.New("subgroup column not found")
		}
	}
	if (param.Detector.Type == "trend" || param.Detector.Type == "tpa") && !isTimeType(colTypes[spec.Source.TimestampColumn]) {
		return errors.New("timestamp column must be time type")
	}
	return nil
}

func needsNumeric(detectorType string) bool {
	switch detectorType {
	case "spec_limit", "shewhart", "range_chart", "trend", "tpa":
		return true
	default:
		return false
	}
}

func isNumericType(t string) bool {
	value := strings.ToLower(t)
	return strings.Contains(value, "int") || strings.Contains(value, "decimal") || strings.Contains(value, "numeric") || strings.Contains(value, "float") || strings.Contains(value, "double") || strings.Contains(value, "real")
}

func isTimeType(t string) bool {
	value := strings.ToLower(t)
	return strings.Contains(value, "time") || strings.Contains(value, "date")
}
