package rules

import (
	"fmt"
	"regexp"
)

var identRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func ValidateRuleSpec(spec RuleSpec, minPoll, maxPoll int) *ParseError {
	var details []ErrorDetail
	if spec.Source.Table == "" || !identRegex.MatchString(spec.Source.Table) {
		details = append(details, ErrorDetail{Field: "source.table", Problem: "invalid", Hint: "Use alphanumeric identifiers"})
	}
	if spec.Source.TimestampColumn == "" || !identRegex.MatchString(spec.Source.TimestampColumn) {
		details = append(details, ErrorDetail{Field: "source.timestampColumn", Problem: "invalid", Hint: "Use alphanumeric identifiers"})
	}
	if spec.PollIntervalSeconds < minPoll || spec.PollIntervalSeconds > maxPoll {
		details = append(details, ErrorDetail{Field: "pollIntervalSeconds", Problem: "out of range", Hint: fmt.Sprintf("min %d, max %d", minPoll, maxPoll)})
	}
	if spec.Aggregation != "" && spec.Aggregation != "latest" {
		if spec.WindowSeconds == nil || *spec.WindowSeconds <= 0 {
			details = append(details, ErrorDetail{Field: "windowSeconds", Problem: "required", Hint: "Provide a window for aggregate rules"})
		} else if *spec.WindowSeconds < spec.PollIntervalSeconds {
			details = append(details, ErrorDetail{Field: "windowSeconds", Problem: "too small", Hint: "Must be >= pollIntervalSeconds"})
		}
	}
	params := normalizeParameters(spec)
	if len(params) == 0 {
		details = append(details, ErrorDetail{Field: "parameters", Problem: "missing", Hint: "Provide at least one parameter"})
	}
	for i, param := range params {
		if param.ValueColumn == "" || !identRegex.MatchString(param.ValueColumn) {
			details = append(details, ErrorDetail{Field: fmt.Sprintf("parameters[%d].valueColumn", i), Problem: "invalid", Hint: "Use alphanumeric identifiers"})
		}
		if param.ParameterName != "" && !identRegex.MatchString(param.ParameterName) {
			details = append(details, ErrorDetail{Field: fmt.Sprintf("parameters[%d].parameterName", i), Problem: "invalid", Hint: "Use alphanumeric identifiers"})
		}
		if err := validateDetector(param.Detector, spec.PollIntervalSeconds, i); err != nil {
			details = append(details, *err)
		}
	}
	if spec.Source.Where != nil {
		for i, clause := range spec.Source.Where.Clauses {
			if !identRegex.MatchString(clause.Column) {
				details = append(details, ErrorDetail{Field: fmt.Sprintf("source.where.clauses[%d].column", i), Problem: "invalid", Hint: "Use alphanumeric identifiers"})
			}
		}
	}

	if len(details) > 0 {
		return &ParseError{Code: "RULE_SCHEMA_INVALID", Message: "rule spec failed validation", Details: details}
	}
	return nil
}

func normalizeParameters(spec RuleSpec) []ParameterSpec {
	if len(spec.Parameters) > 0 {
		return spec.Parameters
	}
	if spec.Source.ValueColumn == "" {
		return nil
	}
	detector := DetectorSpec{Type: "threshold", Threshold: &ThresholdSpec{
		Op:    spec.Condition.Op,
		Value: spec.Condition.Value,
		Min:   spec.Condition.Min,
		Max:   spec.Condition.Max,
	}}
	return []ParameterSpec{{ParameterName: spec.ParameterName, ValueColumn: spec.Source.ValueColumn, Detector: detector}}
}

func validateDetector(detector DetectorSpec, pollInterval int, index int) *ErrorDetail {
	switch detector.Type {
	case "threshold":
		if detector.Threshold == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.threshold", index), Problem: "missing", Hint: "Provide threshold"}
		}
		if detector.Threshold.Op == "between" {
			if detector.Threshold.Min == nil || detector.Threshold.Max == nil || *detector.Threshold.Min >= *detector.Threshold.Max {
				return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.threshold", index), Problem: "invalid between range", Hint: "min < max"}
			}
		} else if detector.Threshold.Op == "" {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.threshold", index), Problem: "missing", Hint: "Example: above 80"}
		}
	case "robust_zscore":
		if detector.RobustZ == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.robustZ", index), Problem: "missing", Hint: "Provide robustZ settings"}
		}
		if detector.RobustZ.BaselineWindowSeconds < detector.RobustZ.EvalWindowSeconds {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.robustZ.baselineWindowSeconds", index), Problem: "invalid", Hint: "baselineWindowSeconds >= evalWindowSeconds"}
		}
		if detector.RobustZ.MinSamples < 20 {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.robustZ.minSamples", index), Problem: "too small", Hint: "minSamples >= 20"}
		}
	case "missing_data":
		if detector.MissingData == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.missingData", index), Problem: "missing", Hint: "Provide missingData settings"}
		}
		if detector.MissingData.MaxGapSeconds < pollInterval {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.missingData.maxGapSeconds", index), Problem: "too small", Hint: "maxGapSeconds >= pollIntervalSeconds"}
		}
	case "spec_limit":
		if detector.SpecLimit == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.specLimit", index), Problem: "missing", Hint: "Provide specLimit settings"}
		}
		mode := detector.SpecLimit.Mode
		if mode == "" {
			mode = "spec"
		}
		if mode != "spec" && mode != "control" && mode != "both" {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.specLimit.mode", index), Problem: "invalid", Hint: "Use spec, control, or both"}
		}
		if (mode == "spec" || mode == "both") && (detector.SpecLimit.SpecLimits == nil || (detector.SpecLimit.SpecLimits.USL == nil && detector.SpecLimit.SpecLimits.LSL == nil)) {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.specLimit.specLimits", index), Problem: "missing", Hint: "Provide USL/LSL"}
		}
		if (mode == "control" || mode == "both") && (detector.SpecLimit.ControlLimits == nil || (detector.SpecLimit.ControlLimits.UCL == nil && detector.SpecLimit.ControlLimits.LCL == nil)) {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.specLimit.controlLimits", index), Problem: "missing", Hint: "Provide UCL/LCL"}
		}
	case "shewhart":
		if detector.Shewhart == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.shewhart", index), Problem: "missing", Hint: "Provide shewhart settings"}
		}
		if err := validateBaseline(detector.Shewhart.Baseline, fmt.Sprintf("parameters[%d].detector.shewhart.baseline", index)); err != nil {
			return err
		}
		if detector.Shewhart.SigmaMultiplier != 0 && (detector.Shewhart.SigmaMultiplier < 2 || detector.Shewhart.SigmaMultiplier > 3) {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.shewhart.sigmaMultiplier", index), Problem: "invalid", Hint: "Use 2 or 3"}
		}
		if detector.Shewhart.MinBaselineN < 0 {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.shewhart.minBaselineN", index), Problem: "invalid", Hint: "minBaselineN must be >= 0"}
		}
	case "range_chart":
		if detector.RangeChart == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.rangeChart", index), Problem: "missing", Hint: "Provide rangeChart settings"}
		}
		if !isSupportedRangeChartSize(detector.RangeChart.SubgroupSize) {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.rangeChart.subgroupSize", index), Problem: "invalid", Hint: "Supported subgroupSize: 2-10"}
		}
		if err := validateBaseline(detector.RangeChart.Baseline, fmt.Sprintf("parameters[%d].detector.rangeChart.baseline", index)); err != nil {
			return err
		}
		mode := detector.RangeChart.Subgrouping.Mode
		if mode == "" {
			mode = "consecutive"
		}
		if mode != "consecutive" && mode != "column" {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.rangeChart.subgrouping.mode", index), Problem: "invalid", Hint: "Use consecutive or column"}
		}
		if mode == "column" && detector.RangeChart.Subgrouping.Column == "" {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.rangeChart.subgrouping.column", index), Problem: "missing", Hint: "Provide column name"}
		}
	case "trend":
		if detector.Trend == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.trend", index), Problem: "missing", Hint: "Provide trend settings"}
		}
		if detector.Trend.WindowSize < 2 {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.trend.windowSize", index), Problem: "invalid", Hint: "windowSize must be >= 2"}
		}
		if detector.Trend.Epsilon < 0 {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.trend.epsilon", index), Problem: "invalid", Hint: "epsilon must be >= 0"}
		}
	case "tpa":
		if detector.TPA == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.tpa", index), Problem: "missing", Hint: "Provide tpa settings"}
		}
		if detector.TPA.WindowN < 3 {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.tpa.windowN", index), Problem: "invalid", Hint: "windowN must be >= 3"}
		}
		basis := detector.TPA.RegressionTimeBasis
		if basis != "" && basis != "index" && basis != "timestamp" {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.tpa.regressionTimeBasis", index), Problem: "invalid", Hint: "Use index or timestamp"}
		}
		if detector.TPA.RequireSpecLimits && detector.TPA.SpecLimits == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.tpa.specLimits", index), Problem: "missing", Hint: "Provide spec limits"}
		}
		if detector.TPA.SlopeThreshold == nil && detector.TPA.TimeToSpecThreshold == nil {
			return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.tpa", index), Problem: "invalid", Hint: "Provide slopeThreshold or timeToSpecThreshold"}
		}
	default:
		return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.type", index), Problem: "unsupported", Hint: "Use threshold, robust_zscore, missing_data, spec_limit, shewhart, range_chart, trend, or tpa"}
	}
	return nil
}

func validateBaseline(baseline BaselineSpec, field string) *ErrorDetail {
	if baseline.LastN != nil && baseline.TimeRange != nil {
		return &ErrorDetail{Field: field, Problem: "invalid", Hint: "Use lastN or timeRange"}
	}
	if baseline.LastN == nil && baseline.TimeRange == nil {
		return &ErrorDetail{Field: field, Problem: "missing", Hint: "Provide lastN or timeRange"}
	}
	if baseline.LastN != nil && *baseline.LastN <= 0 {
		return &ErrorDetail{Field: field + ".lastN", Problem: "invalid", Hint: "lastN must be > 0"}
	}
	if baseline.TimeRange != nil {
		if baseline.TimeRange.Start == "" || baseline.TimeRange.End == "" {
			return &ErrorDetail{Field: field + ".timeRange", Problem: "invalid", Hint: "Provide start and end"}
		}
	}
	return nil
}

func isSupportedRangeChartSize(size int) bool {
	switch size {
	case 2, 3, 4, 5, 6, 7, 8, 9, 10:
		return true
	default:
		return false
	}
}
