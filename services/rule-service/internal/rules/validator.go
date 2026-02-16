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
	default:
		return &ErrorDetail{Field: fmt.Sprintf("parameters[%d].detector.type", index), Problem: "unsupported", Hint: "Use threshold, robust_zscore, or missing_data"}
	}
	return nil
}
