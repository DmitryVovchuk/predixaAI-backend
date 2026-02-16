package rules

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	tableRe     = regexp.MustCompile(`(?i)table\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	valueColRe  = regexp.MustCompile(`(?i)(value|column)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	timestampRe = regexp.MustCompile(`(?i)(timestamp|time)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	betweenRe   = regexp.MustCompile(`(?i)between\s+([0-9]+(?:\.[0-9]+)?)\s+(?:and|to)\s+([0-9]+(?:\.[0-9]+)?)`)
	rangeRe     = regexp.MustCompile(`(?i)(?:in\s+)?range\s+from\s+([0-9]+(?:\.[0-9]+)?)\s+(?:to|up\s+to|through)\s+([0-9]+(?:\.[0-9]+)?)`)
	compareRe   = regexp.MustCompile(`(?i)(above|greater than|>=|>|below|less than|<=|<|==|!=)\s*([0-9]+(?:\.[0-9]+)?)`)
	abnormalRe  = regexp.MustCompile(`(?i)\b(abnormal|anomaly|outlier|spike)\b`)
	missingRe   = regexp.MustCompile(`(?i)(no\s+data|missing|stopped\s+reporting)`)
	windowRe    = regexp.MustCompile(`(?i)(last|over)\s+([0-9]+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours)`)
	intervalRe  = regexp.MustCompile(`(?i)(every|each)\s+([0-9]+)?\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours)`)
	whereClause = regexp.MustCompile(`(?i)where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|=|!=|>=|<=|>|<|in)\s*([^,]+)`)
	andClause   = regexp.MustCompile(`(?i)and\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|=|!=|>=|<=|>|<|in)\s*([^,]+)`)
)

func ParsePrompt(prompt, connectionRef string) (RuleSpec, *ParseError) {
	return ParsePromptWithDraft(prompt, connectionRef, nil)
}

func ParsePromptWithDraft(prompt, connectionRef string, draft *RuleDraft) (RuleSpec, *ParseError) {
	clean := strings.TrimSpace(prompt)
	if clean == "" {
		return RuleSpec{}, &ParseError{Code: "RULE_AMBIGUOUS", Message: "empty rule prompt", Details: []ErrorDetail{{Field: "rulePrompt", Problem: "empty", Hint: "Provide a rule prompt"}}}
	}

	var details []ErrorDetail
	spec := RuleSpec{
		Name:                "",
		Description:         "",
		ConnectionRef:       connectionRef,
		Parameters:          []ParameterSpec{},
		PollIntervalSeconds: 60,
		Enabled:             true,
	}

	tableMatch := tableRe.FindStringSubmatch(clean)
	if len(tableMatch) > 1 {
		spec.Source.Table = tableMatch[1]
	} else if draft != nil && draft.Table != "" {
		spec.Source.Table = draft.Table
	} else {
		details = append(details, ErrorDetail{Field: "source.table", Problem: "missing", Hint: "Example: table telemetry"})
	}

	timeMatch := timestampRe.FindStringSubmatch(clean)
	if len(timeMatch) > 2 {
		spec.Source.TimestampColumn = timeMatch[2]
	} else if draft != nil && draft.TimestampColumn != "" {
		spec.Source.TimestampColumn = draft.TimestampColumn
	} else {
		details = append(details, ErrorDetail{Field: "source.timestampColumn", Problem: "missing", Hint: "Example: timestamp ts"})
	}

	if draft != nil && draft.Where != nil {
		spec.Source.Where = draft.Where
	}

	windowSeconds := 0
	windowMatch := windowRe.FindStringSubmatch(clean)
	if len(windowMatch) > 3 {
		seconds := parseDurationSeconds(windowMatch[2], windowMatch[3])
		if seconds > 0 {
			windowSeconds = seconds
		}
	}

	intervalMatch := intervalRe.FindStringSubmatch(clean)
	if len(intervalMatch) > 3 {
		value := intervalMatch[2]
		if value == "" {
			value = "1"
		}
		seconds := parseDurationSeconds(value, intervalMatch[3])
		if seconds > 0 {
			spec.PollIntervalSeconds = seconds
		}
	}

	detectorType := ""
	if missingRe.MatchString(clean) {
		detectorType = "missing_data"
	} else if abnormalRe.MatchString(clean) {
		detectorType = "robust_zscore"
	}

	var threshold *ThresholdSpec
	if between := betweenRe.FindStringSubmatch(clean); len(between) == 3 {
		minVal, _ := strconv.ParseFloat(between[1], 64)
		maxVal, _ := strconv.ParseFloat(between[2], 64)
		threshold = &ThresholdSpec{Op: "between", Min: &minVal, Max: &maxVal}
	} else if between := rangeRe.FindStringSubmatch(clean); len(between) == 3 {
		minVal, _ := strconv.ParseFloat(between[1], 64)
		maxVal, _ := strconv.ParseFloat(between[2], 64)
		threshold = &ThresholdSpec{Op: "between", Min: &minVal, Max: &maxVal}
	} else if cmp := compareRe.FindStringSubmatch(clean); len(cmp) == 3 {
		op := normalizeOp(cmp[1])
		val, _ := strconv.ParseFloat(cmp[2], 64)
		threshold = &ThresholdSpec{Op: op, Value: val}
	}
	if detectorType == "" && threshold != nil {
		detectorType = "threshold"
	}

	whereClauses := []ClauseSpec{}
	if whereMatch := whereClause.FindStringSubmatch(clean); len(whereMatch) == 4 {
		whereClauses = append(whereClauses, ClauseSpec{Column: whereMatch[1], Op: normalizeOp(whereMatch[2]), Value: parseValue(whereMatch[3])})
	}
	for _, andMatch := range andClause.FindAllStringSubmatch(clean, -1) {
		if len(andMatch) == 4 {
			whereClauses = append(whereClauses, ClauseSpec{Column: andMatch[1], Op: normalizeOp(andMatch[2]), Value: parseValue(andMatch[3])})
		}
	}
	if len(whereClauses) > 0 {
		spec.Source.Where = &WhereSpec{Type: "and", Clauses: whereClauses}
	}

	if draft != nil && len(draft.Parameters) > 0 {
		spec.Parameters = append(spec.Parameters, draft.Parameters...)
	}
	if len(spec.Parameters) == 0 {
		valueMatch := valueColRe.FindStringSubmatch(clean)
		if len(valueMatch) > 2 {
			col := valueMatch[2]
			spec.Parameters = append(spec.Parameters, ParameterSpec{
				ParameterName: col,
				ValueColumn:   col,
				Detector:      buildDetector(detectorType, threshold, spec.PollIntervalSeconds, windowSeconds),
			})
			spec.ParameterName = col
			spec.Source.ValueColumn = col
			spec.Condition = ConditionSpecFromThreshold(threshold)
		} else {
			details = append(details, ErrorDetail{Field: "parameters", Problem: "missing", Hint: "Provide column names or draft.parameters"})
		}
	} else {
		for i := range spec.Parameters {
			if strings.TrimSpace(spec.Parameters[i].ParameterName) == "" {
				spec.Parameters[i].ParameterName = spec.Parameters[i].ValueColumn
			}
			if strings.TrimSpace(spec.Parameters[i].Detector.Type) == "" {
				spec.Parameters[i].Detector = buildDetector(detectorType, threshold, spec.PollIntervalSeconds, windowSeconds)
			}
		}
	}
	if spec.ParameterName == "" && len(spec.Parameters) > 0 {
		spec.ParameterName = spec.Parameters[0].ParameterName
	}

	if detectorType == "" {
		details = append(details, ErrorDetail{Field: "condition", Problem: "missing", Hint: "Example: above 80"})
	} else if detectorType == "threshold" && threshold == nil {
		details = append(details, ErrorDetail{Field: "condition", Problem: "missing", Hint: "Example: above 80"})
	}

	if len(details) > 0 {
		return RuleSpec{}, &ParseError{Code: "RULE_AMBIGUOUS", Message: "rule prompt is missing required fields", Details: details}
	}

	return spec, nil
}

func buildDetector(detectorType string, threshold *ThresholdSpec, pollInterval int, windowSeconds int) DetectorSpec {
	switch detectorType {
	case "robust_zscore":
		evalWindow := 300
		if windowSeconds > 0 {
			evalWindow = windowSeconds
		}
		return DetectorSpec{
			Type: "robust_zscore",
			RobustZ: &RobustZSpec{
				BaselineWindowSeconds: 3600,
				EvalWindowSeconds:     evalWindow,
				ZWarn:                 3,
				ZCrit:                 5,
				MinSamples:            20,
			},
		}
	case "missing_data":
		gap := pollInterval * 2
		if gap < pollInterval {
			gap = pollInterval
		}
		return DetectorSpec{
			Type: "missing_data",
			MissingData: &MissingDataSpec{
				MaxGapSeconds: gap,
			},
		}
	default:
		return DetectorSpec{Type: "threshold", Threshold: threshold}
	}
}

func ConditionSpecFromThreshold(threshold *ThresholdSpec) ConditionSpec {
	if threshold == nil {
		return ConditionSpec{}
	}
	return ConditionSpec{Op: threshold.Op, Value: threshold.Value, Min: threshold.Min, Max: threshold.Max}
}

func parseDurationSeconds(value, unit string) int {
	amount, err := strconv.Atoi(value)
	if err != nil || amount <= 0 {
		return 0
	}
	unit = strings.ToLower(unit)
	switch {
	case strings.HasPrefix(unit, "s"):
		return amount
	case strings.HasPrefix(unit, "m"):
		return amount * 60
	case strings.HasPrefix(unit, "h"):
		return amount * 3600
	default:
		return 0
	}
}

func normalizeOp(op string) string {
	switch strings.TrimSpace(strings.ToLower(op)) {
	case "above", ">", ">=", "greater than":
		if strings.Contains(op, "=") {
			return ">="
		}
		return ">"
	case "below", "<", "<=", "less than":
		if strings.Contains(op, "=") {
			return "<="
		}
		return "<"
	case "=", "==":
		return "=="
	case "!=":
		return "!="
	case "in":
		return "in"
	default:
		return op
	}
}

func parseValue(raw string) interface{} {
	trimmed := strings.Trim(strings.TrimSpace(raw), "'\"")
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
		items := strings.Split(strings.Trim(trimmed, "[]"), ",")
		values := make([]string, 0, len(items))
		for _, item := range items {
			val := strings.Trim(strings.TrimSpace(item), "'\"")
			if val != "" {
				values = append(values, val)
			}
		}
		return values
	}
	if num, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return num
	}
	if strings.EqualFold(trimmed, "true") {
		return true
	}
	if strings.EqualFold(trimmed, "false") {
		return false
	}
	return trimmed
}
