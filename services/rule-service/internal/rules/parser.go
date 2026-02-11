package rules

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	tableRe      = regexp.MustCompile(`(?i)table\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	valueColRe   = regexp.MustCompile(`(?i)(value|column)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	timestampRe  = regexp.MustCompile(`(?i)(timestamp|time)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	betweenRe    = regexp.MustCompile(`(?i)between\s+([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)`)
	compareRe    = regexp.MustCompile(`(?i)(above|greater than|>=|>|below|less than|<=|<|==|!=)\s*([0-9]+(?:\.[0-9]+)?)`)
	windowRe     = regexp.MustCompile(`(?i)(last|over)\s+([0-9]+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours)`)
	intervalRe   = regexp.MustCompile(`(?i)(every|each)\s+([0-9]+)?\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours)`)
	whereClause  = regexp.MustCompile(`(?i)where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|=|!=|>=|<=|>|<|in)\s*([^,]+)`)
	andClause    = regexp.MustCompile(`(?i)and\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|=|!=|>=|<=|>|<|in)\s*([^,]+)`)
)

func ParsePrompt(prompt, connectionRef string) (RuleSpec, *ParseError) {
	clean := strings.TrimSpace(prompt)
	if clean == "" {
		return RuleSpec{}, &ParseError{Code: "RULE_AMBIGUOUS", Message: "empty rule prompt", Details: []ErrorDetail{{Field: "rulePrompt", Problem: "empty", Hint: "Provide a rule prompt"}}}
	}

	var details []ErrorDetail
	spec := RuleSpec{
		Name:                "",
		Description:         "",
		ConnectionRef:       connectionRef,
		ParameterName:       "",
		Aggregation:         "latest",
		PollIntervalSeconds: 60,
		Enabled:             true,
	}

	tableMatch := tableRe.FindStringSubmatch(clean)
	if len(tableMatch) > 1 {
		spec.Source.Table = tableMatch[1]
	} else {
		details = append(details, ErrorDetail{Field: "source.table", Problem: "missing", Hint: "Example: table telemetry"})
	}

	valueMatch := valueColRe.FindStringSubmatch(clean)
	if len(valueMatch) > 2 {
		spec.Source.ValueColumn = valueMatch[2]
		spec.ParameterName = valueMatch[2]
	} else {
		details = append(details, ErrorDetail{Field: "source.valueColumn", Problem: "missing", Hint: "Example: column temperature"})
	}

	timeMatch := timestampRe.FindStringSubmatch(clean)
	if len(timeMatch) > 2 {
		spec.Source.TimestampColumn = timeMatch[2]
	} else {
		details = append(details, ErrorDetail{Field: "source.timestampColumn", Problem: "missing", Hint: "Example: timestamp ts"})
	}

	if strings.Contains(strings.ToLower(clean), "avg") || strings.Contains(strings.ToLower(clean), "average") {
		spec.Aggregation = "avg"
	} else if strings.Contains(strings.ToLower(clean), "min") {
		spec.Aggregation = "min"
	} else if strings.Contains(strings.ToLower(clean), "max") {
		spec.Aggregation = "max"
	} else if strings.Contains(strings.ToLower(clean), "sum") {
		spec.Aggregation = "sum"
	}

	windowMatch := windowRe.FindStringSubmatch(clean)
	if len(windowMatch) > 3 {
		seconds := parseDurationSeconds(windowMatch[2], windowMatch[3])
		if seconds > 0 {
			spec.WindowSeconds = &seconds
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

	if between := betweenRe.FindStringSubmatch(clean); len(between) == 3 {
		minVal, _ := strconv.ParseFloat(between[1], 64)
		maxVal, _ := strconv.ParseFloat(between[2], 64)
		spec.Condition = ConditionSpec{Op: "between", Min: &minVal, Max: &maxVal}
	} else if cmp := compareRe.FindStringSubmatch(clean); len(cmp) == 3 {
		op := normalizeOp(cmp[1])
		val, _ := strconv.ParseFloat(cmp[2], 64)
		spec.Condition = ConditionSpec{Op: op, Value: val}
	} else {
		details = append(details, ErrorDetail{Field: "condition", Problem: "missing", Hint: "Example: above 80"})
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

	if len(details) > 0 {
		return RuleSpec{}, &ParseError{Code: "RULE_AMBIGUOUS", Message: "rule prompt is missing required fields", Details: details}
	}

	return spec, nil
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
