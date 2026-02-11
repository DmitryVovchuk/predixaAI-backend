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
	if spec.Source.ValueColumn == "" || !identRegex.MatchString(spec.Source.ValueColumn) {
		details = append(details, ErrorDetail{Field: "source.valueColumn", Problem: "invalid", Hint: "Use alphanumeric identifiers"})
	}
	if spec.Source.TimestampColumn == "" || !identRegex.MatchString(spec.Source.TimestampColumn) {
		details = append(details, ErrorDetail{Field: "source.timestampColumn", Problem: "invalid", Hint: "Use alphanumeric identifiers"})
	}
	if spec.PollIntervalSeconds < minPoll || spec.PollIntervalSeconds > maxPoll {
		details = append(details, ErrorDetail{Field: "pollIntervalSeconds", Problem: "out of range", Hint: fmt.Sprintf("min %d, max %d", minPoll, maxPoll)})
	}
	if spec.Aggregation != "latest" && spec.WindowSeconds == nil {
		details = append(details, ErrorDetail{Field: "windowSeconds", Problem: "required", Hint: "Provide a window for aggregate rules"})
	}
	if spec.WindowSeconds != nil && *spec.WindowSeconds < spec.PollIntervalSeconds {
		details = append(details, ErrorDetail{Field: "windowSeconds", Problem: "too small", Hint: "Must be >= pollIntervalSeconds"})
	}
	if spec.Condition.Op == "between" {
		if spec.Condition.Min == nil || spec.Condition.Max == nil || *spec.Condition.Min >= *spec.Condition.Max {
			details = append(details, ErrorDetail{Field: "condition", Problem: "invalid between range", Hint: "min < max"})
		}
	} else if spec.Condition.Op == "" {
		details = append(details, ErrorDetail{Field: "condition", Problem: "missing", Hint: "Example: above 80"})
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
