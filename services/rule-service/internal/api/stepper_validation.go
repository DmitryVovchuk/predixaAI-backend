package api

import (
	"net/http"
	"strings"
)

func validateStepperRuleRequest(req stepperRuleRequest) []FieldError {
	fields := []FieldError{}
	if strings.TrimSpace(req.UnitID) == "" {
		fields = append(fields, FieldError{Field: "unitId", Problem: "missing"})
	}
	if strings.TrimSpace(req.RuleType) == "" {
		fields = append(fields, FieldError{Field: "ruleType", Problem: "missing"})
	}
	if strings.TrimSpace(req.ParameterID) == "" {
		fields = append(fields, FieldError{Field: "parameterId", Problem: "missing"})
	}
	if len(req.Config) == 0 {
		fields = append(fields, FieldError{Field: "config", Problem: "missing"})
	}
	return fields
}

func validateBaselineCheck(req baselineCheckRequest) []FieldError {
	fields := []FieldError{}
	if strings.TrimSpace(req.UnitID) == "" {
		fields = append(fields, FieldError{Field: "unitId", Problem: "missing"})
	}
	if strings.TrimSpace(req.ParameterID) == "" {
		fields = append(fields, FieldError{Field: "parameterId", Problem: "missing"})
	}
	if strings.TrimSpace(req.RuleType) == "" {
		fields = append(fields, FieldError{Field: "ruleType", Problem: "missing"})
	}
	if strings.TrimSpace(req.ConnectionRef) == "" {
		fields = append(fields, FieldError{Field: "connectionRef", Problem: "missing"})
	}
	if strings.TrimSpace(req.BaselineSelector.Kind) == "" {
		fields = append(fields, FieldError{Field: "baselineSelector.kind", Problem: "missing"})
	}
	if req.BaselineSelector.Kind != "" && req.BaselineSelector.Kind != "lastN" && req.BaselineSelector.Kind != "timeRange" {
		fields = append(fields, FieldError{Field: "baselineSelector.kind", Problem: "invalid"})
	}
	return fields
}

func validatePreviewRequest(req previewRequest) []FieldError {
	fields := []FieldError{}
	if strings.TrimSpace(req.UnitID) == "" {
		fields = append(fields, FieldError{Field: "unitId", Problem: "missing"})
	}
	if strings.TrimSpace(req.ParameterID) == "" {
		fields = append(fields, FieldError{Field: "parameterId", Problem: "missing"})
	}
	if strings.TrimSpace(req.RuleType) == "" {
		fields = append(fields, FieldError{Field: "ruleType", Problem: "missing"})
	}
	if strings.TrimSpace(req.ConnectionRef) == "" {
		fields = append(fields, FieldError{Field: "connectionRef", Problem: "missing"})
	}
	if len(req.Config) == 0 {
		fields = append(fields, FieldError{Field: "config", Problem: "missing"})
	}
	if req.BaselineSelector != nil && req.BaselineSelector.Kind != "" && req.BaselineSelector.Kind != "lastN" && req.BaselineSelector.Kind != "timeRange" {
		fields = append(fields, FieldError{Field: "baselineSelector.kind", Problem: "invalid"})
	}
	if req.EvalSelector != nil && req.EvalSelector.Kind != "" && req.EvalSelector.Kind != "lastN" && req.EvalSelector.Kind != "timeRange" {
		fields = append(fields, FieldError{Field: "evalSelector.kind", Problem: "invalid"})
	}
	return fields
}

func writeStepperValidationError(w http.ResponseWriter, code, message string, fields []FieldError) {
	writeJSON(w, http.StatusBadRequest, validationErrorResponse{Code: code, Message: message, FieldErrors: fields})
}
