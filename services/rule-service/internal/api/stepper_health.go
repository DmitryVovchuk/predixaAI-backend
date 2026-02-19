package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

func (h *Handler) handleRuleHealth(w http.ResponseWriter, r *http.Request) {
	unitID := chi.URLParam(r, "unitId")
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	unit, err := h.Repo.GetMachineUnit(ctx, unitID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "machine unit not found"})
		return
	}
	rulesList, err := h.Repo.ListStepperRules(ctx, unitID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "message": "failed to fetch rules"})
		return
	}
	connector := dbConnectorClient{BaseURL: h.DBConnectorURL, Client: defaultHTTPClient(h.Timeout)}
	schema, err := connector.DescribeTable(ctx, unit.ConnectionRef, unit.SelectedTable)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "failed to describe table"})
		return
	}
	colTypes := map[string]string{}
	for _, col := range schema.Columns {
		colTypes[col.Name] = col.Type
	}
	items := []ruleHealthItem{}
	for _, rule := range rulesList {
		_, col := parseParameterID(rule.ParameterID)
		if col == "" {
			items = append(items, ruleHealthItem{Severity: "error", Code: "PARAMETER_NOT_FOUND", Message: "parameterId invalid", RuleID: rule.ID, ParameterID: rule.ParameterID})
			continue
		}
		typeName := colTypes[col]
		if strings.TrimSpace(typeName) == "" {
			items = append(items, ruleHealthItem{Severity: "error", Code: "COLUMN_NOT_FOUND", Message: "value column not found", RuleID: rule.ID, ParameterID: rule.ParameterID})
			continue
		}
		if !isNumericType(typeName) {
			items = append(items, ruleHealthItem{Severity: "error", Code: "COLUMN_NOT_NUMERIC", Message: "value column must be numeric", RuleID: rule.ID, ParameterID: rule.ParameterID})
		}
	}
	warnings := 0
	errors := 0
	for _, item := range items {
		if item.Severity == "warning" {
			warnings++
		} else if item.Severity == "error" {
			errors++
		}
	}
	writeJSON(w, http.StatusOK, ruleHealthResponse{UnitID: unitID, WarningsCount: warnings, ErrorsCount: errors, Items: items})
}
