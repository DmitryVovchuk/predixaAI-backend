package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type schedulerPreviewRequest struct {
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

type schedulerBaselineRequest struct {
	ConnectionRef    string        `json:"connectionRef"`
	Table            string        `json:"table"`
	TimestampColumn  string        `json:"timestampColumn"`
	ValueColumn      string        `json:"valueColumn"`
	RuleType         string        `json:"ruleType"`
	BaselineSelector selectorSpec  `json:"baselineSelector"`
	Subgrouping      *subgroupSpec `json:"subgrouping,omitempty"`
}

func (h *Handler) handleRuleBaselineCheck(w http.ResponseWriter, r *http.Request) {
	var req baselineCheckRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	if fieldErrors := validateBaselineCheck(req); len(fieldErrors) > 0 {
		writeStepperValidationError(w, "INVALID_REQUEST", "invalid baseline check", fieldErrors)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	if ok, err := h.Repo.ConnectionExists(ctx, req.ConnectionRef); err != nil || !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "connectionRef not found"})
		return
	}
	paramInfo, err := h.resolveParameter(ctx, req.UnitID, req.ParameterID)
	if err != nil {
		if errors.Is(err, errInvalidParameter) {
			writeStepperValidationError(w, "PARAMETER_NOT_FOUND", "parameterId invalid", []FieldError{{Field: "parameterId", Problem: "not_found"}})
			return
		}
		if errors.Is(err, errInvalidTimestamp) {
			writeStepperValidationError(w, "TIMESTAMP_COLUMN_INVALID", "timestampColumn invalid", []FieldError{{Field: "timestampColumn", Problem: "invalid"}})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "parameter resolution failed"})
		return
	}
	client := schedulerClient{BaseURL: h.SchedulerURL, Client: defaultHTTPClient(h.Timeout)}
	var resp baselineCheckResponse
	payload := schedulerBaselineRequest{
		ConnectionRef:    req.ConnectionRef,
		Table:            paramInfo.Table,
		TimestampColumn:  paramInfo.TimestampColumn,
		ValueColumn:      paramInfo.ValueColumn,
		RuleType:         req.RuleType,
		BaselineSelector: req.BaselineSelector,
		Subgrouping:      req.Subgrouping,
	}
	if err := client.PostJSON(ctx, "/api/rules/baseline/check", payload, &resp); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "baseline check failed"})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleRulePreview(w http.ResponseWriter, r *http.Request) {
	var req previewRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	if fieldErrors := validatePreviewRequest(req); len(fieldErrors) > 0 {
		writeStepperValidationError(w, "INVALID_REQUEST", "invalid preview request", fieldErrors)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	if ok, err := h.Repo.ConnectionExists(ctx, req.ConnectionRef); err != nil || !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "connectionRef not found"})
		return
	}
	paramInfo, err := h.resolveParameter(ctx, req.UnitID, req.ParameterID)
	if err != nil {
		if errors.Is(err, errInvalidParameter) {
			writeStepperValidationError(w, "PARAMETER_NOT_FOUND", "parameterId invalid", []FieldError{{Field: "parameterId", Problem: "not_found"}})
			return
		}
		if errors.Is(err, errInvalidTimestamp) {
			writeStepperValidationError(w, "TIMESTAMP_COLUMN_INVALID", "timestampColumn invalid", []FieldError{{Field: "timestampColumn", Problem: "invalid"}})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "parameter resolution failed"})
		return
	}
	client := schedulerClient{BaseURL: h.SchedulerURL, Client: defaultHTTPClient(h.Timeout)}
	var resp previewResponse
	payload := schedulerPreviewRequest{
		ConnectionRef:    req.ConnectionRef,
		Table:            paramInfo.Table,
		TimestampColumn:  paramInfo.TimestampColumn,
		ValueColumn:      paramInfo.ValueColumn,
		RuleType:         req.RuleType,
		Config:           req.Config,
		BaselineSelector: req.BaselineSelector,
		EvalSelector:     req.EvalSelector,
		Subgrouping:      req.Subgrouping,
	}
	if err := client.PostJSON(ctx, "/api/rules/preview", payload, &resp); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "preview failed"})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type parameterInfo struct {
	Table           string
	ValueColumn     string
	TimestampColumn string
}

func (h *Handler) resolveParameter(ctx context.Context, unitID, parameterID string) (parameterInfo, error) {
	unit, err := h.Repo.GetMachineUnit(ctx, unitID)
	if err != nil {
		return parameterInfo{}, err
	}
	paramTable, paramCol := parseParameterID(parameterID)
	if paramTable == "" || paramCol == "" {
		return parameterInfo{}, errInvalidParameter
	}
	if paramTable != unit.SelectedTable {
		return parameterInfo{}, errInvalidParameter
	}
	found := false
	for _, col := range unit.SelectedColumns {
		if col == paramCol {
			found = true
			break
		}
	}
	if !found {
		return parameterInfo{}, errInvalidParameter
	}
	connector := dbConnectorClient{BaseURL: h.DBConnectorURL, Client: defaultHTTPClient(h.Timeout)}
	schema, err := connector.DescribeTable(ctx, unit.ConnectionRef, unit.SelectedTable)
	if err != nil {
		return parameterInfo{}, err
	}
	columns := map[string]string{}
	timestampCandidates := []string{}
	for _, col := range schema.Columns {
		columns[col.Name] = col.Type
		if isTimeType(col.Type) {
			timestampCandidates = append(timestampCandidates, col.Name)
		}
	}
	unitTimestamp := strings.TrimSpace(unit.TimestampColumn)
	timestampColumn := ""
	if unitTimestamp != "" {
		if _, ok := columns[unitTimestamp]; ok {
			timestampColumn = unitTimestamp
		} else {
			return parameterInfo{}, errInvalidTimestamp
		}
	} else {
		timestampColumn = pickTimestampColumn(timestampCandidates)
	}
	if strings.TrimSpace(timestampColumn) == "" {
		return parameterInfo{}, errInvalidTimestamp
	}
	return parameterInfo{Table: unit.SelectedTable, ValueColumn: paramCol, TimestampColumn: timestampColumn}, nil
}

var errInvalidParameter = errors.New("invalid parameter")
var errInvalidTimestamp = errors.New("invalid timestamp column")
