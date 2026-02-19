package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

func (h *Handler) handleUnitParameters(w http.ResponseWriter, r *http.Request) {
	unitID := chi.URLParam(r, "unitId")
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	unit, err := h.Repo.GetMachineUnit(ctx, unitID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "machine unit not found"})
		return
	}
	if strings.TrimSpace(unit.ConnectionRef) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "connectionRef missing"})
		return
	}
	connector := dbConnectorClient{BaseURL: h.DBConnectorURL, Client: defaultHTTPClient(h.Timeout)}
	schema, err := connector.DescribeTable(ctx, unit.ConnectionRef, unit.SelectedTable)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "failed to describe table"})
		return
	}
	columns := map[string]string{}
	timestampCandidates := []string{}
	for _, col := range schema.Columns {
		columns[col.Name] = col.Type
		if isTimeType(col.Type) {
			timestampCandidates = append(timestampCandidates, col.Name)
		}
	}
	defaultTimestamp := pickTimestampColumn(timestampCandidates)
	unitTimestamp := strings.TrimSpace(unit.TimestampColumn)
	if unitTimestamp != "" {
		if _, ok := columns[unitTimestamp]; ok {
			defaultTimestamp = unitTimestamp
		}
	}
	params := make([]parameterResponse, 0, len(unit.SelectedColumns))
	for _, col := range unit.SelectedColumns {
		typeName := columns[col]
		notes := []string{}
		if typeName == "" {
			notes = append(notes, "column not found in schema")
		}
		if unitTimestamp != "" {
			colType, ok := columns[unitTimestamp]
			if !ok {
				notes = append(notes, "timestampColumn not found in schema")
			} else if !isTimeType(colType) {
				notes = append(notes, "timestampColumn must be time/date")
			}
		}
		parameterID := buildParameterID(unit.SelectedTable, col)
		params = append(params, parameterResponse{
			ParameterID:              parameterID,
			UnitName:                 unit.UnitName,
			Table:                    unit.SelectedTable,
			ValueColumn:              col,
			DataType:                 typeName,
			TimestampColumn:          defaultTimestamp,
			SubgroupCandidateColumns: subgroupCandidates(schema.Columns, col, defaultTimestamp),
			SupportsTrend:            isNumericType(typeName) && defaultTimestamp != "",
			SupportsShewhart:         isNumericType(typeName),
			SupportsRangeChart:       isNumericType(typeName),
			Notes:                    notes,
		})
	}
	writeJSON(w, http.StatusOK, unitParametersResponse{UnitID: unitID, Parameters: params})
}

func buildParameterID(table, column string) string {
	return table + "." + column
}

func parseParameterID(paramID string) (string, string) {
	parts := strings.SplitN(paramID, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func pickTimestampColumn(candidates []string) string {
	for _, name := range candidates {
		lower := strings.ToLower(name)
		if lower == "ts" || lower == "timestamp" || strings.Contains(lower, "time") {
			return name
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	return ""
}

func subgroupCandidates(columns []columnInfo, valueColumn, timestampColumn string) []string {
	values := []string{}
	for _, col := range columns {
		if col.Name == valueColumn || col.Name == timestampColumn {
			continue
		}
		values = append(values, col.Name)
	}
	return values
}

func isNumericType(t string) bool {
	value := strings.ToLower(t)
	return strings.Contains(value, "int") || strings.Contains(value, "decimal") || strings.Contains(value, "numeric") || strings.Contains(value, "float") || strings.Contains(value, "double") || strings.Contains(value, "real")
}

func isTimeType(t string) bool {
	value := strings.ToLower(t)
	return strings.Contains(value, "time") || strings.Contains(value, "date")
}

