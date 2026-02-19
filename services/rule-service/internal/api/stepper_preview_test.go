package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestBaselineCheckProxy(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()

	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/rules/baseline/check" {
			_ = json.NewEncoder(w).Encode(baselineCheckResponse{Status: "OK", Available: map[string]int{"samples": 50}, Required: map[string]int{"minBaselineSamples": 20}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer scheduler.Close()

	schema := tableSchema{Columns: []columnInfo{
		{Name: "gas_ar_flow", Type: "float"},
		{Name: "ts", Type: "timestamp"},
	}}
	connector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/describe" {
			_ = json.NewEncoder(w).Encode(schema)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer connector.Close()

	h := &Handler{Repo: fixture.repo, Timeout: 2 * time.Second, DBConnectorURL: connector.URL, SchedulerURL: scheduler.URL}
	r := chi.NewRouter()
	h.RegisterStepperRoutes(r)

	payload := baselineCheckRequest{
		UnitID:          fixture.unitID,
		ParameterID:     buildParameterID("etchers_data", "gas_ar_flow"),
		RuleType:        "SHEWHART_3SIGMA",
		ConnectionRef:   fixture.connectionRef,
		BaselineSelector: selectorSpec{Kind: "lastN", Value: 50},
	}
	body, _ := json.Marshal(payload)
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rules/baseline/check", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}

func TestPreviewProxy(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()

	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/rules/preview" {
			_ = json.NewEncoder(w).Encode(previewResponse{Status: "VIOLATION", Computed: map[string]interface{}{"mu": 10.0}, Violations: []map[string]interface{}{{"limitName": "UCL"}}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer scheduler.Close()

	schema := tableSchema{Columns: []columnInfo{
		{Name: "gas_ar_flow", Type: "float"},
		{Name: "ts", Type: "timestamp"},
	}}
	connector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/describe" {
			_ = json.NewEncoder(w).Encode(schema)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer connector.Close()

	h := &Handler{Repo: fixture.repo, Timeout: 2 * time.Second, DBConnectorURL: connector.URL, SchedulerURL: scheduler.URL}
	r := chi.NewRouter()
	h.RegisterStepperRoutes(r)

	payload := previewRequest{
		UnitID:        fixture.unitID,
		ParameterID:   buildParameterID("etchers_data", "gas_ar_flow"),
		RuleType:      "SPEC_LIMIT_VIOLATION",
		ConnectionRef: fixture.connectionRef,
		Config:        json.RawMessage(`{"mode":"spec","specLimits":{"usl":10}}`),
	}
	body, _ := json.Marshal(payload)
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rules/preview", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed previewResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if parsed.Status == "" || parsed.Computed == nil {
		t.Fatalf("expected preview payload")
	}
}
