package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestRuleCatalogContainsPhase1Types(t *testing.T) {
	h := &Handler{}
	r := chi.NewRouter()
	h.RegisterStepperRoutes(r)
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/rules/catalog", nil)
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var payload catalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	want := map[string]bool{
		"SPEC_LIMIT_VIOLATION": true,
		"SHEWHART_3SIGMA":      true,
		"SHEWHART_2SIGMA":      true,
		"RANGE_CHART_R":        true,
		"TREND_6_POINTS":       true,
		"TPA":                  true,
	}
	for _, entry := range payload.Types {
		delete(want, entry.Type)
	}
	if len(want) != 0 {
		t.Fatalf("missing catalog types: %v", want)
	}
}
