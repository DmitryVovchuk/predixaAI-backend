package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestUnitParametersEligibility(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()

	schema := tableSchema{Columns: []columnInfo{
		{Name: "gas_ar_flow", Type: "float"},
		{Name: "rf_power", Type: "float"},
		{Name: "ts", Type: "timestamp"},
		{Name: "batch_id", Type: "text"},
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/describe" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(schema)
	}))
	defer server.Close()

	h := &Handler{Repo: fixture.repo, Timeout: 2 * time.Second, DBConnectorURL: server.URL}
	r := chi.NewRouter()
	h.RegisterStepperRoutes(r)

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/machine-units/"+fixture.unitID+"/parameters", nil)
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var payload unitParametersResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(payload.Parameters) == 0 {
		t.Fatalf("expected parameters")
	}
	if payload.Parameters[0].TimestampColumn == "" {
		t.Fatalf("expected timestamp column")
	}
	if !payload.Parameters[0].SupportsRangeChart {
		t.Fatalf("expected range chart eligibility")
	}
}
