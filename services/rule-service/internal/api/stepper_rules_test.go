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

func TestStepperRuleCreateValidation(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(tableSchema{Columns: []columnInfo{{Name: "gas_ar_flow", Type: "float"}, {Name: "ts", Type: "timestamp"}}})
	}))
	defer server.Close()

	h := &Handler{Repo: fixture.repo, Timeout: 2 * time.Second, DBConnectorURL: server.URL}
	r := chi.NewRouter()
	h.RegisterStepperRoutes(r)

	payload := map[string]any{
		"unitId":      fixture.unitID,
		"ruleType":    "SPEC_LIMIT_VIOLATION",
		"parameterId": buildParameterID("etchers_data", "gas_ar_flow"),
	}
	body, _ := json.Marshal(payload)
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
	var errResp validationErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(errResp.FieldErrors) == 0 {
		t.Fatalf("expected field errors")
	}
}
