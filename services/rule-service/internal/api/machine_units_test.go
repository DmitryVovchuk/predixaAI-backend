package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"predixaai-backend/services/rule-service/internal/storage"
)

type machineUnitTestFixture struct {
	repo          *storage.Repository
	cleanup       func()
	connectionRef string
	ruleID        string
	unitID        string
}

func setupMachineUnitFixture(t *testing.T) machineUnitTestFixture {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL or DATABASE_URL not set")
	}
	store, err := storage.NewStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	repo := storage.NewRepository(store)
	cleanup := func() {
		store.Close()
	}

	_, err = repo.Store.Pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS db_connections (
		id uuid PRIMARY KEY,
		name text NOT NULL,
		type text NOT NULL,
		host text NOT NULL,
		port int NOT NULL,
		user_name text NOT NULL,
		password_enc text NOT NULL,
		database text NOT NULL,
		created_at timestamptz NOT NULL
	)`)
	if err != nil {
		t.Fatalf("failed to ensure db_connections: %v", err)
	}
	_, err = repo.Store.Pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS rules (
		id uuid PRIMARY KEY,
		name text NOT NULL,
		description text,
		connection_ref uuid NOT NULL REFERENCES db_connections(id),
		parameter_name text NOT NULL,
		rule_json jsonb NOT NULL,
		enabled boolean NOT NULL,
		status text NOT NULL,
		last_error jsonb,
		last_validated_at timestamptz,
		created_at timestamptz NOT NULL,
		updated_at timestamptz NOT NULL
	)`)
	if err != nil {
		t.Fatalf("failed to ensure rules: %v", err)
	}
	_, err = repo.Store.Pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS machine_units (
		unit_id text PRIMARY KEY,
		unit_name text NOT NULL,
		connection_ref uuid NOT NULL REFERENCES db_connections(id),
		selected_table text NOT NULL,
		timestamp_column text NOT NULL DEFAULT '',
		selected_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
		live_parameters jsonb NOT NULL DEFAULT '[]'::jsonb,
		rule_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err != nil {
		t.Fatalf("failed to ensure machine_units: %v", err)
	}
	_, err = repo.Store.Pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS ui_rules (
		id uuid PRIMARY KEY,
		unit_id text NOT NULL,
		name text NOT NULL,
		rule_type text NOT NULL,
		parameter_id text NOT NULL,
		config jsonb NOT NULL DEFAULT '{}'::jsonb,
		enabled boolean NOT NULL DEFAULT true,
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err != nil {
		t.Fatalf("failed to ensure ui_rules: %v", err)
	}

	connectionRef, err := repo.CreateConnection(context.Background(), storage.DBConnection{
		Name:     "test",
		Type:     "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "secret",
		Database: "db",
	})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	ruleJSON, _ := json.Marshal(map[string]any{"name": "rule"})
	ruleID, err := repo.CreateRule(context.Background(), storage.RuleRecord{
		Name:          "rule",
		Description:   "",
		ConnectionRef: connectionRef,
		ParameterName: "temp",
		RuleJSON:      ruleJSON,
		Enabled:       true,
		Status:        "DRAFT",
	})
	if err != nil {
		t.Fatalf("failed to create rule: %v", err)
	}
	unitID := "machine-" + uuid.NewString()
	_, err = repo.CreateMachineUnit(context.Background(), storage.MachineUnit{
		UnitID:          unitID,
		UnitName:        "unit",
		ConnectionRef:   connectionRef,
		SelectedTable:   "etchers_data",
		TimestampColumn: "ts",
		SelectedColumns: []string{"gas_ar_flow"},
		LiveParameters:  json.RawMessage("[]"),
		RuleIDs:         []string{ruleID},
		PosX:            0.4,
		PosY:            0.6,
	})
	if err != nil {
		t.Fatalf("failed to create machine unit: %v", err)
	}

	return machineUnitTestFixture{repo: repo, cleanup: cleanup, connectionRef: connectionRef, ruleID: ruleID, unitID: unitID}
}

func buildTestRouter(repo *storage.Repository) http.Handler {
	h := &Handler{
		Repo:    repo,
		Timeout: 2 * time.Second,
	}
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func TestMachineUnitAddRulesValidation(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	unknown := uuid.NewString()
	payload := map[string]any{"add": []string{fixture.ruleID, fixture.ruleID, unknown}, "remove": []string{}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units/"+fixture.unitID+"/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
	var parsed errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if parsed.Code != "RULES_NOT_FOUND" {
		t.Fatalf("unexpected code: %s", parsed.Code)
	}
}

func TestMachineUnitRemoveRulesIdempotent(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"add": []string{}, "remove": []string{fixture.ruleID, fixture.ruleID}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units/"+fixture.unitID+"/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed struct {
		Ok   bool                `json:"ok"`
		Unit machineUnitResponse `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(parsed.Unit.RuleIDs) != 0 {
		t.Fatalf("expected rule list to be empty")
	}
}

func TestMachineUnitColumnsValidation(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"add": []string{"bad-column"}, "remove": []string{}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units/"+fixture.unitID+"/columns", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
}

func TestMachineUnitChangeTableClearsColumns(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"selectedTable": "new_table"}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPut, "/machine-units/"+fixture.unitID+"/table", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed struct {
		Ok   bool                `json:"ok"`
		Unit machineUnitResponse `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(parsed.Unit.SelectedColumns) != 0 {
		t.Fatalf("expected selectedColumns to be cleared")
	}
}

func TestMachineUnitColumnsLimit(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	columns := make([]string, 0, maxSelectedColumns+1)
	for i := 0; i < maxSelectedColumns+1; i++ {
		columns = append(columns, "col"+strconv.Itoa(i))
	}
	payload := map[string]any{"add": columns, "remove": []string{}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units/"+fixture.unitID+"/columns", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
}

func TestMachineUnitCreateRuleObject(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{
		"unitName":        "cnc",
		"connectionRef":   fixture.connectionRef,
		"selectedTable":   "etchers_data",
		"timestampColumn": "ts",
		"selectedColumns": []string{"gas_ar_flow"},
		"rule":            map[string]any{},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed struct {
		Ok   bool                `json:"ok"`
		Unit machineUnitResponse `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(parsed.Unit.RuleIDs) != 0 {
		t.Fatalf("expected rule list to be empty")
	}
}

func TestMachineUnitCreateWithExistingIDUpdates(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{
		"unitId":          fixture.unitID,
		"unitName":        "cnc-updated",
		"connectionRef":   fixture.connectionRef,
		"selectedTable":   "etchers_data",
		"timestampColumn": "ts",
		"selectedColumns": []string{"gas_ar_flow"},
		"rule":            []string{},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/machine-units", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed struct {
		Ok   bool                `json:"ok"`
		Unit machineUnitResponse `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if parsed.Unit.UnitID != fixture.unitID {
		t.Fatalf("expected same unitId")
	}
	if parsed.Unit.UnitName != "cnc-updated" {
		t.Fatalf("expected updated unitName")
	}
}

func TestMachineUnitPositionSuccess(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"pos": map[string]any{"x": 0.25, "y": 0.75}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPatch, "/machine-units/"+fixture.unitID+"/position", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	var parsed struct {
		Ok   bool                `json:"ok"`
		Unit machineUnitResponse `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if parsed.Unit.Pos.X != 0.25 || parsed.Unit.Pos.Y != 0.75 {
		t.Fatalf("unexpected position: %v %v", parsed.Unit.Pos.X, parsed.Unit.Pos.Y)
	}
}

func TestMachineUnitPositionValidation(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"pos": map[string]any{"x": -0.1, "y": 1.2}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPatch, "/machine-units/"+fixture.unitID+"/position", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
}

func TestMachineUnitPositionNotFound(t *testing.T) {
	fixture := setupMachineUnitFixture(t)
	defer fixture.cleanup()
	router := buildTestRouter(fixture.repo)

	payload := map[string]any{"pos": map[string]any{"x": 0.2, "y": 0.3}}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPatch, "/machine-units/machine-missing/position", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.Code)
	}
}
