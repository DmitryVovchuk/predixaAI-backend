package storage

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func setupTestRepository(t *testing.T) (*Repository, func()) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL or DATABASE_URL not set")
	}
	store, err := NewStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	repo := NewRepository(store)
	cleanup := func() {
		store.Close()
	}
	return repo, cleanup
}

func ensureMachineUnitSchema(t *testing.T, repo *Repository) {
	ctx := context.Background()
	_, err := repo.Store.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS db_connections (
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
	_, err = repo.Store.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS rules (
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
	_, err = repo.Store.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS machine_units (
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
}

func createConnection(t *testing.T, repo *Repository) string {
	id, err := repo.CreateConnection(context.Background(), DBConnection{
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
	return id
}

func createRule(t *testing.T, repo *Repository, connectionRef string) string {
	ruleJSON, _ := json.Marshal(map[string]any{"name": "test"})
	id, err := repo.CreateRule(context.Background(), RuleRecord{
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
	return id
}

func createMachineUnit(t *testing.T, repo *Repository, connectionRef string, ruleIDs []string, columns []string) MachineUnit {
	unit := MachineUnit{
		UnitID:          "machine-" + uuid.NewString(),
		UnitName:        "unit",
		ConnectionRef:   connectionRef,
		SelectedTable:   "etchers_data",
		TimestampColumn: "ts",
		SelectedColumns: columns,
		LiveParameters:  json.RawMessage("[]"),
		RuleIDs:         ruleIDs,
		PosX:            0.1,
		PosY:            0.2,
	}
	created, err := repo.CreateMachineUnit(context.Background(), unit)
	if err != nil {
		t.Fatalf("failed to create machine unit: %v", err)
	}
	return created
}

func TestUpdateRules(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()
	ensureMachineUnitSchema(t, repo)
	connectionRef := createConnection(t, repo)
	rule1 := createRule(t, repo, connectionRef)
	rule2 := createRule(t, repo, connectionRef)
	unit := createMachineUnit(t, repo, connectionRef, []string{rule1}, []string{"col1"})

	updated, err := repo.UpdateRules(context.Background(), unit.UnitID, []string{rule2, rule2}, []string{rule1})
	if err != nil {
		t.Fatalf("update rules failed: %v", err)
	}
	if len(updated.RuleIDs) != 1 || updated.RuleIDs[0] != rule2 {
		t.Fatalf("unexpected rule ids: %#v", updated.RuleIDs)
	}
	if !updated.UpdatedAt.After(unit.UpdatedAt) {
		t.Fatalf("expected updated_at to change")
	}
}

func TestUpdateColumns(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()
	ensureMachineUnitSchema(t, repo)
	connectionRef := createConnection(t, repo)
	unit := createMachineUnit(t, repo, connectionRef, nil, []string{"col1"})

	updated, err := repo.UpdateColumns(context.Background(), unit.UnitID, []string{"col2"}, []string{"col1"})
	if err != nil {
		t.Fatalf("update columns failed: %v", err)
	}
	if len(updated.SelectedColumns) != 1 || updated.SelectedColumns[0] != "col2" {
		t.Fatalf("unexpected columns: %#v", updated.SelectedColumns)
	}
	if !updated.UpdatedAt.After(unit.UpdatedAt) {
		t.Fatalf("expected updated_at to change")
	}
}

func TestUpdateTableClearsColumns(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()
	ensureMachineUnitSchema(t, repo)
	connectionRef := createConnection(t, repo)
	unit := createMachineUnit(t, repo, connectionRef, nil, []string{"col1"})

	updated, err := repo.UpdateTable(context.Background(), unit.UnitID, "new_table", nil, false)
	if err != nil {
		t.Fatalf("update table failed: %v", err)
	}
	if len(updated.SelectedColumns) != 0 {
		t.Fatalf("expected columns to be cleared")
	}
}

func TestConcurrentUpdates(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()
	ensureMachineUnitSchema(t, repo)
	connectionRef := createConnection(t, repo)
	unit := createMachineUnit(t, repo, connectionRef, nil, []string{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = repo.UpdateColumns(context.Background(), unit.UnitID, []string{"col1"}, nil)
	}()
	go func() {
		defer wg.Done()
		_, _ = repo.UpdateColumns(context.Background(), unit.UnitID, []string{"col2"}, nil)
	}()
	wg.Wait()

	updated, err := repo.GetMachineUnit(context.Background(), unit.UnitID)
	if err != nil {
		t.Fatalf("fetch unit failed: %v", err)
	}
	if len(updated.SelectedColumns) < 1 {
		t.Fatalf("expected columns to be updated")
	}
	if !containsAll(updated.SelectedColumns, []string{"col1", "col2"}) {
		t.Fatalf("expected columns to include updates, got %#v", updated.SelectedColumns)
	}
}

func TestUpdatePosition(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()
	ensureMachineUnitSchema(t, repo)
	connectionRef := createConnection(t, repo)
	unit := createMachineUnit(t, repo, connectionRef, nil, []string{})

	updated, err := repo.UpdatePosition(context.Background(), unit.UnitID, 0.33, 0.77)
	if err != nil {
		t.Fatalf("update position failed: %v", err)
	}
	if updated.PosX != 0.33 || updated.PosY != 0.77 {
		t.Fatalf("unexpected position: %v %v", updated.PosX, updated.PosY)
	}
	if !updated.UpdatedAt.After(unit.UpdatedAt) {
		t.Fatalf("expected updated_at to change")
	}
}

func containsAll(values []string, required []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		seen[value] = true
	}
	for _, value := range required {
		if !seen[value] {
			return false
		}
	}
	return true
}
