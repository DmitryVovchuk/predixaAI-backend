package main

import (
	"encoding/json"
	"testing"
)

func TestBaseRequestContract(t *testing.T) {
	payload := []byte(`{
		"connectionRef": "c-123",
		"connection": {
			"type": "postgres",
			"host": "localhost",
			"port": 5432,
			"user": "app",
			"password": "secret",
			"database": "app"
		}
	}`)
	var req baseRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		t.Fatalf("failed to unmarshal baseRequest: %v", err)
	}
	if req.ConnectionRef != "c-123" {
		t.Fatalf("unexpected connectionRef: %s", req.ConnectionRef)
	}
	if req.Connection.Type != "postgres" || req.Connection.Host != "localhost" {
		t.Fatalf("unexpected connection fields")
	}
}

func TestTableRequestContract(t *testing.T) {
	payload := []byte(`{
		"connectionRef": "c-456",
		"connection": {
			"type": "mysql",
			"host": "db",
			"port": 3306,
			"user": "root",
			"password": "pass",
			"database": "app"
		},
		"table": "etchers_data"
	}`)
	var req tableRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		t.Fatalf("failed to unmarshal tableRequest: %v", err)
	}
	if req.ConnectionRef != "c-456" {
		t.Fatalf("unexpected connectionRef: %s", req.ConnectionRef)
	}
	if req.Table != "etchers_data" {
		t.Fatalf("unexpected table: %s", req.Table)
	}
}
