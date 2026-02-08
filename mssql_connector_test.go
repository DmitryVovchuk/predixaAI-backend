package dbconnector

import "testing"

func TestParseMSSQLTable(t *testing.T) {
	schema, name, err := parseMSSQLTable("sales.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema != "sales" || name != "orders" {
		t.Fatalf("unexpected result: %s %s", schema, name)
	}
}

func TestParseMSSQLTableDefaultSchema(t *testing.T) {
	schema, name, err := parseMSSQLTable("orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema != "dbo" || name != "orders" {
		t.Fatalf("unexpected result: %s %s", schema, name)
	}
}

func TestQuoteMSSQLTable(t *testing.T) {
	quoted, err := quoteMSSQLTable("sales.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if quoted != "[sales].[orders]" {
		t.Fatalf("unexpected quote: %s", quoted)
	}
}
