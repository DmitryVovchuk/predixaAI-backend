package mcp

import "context"

type MockAdapter struct {
	Tables       []string
	Columns      map[string][]Column
	LatestResult LatestValueResult
	AggResult    AggregateResult
	Err          error
}

func (m *MockAdapter) Capabilities() Capabilities {
	return Capabilities{ReadOnly: true, SupportsAggregate: true, SupportsIntrospection: true}
}

func (m *MockAdapter) ListTables(ctx context.Context, connectionRef string) ([]string, error) {
	return m.Tables, m.Err
}

func (m *MockAdapter) ListColumns(ctx context.Context, connectionRef, table string) ([]Column, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Columns[table], nil
}

func (m *MockAdapter) QueryLatestValue(ctx context.Context, req LatestValueRequest) (LatestValueResult, error) {
	return m.LatestResult, m.Err
}

func (m *MockAdapter) QueryAggregate(ctx context.Context, req AggregateRequest) (AggregateResult, error) {
	return m.AggResult, m.Err
}
