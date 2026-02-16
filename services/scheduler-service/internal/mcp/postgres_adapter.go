package mcp

import (
	"context"
	"encoding/json"
)

type PostgresAdapter struct {
	Transport Transport
}

func NewPostgresAdapter(transport Transport) *PostgresAdapter {
	return &PostgresAdapter{Transport: transport}
}

func (a *PostgresAdapter) Capabilities() Capabilities {
	return Capabilities{ReadOnly: true, SupportsAggregate: true, SupportsIntrospection: true}
}

func (a *PostgresAdapter) ListTables(ctx context.Context, connRef string) ([]string, error) {
	resp, err := a.Transport.Call(ctx, "db.list_tables", map[string]any{"connectionRef": connRef})
	if err != nil {
		return nil, err
	}
	var result struct {
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result.Tables, nil
}

func (a *PostgresAdapter) ListColumns(ctx context.Context, connRef, table string) ([]Column, error) {
	resp, err := a.Transport.Call(ctx, "db.list_columns", map[string]any{"connectionRef": connRef, "table": table})
	if err != nil {
		return nil, err
	}
	var result struct {
		Columns []Column `json:"columns"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result.Columns, nil
}

func (a *PostgresAdapter) QueryLatestValue(ctx context.Context, req LatestValueRequest) (LatestValueResult, error) {
	resp, err := a.Transport.Call(ctx, "db.query_latest_value", req)
	if err != nil {
		return LatestValueResult{}, err
	}
	var result LatestValueResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return LatestValueResult{}, err
	}
	return result, nil
}

func (a *PostgresAdapter) QueryAggregate(ctx context.Context, req AggregateRequest) (AggregateResult, error) {
	resp, err := a.Transport.Call(ctx, "db.query_aggregate", req)
	if err != nil {
		return AggregateResult{}, err
	}
	var result AggregateResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return AggregateResult{}, err
	}
	return result, nil
}

func (a *PostgresAdapter) FetchRecentRows(ctx context.Context, req FetchRecentRowsRequest) (FetchRecentRowsResult, error) {
	resp, err := a.Transport.Call(ctx, "db.fetch_recent_rows", req)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	var result FetchRecentRowsResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return FetchRecentRowsResult{}, err
	}
	return result, nil
}
