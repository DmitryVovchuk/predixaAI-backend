package mcp

import (
	"context"
	"encoding/json"
	"time"
)

type MySQLAdapter struct {
	Transport Transport
}

func NewMySQLAdapter(transport Transport) *MySQLAdapter {
	return &MySQLAdapter{Transport: transport}
}

func (a *MySQLAdapter) Capabilities() Capabilities {
	return Capabilities{ReadOnly: true, SupportsAggregate: true, SupportsIntrospection: true}
}

func (a *MySQLAdapter) ListTables(ctx context.Context, connRef string) ([]string, error) {
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

func (a *MySQLAdapter) ListColumns(ctx context.Context, connRef, table string) ([]Column, error) {
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

func (a *MySQLAdapter) QueryLatestValue(ctx context.Context, req LatestValueRequest) (LatestValueResult, error) {
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

func (a *MySQLAdapter) QueryAggregate(ctx context.Context, req AggregateRequest) (AggregateResult, error) {
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

func (a *MySQLAdapter) FetchRecentRows(ctx context.Context, req FetchRecentRowsRequest) (FetchRecentRowsResult, error) {
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

type Transport interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
}

func DefaultHTTPTransport(endpoint string) Transport {
	return &HTTPTransport{Endpoint: endpoint, Timeout: 5 * time.Second}
}

func DefaultStdioTransport(cmd string, args []string) Transport {
	return &StdioTransport{Command: cmd, Args: args, Timeout: 5 * time.Second}
}
