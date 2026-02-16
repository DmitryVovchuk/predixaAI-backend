package mcp

import "context"

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type WhereClause struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  interface{} `json:"value"`
}

type WhereSpec struct {
	Type    string        `json:"type"`
	Clauses []WhereClause `json:"clauses"`
}

type LatestValueRequest struct {
	ConnectionRef   string     `json:"connectionRef"`
	Table           string     `json:"table"`
	ValueColumn     string     `json:"valueColumn"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`
}

type AggregateRequest struct {
	ConnectionRef   string     `json:"connectionRef"`
	Table           string     `json:"table"`
	ValueColumn     string     `json:"valueColumn"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`
	Agg             string     `json:"agg"`
	WindowSeconds   int        `json:"windowSeconds"`
}

type LatestValueResult struct {
	Value any    `json:"value"`
	TS    string `json:"ts"`
}

type AggregateResult struct {
	Value   any    `json:"value"`
	TSStart string `json:"ts_start"`
	TSEnd   string `json:"ts_end"`
}

type Row map[string]any

type FetchRecentRowsRequest struct {
	ConnectionRef   string     `json:"connectionRef"`
	Table           string     `json:"table"`
	Columns         []string   `json:"columns"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`
	Since           string     `json:"since"`
	Limit           int        `json:"limit"`
}

type FetchRecentRowsResult struct {
	Rows []Row `json:"rows"`
}

type Capabilities struct {
	ReadOnly              bool
	SupportsAggregate     bool
	SupportsIntrospection bool
}

type DbMcpAdapter interface {
	Capabilities() Capabilities
	ListTables(ctx context.Context, connRef string) ([]string, error)
	ListColumns(ctx context.Context, connRef, table string) ([]Column, error)
	QueryLatestValue(ctx context.Context, req LatestValueRequest) (LatestValueResult, error)
	QueryAggregate(ctx context.Context, req AggregateRequest) (AggregateResult, error)
	FetchRecentRows(ctx context.Context, req FetchRecentRowsRequest) (FetchRecentRowsResult, error)
}
