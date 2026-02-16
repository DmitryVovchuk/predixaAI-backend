package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"predixaai-backend"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type baseParams struct {
	ConnectionRef string `json:"connectionRef"`
}

type listColumnsParams struct {
	ConnectionRef string `json:"connectionRef"`
	Table         string `json:"table"`
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

type FetchRecentRowsRequest struct {
	ConnectionRef   string     `json:"connectionRef"`
	Table           string     `json:"table"`
	Columns         []string   `json:"columns"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`
	Since           string     `json:"since"`
	Limit           int        `json:"limit"`
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

type FetchRecentRowsResult struct {
	Rows []map[string]any `json:"rows"`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type connectionStore struct {
	dsn       string
	encryptor *aesGcmEncryptor
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	port := getenv("PORT", "9000")
	dsn := getenv("DATABASE_URL", "")
	encKey := getenv("ENCRYPTION_KEY", "")
	mcpType := strings.ToLower(getenv("MCP_DB_TYPE", ""))
	if dsn == "" || encKey == "" || mcpType == "" {
		logger.Error("missing required configuration", slog.String("DATABASE_URL", dsn), slog.String("MCP_DB_TYPE", mcpType))
		os.Exit(1)
	}
	enc, err := newAesGcmEncryptor([]byte(encKey))
	if err != nil {
		logger.Error("invalid encryption key", slog.String("error", err.Error()))
		os.Exit(1)
	}
	store := &connectionStore{dsn: dsn, encryptor: enc}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRPCError(w, nil, http.StatusMethodNotAllowed, -32600, "method not allowed")
			return
		}
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRPCError(w, nil, http.StatusBadRequest, -32700, "invalid json")
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, req.ID, http.StatusBadRequest, -32600, "invalid request")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		switch req.Method {
		case "db.list_tables":
			var params baseParams
			if err := json.Unmarshal(req.Params, &params); err != nil || params.ConnectionRef == "" {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, "invalid params")
				return
			}
			cfg, err := store.getConnection(ctx, params.ConnectionRef, mcpType)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, err.Error())
				return
			}
			connector, err := dbconnector.NewConnector(cfg)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			defer connector.Close()
			tables, err := connector.ListTables(ctx)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			writeRPCResult(w, req.ID, map[string]any{"tables": tables})
		case "db.list_columns":
			var params listColumnsParams
			if err := json.Unmarshal(req.Params, &params); err != nil || params.ConnectionRef == "" || params.Table == "" {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, "invalid params")
				return
			}
			cfg, err := store.getConnection(ctx, params.ConnectionRef, mcpType)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, err.Error())
				return
			}
			connector, err := dbconnector.NewConnector(cfg)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			defer connector.Close()
			schema, err := connector.DescribeTable(ctx, params.Table)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			columns := make([]Column, 0, len(schema.Columns))
			for _, c := range schema.Columns {
				columns = append(columns, Column{Name: c.Name, Type: c.Type})
			}
			writeRPCResult(w, req.ID, map[string]any{"columns": columns})
		case "db.query_latest_value":
			var params LatestValueRequest
			if err := json.Unmarshal(req.Params, &params); err != nil || params.ConnectionRef == "" {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, "invalid params")
				return
			}
			cfg, err := store.getConnection(ctx, params.ConnectionRef, mcpType)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, err.Error())
				return
			}
			result, err := queryLatestValue(ctx, cfg, mcpType, params)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			writeRPCResult(w, req.ID, result)
		case "db.query_aggregate":
			var params AggregateRequest
			if err := json.Unmarshal(req.Params, &params); err != nil || params.ConnectionRef == "" {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, "invalid params")
				return
			}
			cfg, err := store.getConnection(ctx, params.ConnectionRef, mcpType)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, err.Error())
				return
			}
			result, err := queryAggregate(ctx, cfg, mcpType, params)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			writeRPCResult(w, req.ID, result)
		case "db.fetch_recent_rows":
			var params FetchRecentRowsRequest
			if err := json.Unmarshal(req.Params, &params); err != nil || params.ConnectionRef == "" {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, "invalid params")
				return
			}
			cfg, err := store.getConnection(ctx, params.ConnectionRef, mcpType)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusBadRequest, -32602, err.Error())
				return
			}
			result, err := fetchRecentRows(ctx, cfg, mcpType, params)
			if err != nil {
				writeRPCError(w, req.ID, http.StatusInternalServerError, -32603, err.Error())
				return
			}
			writeRPCResult(w, req.ID, result)
		default:
			writeRPCError(w, req.ID, http.StatusNotFound, -32601, "method not found")
		}
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	logger.Info("mcp server listening", slog.String("port", port), slog.String("db_type", mcpType))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func (s *connectionStore) getConnection(ctx context.Context, id string, expectedType string) (dbconnector.ConnectionConfig, error) {
	db, err := openPostgres(ctx, s.dsn)
	if err != nil {
		return dbconnector.ConnectionConfig{}, err
	}
	defer db.Close()

	row := db.QueryRowContext(ctx, `SELECT type, host, port, user_name, password_enc, database FROM db_connections WHERE id=$1`, id)
	var connType, host, userName, passwordEnc, database string
	var port int
	if err := row.Scan(&connType, &host, &port, &userName, &passwordEnc, &database); err != nil {
		return dbconnector.ConnectionConfig{}, errors.New("connection not found")
	}
	if strings.ToLower(connType) != expectedType {
		return dbconnector.ConnectionConfig{}, fmt.Errorf("connection type %s not supported by %s server", connType, expectedType)
	}
	password, err := s.encryptor.Decrypt(passwordEnc)
	if err != nil {
		return dbconnector.ConnectionConfig{}, errors.New("failed to decrypt password")
	}
	return dbconnector.ConnectionConfig{
		Type:     connType,
		Host:     host,
		Port:     port,
		User:     userName,
		Password: password,
		Database: database,
		SSLMode:  "disable",
	}, nil
}

func openPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openTargetDB(ctx context.Context, cfg dbconnector.ConnectionConfig) (*sql.DB, error) {
	switch strings.ToLower(cfg.Type) {
	case "mysql":
		if cfg.Port == 0 {
			cfg.Port = 3306
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		if strings.EqualFold(cfg.SSLMode, "disable") {
			dsn += "&tls=false"
		}
		return sql.Open("mysql", dsn)
	case "postgres", "postgresql":
		if cfg.Port == 0 {
			cfg.Port = 5432
		}
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		return sql.Open("postgres", dsn)
	default:
		return nil, fmt.Errorf("unsupported db type %s", cfg.Type)
	}
}

func queryLatestValue(ctx context.Context, cfg dbconnector.ConnectionConfig, dbType string, req LatestValueRequest) (LatestValueResult, error) {
	if !isSafeIdentifier(req.Table) || !isSafeIdentifier(req.ValueColumn) || !isSafeIdentifier(req.TimestampColumn) {
		return LatestValueResult{}, errors.New("unsafe identifier")
	}
	if req.Table == "" || req.ValueColumn == "" || req.TimestampColumn == "" {
		return LatestValueResult{}, errors.New("missing fields")
	}
	table, err := quoteIdent(dbType, req.Table)
	if err != nil {
		return LatestValueResult{}, err
	}
	valueCol, err := quoteIdent(dbType, req.ValueColumn)
	if err != nil {
		return LatestValueResult{}, err
	}
	tsCol, err := quoteIdent(dbType, req.TimestampColumn)
	if err != nil {
		return LatestValueResult{}, err
	}
	whereSQL, args, _, err := buildWhereClause(dbType, req.Where, 1)
	if err != nil {
		return LatestValueResult{}, err
	}
	query := fmt.Sprintf("SELECT %s, %s FROM %s", valueCol, tsCol, table)
	if whereSQL != "" {
		query += " WHERE " + whereSQL
	}
	query += fmt.Sprintf(" ORDER BY %s DESC LIMIT 1", tsCol)

	db, err := openTargetDB(ctx, cfg)
	if err != nil {
		return LatestValueResult{}, err
	}
	defer db.Close()

	row := db.QueryRowContext(ctx, query, args...)
	var value any
	var ts any
	if err := row.Scan(&value, &ts); err != nil {
		return LatestValueResult{}, err
	}
	return LatestValueResult{Value: normalizeValue(value), TS: fmt.Sprint(ts)}, nil
}

func queryAggregate(ctx context.Context, cfg dbconnector.ConnectionConfig, dbType string, req AggregateRequest) (AggregateResult, error) {
	if !isSafeIdentifier(req.Table) || !isSafeIdentifier(req.ValueColumn) || !isSafeIdentifier(req.TimestampColumn) {
		return AggregateResult{}, errors.New("unsafe identifier")
	}
	if req.WindowSeconds <= 0 {
		return AggregateResult{}, errors.New("windowSeconds required")
	}
	agg := normalizeAgg(req.Agg)
	if agg == "" {
		return AggregateResult{}, errors.New("unsupported aggregation")
	}
	table, err := quoteIdent(dbType, req.Table)
	if err != nil {
		return AggregateResult{}, err
	}
	valueCol, err := quoteIdent(dbType, req.ValueColumn)
	if err != nil {
		return AggregateResult{}, err
	}
	tsCol, err := quoteIdent(dbType, req.TimestampColumn)
	if err != nil {
		return AggregateResult{}, err
	}

	since := time.Now().Add(-time.Duration(req.WindowSeconds) * time.Second)
	whereSQL, args, _, err := buildWhereClause(dbType, req.Where, 2)
	if err != nil {
		return AggregateResult{}, err
	}
	timeClause := fmt.Sprintf("%s >= %s", tsCol, placeholder(dbType, 1))
	clauses := []string{timeClause}
	if whereSQL != "" {
		clauses = append(clauses, "("+whereSQL+")")
	}
	where := "WHERE " + strings.Join(clauses, " AND ")

	aggExpr := "COUNT(*)"
	if agg != "count" {
		aggExpr = fmt.Sprintf("%s(%s)", agg, valueCol)
	}

	query := fmt.Sprintf("SELECT %s, MIN(%s), MAX(%s) FROM %s %s", aggExpr, tsCol, tsCol, table, where)

	db, err := openTargetDB(ctx, cfg)
	if err != nil {
		return AggregateResult{}, err
	}
	defer db.Close()

	args = append([]any{since}, args...)
	row := db.QueryRowContext(ctx, query, args...)
	var value any
	var tsStart any
	var tsEnd any
	if err := row.Scan(&value, &tsStart, &tsEnd); err != nil {
		return AggregateResult{}, err
	}
	return AggregateResult{Value: normalizeValue(value), TSStart: fmt.Sprint(tsStart), TSEnd: fmt.Sprint(tsEnd)}, nil
}

func fetchRecentRows(ctx context.Context, cfg dbconnector.ConnectionConfig, dbType string, req FetchRecentRowsRequest) (FetchRecentRowsResult, error) {
	if req.Since == "" {
		return FetchRecentRowsResult{}, errors.New("since required")
	}
	if !isSafeIdentifier(req.Table) || !isSafeIdentifier(req.TimestampColumn) {
		return FetchRecentRowsResult{}, errors.New("unsafe identifier")
	}
	if len(req.Columns) == 0 {
		return FetchRecentRowsResult{}, errors.New("columns required")
	}
	limit := req.Limit
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	parsedSince, err := time.Parse(time.RFC3339, req.Since)
	if err != nil {
		parsedSince, err = time.Parse(time.RFC3339Nano, req.Since)
		if err != nil {
			return FetchRecentRowsResult{}, errors.New("invalid since timestamp")
		}
	}

	table, err := quoteIdent(dbType, req.Table)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	tsCol, err := quoteIdent(dbType, req.TimestampColumn)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	selectCols := make([]string, 0, len(req.Columns)+1)
	colNames := make([]string, 0, len(req.Columns)+1)
	seen := map[string]struct{}{}
	for _, col := range req.Columns {
		if !isSafeIdentifier(col) {
			return FetchRecentRowsResult{}, errors.New("unsafe identifier")
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		quoted, err := quoteIdent(dbType, col)
		if err != nil {
			return FetchRecentRowsResult{}, err
		}
		selectCols = append(selectCols, quoted)
		colNames = append(colNames, col)
	}
	if _, ok := seen[req.TimestampColumn]; !ok {
		selectCols = append(selectCols, tsCol)
		colNames = append(colNames, req.TimestampColumn)
	}
	whereSQL, args, _, err := buildWhereClause(dbType, req.Where, 2)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	timeClause := fmt.Sprintf("%s >= %s", tsCol, placeholder(dbType, 1))
	clauses := []string{timeClause}
	if whereSQL != "" {
		clauses = append(clauses, "("+whereSQL+")")
	}
	where := "WHERE " + strings.Join(clauses, " AND ")
	query := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s DESC LIMIT %d", strings.Join(selectCols, ", "), table, where, tsCol, limit)

	db, err := openTargetDB(ctx, cfg)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	defer db.Close()

	args = append([]any{parsedSince}, args...)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return FetchRecentRowsResult{}, err
	}
	defer rows.Close()

	results := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return FetchRecentRowsResult{}, err
		}
		row := map[string]any{}
		for i, name := range colNames {
			row[name] = normalizeValue(values[i])
		}
		results = append(results, row)
	}
	return FetchRecentRowsResult{Rows: results}, nil
}

func buildWhereClause(dbType string, where *WhereSpec, startIndex int) (string, []any, int, error) {
	if where == nil || len(where.Clauses) == 0 {
		return "", nil, startIndex, nil
	}
	joiner := "AND"
	if strings.EqualFold(where.Type, "or") {
		joiner = "OR"
	}
	clauses := []string{}
	args := []any{}
	idx := startIndex
	for _, clause := range where.Clauses {
		if !isSafeIdentifier(clause.Column) {
			return "", nil, idx, errors.New("unsafe where identifier")
		}
		op, err := normalizeOp(clause.Op)
		if err != nil {
			return "", nil, idx, err
		}
		col, err := quoteIdent(dbType, clause.Column)
		if err != nil {
			return "", nil, idx, err
		}
		if op == "IN" {
			values, ok := sliceValues(clause.Value)
			if !ok || len(values) == 0 {
				return "", nil, idx, errors.New("invalid IN values")
			}
			placeholders := make([]string, 0, len(values))
			for range values {
				placeholders = append(placeholders, placeholder(dbType, idx))
				idx++
			}
			clauses = append(clauses, fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", ")))
			args = append(args, values...)
			continue
		}
		ph := placeholder(dbType, idx)
		idx++
		clauses = append(clauses, fmt.Sprintf("%s %s %s", col, op, ph))
		args = append(args, clause.Value)
	}
	return strings.Join(clauses, " "+joiner+" "), args, idx, nil
}

func normalizeOp(op string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "=", "==":
		return "=", nil
	case "!=", "<>":
		return "!=", nil
	case ">", ">=", "<", "<=":
		return op, nil
	case "like":
		return "LIKE", nil
	case "in":
		return "IN", nil
	default:
		return "", errors.New("unsupported operator")
	}
}

func normalizeAgg(agg string) string {
	switch strings.ToLower(strings.TrimSpace(agg)) {
	case "avg", "min", "max", "sum", "count":
		return strings.ToLower(agg)
	default:
		return ""
	}
}

func placeholder(dbType string, idx int) string {
	if strings.HasPrefix(strings.ToLower(dbType), "post") {
		return fmt.Sprintf("$%d", idx)
	}
	return "?"
}

func quoteIdent(dbType, name string) (string, error) {
	if !isSafeIdentifier(name) {
		return "", errors.New("unsafe identifier")
	}
	if strings.HasPrefix(strings.ToLower(dbType), "post") {
		return `"` + name + `"`, nil
	}
	return "`" + name + "`", nil
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func isSafeIdentifier(value string) bool {
	return identPattern.MatchString(value)
}

func sliceValues(value any) ([]any, bool) {
	switch v := value.(type) {
	case []any:
		return v, true
	case []string:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, item)
		}
		return out, true
	case []int:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, item)
		}
		return out, true
	case []float64:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, item)
		}
		return out, true
	default:
		return nil, false
	}
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func writeRPCResult(w http.ResponseWriter, id any, result any) {
	resp := rpcResponse{JSONRPC: "2.0", ID: id, Result: result}
	writeRPC(w, http.StatusOK, resp)
}

func writeRPCError(w http.ResponseWriter, id any, status int, code int, message string) {
	resp := rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}}
	writeRPC(w, status, resp)
}

func writeRPC(w http.ResponseWriter, status int, resp rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func getenv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

type aesGcmEncryptor struct {
	key []byte
}

func newAesGcmEncryptor(key []byte) (*aesGcmEncryptor, error) {
	if len(key) != 32 {
		return nil, errors.New("encryption key must be 32 bytes")
	}
	return &aesGcmEncryptor{key: key}, nil
}

func (e *aesGcmEncryptor) Decrypt(cipherText string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(data) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce := data[:gcm.NonceSize()]
	ciphertext := data[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
