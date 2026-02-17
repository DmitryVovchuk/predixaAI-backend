package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	dbconnector "predixaai-backend"
	"predixaai-backend/cmd/service/internal/connections"
)

type ConnectorFactory func(cfg dbconnector.ConnectionConfig) (dbconnector.DbConnector, error)

type Handler struct {
	Resolver         connections.Resolver
	ConnectorFactory ConnectorFactory
}

func NewHandler(resolver connections.Resolver, factory ConnectorFactory) *Handler {
	return &Handler{Resolver: resolver, ConnectorFactory: factory}
}

func (h *Handler) HandleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req baseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	connectionCfg, err := h.resolveOptionalConnection(r, req.ConnectionRef, req.Connection)
	if err != nil {
		h.writeConnectionError(w, err)
		return
	}
	conn, err := h.ConnectorFactory(connectionCfg)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	defer conn.Close()
	if err := conn.TestConnection(r.Context()); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Handler) HandleListTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req baseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateStrictConnectionRef(req.ConnectionRef, req.Connection); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	connectionCfg, err := h.resolveByRef(r, req.ConnectionRef)
	if err != nil {
		h.writeConnectionError(w, err)
		return
	}
	conn, err := h.ConnectorFactory(connectionCfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.Close()
	if err := conn.TestConnection(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	tables, err := conn.ListTables(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": tables})
}

func (h *Handler) HandleDescribeTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req tableRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateStrictConnectionRef(req.ConnectionRef, req.Connection); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Table) == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	connectionCfg, err := h.resolveByRef(r, req.ConnectionRef)
	if err != nil {
		h.writeConnectionError(w, err)
		return
	}
	conn, err := h.ConnectorFactory(connectionCfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.Close()
	if err := conn.TestConnection(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	schema, err := conn.DescribeTable(r.Context(), req.Table)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, schema)
}

func (h *Handler) HandleSampleRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req sampleRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Table) == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	connectionCfg, err := h.resolveOptionalConnection(r, req.ConnectionRef, req.Connection)
	if err != nil {
		h.writeConnectionError(w, err)
		return
	}
	conn, err := h.ConnectorFactory(connectionCfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.Close()
	if err := conn.TestConnection(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	rows, err := conn.SampleRows(r.Context(), req.Table, req.Limit)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (h *Handler) HandleProfileTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req profileRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Table) == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	connectionCfg, err := h.resolveOptionalConnection(r, req.ConnectionRef, req.Connection)
	if err != nil {
		h.writeConnectionError(w, err)
		return
	}
	conn, err := h.ConnectorFactory(connectionCfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.Close()
	if err := conn.TestConnection(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	profile, err := conn.ProfileTable(r.Context(), req.Table, req.Options)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (h *Handler) resolveByRef(r *http.Request, connectionRef string) (dbconnector.ConnectionConfig, error) {
	if h.Resolver == nil {
		return dbconnector.ConnectionConfig{}, connections.ErrNotConfigured
	}
	return h.Resolver.ResolveByRef(r.Context(), connectionRef)
}

func (h *Handler) resolveOptionalConnection(r *http.Request, connectionRef string, fallback dbconnector.ConnectionConfig) (dbconnector.ConnectionConfig, error) {
	if strings.TrimSpace(connectionRef) == "" {
		return fallback, nil
	}
	return h.resolveByRef(r, connectionRef)
}

func (h *Handler) writeConnectionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, connections.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, connections.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, connections.ErrNotConfigured):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func validateStrictConnectionRef(ref string, inline dbconnector.ConnectionConfig) error {
	if strings.TrimSpace(ref) == "" {
		return fmt.Errorf("connectionRef is required: %w", connections.ErrInvalidInput)
	}
	if hasInlineConnection(inline) {
		return fmt.Errorf("inline connection not supported; use connectionRef: %w", connections.ErrInvalidInput)
	}
	return nil
}

func hasInlineConnection(cfg dbconnector.ConnectionConfig) bool {
	return strings.TrimSpace(cfg.Type) != "" || strings.TrimSpace(cfg.Host) != "" || cfg.Port != 0 || strings.TrimSpace(cfg.User) != "" || strings.TrimSpace(cfg.Password) != "" || strings.TrimSpace(cfg.Database) != "" || strings.TrimSpace(cfg.SSLMode) != ""
}
