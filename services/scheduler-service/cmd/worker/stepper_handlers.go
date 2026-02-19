package main

import (
	"encoding/json"
	"net/http"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/scheduler"
	"predixaai-backend/services/scheduler-service/internal/security"
	"predixaai-backend/services/scheduler-service/internal/storage"
)

func registerStepperHandlers(mux *http.ServeMux, repo *storage.Repository, registry *mcp.AdapterRegistry, allowlist security.Allowlist, limits security.Limits) {
	mux.HandleFunc("/api/rules/baseline/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req scheduler.StepperBaselineRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid payload")
			return
		}
		adapter, err := adapterForConnection(r, repo, registry, req.ConnectionRef)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := scheduler.StepperBaselineCheck(r.Context(), adapter, allowlist, limits, req)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeAdminJSON(w, http.StatusOK, resp)
	})

	mux.HandleFunc("/api/rules/preview", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req scheduler.StepperPreviewRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid payload")
			return
		}
		adapter, err := adapterForConnection(r, repo, registry, req.ConnectionRef)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := scheduler.StepperPreview(r.Context(), adapter, allowlist, limits, req)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeAdminJSON(w, http.StatusOK, resp)
	})
}

func adapterForConnection(r *http.Request, repo *storage.Repository, registry *mcp.AdapterRegistry, connectionRef string) (mcp.DbMcpAdapter, error) {
	if registry == nil {
		return nil, errInvalidConfig("adapter registry not configured")
	}
	connType, err := repo.GetConnectionType(r.Context(), connectionRef)
	if err != nil {
		return nil, errInvalidConfig("connection not found")
	}
	adapter, err := registry.AdapterFor(connType)
	if err != nil {
		return nil, errInvalidConfig("adapter not configured")
	}
	return adapter, nil
}

func writeAdminError(w http.ResponseWriter, status int, message string) {
	writeAdminJSON(w, status, map[string]any{"ok": false, "error": message})
}

func writeAdminJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type errInvalidConfig string

func (e errInvalidConfig) Error() string { return string(e) }
