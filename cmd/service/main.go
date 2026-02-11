package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predixaai-backend"
)

type baseRequest struct {
	Connection dbconnector.ConnectionConfig `json:"connection"`
}

type tableRequest struct {
	Connection dbconnector.ConnectionConfig `json:"connection"`
	Table      string                       `json:"table"`
}

type sampleRequest struct {
	Connection dbconnector.ConnectionConfig `json:"connection"`
	Table      string                       `json:"table"`
	Limit      int                          `json:"limit"`
}

type profileRequest struct {
	Connection dbconnector.ConnectionConfig `json:"connection"`
	Table      string                       `json:"table"`
	Options    dbconnector.ProfileOptions   `json:"options"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	port := getenv("PORT", "8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/connection/test", handleTestConnection)
	mux.HandleFunc("/tables", handleListTables)
	mux.HandleFunc("/describe", handleDescribeTable)
	mux.HandleFunc("/sample", handleSampleRows)
	mux.HandleFunc("/profile", handleProfileTable)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownErr := make(chan error, 1)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownErr <- server.Shutdown(ctx)
	}()

	log.Printf("db-connector service listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}

	if err := <-shutdownErr; err != nil {
		log.Fatalf("shutdown error: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req baseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	conn, err := dbconnector.NewConnector(req.Connection)
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

func handleListTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req baseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	conn, err := dbconnector.NewConnector(req.Connection)
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

func handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req tableRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	conn, err := dbconnector.NewConnector(req.Connection)
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

func handleSampleRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req sampleRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	conn, err := dbconnector.NewConnector(req.Connection)
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

func handleProfileTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req profileRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	conn, err := dbconnector.NewConnector(req.Connection)
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

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("invalid json payload")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
