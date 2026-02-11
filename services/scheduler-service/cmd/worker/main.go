package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"predixaai-backend/services/scheduler-service/internal/bus"
	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/scheduler"
	"predixaai-backend/services/scheduler-service/internal/security"
	"predixaai-backend/services/scheduler-service/internal/storage"
	"predixaai-backend/services/scheduler-service/internal/validation"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/rules?sslmode=disable")
	natsURL := getenv("NATS_URL", "nats://localhost:4222")
	workers := getenvInt("WORKER_COUNT", 4)
	jobTimeout := time.Duration(getenvInt("JOB_TIMEOUT_SECONDS", 10)) * time.Second
	adminPort := getenv("ADMIN_PORT", "8091")
	mcpConfigPath := getenv("MCP_CONFIG_PATH", "")
	allowlistTables := splitCSV(getenv("ALLOWLIST_TABLES", ""))
	limits := security.DefaultLimits()

	store, err := storage.NewStore(ctx, dsn)
	if err != nil {
		logger.Error("failed to connect to db", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer store.Close()
	repo := storage.NewRepository(store)

	subscriber, err := bus.NewSubscriber(natsURL)
	if err != nil {
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer subscriber.Close()

	adapterRegistry, err := buildAdapterRegistry(mcpConfigPath)
	if err != nil {
		logger.Error("failed to configure MCP adapters", slog.String("error", err.Error()))
		os.Exit(1)
	}
	allowlist := security.Allowlist{Tables: allowlistTables}

	reg := scheduler.NewRegistry(repo, limits, workers, jobTimeout)
	defer reg.Stop()

	if err := reconcile(ctx, repo, reg, adapterRegistry, allowlist, limits); err != nil {
		logger.Error("reconcile error", slog.String("error", err.Error()))
	}

	go startAdminServer(adminPort, repo, reg, adapterRegistry, allowlist, limits, logger)

	subscribeEvents(subscriber, repo, reg, adapterRegistry, allowlist, limits, logger)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
}

func subscribeEvents(sub *bus.Subscriber, repo *storage.Repository, reg *scheduler.Registry, registry *mcp.AdapterRegistry, allowlist security.Allowlist, limits security.Limits, logger *slog.Logger) {
	subscribe := func(subject string) {
		_, _ = sub.Subscribe(subject, func(evt bus.Event) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := processRule(ctx, repo, reg, registry, allowlist, limits, evt.RuleID); err != nil {
				logger.Error("rule event processing failed", slog.String("subject", subject), slog.String("error", err.Error()))
			}
		})
	}
	subscribe("rule.created")
	subscribe("rule.updated")
	subscribe("rule.enabled")
	subscribe("rule.disabled")
	subscribe("rule.deleted")
}

func startAdminServer(port string, repo *storage.Repository, reg *scheduler.Registry, registry *mcp.AdapterRegistry, allowlist security.Allowlist, limits security.Limits, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(reg.ListJobs())
	})
	mux.HandleFunc("/jobs/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := reconcile(ctx, repo, reg, registry, allowlist, limits); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	logger.Info("scheduler admin server listening", slog.String("port", port))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("admin server error", slog.String("error", err.Error()))
	}
}

func reconcile(ctx context.Context, repo *storage.Repository, reg *scheduler.Registry, registry *mcp.AdapterRegistry, allowlist security.Allowlist, limits security.Limits) error {
	rulesList, err := repo.ListEnabledRules(ctx)
	if err != nil {
		return err
	}
	for _, rec := range rulesList {
		_ = processRule(ctx, repo, reg, registry, allowlist, limits, rec.ID)
	}
	return nil
}

func processRule(ctx context.Context, repo *storage.Repository, reg *scheduler.Registry, registry *mcp.AdapterRegistry, allowlist security.Allowlist, limits security.Limits, ruleID string) error {
	rec, err := repo.GetRule(ctx, ruleID)
	if err != nil {
		return err
	}
	if !rec.Enabled {
		reg.Unschedule(ruleID)
		return nil
	}
	var spec scheduler.RuleSpec
	if err := json.Unmarshal(rec.RuleJSON, &spec); err != nil {
		_ = repo.UpdateRuleStatus(ctx, ruleID, "INVALID", []byte(`{"error":"invalid rule json"}`))
		return err
	}
	connType, err := repo.GetConnectionType(ctx, spec.ConnectionRef)
	if err != nil {
		errJSON, _ := json.Marshal(map[string]any{"error": "connection not found"})
		_ = repo.UpdateRuleStatus(ctx, ruleID, "INVALID", errJSON)
		reg.Unschedule(ruleID)
		return err
	}
	if registry == nil {
		err := errors.New("adapter registry not configured")
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		_ = repo.UpdateRuleStatus(ctx, ruleID, "INVALID", errJSON)
		reg.Unschedule(ruleID)
		return err
	}
	adapter, err := registry.AdapterFor(connType)
	if err != nil {
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		_ = repo.UpdateRuleStatus(ctx, ruleID, "INVALID", errJSON)
		reg.Unschedule(ruleID)
		return err
	}
	if err := validation.RuntimeValidateRule(ctx, adapter, spec, allowlist, limits); err != nil {
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		_ = repo.UpdateRuleStatus(ctx, ruleID, "INVALID", errJSON)
		reg.Unschedule(ruleID)
		return err
	}
	_ = repo.UpdateRuleStatus(ctx, ruleID, "ACTIVE", nil)
	reg.Schedule(ruleID, spec, adapter)
	return nil
}

func getenv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func getenvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	if parsed, err := strconv.Atoi(val); err == nil {
		return parsed
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	results := []string{}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		results = append(results, trimmed)
	}
	return results
}

func buildAdapterRegistry(configPath string) (*mcp.AdapterRegistry, error) {
	if configPath != "" {
		cfg, err := mcp.LoadConfig(configPath)
		if err != nil {
			return nil, err
		}
		return cfg.BuildRegistry()
	}
	adapters := map[string]mcp.DbMcpAdapter{}
	postgresEndpoint := getenv("MCP_POSTGRES_HTTP", "")
	if postgresEndpoint != "" {
		adapters["postgres"] = mcp.NewPostgresAdapter(mcp.DefaultHTTPTransport(postgresEndpoint))
	}
	mysqlEndpoint := getenv("MCP_MYSQL_HTTP", "")
	if mysqlEndpoint != "" {
		adapters["mysql"] = mcp.NewMySQLAdapter(mcp.DefaultHTTPTransport(mysqlEndpoint))
	}
	if len(adapters) == 0 {
		return nil, errors.New("no MCP adapters configured")
	}
	return mcp.NewAdapterRegistry(adapters), nil
}
