package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("failed to connect", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()

	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		logger.Error("failed to list migrations", slog.String("error", err.Error()))
		os.Exit(1)
	}
	sort.Strings(files)
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			logger.Error("failed to read migration", slog.String("file", file), slog.String("error", err.Error()))
			os.Exit(1)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			logger.Error("failed to apply migration", slog.String("file", file), slog.String("error", err.Error()))
			os.Exit(1)
		}
		logger.Info("applied migration", slog.String("file", file))
	}
}
