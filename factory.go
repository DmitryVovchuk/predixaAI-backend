// file: factory.go
package dbconnector

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func NewConnector(cfg ConnectionConfig) (DbConnector, error) {
	if strings.TrimSpace(cfg.Type) == "" {
		return nil, errors.New("connection type is required")
	}
	switch strings.ToLower(cfg.Type) {
	case "mysql":
		return newMySQLConnector(cfg)
	case "postgres", "postgresql":
		return newPostgresConnector(cfg)
	case "mssql", "sqlserver":
		return newMSSQLConnector(cfg)
	default:
		return nil, fmt.Errorf("unsupported database type %q", cfg.Type)
	}
}

func openDatabase(driverName, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
