package connections

import (
	"context"
	"strings"

	dbconnector "predixaai-backend"
)

type Resolver interface {
	ResolveByRef(ctx context.Context, connectionRef string) (dbconnector.ConnectionConfig, error)
}

type Store interface {
	GetConnection(ctx context.Context, connectionRef string) (dbconnector.ConnectionConfig, error)
}

type resolver struct {
	store Store
}

func NewResolver(store Store) Resolver {
	return &resolver{store: store}
}

func (r *resolver) ResolveByRef(ctx context.Context, connectionRef string) (dbconnector.ConnectionConfig, error) {
	if strings.TrimSpace(connectionRef) == "" {
		return dbconnector.ConnectionConfig{}, ErrInvalidInput
	}
	if r.store == nil {
		return dbconnector.ConnectionConfig{}, ErrNotConfigured
	}
	return r.store.GetConnection(ctx, connectionRef)
}
