package connections

import (
	"context"
	"errors"
	"testing"

	dbconnector "predixaai-backend"
)

type fakeStore struct {
	config dbconnector.ConnectionConfig
	err    error
}

func (f *fakeStore) GetConnection(ctx context.Context, connectionRef string) (dbconnector.ConnectionConfig, error) {
	return f.config, f.err
}

func TestResolverSuccess(t *testing.T) {
	store := &fakeStore{config: dbconnector.ConnectionConfig{Type: "postgres", Host: "db"}}
	resolver := NewResolver(store)

	cfg, err := resolver.ResolveByRef(context.Background(), "abc")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if cfg.Type != "postgres" || cfg.Host != "db" {
		t.Fatalf("unexpected config")
	}
}

func TestResolverNotFound(t *testing.T) {
	store := &fakeStore{err: ErrNotFound}
	resolver := NewResolver(store)

	_, err := resolver.ResolveByRef(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestResolverNotConfigured(t *testing.T) {
	resolver := NewResolver(nil)

	_, err := resolver.ResolveByRef(context.Background(), "ref")
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got %v", err)
	}
}

func TestResolverInvalidInput(t *testing.T) {
	resolver := NewResolver(&fakeStore{})

	_, err := resolver.ResolveByRef(context.Background(), " ")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}
