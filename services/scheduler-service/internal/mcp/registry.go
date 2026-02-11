package mcp

import (
	"fmt"
	"strings"
)

type AdapterRegistry struct {
	adapters map[string]DbMcpAdapter
}

func NewAdapterRegistry(adapters map[string]DbMcpAdapter) *AdapterRegistry {
	normalized := map[string]DbMcpAdapter{}
	for key, adapter := range adapters {
		normalized[strings.ToLower(key)] = adapter
	}
	return &AdapterRegistry{adapters: normalized}
}

func (r *AdapterRegistry) AdapterFor(dbType string) (DbMcpAdapter, error) {
	if r == nil {
		return nil, fmt.Errorf("adapter registry not configured")
	}
	adapter, ok := r.adapters[strings.ToLower(dbType)]
	if !ok {
		return nil, fmt.Errorf("no adapter configured for %s", dbType)
	}
	return adapter, nil
}
