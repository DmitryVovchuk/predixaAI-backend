package mcp

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type AdapterConfig struct {
	Type     string   `yaml:"type"`
	Endpoint string   `yaml:"endpoint"`
	Command  string   `yaml:"command"`
	Args     []string `yaml:"args"`
}

type Config struct {
	Adapters map[string]AdapterConfig `yaml:"adapters"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if len(cfg.Adapters) == 0 {
		return Config{}, fmt.Errorf("no adapters configured")
	}
	return cfg, nil
}

func (c Config) BuildRegistry() (*AdapterRegistry, error) {
	adapters := map[string]DbMcpAdapter{}
	for dbType, cfg := range c.Adapters {
		adapter, err := buildAdapter(strings.ToLower(dbType), cfg)
		if err != nil {
			return nil, err
		}
		adapters[strings.ToLower(dbType)] = adapter
	}
	return NewAdapterRegistry(adapters), nil
}

func buildAdapter(dbType string, cfg AdapterConfig) (DbMcpAdapter, error) {
	transport, err := buildTransport(cfg)
	if err != nil {
		return nil, err
	}
	switch dbType {
	case "mysql":
		return NewMySQLAdapter(transport), nil
	case "postgres", "postgresql":
		return NewPostgresAdapter(transport), nil
	default:
		return nil, fmt.Errorf("unsupported adapter type %q", dbType)
	}
}

func buildTransport(cfg AdapterConfig) (Transport, error) {
	switch strings.ToLower(cfg.Type) {
	case "http":
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("http endpoint required")
		}
		return DefaultHTTPTransport(cfg.Endpoint), nil
	case "stdio":
		if cfg.Command == "" {
			return nil, fmt.Errorf("stdio command required")
		}
		return DefaultStdioTransport(cfg.Command, cfg.Args), nil
	default:
		return nil, fmt.Errorf("unsupported transport type %q", cfg.Type)
	}
}
