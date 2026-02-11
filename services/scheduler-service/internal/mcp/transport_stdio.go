package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"time"
)

type StdioTransport struct {
	Command string
	Args    []string
	Timeout time.Duration
}

func (t *StdioTransport) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	payload := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, t.Command, t.Args...)
	cmd.Stdin = strings.NewReader(string(data))
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  any             `json:"error"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New("mcp error")
	}
	return resp.Result, nil
}
