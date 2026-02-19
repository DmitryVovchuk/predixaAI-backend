package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

type dbConnectorClient struct {
	BaseURL string
	Client  *http.Client
}

type describeRequest struct {
	ConnectionRef string `json:"connectionRef"`
	Table         string `json:"table"`
}

type tableSchema struct {
	Columns []columnInfo `json:"columns"`
}

type columnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type schedulerClient struct {
	BaseURL string
	Client  *http.Client
}

func (c dbConnectorClient) DescribeTable(ctx context.Context, connectionRef, table string) (tableSchema, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return tableSchema{}, errors.New("db-connector url not configured")
	}
	client := c.Client
	if client == nil {
		client = defaultHTTPClient(5 * time.Second)
	}
	payload, _ := json.Marshal(describeRequest{ConnectionRef: connectionRef, Table: table})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/describe", bytes.NewReader(payload))
	if err != nil {
		return tableSchema{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return tableSchema{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return tableSchema{}, errors.New("describe failed")
	}
	var schema tableSchema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return tableSchema{}, err
	}
	return schema, nil
}

func (c schedulerClient) PostJSON(ctx context.Context, path string, reqBody any, respBody any) error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return errors.New("scheduler url not configured")
	}
	client := c.Client
	if client == nil {
		client = defaultHTTPClient(5 * time.Second)
	}
	payload, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return errors.New("scheduler request failed")
	}
	return json.NewDecoder(resp.Body).Decode(respBody)
}

func defaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}
