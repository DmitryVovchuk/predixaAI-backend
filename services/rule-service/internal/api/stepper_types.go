package api

import "encoding/json"

type FieldError struct {
	Field   string `json:"field"`
	Problem string `json:"problem"`
	Hint    string `json:"hint,omitempty"`
}

type validationErrorResponse struct {
	Code        string       `json:"code"`
	Message     string       `json:"message"`
	FieldErrors []FieldError `json:"fieldErrors"`
}

type catalogResponse struct {
	Version string        `json:"version"`
	Types   []catalogType `json:"types"`
}

type catalogType struct {
	Type                string          `json:"type"`
	Title               string          `json:"title"`
	Description         string          `json:"description"`
	Phase               int             `json:"phase"`
	Category            string          `json:"category"`
	RequiresBaseline    bool            `json:"requiresBaseline"`
	SupportsSubgrouping bool            `json:"supportsSubgrouping"`
	MinData             minDataSpec     `json:"minData"`
	RequiredInputs      []string        `json:"requiredInputs"`
	ConfigSchema        configSchema    `json:"configSchema"`
	Examples            []catalogExample `json:"examples"`
}

type minDataSpec struct {
	MinBaselineSamples  int `json:"minBaselineSamples"`
	MinBaselineSubgroups int `json:"minBaselineSubgroups"`
	MinEvalSamples      int `json:"minEvalSamples"`
}

type configSchema struct {
	Fields []configField `json:"fields"`
}

type configField struct {
	Key         string      `json:"key"`
	Label       string      `json:"label"`
	Type        string      `json:"type"`
	Required    bool        `json:"required"`
	Default     interface{} `json:"default,omitempty"`
	EnumOptions []string    `json:"enumOptions,omitempty"`
	Min         *float64    `json:"min,omitempty"`
	Max         *float64    `json:"max,omitempty"`
	HelpText    string      `json:"helpText,omitempty"`
	VisibleWhen *visibleWhen `json:"visibleWhen,omitempty"`
}

type visibleWhen struct {
	Field string      `json:"field"`
	Is    interface{} `json:"is"`
}

type catalogExample struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type selectorSpec struct {
	Kind  string `json:"kind"`
	Value int    `json:"value,omitempty"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

type subgroupSpec struct {
	Kind         string `json:"kind"`
	Column       string `json:"column,omitempty"`
	SubgroupSize int    `json:"subgroupSize,omitempty"`
}

type parameterResponse struct {
	ParameterID               string   `json:"parameterId"`
	UnitName                  string   `json:"unitName"`
	Table                     string   `json:"table"`
	ValueColumn               string   `json:"valueColumn"`
	DataType                  string   `json:"dataType"`
	TimestampColumn           string   `json:"timestampColumn"`
	SubgroupCandidateColumns  []string `json:"subgroupCandidateColumns"`
	SupportsTrend             bool     `json:"supportsTrend"`
	SupportsShewhart          bool     `json:"supportsShewhart"`
	SupportsRangeChart        bool     `json:"supportsRangeChart"`
	Notes                     []string `json:"notes"`
}

type unitParametersResponse struct {
	UnitID     string              `json:"unitId"`
	Parameters []parameterResponse `json:"parameters"`
}

type baselineCheckRequest struct {
	UnitID          string      `json:"unitId"`
	ParameterID     string      `json:"parameterId"`
	RuleType        string      `json:"ruleType"`
	ConnectionRef   string      `json:"connectionRef"`
	BaselineSelector selectorSpec `json:"baselineSelector"`
	Subgrouping     *subgroupSpec `json:"subgrouping"`
}

type baselineCheckResponse struct {
	Status      string           `json:"status"`
	Available   map[string]int   `json:"available"`
	Required    map[string]int   `json:"required"`
	Continuity continuitySummary `json:"continuity"`
	Messages    []string         `json:"messages"`
}

type continuitySummary struct {
	GapsDetected       bool    `json:"gapsDetected"`
	LargestGapSeconds  float64 `json:"largestGapSeconds"`
}

type previewRequest struct {
	UnitID          string        `json:"unitId"`
	ParameterID     string        `json:"parameterId"`
	RuleType        string        `json:"ruleType"`
	ConnectionRef   string        `json:"connectionRef"`
	Config          json.RawMessage `json:"config"`
	BaselineSelector *selectorSpec `json:"baselineSelector,omitempty"`
	EvalSelector    *selectorSpec `json:"evalSelector,omitempty"`
	Subgrouping     *subgroupSpec `json:"subgrouping,omitempty"`
}

type previewResponse struct {
	Status    string                 `json:"status"`
	Window    map[string]string      `json:"window"`
	Baseline  map[string]interface{} `json:"baseline"`
	Computed  map[string]interface{} `json:"computed"`
	Violations []map[string]interface{} `json:"violations"`
	Explain   string                 `json:"explain"`
}

type stepperRuleRequest struct {
	UnitID      string          `json:"unitId"`
	Name        string          `json:"name"`
	RuleType    string          `json:"ruleType"`
	ParameterID string          `json:"parameterId"`
	Enabled     *bool           `json:"enabled"`
	Config      json.RawMessage `json:"config"`
}

type stepperRuleResponse struct {
	ID         string          `json:"id"`
	UnitID     string          `json:"unitId"`
	Name       string          `json:"name"`
	RuleType   string          `json:"ruleType"`
	ParameterID string         `json:"parameterId"`
	Enabled    bool            `json:"enabled"`
	Config     json.RawMessage `json:"config"`
	CreatedAt  string          `json:"createdAt"`
	UpdatedAt  string          `json:"updatedAt"`
}

type ruleHealthResponse struct {
	UnitID        string              `json:"unitId"`
	WarningsCount int                 `json:"warningsCount"`
	ErrorsCount   int                 `json:"errorsCount"`
	Items         []ruleHealthItem    `json:"items"`
}

type ruleHealthItem struct {
	Severity    string `json:"severity"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	RuleID      string `json:"ruleId"`
	ParameterID string `json:"parameterId"`
}
