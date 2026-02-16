package scheduler

type RuleSpec struct {
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	ConnectionRef       string          `json:"connectionRef"`
	Source              SourceSpec      `json:"source"`
	Parameters          []ParameterSpec `json:"parameters"`
	PollIntervalSeconds int             `json:"pollIntervalSeconds"`
	CooldownSeconds     *int            `json:"cooldownSeconds"`
	Enabled             bool            `json:"enabled"`

	// Legacy fields
	ParameterName string        `json:"parameterName,omitempty"`
	Aggregation   string        `json:"aggregation,omitempty"`
	WindowSeconds *int          `json:"windowSeconds,omitempty"`
	Condition     ConditionSpec `json:"condition,omitempty"`
}

type SourceSpec struct {
	Table           string     `json:"table"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`

	// Legacy field
	ValueColumn string `json:"valueColumn,omitempty"`
}

type ParameterSpec struct {
	ParameterName string       `json:"parameterName"`
	ValueColumn   string       `json:"valueColumn"`
	Detector      DetectorSpec `json:"detector"`
}

type DetectorSpec struct {
	Type        string           `json:"type"`
	Threshold   *ThresholdSpec   `json:"threshold,omitempty"`
	RobustZ     *RobustZSpec     `json:"robustZ,omitempty"`
	MissingData *MissingDataSpec `json:"missingData,omitempty"`
}

type ThresholdSpec struct {
	Op    string      `json:"op"`
	Value interface{} `json:"value,omitempty"`
	Min   *float64    `json:"min,omitempty"`
	Max   *float64    `json:"max,omitempty"`
}

type RobustZSpec struct {
	BaselineWindowSeconds int     `json:"baselineWindowSeconds"`
	EvalWindowSeconds     int     `json:"evalWindowSeconds"`
	ZWarn                 float64 `json:"zWarn"`
	ZCrit                 float64 `json:"zCrit"`
	MinSamples            int     `json:"minSamples"`
}

type MissingDataSpec struct {
	MaxGapSeconds int `json:"maxGapSeconds"`
}

type WhereSpec struct {
	Type    string       `json:"type"`
	Clauses []ClauseSpec `json:"clauses"`
}

type ClauseSpec struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  interface{} `json:"value"`
}

type ConditionSpec struct {
	Op    string      `json:"op"`
	Value interface{} `json:"value"`
	Min   *float64    `json:"min"`
	Max   *float64    `json:"max"`
}
