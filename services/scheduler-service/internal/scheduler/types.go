package scheduler

type RuleSpec struct {
	Name                 string            `json:"name"`
	Description          string            `json:"description"`
	ConnectionRef        string            `json:"connectionRef"`
	Source               SourceSpec        `json:"source"`
	ParameterName        string            `json:"parameterName"`
	Aggregation          string            `json:"aggregation"`
	WindowSeconds        *int              `json:"windowSeconds"`
	Condition            ConditionSpec     `json:"condition"`
	PollIntervalSeconds  int               `json:"pollIntervalSeconds"`
	CooldownSeconds      *int              `json:"cooldownSeconds"`
	Enabled              bool              `json:"enabled"`
}

type SourceSpec struct {
	Table           string     `json:"table"`
	ValueColumn     string     `json:"valueColumn"`
	TimestampColumn string     `json:"timestampColumn"`
	Where           *WhereSpec `json:"where"`
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
