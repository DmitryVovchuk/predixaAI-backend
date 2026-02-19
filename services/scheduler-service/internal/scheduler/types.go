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
	SpecLimit   *SpecLimitSpec   `json:"specLimit,omitempty"`
	Shewhart    *ShewhartSpec    `json:"shewhart,omitempty"`
	RangeChart  *RangeChartSpec  `json:"rangeChart,omitempty"`
	Trend       *TrendSpec       `json:"trend,omitempty"`
	TPA         *TPASpec         `json:"tpa,omitempty"`
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

type SpecLimitSpec struct {
	SpecLimits    *SpecLimitBounds    `json:"specLimits,omitempty"`
	ControlLimits *ControlLimitBounds `json:"controlLimits,omitempty"`
	Mode          string              `json:"mode"`
	Epsilon       *float64            `json:"epsilon,omitempty"`
}

type SpecLimitBounds struct {
	USL *float64 `json:"usl,omitempty"`
	LSL *float64 `json:"lsl,omitempty"`
}

type ControlLimitBounds struct {
	UCL *float64 `json:"ucl,omitempty"`
	LCL *float64 `json:"lcl,omitempty"`
}

type ShewhartSpec struct {
	Baseline         BaselineSpec `json:"baseline"`
	SigmaMultiplier float64     `json:"sigmaMultiplier"`
	MinBaselineN    int         `json:"minBaselineN"`
	PopulationSigma bool        `json:"populationSigma"`
}

type BaselineSpec struct {
	LastN     *int           `json:"lastN,omitempty"`
	TimeRange *TimeRangeSpec `json:"timeRange,omitempty"`
}

type TimeRangeSpec struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type RangeChartSpec struct {
	SubgroupSize         int             `json:"subgroupSize"`
	Subgrouping          SubgroupingSpec `json:"subgrouping"`
	Baseline             BaselineSpec    `json:"baseline"`
	MinBaselineSubgroups int             `json:"minBaselineSubgroups"`
}

type SubgroupingSpec struct {
	Mode   string `json:"mode"`
	Column string `json:"column,omitempty"`
}

type TrendSpec struct {
	WindowSize                  int     `json:"windowSize"`
	Epsilon                     float64 `json:"epsilon"`
	RequireConsecutiveTimestamps bool    `json:"requireConsecutiveTimestamps"`
}

type TPASpec struct {
	WindowN             int              `json:"windowN"`
	RegressionTimeBasis string           `json:"regressionTimeBasis"`
	SlopeThreshold      *float64         `json:"slopeThreshold,omitempty"`
	TimeToSpecThreshold *float64         `json:"timeToSpecThreshold,omitempty"`
	RequireSpecLimits   bool             `json:"requireSpecLimits"`
	SpecLimits          *SpecLimitBounds `json:"specLimits,omitempty"`
	Epsilon             float64          `json:"epsilon"`
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
