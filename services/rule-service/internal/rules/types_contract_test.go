package rules

import (
	"encoding/json"
	"testing"
)

func TestRuleSpecContractPhase1Detectors(t *testing.T) {
	cases := []struct {
		name string
		json string
		assert func(t *testing.T, spec RuleSpec)
	}{
		{
			name: "spec_limit",
			json: `{"connectionRef":"conn","source":{"table":"telemetry","timestampColumn":"ts"},"parameters":[{"parameterName":"temp","valueColumn":"temp","detector":{"type":"spec_limit","specLimit":{"mode":"spec","specLimits":{"usl":10}}}}],"pollIntervalSeconds":10,"enabled":true}`,
			assert: func(t *testing.T, spec RuleSpec) {
				if spec.Parameters[0].Detector.SpecLimit == nil || spec.Parameters[0].Detector.SpecLimit.SpecLimits.USL == nil {
					t.Fatalf("expected spec limit USL")
				}
			},
		},
		{
			name: "shewhart",
			json: `{"connectionRef":"conn","source":{"table":"telemetry","timestampColumn":"ts"},"parameters":[{"parameterName":"temp","valueColumn":"temp","detector":{"type":"shewhart","shewhart":{"baseline":{"lastN":50},"sigmaMultiplier":3}}}],"pollIntervalSeconds":10,"enabled":true}`,
			assert: func(t *testing.T, spec RuleSpec) {
				if spec.Parameters[0].Detector.Shewhart == nil || spec.Parameters[0].Detector.Shewhart.Baseline.LastN == nil {
					t.Fatalf("expected shewhart baseline lastN")
				}
			},
		},
		{
			name: "range_chart",
			json: `{"connectionRef":"conn","source":{"table":"telemetry","timestampColumn":"ts"},"parameters":[{"parameterName":"temp","valueColumn":"temp","detector":{"type":"range_chart","rangeChart":{"subgroupSize":2,"subgrouping":{"mode":"consecutive"},"baseline":{"lastN":50}}}}],"pollIntervalSeconds":10,"enabled":true}`,
			assert: func(t *testing.T, spec RuleSpec) {
				if spec.Parameters[0].Detector.RangeChart == nil || spec.Parameters[0].Detector.RangeChart.SubgroupSize != 2 {
					t.Fatalf("expected range chart subgroup size")
				}
			},
		},
		{
			name: "trend",
			json: `{"connectionRef":"conn","source":{"table":"telemetry","timestampColumn":"ts"},"parameters":[{"parameterName":"temp","valueColumn":"temp","detector":{"type":"trend","trend":{"windowSize":6,"epsilon":0}}}],"pollIntervalSeconds":10,"enabled":true}`,
			assert: func(t *testing.T, spec RuleSpec) {
				if spec.Parameters[0].Detector.Trend == nil || spec.Parameters[0].Detector.Trend.WindowSize != 6 {
					t.Fatalf("expected trend window size")
				}
			},
		},
		{
			name: "tpa",
			json: `{"connectionRef":"conn","source":{"table":"telemetry","timestampColumn":"ts"},"parameters":[{"parameterName":"temp","valueColumn":"temp","detector":{"type":"tpa","tpa":{"windowN":5,"regressionTimeBasis":"index","slopeThreshold":0.5}}}],"pollIntervalSeconds":10,"enabled":true}`,
			assert: func(t *testing.T, spec RuleSpec) {
				if spec.Parameters[0].Detector.TPA == nil || spec.Parameters[0].Detector.TPA.WindowN != 5 {
					t.Fatalf("expected tpa windowN")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var spec RuleSpec
			if err := json.Unmarshal([]byte(tc.json), &spec); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if len(spec.Parameters) == 0 {
				t.Fatalf("expected parameters")
			}
			tc.assert(t, spec)
		})
	}
}
