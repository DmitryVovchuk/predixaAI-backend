package dbconnector

import (
	"reflect"
	"testing"
	"time"
)

func TestQuoteQualified(t *testing.T) {
	quoted, parts, err := quoteQualified("public.users", 2, func(s string) string { return "\"" + s + "\"" })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if quoted != "\"public\".\"users\"" {
		t.Fatalf("unexpected quoted value: %s", quoted)
	}
	if !reflect.DeepEqual(parts, []string{"public", "users"}) {
		t.Fatalf("unexpected parts: %#v", parts)
	}
}

func TestQuoteQualifiedTooManySegments(t *testing.T) {
	_, _, err := quoteQualified("a.b.c", 2, func(s string) string { return s })
	if err == nil {
		t.Fatalf("expected error for too many segments")
	}
}

func TestQuoteList(t *testing.T) {
	out, err := quoteList([]string{"id", "name"}, func(s string) string { return "`" + s + "`" })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "`id`, `name`" {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestNormalizeProfileOptions(t *testing.T) {
	opts := normalizeProfileOptions(ProfileOptions{})
	if opts.MaxColumns != defaultMaxColumns {
		t.Fatalf("expected default max columns")
	}
	if opts.SampleLimit != defaultSampleLimit {
		t.Fatalf("expected default sample limit")
	}
}

func TestProfileFromSample(t *testing.T) {
	schema := TableSchema{Columns: []ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "created_at", Type: "timestamp"},
		{Name: "name", Type: "text", Nullable: true},
	}}
	now := time.Now().UTC().Truncate(time.Second)
	sample := []map[string]any{
		{"id": 2, "created_at": now.Add(-time.Hour), "name": "alice"},
		{"id": 5, "created_at": now, "name": nil},
		{"id": 3, "created_at": now.Add(-2 * time.Hour), "name": "bob"},
	}
	profiles := profileFromSample(schema, sample, 10)
	if len(profiles) != 3 {
		t.Fatalf("expected 3 profiles")
	}
	idProfile := profiles[0]
	if idProfile.DistinctInSample != 3 {
		t.Fatalf("unexpected distinct count: %d", idProfile.DistinctInSample)
	}
	if idProfile.Min.(float64) != 2 || idProfile.Max.(float64) != 5 {
		t.Fatalf("unexpected min/max: %#v %#v", idProfile.Min, idProfile.Max)
	}
	nameProfile := profiles[2]
	if nameProfile.Nulls != 1 {
		t.Fatalf("unexpected null count: %d", nameProfile.Nulls)
	}
	if nameProfile.NullRate <= 0 {
		t.Fatalf("expected null rate > 0")
	}
}

func TestUpdateMinMaxWithTime(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(2 * time.Hour)
	minVal, maxVal := updateMinMax(nil, nil, t2)
	minVal, maxVal = updateMinMax(minVal, maxVal, t1)
	if minVal.(time.Time) != t1 {
		t.Fatalf("unexpected min time")
	}
	if maxVal.(time.Time) != t2 {
		t.Fatalf("unexpected max time")
	}
}
