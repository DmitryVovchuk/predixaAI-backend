package scheduler

import (
	"fmt"
	"strconv"
)

func EvaluateCondition(cond ConditionSpec, value any) (bool, string, string) {
	floatVal, err := toFloat(value)
	if err != nil {
		return false, fmt.Sprint(value), fmt.Sprintf("%s %v", cond.Op, cond.Value)
	}
	switch cond.Op {
	case ">":
		target, _ := toFloat(cond.Value)
		return floatVal > target, fmt.Sprint(floatVal), fmt.Sprintf("> %v", target)
	case ">=":
		target, _ := toFloat(cond.Value)
		return floatVal >= target, fmt.Sprint(floatVal), fmt.Sprintf(">= %v", target)
	case "<":
		target, _ := toFloat(cond.Value)
		return floatVal < target, fmt.Sprint(floatVal), fmt.Sprintf("< %v", target)
	case "<=":
		target, _ := toFloat(cond.Value)
		return floatVal <= target, fmt.Sprint(floatVal), fmt.Sprintf("<= %v", target)
	case "==":
		target, _ := toFloat(cond.Value)
		return floatVal == target, fmt.Sprint(floatVal), fmt.Sprintf("== %v", target)
	case "!=":
		target, _ := toFloat(cond.Value)
		return floatVal != target, fmt.Sprint(floatVal), fmt.Sprintf("!= %v", target)
	case "between":
		if cond.Min == nil || cond.Max == nil {
			return false, fmt.Sprint(floatVal), "between"
		}
		return floatVal >= *cond.Min && floatVal <= *cond.Max, fmt.Sprint(floatVal), fmt.Sprintf("between %v and %v", *cond.Min, *cond.Max)
	default:
		return false, fmt.Sprint(value), cond.Op
	}
}

func toFloat(val any) (float64, error) {
	switch t := val.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case string:
		return strconv.ParseFloat(t, 64)
	default:
		return 0, fmt.Errorf("unsupported type")
	}
}
