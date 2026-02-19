package scheduler

import "math"

func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func StdDev(values []float64, population bool) float64 {
	if len(values) == 0 {
		return 0
	}
	mean := Mean(values)
	sum := 0.0
	for _, v := range values {
		diff := v - mean
		sum += diff * diff
	}
	denom := float64(len(values))
	if !population {
		if len(values) < 2 {
			return 0
		}
		denom = float64(len(values) - 1)
	}
	return math.Sqrt(sum / denom)
}

func LinearRegression(xVals []float64, yVals []float64) (slope float64, intercept float64, r2 float64, ok bool) {
	if len(xVals) != len(yVals) || len(xVals) < 2 {
		return 0, 0, 0, false
	}
	n := float64(len(xVals))
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0
	for i := range xVals {
		sumX += xVals[i]
		sumY += yVals[i]
		sumXY += xVals[i] * yVals[i]
		sumX2 += xVals[i] * xVals[i]
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0, 0, 0, false
	}
	slope = (n*sumXY - sumX*sumY) / denom
	intercept = (sumY - slope*sumX) / n
	meanY := sumY / n
	ssTot := 0.0
	ssRes := 0.0
	for i := range xVals {
		est := slope*xVals[i] + intercept
		diff := yVals[i] - meanY
		ssTot += diff * diff
		res := yVals[i] - est
		ssRes += res * res
	}
	if ssTot == 0 {
		return slope, intercept, 1, true
	}
	r2 = 1 - ssRes/ssTot
	return slope, intercept, r2, true
}
