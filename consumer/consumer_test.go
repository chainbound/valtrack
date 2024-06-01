package consumer

import "testing"

func TestComputeNewAvg(t *testing.T) {
	currValidatorCount := 1 + (0-1)/2
	t.Log("count", currValidatorCount)

	avg := ComputeNewAvg(0, 0, currValidatorCount)

	t.Log("avg", avg)
}
