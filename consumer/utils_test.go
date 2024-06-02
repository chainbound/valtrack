package consumer

import "testing"

func TestComputeNewAvg(t *testing.T) {
	currValidatorCount := 1 + (4-1)/2
	t.Log("count", currValidatorCount)

	avg := ComputeNewAvg(1, 10, 0)

	t.Log("avg", avg)
}
