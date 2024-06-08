package consumer

import "testing"

func TestComputeNewAverage(t *testing.T) {
	avg := ComputeNewAverage(1, 10, 0)

	t.Log("avg", avg)
}
