package consumer

import "testing"

func TestComputeNewAvg(t *testing.T) {
	avg := ComputeNewAvg(1, 10, 0)

	t.Log("avg", avg)
}
