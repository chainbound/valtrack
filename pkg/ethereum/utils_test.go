package ethereum

import "testing"

func TestMaddrFrom(t *testing.T) {
	ipv4, err := MaddrFrom("::", 30303)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ipv4", ipv4)
}
