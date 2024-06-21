package consumer

import "testing"

func TestExtractShortLivedSubnets(t *testing.T) {
	subscribed := []int64{23, 24, 57, 25, 38, 6, 20, 49, 11, 39, 35, 42, 16, 50, 18, 15, 9, 30, 47, 40, 64, 7, 48, 46, 32, 10, 62, 13, 3, 55, 37, 26, 51, 59, 12, 31, 17, 53, 54, 4, 33, 36, 21, 56, 58, 1, 44, 63, 22, 14, 5, 27, 8, 28, 52, 34, 60, 41, 43, 45, 61, 19, 2, 29}
	longLived := []int64{}

	shortLived := extractShortLivedSubnets(subscribed, longLived)
	t.Log("shortLived", shortLived)
	t.Log("len(shortLived)", len(shortLived))
}
