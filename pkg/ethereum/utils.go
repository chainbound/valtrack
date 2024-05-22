package ethereum

import "os"

func getCrawlerLocation() string {
	if region := os.Getenv("FLY_REGION"); region != "" {
		return region
	}
	return "FLY_REGION"
}

func getCrawlerMachineID() string {
	if id := os.Getenv("FLY_MACHINE_ID"); id != "" {
		return id
	}
	return "FLY_MACHINE_ID"
}
