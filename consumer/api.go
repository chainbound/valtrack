package consumer

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
)

type ValidatorTracker struct {
	PeerID                 string  `json:"peer_id"`
	ENR                    string  `json:"enr,omitempty"`
	Multiaddr              string  `json:"multiaddr,omitempty"`
	IP                     string  `json:"ip,omitempty"`
	Port                   int     `json:"port"`
	LastSeen               int     `json:"last_seen"`
	LastEpoch              int     `json:"last_epoch"`
	ClientVersion          string  `json:"client_version"`
	ValidatorCount         int     `json:"validator_count"`
	ValidatorCountAccuracy float64 `json:"validator_count_accuracy"`
	Hostname               *string `json:"hostname,omitempty"`
	City                   string  `json:"city"`
	Region                 string  `json:"region"`
	Country                string  `json:"country"`
	Latitude               float64 `json:"latitude"`
	Longitude              float64 `json:"longitude"`
	PostalCode             string  `json:"postal_code"`
	ASN                    string  `json:"asn"`
	ASNOrganization        string  `json:"asn_organization"`
	ASNType                string  `json:"asn_type"`
}

// Query to fetch data
var selectQuery = `
SELECT vt.peer_id, vt.enr, vt.multiaddr, vt.ip, vt.port, vt.last_seen, vt.last_epoch,
	   vt.client_version, vc.validator_count, 
	   CAST(vc.n_observations AS FLOAT) / vt.total_observations AS validator_count_accuracy,
	   im.hostname, im.city, im.region, im.country, im.latitude, im.longitude,
	   im.postal_code, im.asn, im.asn_organization, im.asn_type
FROM validator_tracker vt
LEFT JOIN validator_counts vc ON vt.peer_id = vc.peer_id
LEFT JOIN ip_metadata im ON vt.ip = im.ip
WHERE (vc.peer_id, vc.n_observations) IN (
	SELECT peer_id, MAX(n_observations)
	FROM validator_counts
	GROUP BY peer_id
	)
`

// LoadAPIKeys reads the API keys from a file and returns a map of keys
func loadAPIKeys(filePath string, apiKey string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	apiKeys := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			apiKeys[line] = true
		}
	}
	return apiKeys[apiKey], scanner.Err()
}

func createGetValidatorsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")

		isAdmin, _ := loadAPIKeys("api_keys.txt", apiKey)

		// Map to store unique entries per peer_id
		peerIDMap := make(map[string]ValidatorTracker)

		rows, err := db.Query(selectQuery)
		if err != nil {
			http.Error(w, "Error querying database", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var validators []ValidatorTracker
		for rows.Next() {
			var vm ValidatorTracker
			err := rows.Scan(
				&vm.PeerID, &vm.ENR, &vm.Multiaddr, &vm.IP, &vm.Port,
				&vm.LastSeen, &vm.LastEpoch, &vm.ClientVersion, &vm.ValidatorCount,
				&vm.ValidatorCountAccuracy, &vm.Hostname, &vm.City, &vm.Region,
				&vm.Country, &vm.Latitude, &vm.Longitude, &vm.PostalCode,
				&vm.ASN, &vm.ASNOrganization, &vm.ASNType,
			)
			if err != nil {
				log.Fatalf("Error scanning row: %v\n", err)
			}

			// Dont return sensitive information if not admin
			if !isAdmin {
				vm.ENR = ""
				vm.Multiaddr = ""
				vm.IP = ""
				vm.Hostname = nil
				vm.Latitude = math.Round(vm.Latitude*10) / 10
				vm.Longitude = math.Round(vm.Longitude*10) / 10
			}
			vm.ValidatorCountAccuracy = math.Round(vm.ValidatorCountAccuracy*100) / 100

			// Check if peer_id already exists in map, update if the validator count is higher
			existing, ok := peerIDMap[vm.PeerID]
			if !ok || vm.ValidatorCount > existing.ValidatorCount {
				peerIDMap[vm.PeerID] = vm
			}

		}

		for _, vm := range peerIDMap {
			validators = append(validators, vm)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(validators); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		}
	}
}
