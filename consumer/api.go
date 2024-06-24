package consumer

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
)

type ValidatorTracker struct {
	PeerID            string  `json:"peer_id"`
	ENR               string  `json:"enr"`
	Multiaddr         string  `json:"multiaddr"`
	IP                string  `json:"ip"`
	Port              int     `json:"port"`
	LastSeen          int     `json:"last_seen"`
	LastEpoch         int     `json:"last_epoch"`
	ClientVersion     string  `json:"client_version"`
	PossibleValidator bool    `json:"possible_validator"`
	MaxValidatorCount int     `json:"max_validator_count"`
	NumObservations   int     `json:"num_observations"`
	Hostname          string  `json:"hostname"`
	City              string  `json:"city"`
	Region            string  `json:"region"`
	Country           string  `json:"country"`
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
	PostalCode        string  `json:"postal_code"`
	ASN               string  `json:"asn"`
	ASNOrganization   string  `json:"asn_organization"`
	ASNType           string  `json:"asn_type"`
}

var selectQuery = `SELECT peer_id, enr, multiaddr, validator_tracker.ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type FROM validator_tracker JOIN ip_metadata ON validator_tracker.ip = ip_metadata.ip`

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

		rows, err := db.Query(selectQuery)
		if err != nil {
			http.Error(w, "Error querying database", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var validators []ValidatorTracker
		for rows.Next() {
			var vm ValidatorTracker
			err := rows.Scan(&vm.PeerID, &vm.ENR, &vm.Multiaddr, &vm.IP, &vm.Port, &vm.LastSeen, &vm.LastEpoch, &vm.ClientVersion, &vm.PossibleValidator, &vm.MaxValidatorCount, &vm.NumObservations, &vm.Hostname, &vm.City, &vm.Region, &vm.Country, &vm.Latitude, &vm.Longitude, &vm.PostalCode, &vm.ASN, &vm.ASNOrganization, &vm.ASNType)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error scanning row: %s", err), http.StatusInternalServerError)
				return
			}

			// If the user is not an admin, we should not return the sensitive data
			if !isAdmin {
				vm.ENR = ""
				vm.Multiaddr = ""
				vm.IP = ""
				vm.Latitude = math.Round(vm.Latitude*10) / 10
				vm.Longitude = math.Round(vm.Longitude*10) / 10
			}
			validators = append(validators, vm)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(validators); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		}
	}
}
