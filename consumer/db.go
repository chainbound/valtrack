package consumer

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	ma "github.com/multiformats/go-multiaddr"
)

var (
	createTableQuery = `
    CREATE TABLE IF NOT EXISTS validator_tracker (
        PeerID TEXT PRIMARY KEY,
        ENR TEXT,
        Multiaddr TEXT,
        IP TEXT,
        Port INTEGER,
        LastSeen TEXT,
        LastEpoch INTEGER,
        PossibleValidator BOOLEAN,
        AverageValidatorCount INTEGER,
        NumObservations INTEGER
    );
    `
	insertQuery = `
				INSERT INTO validator_tracker (PeerID, ENR, Multiaddr, IP, Port, LastSeen, LastEpoch, PossibleValidator, AverageValidatorCount, NumObservations)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	updateQuery = `
				UPDATE validator_tracker
				SET ENR = ?, Multiaddr = ?, IP = ?, Port = ?, LastSeen = ?, LastEpoch = ?, PossibleValidator = ?, AverageValidatorCount = ?, NumObservations = ?
				WHERE PeerID = ?
				`
	selectQuery = `
				SELECT PeerID, ENR, Multiaddr, IP, Port, LastSeen, LastEpoch, PossibleValidator, AverageValidatorCount, NumObservations 
				FROM validator_tracker`
)

type ValidatorTracker struct {
	PeerID                string `json:"peer_id"`
	ENR                   string `json:"enr"`
	Multiaddr             string `json:"multiaddr"`
	IP                    string `json:"ip"`
	Port                  int    `json:"port"`
	LastSeen              string `json:"last_seen"`
	LastEpoch             int    `json:"last_epoch"`
	PossibleValidator     bool   `json:"possible_validator"`
	AverageValidatorCount int    `json:"average_validator_count"`
	NumObservations       int    `json:"num_observations"`
}

func setupDatabase(db *sql.DB) error {
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return err
	}
	return nil
}

func createGetValidatorsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query(selectQuery)
		if err != nil {
			http.Error(w, "Error querying database", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var validators []ValidatorTracker
		for rows.Next() {
			var vm ValidatorTracker
			if err := rows.Scan(&vm.PeerID, &vm.ENR, &vm.Multiaddr, &vm.IP, &vm.Port, &vm.LastSeen, &vm.LastEpoch, &vm.PossibleValidator, &vm.AverageValidatorCount, &vm.NumObservations); err != nil {
				http.Error(w, "Error scanning row", http.StatusInternalServerError)
				return
			}
			validators = append(validators, vm)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(validators); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		}
	}
}

func (c *Consumer) HandleValidatorMetadataEvent() error {
	for {
		select {
		case event := <-c.validatorMetadataChan:
			c.log.Trace().Any("event", event).Msg("Received validator event")

			maddr, err := ma.NewMultiaddr(event.Multiaddr)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid multiaddr")
				continue
			}

			ip, err := maddr.ValueForProtocol(ma.P_IP4)
			if err != nil {
				ip, err = maddr.ValueForProtocol(ma.P_IP6)
				if err != nil {
					c.log.Error().Err(err).Msg("Invalid IP in multiaddr")
					continue
				}
			}

			portStr, err := maddr.ValueForProtocol(ma.P_TCP)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid port in multiaddr")
				continue
			}

			port, err := strconv.Atoi(portStr)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid port number")
				continue
			}

			isValidator := true
			longLived := indexesFromBitfield(event.MetaData.Attnets)
			shortLived := extractShortLivedSubnets(event.SubscribedSubnets, longLived)
			// If there are no short lived subnets, then the peer is not a validator
			if len(shortLived) == 0 {
				isValidator = false
			}

			prevNumObservations := uint64(0)
			prevAvgValidatorCount := int32(0)
			err = c.db.QueryRow("SELECT NumObservations, AverageValidatorCount FROM validator_tracker WHERE PeerID = ?", event.ID).Scan(&prevNumObservations, &prevAvgValidatorCount)

			currValidatorCount := 1 + (len(shortLived)-1)/2
			// If there are no short lived subnets, then the validator count is 0
			if len(shortLived) == 0 {
				currValidatorCount = 0
			}
			currAvgValidatorCount := ComputeNewAverage(prevAvgValidatorCount, prevNumObservations, currValidatorCount)

			if err == sql.ErrNoRows {
				// Insert new row
				_, err = c.db.Exec(insertQuery, event.ID, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, isValidator, currAvgValidatorCount, prevNumObservations+1)
				if err != nil {
					c.log.Error().Err(err).Msg("Error inserting row")
				}
				c.log.Trace().Str("PeerID", event.ID).Msg("Inserted new row")
			} else if err != nil {
				c.log.Error().Err(err).Msg("Error querying database")
			} else {
				// Update existing row
				_, err = c.db.Exec(updateQuery, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, isValidator, currAvgValidatorCount, prevNumObservations+1, event.ID)
				if err != nil {
					c.log.Error().Err(err).Msg("Error updating row")
				}
				c.log.Trace().Str("PeerID", event.ID).Msg("Updated row")
			}
		default:
			c.log.Debug().Msg("No validator metadata event")
			time.Sleep(1 * time.Second) // Prevents busy waiting
		}
	}
}
