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
        peer_id TEXT PRIMARY KEY,
        enr TEXT,
        multiaddr TEXT,
        ip TEXT,
        port INTEGER,
        last_seen INTEGER,
        last_epoch INTEGER,
		client_version TEXT,
        possible_validator BOOLEAN,
        max_validator_count INTEGER,
        num_observations INTEGER
    );
    `
	insertQuery = `
				INSERT INTO validator_tracker (peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	updateQuery = `
				UPDATE validator_tracker
				SET enr = ?, multiaddr = ?, ip = ?, port = ?, last_seen = ?, last_epoch = ?, client_version = ?, possible_validator = ?, max_validator_count = ?, num_observations = ?
				WHERE peer_id = ?
				`
	selectQuery = `
				SELECT peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations 
				FROM validator_tracker`
)

type ValidatorTracker struct {
	PeerID            string `json:"peer_id"`
	ENR               string `json:"enr"`
	Multiaddr         string `json:"multiaddr"`
	IP                string `json:"ip"`
	Port              int    `json:"port"`
	LastSeen          int    `json:"last_seen"`
	LastEpoch         int    `json:"last_epoch"`
	ClientVersion     string `json:"client_version"`
	PossibleValidator bool   `json:"possible_validator"`
	MaxValidatorCount int    `json:"max_validator_count"`
	NumObservations   int    `json:"num_observations"`
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
			if err := rows.Scan(&vm.PeerID, &vm.ENR, &vm.Multiaddr, &vm.IP, &vm.Port, &vm.LastSeen, &vm.LastEpoch, &vm.ClientVersion, &vm.PossibleValidator, &vm.MaxValidatorCount, &vm.NumObservations); err != nil {
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

			prevNumObservations := uint64(0)
			prevValidatorCount := int32(0)
			err = c.db.QueryRow("SELECT max_validator_count, num_observations FROM validator_tracker WHERE peer_id = ?", event.ID).Scan(&prevValidatorCount, &prevNumObservations)

			// If current short lived subnets are empty and
			// also never had short-lived subnets before
			// then the peer is not a validator
			if len(shortLived) == 0 && prevValidatorCount == 0 {
				isValidator = false
			}

			// Assumption: Validator selected for attestation aggregation
			// is subscribed to a single subnet for a 1 epoch duration
			currValidatorCount := len(shortLived)

			// If the previous validator count is greater then we set
			// that as the value because it's possible the validator isn't
			// selected for attestation aggregation in the current epoch
			if int(prevValidatorCount) > currValidatorCount {
				currValidatorCount = int(prevValidatorCount)
			}

			if err == sql.ErrNoRows {
				// Insert new row
				_, err = c.db.Exec(insertQuery, event.ID, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion, isValidator, currValidatorCount, prevNumObservations+1)
				if err != nil {
					c.log.Error().Err(err).Msg("Error inserting row")
				}
				c.log.Trace().Str("peer_id", event.ID).Msg("Inserted new row")
			} else if err != nil {
				c.log.Error().Err(err).Msg("Error querying database")
			} else {
				// Update existing row
				_, err = c.db.Exec(updateQuery, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion, isValidator, currValidatorCount, prevNumObservations+1, event.ID)
				if err != nil {
					c.log.Error().Err(err).Msg("Error updating row")
				}
				c.log.Trace().Str("peer_id", event.ID).Msg("Updated row")
			}
		default:
			c.log.Debug().Msg("No validator metadata event")
			time.Sleep(1 * time.Second) // Prevents busy waiting
		}
	}
}
