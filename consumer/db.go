package consumer

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ipinfo/go/v2/ipinfo"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

var (
	createTrackerTableQuery = `
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

	createIpMetadataTableQuery = `
		CREATE TABLE IF NOT EXISTS ip_metadata (
			ip TEXT PRIMARY KEY,
			hostname TEXT,
			city TEXT,
			region TEXT,
			country TEXT,
			latitude REAL,
			longitude REAL,
			postal_code TEXT,
			asn TEXT,	
			asn_organization TEXT,
			asn_type TEXT
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
	selectQuery = `SELECT peer_id, enr, multiaddr, validator_tracker.ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type FROM validator_tracker JOIN ip_metadata ON validator_tracker.ip = ip_metadata.ip`

	selectIpMetadataQuery = `SELECT * FROM ip_metadata WHERE ip = ?`

	insertIpMetadataQuery = `INSERT INTO ip_metadata (ip, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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

func setupDatabase(db *sql.DB) error {
	_, err := db.Exec(createTrackerTableQuery)
	if err != nil {
		return err
	}

	_, err = db.Exec(createIpMetadataTableQuery)
	if err != nil {
		return err
	}

	return nil
}

type asnJSON struct {
	Asn             string `json:"asn"`
	AsnOrganization string `json:"name"`
	Type            string `json:"type"`
}

func loadIPMetadataFromCSV(db *sql.DB, path string) error {
	file, err := os.Open(path)
	if err != nil {
		errors.Wrap(err, fmt.Sprintf("Error opening csv file: %s", path))
	}

	defer file.Close()

	r := csv.NewReader(file)

	ips, err := r.ReadAll()
	if err != nil {
		errors.Wrap(err, "Error reading csv records")
	}

	var rowCountStr string
	err = db.QueryRow("SELECT COUNT(ip) FROM ip_metadata").Scan(&rowCountStr)
	if err != nil {
		errors.Wrap(err, "Error querying database")
	}

	rowCount, _ := strconv.Atoi(rowCountStr)
	if rowCount == 0 {
		tx, err := db.Begin()
		if err != nil {
			errors.Wrap(err, "Error beginning transaction")
		}

		stmt, err := tx.Prepare(insertIpMetadataQuery)
		if err != nil {
			errors.Wrap(err, "Error preparing insert statement")
		}

		for _, ip := range ips {
			parts := strings.Split(ip[5], ",")
			lat, _ := strconv.ParseFloat(parts[0], 64)
			long, _ := strconv.ParseFloat(parts[1], 64)

			var asnJson asnJSON
			if err := json.Unmarshal([]byte(ip[8]), &asnJson); err != nil {
				errors.Wrap(err, fmt.Sprintf("Error unmarshalling ASN JSON: %s", ip[8]))
			}
			// `INSERT INTO ip_metadata (ip, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
			_, err := stmt.Exec(ip[0], ip[1], ip[2], ip[3], ip[4], lat, long, ip[7], asnJson.Asn, asnJson.AsnOrganization, asnJson.Type)
			if err != nil {
				errors.Wrap(err, "Error inserting row")
			}
		}

		err = tx.Commit()
		if err != nil {
			errors.Wrap(err, "Error committing transaction")
		}

	}

	return nil
}

type IPMetaData struct {
	IP       string `json:"ip"`
	Hostname string `json:"hostname"`
	City     string `json:"city"`
	Region   string `json:"region"`
	Country  string `json:"country"`
	LatLong  string `json:"lat_long"`
	Postal   string `json:"postal"`
	ASN      string `json:"asn"`
	ASNOrg   string `json:"asn_organization"`
	ASNType  string `json:"asn_type"`
}

func insertIPMetadata(db *sql.DB, ip IPMetaData) error {
	parts := strings.Split(ip.LatLong, ",")
	lat, _ := strconv.ParseFloat(parts[0], 64)
	long, _ := strconv.ParseFloat(parts[1], 64)

	_, err := db.Exec(insertIpMetadataQuery, ip.IP, ip.Hostname, ip.City, ip.Region, ip.Country, lat, long, ip.Postal, ip.ASN, ip.ASNOrg, ip.ASNType)
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
			// SELECT peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version, possible_validator, max_validator_count, num_observations, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type
			if err := rows.Scan(&vm.PeerID, &vm.ENR, &vm.Multiaddr, &vm.IP, &vm.Port, &vm.LastSeen, &vm.LastEpoch, &vm.ClientVersion, &vm.PossibleValidator, &vm.MaxValidatorCount, &vm.NumObservations, &vm.Hostname, &vm.City, &vm.Region, &vm.Country, &vm.Latitude, &vm.Longitude, &vm.PostalCode, &vm.ASN, &vm.ASNOrganization, &vm.ASNType); err != nil {
				http.Error(w, fmt.Sprintf("Error scanning row: %s", err), http.StatusInternalServerError)
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

func (c *Consumer) runValidatorMetadataEventHandler(token string) error {
	client := ipinfo.NewClient(nil, nil, token)

	for event := range c.validatorMetadataChan {
		// TODO: batching!
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

			// If we have no IP yet, fetch it and insert
			if err := c.db.QueryRow(selectIpMetadataQuery, ip).Scan(); err == sql.ErrNoRows {
				c.log.Info().Str("ip", ip).Msg("Unknown IP, fetching IP info...")
				go func() {
					ip, err := client.GetIPInfo(net.ParseIP(ip))
					if err != nil {
						c.log.Error().Err(err).Msg("Error fetching IP info")
						return
					}

					ipMeta := IPMetaData{
						IP:       ip.IP.String(),
						Hostname: ip.Hostname,
						City:     ip.City,
						Region:   ip.Region,
						Country:  ip.Country,
						LatLong:  ip.Location,
						Postal:   ip.Postal,
						ASN:      ip.ASN.ASN,
						ASNOrg:   ip.ASN.Name,
						ASNType:  ip.ASN.Type,
					}

					if err := insertIPMetadata(c.db, ipMeta); err != nil {
						c.log.Error().Err(err).Msg("Error inserting IP metadata")
					}
				}()
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
	}

	return nil
}
