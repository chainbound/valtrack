package consumer

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
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
		client_version TEXT
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

	createValidatorCountsTableQuery = `
		CREATE TABLE IF NOT EXISTS validator_counts (
			peer_id TEXT,
			validator_count INTEGER,
			n_observations INTEGER DEFAULT 1,
			PRIMARY KEY (peer_id, validator_count)
	);`

	insertTrackerQuery = `
		INSERT INTO validator_tracker (peer_id, enr, multiaddr, ip, port, last_seen, last_epoch, client_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`

	updateTrackerQuery = `
		UPDATE validator_tracker
		SET enr = ?, multiaddr = ?, ip = ?, port = ?, last_seen = ?, last_epoch = ?, client_version = ?
		WHERE peer_id = ?;`

	selectIpMetadataQuery = `SELECT * FROM ip_metadata WHERE ip = ?`

	insertIpMetadataQuery = `INSERT INTO ip_metadata (ip, hostname, city, region, country, latitude, longitude, postal_code, asn, asn_organization, asn_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertValidatorCountsQuery = `INSERT INTO validator_counts (peer_id, validator_count, n_observations) VALUES (?, ?, 1) ON CONFLICT (peer_id, validator_count) DO UPDATE SET n_observations = validator_counts.n_observations + 1;`
)

func setupDatabase(db *sql.DB) error {
	_, err := db.Exec(createTrackerTableQuery)
	if err != nil {
		return err
	}

	_, err = db.Exec(createIpMetadataTableQuery)
	if err != nil {
		return err
	}

	_, err = db.Exec(createValidatorCountsTableQuery)
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

func insertIPMetadata(tx *sql.Tx, ip IPMetaData) error {
	parts := strings.Split(ip.LatLong, ",")
	lat, _ := strconv.ParseFloat(parts[0], 64)
	long, _ := strconv.ParseFloat(parts[1], 64)

	_, err := tx.Exec(insertIpMetadataQuery, ip.IP, ip.Hostname, ip.City, ip.Region, ip.Country, lat, long, ip.Postal, ip.ASN, ip.ASNOrg, ip.ASNType)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) runValidatorMetadataEventHandler(token string) error {
	client := ipinfo.NewClient(nil, nil, token)

	batchSize := 0

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	for {
		event := <-c.validatorMetadataChan
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

		longLived := indexesFromBitfield(event.MetaData.Attnets)
		shortLived := extractShortLivedSubnets(event.SubscribedSubnets, longLived)

		// Assumption: Validator selected for attestation aggregation
		// is subscribed to a single subnet for a 1 epoch duration
		currValidatorCount := len(shortLived)

		var exists bool
		query := "SELECT EXISTS(SELECT 1 FROM validator_tracker WHERE peer_id = ?)"

		// QueryRow executes a query that is expected to return at most one row.
		err = c.db.QueryRow(query, event.ID).Scan(&exists)

		if err == sql.ErrNoRows {
			// Insert new row
			_, err = tx.Exec(insertTrackerQuery, event.ID, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion)
			if err != nil {
				c.log.Error().Err(err).Msg("Error inserting row")
			}

			_, err = tx.Exec(insertValidatorCountsQuery, event.ID, currValidatorCount)
			if err != nil {
				c.log.Error().Err(err).Msg("Error inserting validator count")
			}

			batchSize++

			// If we have no IP yet, fetch it and insert
			if err := c.db.QueryRow(selectIpMetadataQuery, ip).Scan(); err == sql.ErrNoRows {
				c.log.Info().Str("ip", ip).Msg("Unknown IP, fetching IP info...")
				go func() {
					ip, err := client.GetIPInfo(net.ParseIP(ip))
					if err != nil {
						c.log.Error().Err(err).Msg("Error fetching IP info")
						return
					}

					asn := ""
					asnOrg := ""
					asnType := ""
					if ip.ASN != nil {
						asn = ip.ASN.ASN
						asnOrg = ip.ASN.Name
						asnType = ip.ASN.Type
					}

					ipMeta := IPMetaData{
						IP:       ip.IP.String(),
						Hostname: ip.Hostname,
						City:     ip.City,
						Region:   ip.Region,
						Country:  ip.Country,
						LatLong:  ip.Location,
						Postal:   ip.Postal,
						ASN:      asn,
						ASNOrg:   asnOrg,
						ASNType:  asnType,
					}

					if err := insertIPMetadata(tx, ipMeta); err != nil {
						c.log.Error().Err(err).Msg("Error inserting IP metadata")
						return
					}

					batchSize++
				}()
			}
			c.log.Trace().Str("peer_id", event.ID).Msg("Inserted new row")
		} else if err != nil {
			c.log.Error().Err(err).Msg("Error querying database")
		} else {
			// Update existing row
			_, err = tx.Exec(updateTrackerQuery, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, event.ClientVersion, event.ID)
			if err != nil {
				c.log.Error().Err(err).Msg("Error updating row")
			}

			_, err = tx.Exec(insertValidatorCountsQuery, event.ID, currValidatorCount)
			if err != nil {
				c.log.Error().Err(err).Msg("Error inserting validator count")
			}

			batchSize++

			c.log.Trace().Str("peer_id", event.ID).Msg("Updated row")
		}

		if batchSize >= 32 {
			// Commit transaction
			tx.Commit()

			// Reset transaction
			tx, err = c.db.Begin()
			if err != nil {
				return err
			}

			batchSize = 0
		}
	}
}
