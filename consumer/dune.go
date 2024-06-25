package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/chainbound/valtrack/log"
	"github.com/rs/zerolog"
)

const (
	tableName      = "validator_metadata"
	apiURL         = "http://localhost:8080/validators"
	createEndpoint = "https://api.dune.com/api/v1/table/create"
	clearEndpoint  = "https://api.dune.com/api/v1/table/%s/%s/clear"
	insertEndpoint = "https://api.dune.com/api/v1/table/%s/%s/insert"
	contentType    = "application/x-ndjson"
)

type CreateTableRequest struct {
	Namespace   string   `json:"namespace"`
	TableName   string   `json:"table_name"`
	Schema      []Column `json:"schema"`
	IsPrivate   bool     `json:"is_private"`
	Description string   `json:"description"`
}

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type ValidatorNonAdminTracker struct {
	PeerID            string  `json:"peer_id"`
	Port              int     `json:"port"`
	LastSeen          int     `json:"last_seen"`
	LastSeenDate      string  `json:"last_seen_date"`
	LastEpoch         int     `json:"last_epoch"`
	ClientVersion     string  `json:"client_version"`
	MaxValidatorCount int     `json:"max_validator_count"`
	NumObservations   int     `json:"num_observations"`
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

type Dune struct {
	log       zerolog.Logger
	namespace string
	apiKey    string
}

func NewDune(namespace, apiKey string) *Dune {
	return &Dune{
		log:       log.NewLogger("dune"),
		namespace: namespace,
		apiKey:    apiKey,
	}
}

// CreateTable creates a table in Dune with the specified schema
func (d *Dune) CreateTable(tableName string, schema []Column) error {
	requestBody := CreateTableRequest{
		Namespace:   d.namespace,
		TableName:   tableName,
		Schema:      schema,
		IsPrivate:   false,
		Description: "Ethereum validators location data",
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal create table request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, createEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create table request: %w", err)
	}
	req.Header.Set("x-dune-api-key", d.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 201 && resp.StatusCode != 409 { // 409 is the status code for table already exists
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read error response body for status code %d: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("HTTP error response: %d / %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// fetchDataFromAPI fetches data from your API endpoint
func fetchDataFromAPI(url string) ([]ValidatorNonAdminTracker, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data from API: %w", err)
	}
	defer resp.Body.Close()

	var validators []ValidatorNonAdminTracker
	err = json.NewDecoder(resp.Body).Decode(&validators)
	if err != nil {
		return nil, fmt.Errorf("failed to decode API response: %w", err)
	}

	// Add readable date for each entry
	for i := range validators {
		validators[i].LastSeenDate = convertToReadableDate(int64(validators[i].LastSeen))
	}

	return validators, nil
}

// convertToReadableDate converts a UNIX timestamp in milliseconds to a human-readable date string
func convertToReadableDate(unixTimestampMs int64) string {
	// Convert milliseconds to seconds
	unixTimestamp := unixTimestampMs / 1000

	// Create a time.Time object
	t := time.Unix(unixTimestamp, 0)

	// Format the time.Time object to a readable date string
	return t.Format("2006-01-02 15:04:05")
}

// clearTableData clears the data in the Dune table
func (d *Dune) ClearTableData(tableName string) error {
	url := fmt.Sprintf(clearEndpoint, d.namespace, tableName)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create clear table request: %w", err)
	}
	req.Header.Set("x-dune-api-key", d.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to clear table data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-OK response from clear table endpoint: %d", resp.StatusCode)
	}

	return nil
}

// insertDataIntoTable inserts data into the Dune table
func (d *Dune) Insert(tableName string, data []ValidatorNonAdminTracker) error {
	url := fmt.Sprintf(insertEndpoint, d.namespace, tableName)

	var buffer bytes.Buffer
	for _, validator := range data {
		line, err := json.Marshal(validator)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
		buffer.Write(line)
	}

	req, err := http.NewRequest(http.MethodPost, url, &buffer)
	if err != nil {
		return fmt.Errorf("failed to create insert data request: %w", err)
	}
	req.Header.Set("x-dune-api-key", d.apiKey)
	req.Header.Set("Content-Type", contentType)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to insert data into table: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read error response body for status code %d: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("HTTP error response: %d / %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func (c *Consumer) publishToDune() error {
	// Create the table in Dune
	err := c.dune.CreateTable(tableName, []Column{
		{Name: "peer_id", Type: "varchar", Nullable: false},
		{Name: "port", Type: "integer", Nullable: true},
		{Name: "last_seen", Type: "bigint", Nullable: true},
		{Name: "last_seen_date", Type: "timestamp", Nullable: true},
		{Name: "last_epoch", Type: "integer", Nullable: true},
		{Name: "client_version", Type: "varchar", Nullable: true},
		{Name: "max_validator_count", Type: "integer", Nullable: true},
		{Name: "num_observations", Type: "integer", Nullable: true},
		{Name: "city", Type: "varchar", Nullable: true},
		{Name: "region", Type: "varchar", Nullable: true},
		{Name: "country", Type: "varchar", Nullable: true},
		{Name: "latitude", Type: "double", Nullable: true},
		{Name: "longitude", Type: "double", Nullable: true},
		{Name: "postal_code", Type: "varchar", Nullable: true},
		{Name: "asn", Type: "varchar", Nullable: true},
		{Name: "asn_organization", Type: "varchar", Nullable: true},
		{Name: "asn_type", Type: "varchar", Nullable: true},
	})
	if err != nil {
		return fmt.Errorf("failed to create table in Dune: %w", err)
	}
	c.log.Debug().Msg("Created table in Dune")

	// Fetch data from the API
	validators, err := fetchDataFromAPI(apiURL)
	if err != nil {
		return fmt.Errorf("failed to fetch data from API: %w", err)
	}
	c.log.Debug().Msg("Fetched data from API")

	// Clear the old data in the table
	err = c.dune.ClearTableData(tableName)
	if err != nil {
		return fmt.Errorf("failed to clear table data: %w", err)
	}
	c.log.Debug().Msg("Cleared table data in Dune")

	// Insert the fresh data into the table
	err = c.dune.Insert(tableName, validators)
	if err != nil {
		return fmt.Errorf("failed to insert data into table: %w", err)
	}
	c.log.Debug().Msg("Inserted data into table in Dune")

	return nil
}
