package clickhouse

import "fmt"

func PeerDiscoveredDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.peer_discovered (
		enr String,
		id String,
		ip String,
		port Int32,
		crawler_id String,
		crawler_location String,
		timestamp Int64
	) ENGINE = MergeTree()
	PRIMARY KEY (enode, timestamp)`, db)
}

func MetadataReceivedDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.metadata_received (
	enr String,
	id String,
	multiaddr String,
	epoch UInt32,
	client_version String,
	subscribed_subnets Array(UInt64),
	crawler_id String,
	crawler_location String,
	timestamp UInt64
) ENGINE = MergeTree()
PRIMARY KEY (enr, timestamp)`, db)
}

func ValidatorEventDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.validator_event (
		enr String,
		id String,
		multiaddr String,
		epoch Int32,
		long_lived_subnets Array(Int64),
		subscribed_subnets Array(Int64),
		client_version String,
		crawler_id String,
		crawler_location String,
		timestamp Int64
) ENGINE = MergeTree()
PRIMARY KEY (enr, timestamp)`, db)
}
