package clickhouse

type ClickHousePeerDiscoveredEvent struct {
	ENR        string `ch:"enr"`
	ID         string `ch:"id"`
	IP         string `ch:"ip"`
	Port       int32  `ch:"port"`
	CrawlerID  string `ch:"crawler_id"`
	CrawlerLoc string `ch:"crawler_location"`
	Timestamp  int64  `ch:"timestamp"`
}

type ClickHouseMetadataReceivedEvent struct {
	ENR           string             `ch:"enr"`
	ID            string             `ch:"id"`
	Multiaddr     string             `ch:"multiaddr"`
	Epoch         int32              `ch:"epoch"`
	MetaData      ClickHouseMetaData `ch:"metadata"`
	ClientVersion string             `ch:"client_version"`
	CrawlerID     string             `ch:"crawler_id"`
	CrawlerLoc    string             `ch:"crawler_location"`
	Timestamp     int64              `ch:"timestamp"`
}

type ClickHouseValidatorEvent struct {
	ENR               string             `ch:"enr"`
	ID                string             `ch:"id"`
	Multiaddr         string             `ch:"multiaddr"`
	Epoch             int32              `ch:"epoch"`
	MetaData          ClickHouseMetaData `ch:"metadata"`
	LongLivedSubnets  []int64            `ch:"long_lived_subnets"`
	SubscribedSubnets []int64            `ch:"subscribed_subnets"`
	ClientVersion     string             `ch:"client_version"`
	CrawlerID         string             `ch:"crawler_id"`
	CrawlerLoc        string             `ch:"crawler_location"`
	Timestamp         int64              `ch:"timestamp"`
}

type ClickHouseMetaData struct {
	SeqNumber int64  `ch:"seq_number"`
	Attnets   string `ch:"attnets"`
	Syncnets  string `ch:"syncnets"`
}
