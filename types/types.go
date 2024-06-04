package types

import "github.com/prysmaticlabs/go-bitfield"

type ValidatorEvent struct {
	ENR       string `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID        string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	Multiaddr string `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8" json:"multiaddr" ch:"multiaddr"`
	Epoch     int    `parquet:"name=epoch, type=INT32" json:"epoch" ch:"epoch"`
	// MetaData          *SimpleMetaData `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8" json:"metadata" ch:"metadata"`
	SeqNumber         int64                `parquet:"name=seq_number, type=INT64" json:"seq_number" ch:"seq_number"`
	Attnets           bitfield.Bitvector64 `parquet:"name=attnets, type=LIST, valuetype=BYTE_ARRAY" json:"attnets" ch:"attnets"`
	Syncnets          bitfield.Bitvector4  `parquet:"name=syncnets, type=LIST, valuetype=BYTE_ARRAY" json:"syncnets" ch:"syncnets"`
	LongLivedSubnets  []int64              `parquet:"name=long_lived_subnets, type=LIST, valuetype=INT64" json:"long_lived_subnets" ch:"long_lived_subnets"`
	SubscribedSubnets []int64              `parquet:"name=subscribed_subnets, type=LIST, valuetype=INT64" json:"subscribed_subnets" ch:"subscribed_subnets"`
	ClientVersion     string               `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8" json:"client_version" ch:"client_version"`
	CrawlerID         string               `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc        string               `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp         int64                `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type PeerDiscoveredEvent struct {
	ENR        string `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID         string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	IP         string `parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8" json:"ip" ch:"ip"`
	Port       int    `parquet:"name=port, type=INT32" json:"port" ch:"port"`
	CrawlerID  string `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc string `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp  int64  `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type MetadataReceivedEvent struct {
	ENR               string          `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8" json:"enr" ch:"enr"`
	ID                string          `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8" json:"id" ch:"id"`
	Multiaddr         string          `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8" json:"multiaddr" ch:"multiaddr"`
	Epoch             int             `parquet:"name=epoch, type=INT32" json:"epoch" ch:"epoch"`
	MetaData          *SimpleMetaData `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8" json:"metadata" ch:"metadata"`
	SubscribedSubnets []int64         `parquet:"name=subscribed_subnets, type=LIST, valuetype=INT64" json:"subscribed_subnets" ch:"subscribed_subnets"`
	ClientVersion     string          `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8" json:"client_version" ch:"client_version"`
	CrawlerID         string          `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_id" ch:"crawler_id"`
	CrawlerLoc        string          `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8" json:"crawler_location" ch:"crawler_location"`
	Timestamp         int64           `parquet:"name=timestamp, type=INT64" json:"timestamp" ch:"timestamp"`
}

type SimpleMetaData struct {
	SeqNumber int64                `parquet:"name=seq_number, type=INT64" json:"seq_number" ch:"seq_number"`
	Attnets   bitfield.Bitvector64 `parquet:"name=attnets, type=LIST, valuetype=BYTE_ARRAY" json:"attnets" ch:"attnets"`
	Syncnets  bitfield.Bitvector4  `parquet:"name=syncnets, type=LIST, valuetype=BYTE_ARRAY" json:"syncnets" ch:"syncnets"`
}
