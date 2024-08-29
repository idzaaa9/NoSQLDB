package sstable

import "NoSQLDB/lib/pds"

type DataBlock struct {
	Key   string
	Value []byte
}

type IndexEntry struct {
	Key    string
	Offset int64
}

type SummaryEntry struct {
	FirstKey string
	LastKey  string
	Offset   int64
}

type MetadataEntry struct {
	// todo
}

type SSTable struct {
	DataBlocks []DataBlock
	Index      []IndexEntry
	Summary    []SummaryEntry
	Filter     pds.BloomFilter
	TOC        []string
	Metadata   MetadataEntry
}
