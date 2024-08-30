package sstable

import (
	"NoSQLDB/lib/merkle-tree"
	"NoSQLDB/lib/pds"
)

type DataBlock struct {
	Key       string
	Value     []byte
	Tombstone bool
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

type SSTable struct {
	DataBlocks []DataBlock
	Index      []IndexEntry
	Summary    []SummaryEntry
	Filter     pds.BloomFilter
	TOC        []string
	Metadata   merkle.MerkleTree
}
