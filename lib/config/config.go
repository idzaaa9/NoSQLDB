package config

import (
	"encoding/json"
	"os"
)

const (
	KB = 1 << (10 * iota) // 1 kilobyte
	MB                    // 1 megabyte
	GB                    // 1 gigabyte
	TB                    // 1 terabyte
)

// configurable values go here
type Config struct {
	// BloomFilterFalsePositiveRate float32 `json:"bloom_filter_false_positive_rate"`
	// BloomFilterExpectedElements  int     `json:"bloom_filter_expected_elements"`

	// WAL
	WALSegmentSize int    `json:"wal_segment_size"`
	WALDir         string `json:"wal_folder"`

	// Mempool
	NumTables        int    `json:"num_tables"`
	MemtableSize     int    `json:"memtable_size"`
	SkipListMaxLevel int    `json:"skip_list_max_level"`
	BTreeMinDegree   int    `json:"btree_min_degree"`
	OutputDir        string `json:"output_dir"`
	MemtableType     string `json:"memtable_type"`
}

// default values go here
var DefaultConfig = Config{
	// BloomFilterFalsePositiveRate: 0.2,
	// BloomFilterExpectedElements:  50000,
	WALSegmentSize:   64 * KB,
	WALDir:           "data/wal/",
	NumTables:        4,
	MemtableSize:     100,
	SkipListMaxLevel: 16,
	BTreeMinDegree:   16,
	OutputDir:        "data/sstable/",
	MemtableType:     "map",
}

func LoadConfig(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(data, &config)

	if err != nil {
		return nil, err
	}
	// checking values goes here

	/*
		if config.BloomFilterFalsePositiveRate <= 0 || config.BloomFilterFalsePositiveRate > 1 {
			config.BloomFilterFalsePositiveRate = DefaultConfig.BloomFilterFalsePositiveRate
		}
		if config.BloomFilterExpectedElements <= 0 {
			config.BloomFilterExpectedElements = DefaultConfig.BloomFilterExpectedElements
		}
	*/

	if config.WALSegmentSize <= KB {
		config.WALSegmentSize = DefaultConfig.WALSegmentSize
	}

	if config.WALDir == "" {
		config.WALDir = DefaultConfig.WALDir
	}

	if config.NumTables <= 0 {
		config.NumTables = DefaultConfig.NumTables
	}

	if config.MemtableSize <= 0 {
		config.MemtableSize = DefaultConfig.MemtableSize
	}

	if config.SkipListMaxLevel <= 0 {
		config.SkipListMaxLevel = DefaultConfig.SkipListMaxLevel
	}

	if config.BTreeMinDegree <= 0 {
		config.BTreeMinDegree = DefaultConfig.BTreeMinDegree
	}

	if config.OutputDir == "" {
		config.OutputDir = DefaultConfig.OutputDir
	}

	if config.MemtableType != "map" &&
		config.MemtableType != "btree" &&
		config.MemtableType != "skip_list" {
		config.MemtableType = DefaultConfig.MemtableType
	}

	return &config, err
}
