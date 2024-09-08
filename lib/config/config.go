package config

import (
	"encoding/json"
	"os"
	"regexp"
)

const (
	KB = 1 << (10 * iota) // 1 kilobyte
	MB                    // 1 megabyte
	GB                    // 1 gigabyte
	TB                    // 1 terabyte
)

// configurable values go here
type Config struct {

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

	// SStable
	IndexStride   int `json:"index_stride"`
	SummaryStride int `json:"summary_stride"`

	// Bloom filter
	BFExpectedElements  int     `json:"bf_expected_elements"`
	BFFalsePositiveRate float64 `json:"bf_false_positive_rate"`

	//Token bucket
	TokenBucketSize int    `json:"token_bucket_size"`
	TokenBucketRate int    `json:"token_bucket_rate"`
	FillInterval    string `json:"fill_interval"`

	//Cache
	CacheSize int `json:"cache_size"`
}

// default values go here
var DefaultConfig = Config{
	WALSegmentSize: 64 * KB,
	WALDir:         "data/wal/",

	NumTables:        4,
	MemtableSize:     100,
	SkipListMaxLevel: 16,
	BTreeMinDegree:   16,
	OutputDir:        "data/sstable/",
	MemtableType:     "map",

	IndexStride:   5,
	SummaryStride: 4,

	BFExpectedElements:  100,
	BFFalsePositiveRate: 0.2,

	TokenBucketSize: 100,
	TokenBucketRate: 10,
	FillInterval:    "500ms",

	CacheSize: 100,
}

func GetDefaultConfig() *Config {
	return &Config{
		WALSegmentSize: 64 * KB,
		WALDir:         "data/wal/",

		NumTables:        4,
		MemtableSize:     100,
		SkipListMaxLevel: 16,
		BTreeMinDegree:   16,
		OutputDir:        "data/sstable/",
		MemtableType:     "map",

		IndexStride:   5,
		SummaryStride: 4,

		BFExpectedElements:  100,
		BFFalsePositiveRate: 0.2,

		TokenBucketSize: 100,
		TokenBucketRate: 10,
		FillInterval:    "500ms",

		CacheSize: 100,
	}
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

	if config.TokenBucketSize <= 0 {
		config.TokenBucketSize = DefaultConfig.TokenBucketSize
	}

	if config.TokenBucketRate <= 0 {
		config.TokenBucketRate = DefaultConfig.TokenBucketRate
	}

	if !isFillIntervalValid(config.FillInterval) {
		config.FillInterval = DefaultConfig.FillInterval
	}

	if config.CacheSize <= 0 {
		config.CacheSize = DefaultConfig.CacheSize
	}

	return &config, err
}

func isFillIntervalValid(duration string) bool {
	// Regular expression to match valid duration formats
	// This regex matches:
	// - An optional integer part before the unit (e.g., 20)
	// - A unit (ms, s, m, h)
	// - Unit must be exactly one of 'ms', 's', 'm', or 'h'
	re := `^\d+(ms|s|m|h)$`

	// Compile the regular expression
	regexp := regexp.MustCompile(re)

	// Check if the duration string matches the regular expression
	return regexp.MatchString(duration)
}
