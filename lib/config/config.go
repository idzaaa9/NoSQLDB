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
	WALSegmentSize int    `json:"wal_segment_size"`
	WALFolder      string `json:"wal_folder"`
}

// default values go here
var DefaultConfig = Config{
	// BloomFilterFalsePositiveRate: 0.2,
	// BloomFilterExpectedElements:  50000,
	WALSegmentSize: 64 * KB,
	WALFolder:      "data/wal/",
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

	if config.WALSegmentSize <= 0 {
		config.WALSegmentSize = DefaultConfig.WALSegmentSize
	}

	if config.WALFolder == "" {
		config.WALFolder = DefaultConfig.WALFolder
	}

	return &config, err
}
