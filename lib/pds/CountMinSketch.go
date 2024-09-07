package pds

import (
	"NoSQLDB/lib/utils"
	"bytes"
	"encoding/gob"
)

// CountMinSketch represents the Count-min sketch data structure
type CountMinSketch struct {
	Table         [][]int              // 2D array to store the counts
	HashFunctions []utils.HashWithSeed // Slice of hash functions with seeds
	Width         uint                 // Width of the table (number of columns)
	Depth         uint                 // Depth of the table (number of rows)
}

// NewCountMinSketch creates a new Count-min sketch with given epsilon and delta
func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
	depth := utils.CalculateKCMS(delta)   // Calculate the number of hash functions
	width := utils.CalculateMCMS(epsilon) // Calculate the width of the table

	cms := &CountMinSketch{
		Table:         make([][]int, depth),             // Initialize the 2D array with the calculated depth
		HashFunctions: utils.CreateHashFunctions(depth), // Create the hash functions
		Width:         width,                            // Set the width
		Depth:         depth,                            // Set the depth
	}

	// Initialize the table with zeros
	for i := range cms.Table {
		cms.Table[i] = make([]int, width)
	}

	return cms
}

// Insert adds an element to the Count-min sketch
func (cms *CountMinSketch) Insert(data []byte) {
	for i, hashFunc := range cms.HashFunctions {
		index := hashFunc.Hash(data) % uint64(cms.Width) // Calculate the index for each hash function
		cms.Table[i][index]++                            // Increment the count at the calculated index
	}
}

// Count returns the estimated count of an element in the Count-min sketch
func (cms *CountMinSketch) Count(data []byte) int {
	minCount := int(^uint(0) >> 1) // Initialize minCount to the maximum possible int value
	for i, hashFunc := range cms.HashFunctions {
		index := hashFunc.Hash(data) % uint64(cms.Width) // Calculate the index for each hash function
		if cms.Table[i][index] < minCount {
			minCount = cms.Table[i][index] // Update minCount with the smallest count found
		}
	}
	return minCount
}

// SerializeToBytes serializes the Count-min sketch and returns a byte slice.
func (cms *CountMinSketch) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(cms); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeCMSFromBytes deserializes the Count-min sketch from a byte slice.
func DeserializeCMSFromBytes(data []byte) (*CountMinSketch, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	var cms CountMinSketch
	err := decoder.Decode(&cms)
	if err != nil {
		return nil, err
	}
	return &cms, nil
}

// Delete frees the memory used by the Count-min sketch
func (cms *CountMinSketch) Delete() {
	cms.Table = nil
	cms.HashFunctions = nil
	cms.Width = 0
	cms.Depth = 0
}
