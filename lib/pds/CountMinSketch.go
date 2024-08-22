package pds

import (
	"NoSQLDB/lib/utils"
	"encoding/gob"
	"os"
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

// Serialize saves the Count-min sketch to a file
func (cms *CountMinSketch) Serialize(filename string) error {
	file, err := os.Create(filename) // Create a file to save the data
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms) // Encode the Count-min sketch into the file
	if err != nil {
		return err
	}

	return nil
}

// DeserializeCMS loads the Count-min sketch from a file
func DeserializeCMS(filename string) (*CountMinSketch, error) {
	file, err := os.Open(filename) // Open the file to read the data
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cms CountMinSketch
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&cms) // Decode the data into a Count-min sketch
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
