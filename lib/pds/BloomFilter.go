package pds

import (
	"NoSQLDB/lib/utils"
	"bytes"
	"encoding/gob"
)

// BloomFilter represents a probabilistic data structure for set membership testing.
type BloomFilter struct {
	Bitset []bool               // Bitset to store the presence of elements
	Size   uint                 // Size of the bitset
	Hashes []utils.HashWithSeed // Array of hash functions with seeds
}

// NewBloomFilter creates a new Bloom filter with the given expected number of elements and false positive rate.
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := utils.CalculateMBF(expectedElements, falsePositiveRate) // Calculate the size of the bitset
	k := utils.CalculateKBF(expectedElements, m)                 // Calculate the number of hash functions
	hashes := utils.CreateHashFunctions(k)                       // Create the hash functions

	return &BloomFilter{
		Bitset: make([]bool, m),
		Size:   m,
		Hashes: hashes,
	}
}

// Add inserts an element into the Bloom filter.
func (bf *BloomFilter) Add(element string) {
	data := []byte(element)
	for _, hash := range bf.Hashes {
		index := hash.Hash(data) % uint64(bf.Size)
		bf.Bitset[index] = true
	}
}

// Query checks if an element is possibly in the Bloom filter.
func (bf *BloomFilter) Query(element string) bool {
	data := []byte(element)
	for _, hash := range bf.Hashes {
		index := hash.Hash(data) % uint64(bf.Size)
		if !bf.Bitset[index] {
			return false // If any bit is not set, the element is definitely not in the set
		}
	}
	return true // All bits are set, the element is possibly in the set
}

// Clear resets the Bloom filter, removing all elements.
func (bf *BloomFilter) Clear() {
	for i := range bf.Bitset {
		bf.Bitset[i] = false
	}
}

// SerializeToBytes serializes the Bloom filter and returns a byte slice.
func (bf *BloomFilter) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(bf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeFromBytes deserializes the Bloom filter from a byte slice.
func (bf *BloomFilter) DeserializeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(bf)
}
