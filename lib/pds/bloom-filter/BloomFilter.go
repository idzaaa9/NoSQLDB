package pds

import (
	"NoSQLDB/lib/utils"
	"encoding/gob"
	"os"
)

type BloomFilter struct {
	Bitset []bool
	Size   uint
	Hashes []utils.HashWithSeed
}

func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := utils.CalculateM(expectedElements, falsePositiveRate)
	k := utils.CalculateK(expectedElements, m)
	hashes := utils.CreateHashFunctions(k)

	return &BloomFilter{
		Bitset: make([]bool, m),
		Size:   m,
		Hashes: hashes,
	}
}

func (bf *BloomFilter) Add(element string) {
	data := []byte(element)
	for _, hash := range bf.Hashes {
		index := hash.Hash(data) % uint64(bf.Size)
		bf.Bitset[index] = true
	}
}

func (bf *BloomFilter) Query(element string) bool {
	data := []byte(element)
	for _, hash := range bf.Hashes {
		index := hash.Hash(data) % uint64(bf.Size)
		if !bf.Bitset[index] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Clear() {
	for i := range bf.Bitset {
		bf.Bitset[i] = false
	}
}

func (bf *BloomFilter) Serialize(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(bf)
}

func (bf *BloomFilter) Deserialize(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(bf)
}
