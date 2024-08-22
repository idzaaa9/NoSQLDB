package pds

import (
	"encoding/gob"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// firstKbits returns the first k bits of a value.
func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

// trailingZeroBits returns the number of trailing zero bits in a value.
func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

// HLL represents the HyperLogLog data structure.
type HLL struct {
	M   uint64  // Number of registers (2^p)
	P   uint8   // Precision parameter
	Reg []uint8 // Array of registers
}

// NewHLL creates a new HyperLogLog instance with the given precision p.
func NewHLL(p uint8) *HLL {
	m := uint64(1) << p
	return &HLL{
		M:   m,
		P:   p,
		Reg: make([]uint8, m),
	}
}

// Add adds a value to the HyperLogLog structure.
func (hll *HLL) Add(value string) {
	hashValue := hll.hash(value)
	index := firstKbits(hashValue, uint64(hll.P))
	rank := trailingZeroBits(hashValue>>hll.P) + 1
	if rank > int(hll.Reg[index]) {
		hll.Reg[index] = uint8(rank)
	}
}

// hash computes the hash value of a string using FNV-1a.
func (hll *HLL) hash(value string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(value))
	return hasher.Sum64()
}

// Estimate estimates the number of unique elements in the HyperLogLog structure.
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // Small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // Large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

// emptyCount returns the number of empty registers in the HyperLogLog structure.
func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// Serialize serializes the HyperLogLog structure to a file.
func (hll *HLL) Serialize(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(hll)
}

// DeserializeHLL deserializes the HyperLogLog structure from a file.
func DeserializeHLL(filename string) (*HLL, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var hll HLL
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&hll)
	return &hll, err
}

// Delete deletes the HyperLogLog instance by setting its registers to nil.
func (hll *HLL) Delete() {
	hll.Reg = nil
}
