package test

import (
	bloom "NoSQLDB/lib/pds/bloom-filter"
	"NoSQLDB/lib/utils"
	"os"
	"testing"
)

func TestBloomFilter_AddAndQuery(t *testing.T) {
	expectedElements := 1000
	falsePositiveRate := 0.01

	bf := bloom.NewBloomFilter(expectedElements, falsePositiveRate)
	bf.Add("example")
	bf.Add("test")

	if !bf.Query("example") {
		t.Errorf("Expected 'example' to be present in the Bloom filter")
	}

	if !bf.Query("test") {
		t.Errorf("Expected 'test' to be present in the Bloom filter")
	}

	if bf.Query("hello") {
		t.Errorf("Did not expect 'hello' to be present in the Bloom filter")
	}
}

func TestBloomFilter_SerializeAndDeserialize(t *testing.T) {
	expectedElements := 1000
	falsePositiveRate := 0.01

	bf := bloom.NewBloomFilter(expectedElements, falsePositiveRate)
	bf.Add("example")
	bf.Add("test")

	// Serialize
	err := bf.Serialize("bloom_filter_test.gob")
	if err != nil {
		t.Fatalf("Failed to serialize Bloom filter: %v", err)
	}
	defer os.Remove("bloom_filter_test.gob")

	// Deserialize
	newBf := &bloom.BloomFilter{}
	err = newBf.Deserialize("bloom_filter_test.gob")
	if err != nil {
		t.Fatalf("Failed to deserialize Bloom filter: %v", err)
	}

	if !newBf.Query("example") {
		t.Errorf("Expected 'example' to be present in the deserialized Bloom filter")
	}

	if !newBf.Query("test") {
		t.Errorf("Expected 'test' to be present in the deserialized Bloom filter")
	}

	if newBf.Query("hello") {
		t.Errorf("Did not expect 'hello' to be present in the deserialized Bloom filter")
	}
}

func TestBloomFilter_Clear(t *testing.T) {
	expectedElements := 1000
	falsePositiveRate := 0.01

	bf := bloom.NewBloomFilter(expectedElements, falsePositiveRate)
	bf.Add("example")
	bf.Add("test")

	bf.Clear()

	if bf.Query("example") {
		t.Errorf("Did not expect 'example' to be present after clearing the Bloom filter")
	}

	if bf.Query("test") {
		t.Errorf("Did not expect 'test' to be present after clearing the Bloom filter")
	}
}

func TestCalculateParameters(t *testing.T) {
	expectedElements := 1000
	falsePositiveRate := 0.01

	m := utils.CalculateM(expectedElements, falsePositiveRate)
	k := utils.CalculateK(expectedElements, m)

	if m <= 0 {
		t.Errorf("Expected m to be greater than 0, got %d", m)
	}

	if k <= 0 {
		t.Errorf("Expected k to be greater than 0, got %d", k)
	}
}
