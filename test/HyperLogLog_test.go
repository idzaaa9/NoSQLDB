package test

import (
	"NoSQLDB/lib/pds"
	"fmt"
	"testing"
)

func TestAddAndEstimate(t *testing.T) {
	hll := pds.NewHLL(10)

	for i := 0; i < 15; i++ {
		hll.Add("blue")
	}

	for i := 0; i < 23; i++ {
		hll.Add("red")
	}

	for i := 0; i < 10; i++ {
		hll.Add("green")
	}

	for i := 0; i < 21; i++ {
		hll.Add("orange")
	}

	estimated := hll.Estimate()
	if estimated < 3 || estimated > 5 {
		t.Errorf("Estimated number of unique elements is out of expected range: %f", estimated)
	}
}

func TestSerializeDeserializeHLL(t *testing.T) {
	hll := pds.NewHLL(10)
	for i := 0; i < 15; i++ {
		hll.Add("blue")
	}

	for i := 0; i < 23; i++ {
		hll.Add("red")
	}

	for i := 0; i < 10; i++ {
		hll.Add("green")
	}

	for i := 0; i < 21; i++ {
		hll.Add("orange")
	}

	// Serialize to bytes
	serializedBytes, err := hll.SerializeToBytes()
	if err != nil {
		t.Fatalf("Error serializing: %v", err)
	}

	// Deserialize from bytes
	hll2, err := pds.DeserializeHLLFromBytes(serializedBytes)
	if err != nil {
		t.Fatalf("Error deserializing: %v", err)
	}

	estimated := hll2.Estimate()
	if estimated < 3 || estimated > 5 {
		t.Errorf("Estimated number of unique elements after deserialization is out of expected range: %f", estimated)
	}
}

func TestDeleteHLL(t *testing.T) {
	hll := pds.NewHLL(10) // Povećana vrednost p
	for i := 0; i < 1000; i++ {
		hll.Add(fmt.Sprintf("example%d", i))
	}

	hll.Delete()
	if hll.Reg != nil {
		t.Errorf("Expected registers to be nil after delete, but got: %v", hll.Reg)
	}
}

func TestEmptyCount(t *testing.T) {
	hll := pds.NewHLL(10) // Povećana vrednost p
	emptyCount := hll.EmptyCount()
	if emptyCount != int(hll.M) {
		t.Errorf("Expected empty count to be %d, but got: %d", hll.M, emptyCount)
	}

	hll.Add("example1")
	emptyCount = hll.EmptyCount()
	if emptyCount == int(hll.M) {
		t.Errorf("Expected empty count to be less than %d after adding an element, but got: %d", hll.M, emptyCount)
	}
}
