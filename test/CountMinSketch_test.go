package test

import (
	"NoSQLDB/lib/pds"
	"NoSQLDB/lib/utils"
	"os"
	"testing"
)

// TestNewCountMinSketch tests the creation of a new Count-min sketch
func TestNewCountMinSketch(t *testing.T) {
	epsilon := 0.01
	delta := 0.01
	cms := pds.NewCountMinSketch(epsilon, delta)

	if cms.Width != utils.CalculateMCMS(epsilon) {
		t.Errorf("Expected width %d, got %d", utils.CalculateMCMS(epsilon), cms.Width)
	}

	if cms.Depth != utils.CalculateKCMS(delta) {
		t.Errorf("Expected depth %d, got %d", utils.CalculateKCMS(delta), cms.Depth)
	}

	if len(cms.Table) != int(cms.Depth) {
		t.Errorf("Expected table depth %d, got %d", cms.Depth, len(cms.Table))
	}

	for i := range cms.Table {
		if len(cms.Table[i]) != int(cms.Width) {
			t.Errorf("Expected table width %d, got %d", cms.Width, len(cms.Table[i]))
		}
	}
}

// TestInsert tests the insertion of elements into the Count-min sketch
func TestInsert(t *testing.T) {
	epsilon := 0.01
	delta := 0.01
	cms := pds.NewCountMinSketch(epsilon, delta)

	data := []byte("example")
	cms.Insert(data)

	count := cms.Count(data)
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}
}

// TestCount tests the counting of elements in the Count-min sketch
func TestCount(t *testing.T) {
	epsilon := 0.01
	delta := 0.01
	cms := pds.NewCountMinSketch(epsilon, delta)

	data := []byte("example")
	cms.Insert(data)
	cms.Insert(data)

	count := cms.Count(data)
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}
}

// TestSerializeDeserialize tests the serialization and deserialization of the Count-min sketch
func TestSerializeDeserialize(t *testing.T) {
	epsilon := 0.01
	delta := 0.01
	cms := pds.NewCountMinSketch(epsilon, delta)

	data := []byte("example")
	cms.Insert(data)

	// Serialize to file
	filename := "cms_test.gob"
	err := cms.Serialize(filename)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}
	defer os.Remove(filename) // Clean up the file after test

	// Deserialize from file
	deserializedCMS, err := pds.Deserialize(filename)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	count := deserializedCMS.Count(data)
	if count != 1 {
		t.Errorf("Expected count 1 after deserialization, got %d", count)
	}
}

// TestDelete tests the deletion of the Count-min sketch
func TestDelete(t *testing.T) {
	epsilon := 0.01
	delta := 0.01
	cms := pds.NewCountMinSketch(epsilon, delta)

	cms.Delete()

	if cms.Table != nil {
		t.Errorf("Expected Table to be nil, got %v", cms.Table)
	}

	if cms.HashFunctions != nil {
		t.Errorf("Expected HashFunctions to be nil, got %v", cms.HashFunctions)
	}

	if cms.Width != 0 {
		t.Errorf("Expected Width to be 0, got %d", cms.Width)
	}

	if cms.Depth != 0 {
		t.Errorf("Expected Depth to be 0, got %d", cms.Depth)
	}
}
