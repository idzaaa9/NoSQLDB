package btree

import (
	"testing"
)

func TestBTreeOperations(t *testing.T) {
	tree := NewBTree(2)

	// Test 1: Insertion
	tree.Put("A", []byte("ValueA"), false)
	tree.Put("B", []byte("ValueB"), false)
	tree.Put("C", []byte("ValueC"), false)

	// Test retrieval
	testGet(t, tree, "A", []byte("ValueA"), false)
	testGet(t, tree, "B", []byte("ValueB"), false)
	testGet(t, tree, "C", []byte("ValueC"), false)
	testGet(t, tree, "D", nil, false) // key not present

	// Test 2: Update
	tree.Put("A", []byte("UpdatedValueA"), false)
	tree.Put("B", []byte("UpdatedValueB"), true) // Logical deletion
	testGet(t, tree, "A", []byte("UpdatedValueA"), false)
	testGet(t, tree, "B", []byte("UpdatedValueB"), true) // Key B is logically deleted
	testGet(t, tree, "C", []byte("ValueC"), false)

	// Test 3: Size
	expectedSize := 3
	if size := tree.Size(); size != expectedSize {
		t.Errorf("Size of B-Tree: got %d, want %d", size, expectedSize)
	}

	// Test 5: Edge Case - Empty Tree
	emptyTree := NewBTree(2)
	if size := emptyTree.Size(); size != 0 {
		t.Errorf("Size of new empty B-Tree: got %d, want %d", size, 0)
	}
}

// Helper function to test retrieval
func testGet(t *testing.T, tree *BTree, key string, expectedValue []byte, expectedTombstone bool) {
	entry, _ := tree.Get(key, nil)
	if entry != nil {
		if string(entry.value) == string(expectedValue) && entry.tombstone == expectedTombstone {
			// Test passed
			return
		}
		t.Errorf("Key: %s - FAILED (Expected Value: %s, Got Value: %s, Expected Tombstone: %v, Got Tombstone: %v)",
			key, expectedValue, entry.value, expectedTombstone, entry.tombstone)
	} else {
		if expectedValue == nil && expectedTombstone == false {
			// Test passed
			return
		}
		t.Errorf("Key: %s - FAILED (Expected Value: %s, Got: nil, Expected Tombstone: %v, Got: nil)",
			key, expectedValue, expectedTombstone)
	}
}
