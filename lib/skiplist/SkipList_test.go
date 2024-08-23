package skiplist

import (
	"testing"
)

// TestSkipList tests the SkipList methods.
func TestSkipList(t *testing.T) {
	sl := NewSkipList(4)

	// Test Put and Get
	sl.Put("key1", []byte("value1"))
	sl.Put("key2", []byte("value2"))
	sl.Put("key3", []byte("value3"))

	tests := []struct {
		key   string
		value []byte
		found bool
	}{
		{"key1", []byte("value1"), true},
		{"key2", []byte("value2"), true},
		{"key3", []byte("value3"), true},
		{"key4", nil, false},
	}

	for _, test := range tests {
		got, found := sl.Get(test.key)
		if found != test.found || (found && string(got) != string(test.value)) {
			t.Errorf("Get(%s) = (%v, %v); want (%v, %v)", test.key, got, found, test.value, test.found)
		}
	}

	// Test LogicallyDelete
	if !sl.LogicallyDelete("key2") {
		t.Errorf("LogicallyDelete('key2') = false; want true")
	}

	// Check that key2 is logically deleted
	if _, found := sl.Get("key2"); found {
		t.Errorf("Get('key2') after LogicallyDelete = found; want not found")
	}

	// Ensure other keys are still accessible
	if got, found := sl.Get("key1"); !found || string(got) != "value1" {
		t.Errorf("Get('key1') = (%v, %v); want (value1, true)", got, found)
	}

	if got, found := sl.Get("key3"); !found || string(got) != "value3" {
		t.Errorf("Get('key3') = (%v, %v); want (value3, true)", got, found)
	}

	// Test Put after LogicallyDelete
	sl.Put("key2", []byte("new_value2"))
	if got, found := sl.Get("key2"); !found || string(got) != "new_value2" {
		t.Errorf("Get('key2') after Put = (%v, %v); want (new_value2, true)", got, found)
	}
}

// TestSkipListEmpty tests operations on an empty SkipList.
func TestSkipListEmpty(t *testing.T) {
	sl := NewSkipList(4)

	// Test Get on empty list
	if _, found := sl.Get("nonexistent_key"); found {
		t.Errorf("Get('nonexistent_key') on empty list = found; want not found")
	}

	// Test LogicallyDelete on empty list
	if sl.LogicallyDelete("nonexistent_key") {
		t.Errorf("LogicallyDelete('nonexistent_key') on empty list = true; want false")
	}

	// Test Put and Get on empty list
	sl.Put("key1", []byte("value1"))
	if got, found := sl.Get("key1"); !found || string(got) != "value1" {
		t.Errorf("Get('key1') after Put on empty list = (%v, %v); want (value1, true)", got, found)
	}
}
