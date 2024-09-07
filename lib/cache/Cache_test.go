package cache

import (
	"NoSQLDB/lib/memtable"
	"testing"
)

func TestCache(t *testing.T) {
	// Initialize cache with a capacity of 2
	c := NewCache(2)

	// Test Put and Get
	entry1 := memtable.NewEntry("key1", []byte("value1"))
	c.Put(entry1)
	if got := c.Get("key1"); got == nil || string(got.entry.Value()) != "value1" {
		t.Errorf("Get(key1) = %v; want value1", got)
	}

	// Test LRU eviction
	entry2 := memtable.NewEntry("key2", []byte("value2"))
	c.Put(entry2)
	if got := c.Get("key2"); got == nil || string(got.entry.Value()) != "value2" {
		t.Errorf("Get(key2) = %v; want value2", got)
	}

	entry3 := memtable.NewEntry("key3", []byte("value3")) // This should evict key1
	c.Put(entry3)
	if got := c.Get("key1"); got != nil {
		t.Errorf("Get(key1) = %v; want nil (evicted)", got)
	}

	if got := c.Get("key2"); got == nil || string(got.entry.Value()) != "value2" {
		t.Errorf("Get(key2) = %v; want value2", got)
	}

	if got := c.Get("key3"); got == nil || string(got.entry.Value()) != "value3" {
		t.Errorf("Get(key3) = %v; want value3", got)
	}

	// Test Update Existing Key
	entry2Updated := memtable.NewEntry("key2", []byte("value2Updated"))
	c.Put(entry2Updated)
	if got := c.Get("key2"); got == nil || string(got.entry.Value()) != "value2Updated" {
		t.Errorf("Get(key2) after update = %v; want value2Updated", got)
	}

	// Test Capacity
	entry4 := memtable.NewEntry("key4", []byte("value4")) // This should evict key3
	c.Put(entry4)
	if got := c.Get("key3"); got != nil {
		t.Errorf("Get(key3) = %v; want nil (evicted)", got)
	}

	if got := c.Get("key4"); got == nil || string(got.entry.Value()) != "value4" {
		t.Errorf("Get(key4) = %v; want value4", got)
	}
}
