package memtable

import "testing"

func TestSkipListMemtable(t *testing.T) {
	// Create a new SkipListMemtable with a threshold of 5 and maxLevel of 4
	memtable := NewSkipListMemtable(5, 4)

	// Test Put and Get
	err := memtable.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put() = %v; want nil", err)
	}

	entry, err := memtable.Get("key1")
	if err != nil {
		t.Fatalf("Get() = %v; want nil", err)
	}
	if entry == nil || string(entry.Value()) != "value1" {
		t.Errorf("Get('key1') = %v; want value1", entry)
	}

	// Test Size
	if got := memtable.Size(); got != 1 {
		memtable.data.Print()
		t.Errorf("Size() = %d; want 1", got)
	}

	// Test IsFull
	if got := memtable.IsFull(); got {
		t.Errorf("IsFull() = %v; want false", got)
	}

	// Test LogicallyDelete
	err = memtable.Delete("key1")
	if err != nil {
		t.Fatalf("Delete() = %v; want nil", err)
	}

	entry, err = memtable.Get("key1")
	if err != nil {
		t.Fatalf("Get() after Delete() = %v; want nil", err)
	}
	if entry != nil {
		t.Errorf("Get('key1') after Delete() = %v; want nil", entry)
	}

	// Test Size after deletion
	if got := memtable.Size(); got != 1 {
		t.Errorf("Size() after Delete() = %d; want 1", got)
	}

	// Test IsFull after deletion
	if got := memtable.IsFull(); got {
		t.Errorf("IsFull() after Delete() = %v; want false", got)
	}

	// Test Put and Size to ensure new entries are added
	err = memtable.Put("key2", []byte("value2"))
	if err != nil {
		t.Fatalf("Put() = %v; want nil", err)
	}

	if got := memtable.Size(); got != 1 {
		memtable.data.Print()
		t.Errorf("Size() after new Put() = %d; want 1", got)
	}

	if got := memtable.IsFull(); got {
		t.Errorf("IsFull() after new Put() = %v; want false", got)
	}

	// Test Flush (assumes Flush implementation is completed)
	err = memtable.Flush()
	if err != nil {
		t.Fatalf("Flush() = %v; want nil", err)
	}
}
