package memtable

import (
	"errors"
	"strconv"
	"testing"
)

// Mock implementations for Memtable, Entry, etc. for testing
type MockMemtable struct {
	data map[string]*Entry
}

func NewMockMemtable() *MockMemtable {
	return &MockMemtable{data: make(map[string]*Entry)}
}

func (m *MockMemtable) Put(key string, value []byte) error {
	m.data[key] = &Entry{key: key, value: value}
	return nil
}

func (m *MockMemtable) Get(key string) (*Entry, error) {
	if entry, exists := m.data[key]; exists {
		return entry, nil
	}
	return nil, errors.New("entry not found")
}

func (m *MockMemtable) IsFull() bool {
	return len(m.data) >= 10 // arbitrary full condition for testing
}

func (m *MockMemtable) Flush() error {
	m.data = make(map[string]*Entry)
	return nil
}

// Tests for Mempool
func TestMempoolPutAndGet(t *testing.T) {
	mempool, err := NewMempool(3, 10, 3, 2, "outputDir", USE_MAP)
	if err != nil {
		t.Fatalf("Failed to create Mempool: %v", err)
	}

	entry := &Entry{key: "testKey", value: []byte("testValue")}
	if err := mempool.Put(entry); err != nil {
		t.Fatalf("Put operation failed: %v", err)
	}

	got, err := mempool.Get("testKey")
	if err != nil {
		t.Fatalf("Get operation failed: %v", err)
	}
	if string(got.value) != "testValue" {
		t.Errorf("Expected value %s, got %s", "testValue", got.value)
	}
}

func TestMempoolFlushAndRotation(t *testing.T) {
	mempool, err := NewMempool(3, 10, 3, 2, "outputDir", USE_MAP)
	if err != nil {
		t.Fatalf("Failed to create Mempool: %v", err)
	}

	// Add entries to the mempool until it needs to flush and rotate
	for i := 0; i < 11; i++ { // assuming IsFull() becomes true at 10
		entry := &Entry{key: "A" + strconv.Itoa(i), value: []byte("Value")}
		if err := mempool.Put(entry); err != nil {
			t.Fatalf("Put operation failed: %v", err)
		}
	}

	// Check that flush occurred
	if err := mempool.flushIfNeeded(); err != nil {
		t.Fatalf("Flush operation failed: %v", err)
	}

	// Ensure rotation happened
	if mempool.activeTableIdx != 1 {
		t.Errorf("Expected active table index to be 1 after rotation, got %d", mempool.activeTableIdx)
	}
}

func TestMempoolDelete(t *testing.T) {
	mempool, err := NewMempool(3, 10, 3, 2, "outputDir", USE_MAP)
	if err != nil {
		t.Fatalf("Failed to create Mempool: %v", err)
	}

	entry := &Entry{key: "testKey", value: []byte("testValue")}
	if err := mempool.Put(entry); err != nil {
		t.Fatalf("Put operation failed: %v", err)
	}

	if err := mempool.Delete("testKey"); err != nil {
		t.Fatalf("Delete operation failed: %v", err)
	}

}

// Add additional helper functions and mock implementations if needed

func TestMempoolFlushIfNeeded(t *testing.T) {
	mempool, err := NewMempool(3, 10, 3, 2, "outputDir", USE_MAP)
	if err != nil {
		t.Fatalf("Failed to create Mempool: %v", err)
	}

	for i := 0; i < 10; i++ {
		entry := &Entry{key: "A" + strconv.Itoa(i), value: []byte("Value")}
		if err := mempool.Put(entry); err != nil {
			t.Fatalf("Put operation failed: %v", err)
		}
	}

	// Verify no flush required yet
	if err := mempool.flushIfNeeded(); err != nil {
		t.Fatalf("Flush operation failed: %v", err)
	}

	// Insert one more to trigger flush
	if err := mempool.Put(&Entry{key: "triggerFlush", value: []byte("flushValue")}); err != nil {
		t.Fatalf("Put operation failed: %v", err)
	}

	if err := mempool.flushIfNeeded(); err != nil {
		t.Fatalf("Flush operation failed: %v", err)
	}
}
