package memtable

import (
	"testing"
)

func TestNewMapMemtable(t *testing.T) {
	m := NewMapMemtable(10)
	if m.threshhold != 10 {
		t.Errorf("expected threshold to be 10, got %d", m.threshhold)
	}
	if m.Size() != 0 {
		t.Errorf("expected size to be 0, got %d", m.Size())
	}
}

func TestPut(t *testing.T) {
	m := NewMapMemtable(10)
	err := m.Put("key1", "value1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	entry, err := m.Get("key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if entry.value != "value1" {
		t.Errorf("expected value to be 'value1', got '%s'", entry.value)
	}
}

func TestGetNonExistentKey(t *testing.T) {
	m := NewMapMemtable(10)
	_, err := m.Get("nonexistent")
	if err == nil {
		t.Errorf("expected an error for nonexistent key, got nil")
	}
}

func TestDelete(t *testing.T) {
	m := NewMapMemtable(10)
	err := m.Put("key1", "value1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = m.Delete("key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	entry, err := m.Get("key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !entry.tombstone {
		t.Errorf("expected entry to be a tombstone, got tombstone %v", entry.tombstone)
	}
	if entry.value != "" {
		t.Errorf("expected value to be empty, got '%s'", entry.value)
	}
}

func TestSize(t *testing.T) {
	m := NewMapMemtable(10)
	m.Put("key1", "value1")
	m.Put("key2", "value2")
	if m.Size() != 2 {
		t.Errorf("expected size to be 2, got %d", m.Size())
	}

	m.Delete("key1")
	if m.Size() != 2 {
		t.Errorf("expected size to be 2 after delete, got %d", m.Size())
	}
}

func TestShouldFlush(t *testing.T) {
	m := NewMapMemtable(2)
	if m.ShouldFlush() {
		t.Errorf("expected ShouldFlush to be false when size is 0")
	}

	m.Put("key1", "value1")
	if m.ShouldFlush() {
		t.Errorf("expected ShouldFlush to be false when size is 1")
	}

	m.Put("key2", "value2")
	if !m.ShouldFlush() {
		t.Errorf("expected ShouldFlush to be true when size equals threshold")
	}

	m.Put("key3", "value3")
	if !m.ShouldFlush() {
		t.Errorf("expected ShouldFlush to be true when size exceeds threshold")
	}
}
