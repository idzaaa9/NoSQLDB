package memtable

import (
	"errors"
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
