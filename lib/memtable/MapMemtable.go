package memtable

import (
	"errors"
	"fmt"
)

type MapMemTable struct {
	data       map[string]Entry
	threshhold int
}

func NewMapMemTable(threshold int) *MapMemTable {
	return &MapMemTable{
		data:       make(map[string]Entry),
		threshhold: threshold,
	}
}

func (m *MapMemTable) Put(key string, value string) error {
	m.data[key] = Entry{
		key:       key,
		value:     value,
		tombstone: false,
	}
	return nil
}

func (m *MapMemTable) Get(key string) (Entry, error) {
	value, ok := m.data[key]
	if !ok {
		return value, errors.New(fmt.Sprintf("entry with key %s not found", key))
	}
	return value, nil
}

func (m *MapMemTable) Delete(key string) error {
	m.data[key] = Entry{
		key:       key,
		value:     "",
		tombstone: true,
	}
	return nil
}

func (m *MapMemTable) Size() int {
	return len(m.data)
}

func (m *MapMemTable) ShouldFlush() bool {
	return m.Size() >= m.threshhold
}
