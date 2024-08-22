package memtable

import (
	"errors"
	"fmt"
)

type MapMemtable struct {
	data       map[string]Entry
	threshhold int
}

func NewMapMemtable(threshold int) *MapMemtable {
	return &MapMemtable{
		data:       make(map[string]Entry),
		threshhold: threshold,
	}
}

func (m *MapMemtable) Put(key string, value string) error {
	m.data[key] = Entry{
		key:       key,
		value:     value,
		tombstone: false,
	}
	return nil
}

func (m *MapMemtable) Get(key string) (Entry, error) {
	value, ok := m.data[key]
	if !ok {
		return value, errors.New(fmt.Sprintf("entry with key %s not found", key))
	}
	return value, nil
}

func (m *MapMemtable) Delete(key string) error {
	m.data[key] = Entry{
		key:       key,
		value:     "",
		tombstone: true,
	}
	return nil
}

func (m *MapMemtable) Size() int {
	return len(m.data)
}

func (m *MapMemtable) ShouldFlush() bool {
	return m.Size() >= m.threshhold
}
