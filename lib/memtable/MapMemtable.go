package memtable

import (
	"errors"
	"sort"
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

func (m *MapMemtable) Put(key string, value []byte) error {
	m.data[key] = Entry{
		key:       key,
		value:     value,
		tombstone: false,
	}
	return nil
}

func (m *MapMemtable) Get(key string) (*Entry, error) {
	value, ok := m.data[key]
	if !ok {
		return &value, errors.New("entry not found")
	}
	return &value, nil
}

func (m *MapMemtable) Delete(key string) error {
	m.data[key] = Entry{
		key:       key,
		value:     nil,
		tombstone: true,
	}
	return nil
}

func (m *MapMemtable) Size() int {
	return len(m.data)
}

func (m *MapMemtable) IsFull() bool {
	return m.Size() >= m.threshhold
}

// TODO: Implement this
func (m *MapMemtable) Flush() error {
	return nil
}

func SortEntriesByKey(memtable *MapMemtable) []*Entry {
	var sortedEntries []*Entry

	for _, entry := range memtable.data {
		sortedEntries = append(sortedEntries, &entry)
	}

	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].key < sortedEntries[j].key
	})

	return sortedEntries
}
