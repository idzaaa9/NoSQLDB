package memtable

import (
	"NoSQLDB/lib/skiplist"
	"fmt"
	"os"
	"sort"
)

type SkipListMemtable struct {
	data       *skiplist.SkipList
	threshhold int
}

func NewSkipListMemtable(threshold, maxLevel int) *SkipListMemtable {
	return &SkipListMemtable{
		data:       skiplist.NewSkipList(maxLevel),
		threshhold: threshold,
	}
}

func (slm *SkipListMemtable) Put(key string, value []byte) error {
	slm.data.Put(key, value)
	return nil
}

func (slm *SkipListMemtable) Get(key string) (*Entry, error) {
	node, _ := slm.data.Get(key)
	if node == nil {
		return nil, nil
	}
	return NodeToEntry(node), nil
}

func (slm *SkipListMemtable) Delete(key string) error {
	slm.data.LogicallyDelete(key)
	return nil
}

func (slm *SkipListMemtable) Flush() error {
	entries := slm.GetSortedEntries()

	fileCounter := 1
	fileName := fmt.Sprintf("usertable-%02d-Data.txt", fileCounter)

	for fileExists(fileName) {
		fileCounter++
		fileName = fmt.Sprintf("usertable-%02d-Data.txt", fileCounter)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range entries {
		serializedData := entry.Serialize()
		_, err := file.Write(serializedData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (slm *SkipListMemtable) Size() int {
	return slm.data.Size()
}

func (slm *SkipListMemtable) IsFull() bool {
	return slm.Size() >= slm.threshhold
}

func NodeToEntry(n *skiplist.Node) *Entry {
	if n == nil {
		return nil
	}
	return &Entry{
		key:       n.Key(),
		value:     n.Value(),
		tombstone: n.Tombstone(),
	}
}

func (sm *SkipListMemtable) GetSortedEntries() []Entry {
	nodes := sm.data.GetAllNodes()

	entries := make([]Entry, 0, len(nodes))
	for _, node := range nodes {
		entry := Entry{
			key:       node.Key(),
			value:     node.Value(),
			tombstone: node.Tombstone(),
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	return entries
}
