package memtable

import (
	"NoSQLDB/lib/skiplist"
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

func (sm *SkipListMemtable) SortKeys() []string {
	nodes := sm.data.GetAllNodes()

	keys := make([]string, 0, len(nodes))
	for _, node := range nodes {
		keys = append(keys, node.Key())
	}

	sort.Strings(keys)

	return keys
}
