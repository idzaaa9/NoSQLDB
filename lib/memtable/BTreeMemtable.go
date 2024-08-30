package memtable

import (
	"NoSQLDB/lib/btree"
	"sort"
)

type BTreeMemtable struct {
	data      *btree.BTree
	threshold int
}

func NewBTreeMemtable(minDegree, threshold int) *BTreeMemtable {
	return &BTreeMemtable{
		data:      btree.NewBTree(minDegree),
		threshold: threshold,
	}
}

func (btm *BTreeMemtable) Put(key string, value []byte) error {
	btm.data.Put(key, value, false)
	return nil
}

func (btm *BTreeMemtable) Get(key string) (*Entry, error) {
	value, ok := btm.data.Get(key, nil)
	if ok != -1 {
		return toEntry(value), nil
	}
	return nil, nil
}

func (btm *BTreeMemtable) Delete(key string) error {
	btm.data.Put(key, nil, true)
	return nil
}

func (btm *BTreeMemtable) Size() int {
	return btm.data.Size()
}

func (btm *BTreeMemtable) IsFull() bool {
	return false
}

func toEntry(be *btree.Entry) *Entry {
	return &Entry{
		key:       be.Key(),
		value:     be.Value(),
		tombstone: be.Tombstone(),
	}
}

func (b *BTreeMemtable) SortKeys() []string {
	var keys []string
	b.collectKeysRecursive(b.data.Root(), &keys)
	sort.Strings(keys)
	return keys
}

func (b *BTreeMemtable) collectKeysRecursive(node *btree.Node, keys *[]string) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.Keys()); i++ {
		*keys = append(*keys, node.Keys()[i])
	}

	// Recursively process child nodes
	if !node.IsLeaf() {
		for i := 0; i < len(node.Children()); i++ {
			b.collectKeysRecursive(node.Children()[i], keys)
		}
	}
}
