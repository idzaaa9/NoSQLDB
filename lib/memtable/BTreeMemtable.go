package memtable

import "NoSQLDB/lib/btree"

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

// TODO: Implement this
func (btm *BTreeMemtable) Flush() error {
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
