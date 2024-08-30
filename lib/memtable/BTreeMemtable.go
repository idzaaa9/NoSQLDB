package memtable

import (
	"NoSQLDB/lib/btree"
	"fmt"
	"os"
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

func (btm *BTreeMemtable) Flush() error {
	entries := btm.CollectKeyValuePairs()

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

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
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

// CollectKeyValuePairs traverses the B-tree and returns key-value pairs sorted by keys.
func (b *BTreeMemtable) CollectKeyValuePairs() []*btree.Entry {
	var result []*btree.Entry
	b.collectKeyValuePairsRecursive(b.data.Root(), &result)
	return result
}

func (b *BTreeMemtable) collectKeyValuePairsRecursive(node *btree.Node, result *[]*btree.Entry) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.Keys()); i++ {
		// Add the current key-value pair to the result
		entry := node.Values()[i]
		*result = append(*result, entry)
	}

	// Recursively process child nodes
	if !node.IsLeaf() {
		for i := 0; i < len(node.Children()); i++ {
			b.collectKeyValuePairsRecursive(node.Children()[i], result)
		}
	}
}
