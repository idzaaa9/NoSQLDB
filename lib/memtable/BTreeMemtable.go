package memtable

type BTreeMemtable struct {
}

func NewBTreeMemtable() *BTreeMemtable {
	return &BTreeMemtable{}
}

func (btm *BTreeMemtable) Put(key string, value []byte) error {
	return nil
}

func (btm *BTreeMemtable) Get(key string) (*Entry, error) {
	return nil, nil
}

func (btm *BTreeMemtable) Delete(key string) error {
	return nil
}

func (btm *BTreeMemtable) Flush() error {
	return nil
}

func (btm *BTreeMemtable) Size() int {
	return 0
}

func (btm *BTreeMemtable) IsFull() bool {
	return false
}
