package memtable

type SkipListMemtable struct {
}

func NewSkipListMemtable() *SkipListMemtable {
	return &SkipListMemtable{}
}

func (slm *SkipListMemtable) Put(key string, value []byte) error {
	return nil
}

func (slm *SkipListMemtable) Get(key string) (*Entry, error) {
	return nil, nil
}

func (slm *SkipListMemtable) Delete(key string) error {
	return nil
}

func (slm *SkipListMemtable) Flush() error {
	return nil
}

func (slm *SkipListMemtable) Size() int {
	return 0
}

func (slm *SkipListMemtable) IsFull() bool {
	return false
}
