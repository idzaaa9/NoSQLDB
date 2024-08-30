package memtable

type Memtable interface {
	Put(key string, value []byte) error
	Get(key string) (*Entry, error)
	Delete(key string) error
	Size() int
	IsFull() bool
	SortKeys() []string
}
