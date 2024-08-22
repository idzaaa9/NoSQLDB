package memtable

type Memtable interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Flush() error
	Size() int
	ShouldFlush() bool
}
