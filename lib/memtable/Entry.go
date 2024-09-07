package memtable

import "encoding/binary"

type Entry struct {
	key       string
	value     []byte
	tombstone bool
}

func (e *Entry) Key() string {
	return e.key
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) Tombstone() bool {
	return e.tombstone
}

func (e *Entry) Serialize() []byte {
	tombstone := make([]byte, TOMBSTONE_SIZE)

	keysize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint32(keysize, uint32(len(e.key)))

	if e.tombstone {
		tombstone[0] = 1
		data := append(tombstone, keysize...)
		return append(data, []byte(e.key)...)
	} else {
		tombstone[0] = 0
	}

	valuesize := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint32(valuesize, uint32(len(e.value)))

	data := append(tombstone, keysize...)
	data = append(data, []byte(e.key)...)

	data = append(data, valuesize...)
	return append(data, []byte(e.value)...)
}

func NewEntry(key string, value []byte) *Entry {
	return &Entry{
		key:       key,
		value:     value,
		tombstone: false,
	}
}
