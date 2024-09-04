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
	// Tombstone
	tombstone := make([]byte, TOMBSTONE_SIZE)

	// Key
	keyLen := uint32(len(e.key))
	keyLenBytes := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(keyLenBytes, uint64(keyLen))

	if e.tombstone {
		tombstone[0] = 1
		data := append(tombstone, keyLenBytes[:n]...)
		return append(data, []byte(e.key)...)
	} else {
		tombstone[0] = 0
	}

	// Value
	valueLen := uint32(len(e.value))
	valueLenBytes := make([]byte, binary.MaxVarintLen32)
	m := binary.PutUvarint(valueLenBytes, uint64(valueLen))

	// Construct the serialized data
	data := append(tombstone, keyLenBytes[:n]...)
	data = append(data, []byte(e.key)...)
	data = append(data, valueLenBytes[:m]...)
	data = append(data, []byte(e.value)...)

	return data
}
