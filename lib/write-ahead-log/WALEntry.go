package writeaheadlog

import (
	hash "NoSQLDB/lib/utils"
	"encoding/binary"
	"errors"
	"time"
)

/*
+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
|    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
CRC = 32bit hash computed over the payload using CRC
Key Size = Length of the Key data
Tombstone = If this record was deleted and has a value
Value Size = Length of the Value data
Key = Key data
Value = Value data
Timestamp = Timestamp of the operation in seconds
*/
type WriteAheadLogEntry struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Tombstone bool
}

// key:value are the only things we need to generate an entry
// the rest is metadata which we can generate ourselves
// NewEntry creates a new WriteAheadLogEntry
func NewEntry(key, value []byte, operation int) (*WriteAheadLogEntry, error) {
	if !(operation == WAL_DELETE || operation == WAL_PUT) {
		return nil, errors.New("value must be 0 or 1")
	}
	if len(key) == 0 {
		return nil, errors.New("key is an empty array")
	}
	if key == nil {
		return nil, errors.New("key is nil")
	}
	if (value == nil || len(value) == 0) && operation != WAL_DELETE {
		return nil, errors.New("value is not provided while putting an entry")
	}

	tombstone := false
	if operation == WAL_DELETE {
		tombstone = true
	}

	return &WriteAheadLogEntry{
		key,
		value,
		time.Now(),
		tombstone,
	}, nil
}

// RecoverEntry creates a new WriteAheadLogEntry from the given data
// used when reading from segments
func RecoverEntry(key, value []byte, timestamp time.Time, tombstone bool) *WriteAheadLogEntry {

	return &WriteAheadLogEntry{
		key,
		value,
		timestamp,
		tombstone,
	}
}

// Serialize converts WriteAheadLogEntry to a byte array
// Returns the byte array and the size of the byte array
func (entry *WriteAheadLogEntry) Serialize() []byte {
	crc := make([]byte, CRC_SIZE)
	timestamp := make([]byte, TIMESTAMP_SIZE)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	keysize := make([]byte, KEY_SIZE_SIZE)
	valuesize := make([]byte, VALUE_SIZE_SIZE)

	binary.BigEndian.PutUint64(timestamp, uint64(entry.Timestamp.Unix()))

	if entry.Tombstone {
		tombstone[0] = 1
	} else {
		tombstone[0] = 0
	}

	binary.BigEndian.PutUint64(keysize, uint64(len(entry.Key)))
	binary.BigEndian.PutUint64(valuesize, uint64(len(entry.Value)))

	returnArray := append(timestamp, tombstone...)
	returnArray = append(returnArray, keysize...)
	returnArray = append(returnArray, valuesize...)
	returnArray = append(returnArray, entry.Key...)
	returnArray = append(returnArray, entry.Value...)

	crc = hash.Crc32Byte(returnArray)

	returnArray = append(crc, returnArray...)

	return returnArray
}
