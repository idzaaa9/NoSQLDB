package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE

	WAL_PUT    = 0
	WAL_DELETE = 1
)

type WriteAheadLogEntry struct {
	// key:value are the only things we need to generate an entry
	// the rest is metadata which we can generate ourselves
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Tombstone bool
}

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

	ts := false
	if operation == WAL_DELETE {
		ts = true
	}

	return &WriteAheadLogEntry{
		key,
		value,
		time.Now(),
		ts,
	}, nil
}

func SerializeEntry(entry WriteAheadLogEntry) ([]byte, int) {
	// returns the serialized entry and the size of it
	// first we create all of the parts of the serialized entry, and join them in the end
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

	crc = hashfunc.Crc32AsBytes(returnArray)

	returnArray = append(crc, returnArray...)

	return returnArray, len(returnArray)
}

func deserializeEntry(data []byte) (*WriteAheadLogEntry, []byte, error) {
	reader := bytes.NewReader(data)

	crc := make([]byte, CRC_SIZE)
	timestampBytes := make([]byte, TIMESTAMP_SIZE)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	keysize := make([]byte, KEY_SIZE_SIZE)
	valuesize := make([]byte, VALUE_SIZE_SIZE)

	err := binary.Read(reader, binary.BigEndian, &crc)
	if err != nil {
		return nil, nil, errors.New("error while reading crc from a wal entry")
	}
	err = binary.Read(reader, binary.BigEndian, &timestampBytes)
	if err != nil {
		return nil, nil, errors.New("error while reading timestamp from a wal entry")
	}
	if _, err = reader.Read(tombstone); err != nil {
		return nil, nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &keysize)
	if err != nil {
		return nil, nil, errors.New("error while reading keysize from a wal entry")
	}
	err = binary.Read(reader, binary.BigEndian, &valuesize)
	if err != nil {
		return nil, nil, errors.New("error while reading valuesize from a wal entry")
	}

	timestamp := time.Unix(int64(binary.BigEndian.Uint64(timestampBytes)), 0)

	key := make([]byte, binary.BigEndian.Uint64(keysize))
	value := make([]byte, binary.BigEndian.Uint64(valuesize))

	if _, err := reader.Read(key); err != nil {
		return nil, nil, err
	}

	if _, err := reader.Read(value); err != nil {
		return nil, nil, err
	}

	entry := &WriteAheadLogEntry{
		Timestamp: timestamp,
		Tombstone: tombstone[0] == 1,
		Key:       key,
		Value:     value,
	}

	return entry, crc, nil
}

type WALBuffer struct {
	Data []WriteAheadLogEntry
	Size int // size defines number of entries
}

func NewWALBuffer(size int) *WALBuffer {
	data := make([]WriteAheadLogEntry, size)
	return &WALBuffer{
		Data: data,
		Size: size,
	}
}

type WriteAheadLog struct {
	// since the WAL is segmented, we will need to keep track of segment indexes
	CurrentFile    *os.File
	Index          int
	SegmentSize    int
	Buffer         WALBuffer
	BytesRemaining int    // remaining bytes from the last segment
	Path           string // contains path to the WAL folder
}

func ScanWALFolder(path string) (int, error) {
	/* this function will go through the folder with the WAL data, and return the
	   largest value of a segment
	   ALL OF THE WAL FILE NAMES MUST BE IN THE FORMAT "wal_00001.log" !!!!!!!!!!*/

	files, err := os.ReadDir(path)
	if err != nil {
		return -1, err
	}

	// this regex returns all of the files which match our format
	re := regexp.MustCompile(`wal_(\d{5})\.log`)

	highestIndex := 0

	for _, file := range files {
		reMatch := re.FindAllStringSubmatch(file.Name(), -1)
		if reMatch != nil && len(reMatch) == 2 {
			currIndex, err := strconv.Atoi(reMatch[1][0])
			if err == nil {
				if currIndex > highestIndex {
					highestIndex = currIndex
				}
			}
		}
	}

	return highestIndex, nil
}

func (wal *WriteAheadLog) Dump() error {
	if wal.CurrentFile == nil {
		wal.Index++
		filename := fmt.Sprintf("wal_%05d.log", wal.Index)
		filePath := filepath.Join(wal.Path, filename)

		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		wal.CurrentFile = file
	}

	for i := 0; i < wal.Buffer.Size; i++ {
		entry := wal.Buffer.Data[i]

		serializedEntry, entrySize := SerializeEntry(entry)
		if wal.BytesRemaining < entrySize {
			if _, err := wal.CurrentFile.Write(serializedEntry[:wal.BytesRemaining]); err != nil {
				return err
			}

			if err := wal.CurrentFile.Close(); err != nil {
				return err
			}

			wal.Index++
			filename := fmt.Sprintf("wal_%05d.log", wal.Index)
			filePath := filepath.Join(wal.Path, filename)

			file, err := os.Create(filePath)
			if err != nil {
				return err
			}

			wal.CurrentFile = file

			breakBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(breakBytes, uint32(entrySize-wal.BytesRemaining))
			if _, err := wal.CurrentFile.Write(breakBytes); err != nil {
				return err
			}

			if _, err := wal.CurrentFile.Write(serializedEntry[wal.BytesRemaining:]); err != nil {
				return err
			}

			// we create a new segment, decrease the 4 bytes which represent number of bytes from the last entry
			wal.BytesRemaining = wal.SegmentSize - entrySize + wal.BytesRemaining - 4
		} else {
			if wal.BytesRemaining == wal.SegmentSize {
				breakBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(breakBytes, 0)
				if _, err := wal.CurrentFile.Write(breakBytes); err != nil {
					return err
				}
				wal.BytesRemaining -= 4
			}
			if _, err := wal.CurrentFile.Write(serializedEntry); err != nil {
				return err
			}
			wal.BytesRemaining -= entrySize
		}
	}

	wal.Buffer.Data = make([]WriteAheadLogEntry, wal.Buffer.Size)
	return nil
}

func NewWriteAheadLog(filepath string, segmentSize, bufferSize int) (*WriteAheadLog, error) {
	index, err := ScanWALFolder(filepath)

	if err != nil {
		return nil, errors.New("error while reading the wal folder")
	}

	return &WriteAheadLog{
		CurrentFile:    nil,
		Index:          index,
		SegmentSize:    segmentSize,
		Buffer:         *NewWALBuffer(bufferSize),
		BytesRemaining: segmentSize,
		Path:           filepath,
	}, nil
}

func (wal *WriteAheadLog) Log(key, value []byte, operation int) error {
	entry, err := NewEntry(key, value, operation)
	if err != nil {
		return err
	}
	wal.Buffer.Data = append(wal.Buffer.Data, *entry)
	if len(wal.Buffer.Data) == cap(wal.Buffer.Data) {
		err = wal.Dump()
	}
	return err
}
