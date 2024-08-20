package writeaheadlog

import (
	"errors"
	"fmt"
	"os"
)

type WALReader struct {
	CurrentFile        *os.File
	Cursor             int    // current WAL segment index
	Path               string // path to the WAL directory
	CurrentSegmentSize int    // size of each WAL segment, does not matter if it changed
	BytesRemaining     int    // remaining bytes in the current segment
	LastSegment        int    // last segment index
}

func NewWALReader(path string, segmentSize int, segmentCount int, cursor int) (*WALReader, error) {
	segmentName := fmt.Sprintf("wal_%05d.log", cursor)
	file, err := os.OpenFile(path+segmentName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &WALReader{
		CurrentFile:        file,
		Cursor:             cursor,
		Path:               path,
		CurrentSegmentSize: int(fileInfo.Size()),
		BytesRemaining:     int(fileInfo.Size()),
		LastSegment:        segmentCount,
	}, nil
}

func (wal *WriteAheadLog) NewWALReader() (*WALReader, error) {
	return NewWALReader(wal.Path, wal.SegmentSize, wal.Index, wal.First)
}

func (reader *WALReader) loadKeyOrValue(size uint64) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("requested size to read is %d", size) // Handle zero size gracefully
	}

	var data []byte

	// Case 1: Enough bytes remaining in the current segment
	if reader.BytesRemaining >= int(size) {
		data = make([]byte, size)
		if _, err := reader.CurrentFile.Read(data); err != nil {
			return nil, err
		}
		reader.BytesRemaining -= int(size)
		return data, nil
	}

	// Case 2: Not enough bytes remaining, read from current segment and then from next segment
	firstBuffer := make([]byte, reader.BytesRemaining)
	if _, err := reader.CurrentFile.Read(firstBuffer); err != nil {
		return nil, err
	}

	// Update size to read from the next segment
	size -= uint64(reader.BytesRemaining)

	// Open the next segment

	reader.openNextSegment()
	if reader.CurrentFile == nil {
		return nil, fmt.Errorf("failed to open next segment") // Handle case where next segment cannot be opened
	}

	secondBuffer := make([]byte, size)
	if _, err := reader.CurrentFile.Read(secondBuffer); err != nil {
		return nil, err
	}

	// Combine buffers
	data = append(firstBuffer, secondBuffer...)

	return data, nil
}

func (reader *WALReader) loadEntryHeader() ([]byte, error) {
	bytesToRead := CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE // 29 bytes

	// if we have enough bytes to read the whole header
	if reader.BytesRemaining >= bytesToRead {
		data := make([]byte, bytesToRead)
		if _, err := reader.CurrentFile.Read(data); err != nil {
			return nil, err
		}
		reader.BytesRemaining -= bytesToRead

		return data, nil
	}

	// if we don't have enough bytes to read the whole header
	// create 2 buffers and join them in the end

	firstBuffer := make([]byte, reader.BytesRemaining)
	if _, err := reader.CurrentFile.Read(firstBuffer); err != nil {
		return nil, err
	}
	bytesToRead -= reader.BytesRemaining

	reader.openNextSegment()

	// read the remaining bytes from the next segment
	secondBuffer := make([]byte, bytesToRead)
	if _, err := reader.CurrentFile.Read(secondBuffer); err != nil {
		return nil, err
	}

	reader.BytesRemaining -= bytesToRead

	// join the buffers
	data := append(firstBuffer, secondBuffer...)

	return data, nil
}

func (reader *WALReader) DeserializeEntry() (*WriteAheadLogEntry, error) {
	header, err := reader.loadEntryHeader()

	if err != nil {
		return nil, err
	}

	timestamp, tombstone, keysize, valuesize := deserializeHeader(header)

	fmt.Println("keysize: ", keysize)
	fmt.Println("valuesize: ", valuesize)

	key, err := reader.loadKeyOrValue(keysize)
	if err != nil {
		return nil, err
	}

	value, err := reader.loadKeyOrValue(valuesize)
	if err != nil {
		return nil, err
	}

	// check if the crc is correct
	serEntry := append(append([]byte{}, header...), append(key, value...)...)
	if !checkCRC(serEntry) {
		errorMsg := fmt.Sprintf("crc check failed, key is %s", string(key))
		return nil, errors.New(errorMsg)
	}

	return RecoverEntry(key, value, timestamp, tombstone), nil
}

func (reader *WALReader) Recover() ([]*WriteAheadLogEntry, error) {
	var entries []*WriteAheadLogEntry

	for reader.Cursor <= reader.LastSegment {
		entry, err := reader.DeserializeEntry()
		if err != nil {
			if err.Error() == "no more segments to read" {
				break
			}
			return nil, err
		}

		// Ensure entry is not nil before dereferencing
		if entry != nil {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}
