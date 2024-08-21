package writeaheadlog

import (
	utils "NoSQLDB/lib/utils"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	segmentPath := filepath.Join(path, segmentName)
	file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)
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

func (reader *WALReader) loadKeyOrValue(size int) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("requested size to read is %d", size)
	}

	// Case 1: Enough bytes remaining in the current segment
	if reader.BytesRemaining >= size {
		return reader.loadKeyValFromSingleSegment(size)
	}

	// Case 2: Not enough bytes remaining, read from current segment and then from next segment
	return reader.loadKeyValFromMultipleSegments(size)
}

func (reader *WALReader) loadKeyValFromSingleSegment(size int) ([]byte, error) {
	data := make([]byte, size)
	if _, err := reader.CurrentFile.Read(data); err != nil {
		return nil, err
	}
	reader.BytesRemaining -= size
	return data, nil
}

func (reader *WALReader) loadKeyValFromMultipleSegments(size int) ([]byte, error) {
	var data []byte

	// Read from the current segment
	firstBuffer := make([]byte, reader.BytesRemaining)
	if _, err := reader.CurrentFile.Read(firstBuffer); err != nil {
		return nil, err
	}

	size -= reader.BytesRemaining
	data = append(data, firstBuffer...)

	for size > reader.CurrentSegmentSize {
		// Open the next segment
		if err := reader.openNextSegment(); err != nil {
			return nil, err
		}

		// Read the whole segment
		secondBuffer := make([]byte, reader.CurrentSegmentSize)
		if _, err := reader.CurrentFile.Read(secondBuffer); err != nil {
			return nil, err
		}

		size -= reader.CurrentSegmentSize
		data = append(data, secondBuffer...)
	}

	// Read the remaining bytes from the next segment
	if err := reader.openNextSegment(); err != nil {
		return nil, err
	}

	thirdBuffer := make([]byte, size)
	if _, err := reader.CurrentFile.Read(thirdBuffer); err != nil {
		return nil, err
	}

	data = append(data, thirdBuffer...)

	return data, nil
}

func (reader *WALReader) loadEntryHeader() ([]byte, error) {
	bytesToRead := HEADER_SIZE

	// if we have enough bytes to read the whole header
	if reader.BytesRemaining >= bytesToRead {
		return reader.loadHeaderFromSingleSegment()
	}

	return reader.loadHeaderFromMultipleSegments()
}

func (reader *WALReader) loadHeaderFromSingleSegment() ([]byte, error) {
	data := make([]byte, HEADER_SIZE)
	if _, err := reader.CurrentFile.Read(data); err != nil {
		return nil, err
	}
	reader.BytesRemaining -= HEADER_SIZE

	return data, nil
}

func (reader *WALReader) loadHeaderFromMultipleSegments() ([]byte, error) {
	firstBuffer := make([]byte, reader.BytesRemaining)
	if _, err := reader.CurrentFile.Read(firstBuffer); err != nil {
		return nil, err
	}

	reader.openNextSegment()

	secondBuffer := make([]byte, HEADER_SIZE-reader.BytesRemaining)
	if _, err := reader.CurrentFile.Read(secondBuffer); err != nil {
		return nil, err
	}

	reader.BytesRemaining -= len(secondBuffer)

	data := append(firstBuffer, secondBuffer...)

	return data, nil
}

func (reader *WALReader) DeserializeEntry() (*WriteAheadLogEntry, error) {
	header, err := reader.loadEntryHeader()

	if err != nil {
		return nil, err
	}

	timestamp, tombstone, keysize, valuesize := deserializeHeader(header)

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
		errorMsg := fmt.Sprintf("crc check failed")
		errorMsg += fmt.Sprintf("\nkey: %s", key)
		errorMsg += fmt.Sprintf("\nvalue: %s", value)
		errorMsg += fmt.Sprintf("\ntimestamp: %s", timestamp)
		errorMsg += fmt.Sprintf("\ntombstone: %t", tombstone)
		return nil, errors.New(errorMsg)
	}

	return RecoverEntry(key, value, timestamp, tombstone), nil
}

func (reader *WALReader) Recover() ([]*WriteAheadLogEntry, error) {
	if utils.IsEmptyDir(reader.Path) {
		fmt.Println("WAL directory is empty")
		return nil, nil
	}

	var entries []*WriteAheadLogEntry

	for reader.Cursor <= reader.LastSegment {
		entry, err := reader.DeserializeEntry()
		if entry == nil {
			break
		}
		if err != nil {
			if err.Error() == "no more segments to read" {
				break
			}
			return nil, err
		}

		// Ensure entry is not nil before dereferencing
		entries = append(entries, entry)
	}

	return entries, nil
}
