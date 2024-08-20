package writeaheadlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	fp "path/filepath"
)

/* TODO:
- Once memtables and mempool is implemented, we need to expand the WAL to work with multiple memtables,
  while keeping the data consistent. My suggestion is to have a map[int][]string where the key is the
	ID of the memtable and the value is all of the segments which are associated with that memtable.
*/

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

type WriteAheadLog struct {
	CurrentFile    *os.File
	Index          int    // last(current) wal segment
	First          int    // first wal segment
	SegmentSize    int    // segment size in bytes
	Buffer         []byte // buffer for the entries
	BytesRemaining int    // remaining bytes in the current segment
	Path           string // contains path to the WAL folder
}

func NewWriteAheadLog(filepath string, segmentSize int) (*WriteAheadLog, error) {
	err := createWorkDir(filepath)
	maxIndex, minIndex, err := ScanWALFolder(filepath)

	if err != nil {
		return nil, errors.New("error while reading the wal folder")
	}

	lastSegment := fmt.Sprintf("wal_%05d.log", maxIndex)
	lastSegmentPath := fp.Join(filepath, lastSegment)

	file := new(os.File)
	bytesRemaining := segmentSize

	// if the last segment is empty, we don't need to create a new one
	stat, err := os.Stat(lastSegmentPath)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(lastSegmentPath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if stat.Size() < int64(segmentSize) {
		file, err = os.OpenFile(lastSegmentPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		bytesRemaining = segmentSize - int(stat.Size())
	}

	return &WriteAheadLog{
		CurrentFile:    file,
		Index:          maxIndex,
		First:          minIndex,
		SegmentSize:    segmentSize,
		Buffer:         make([]byte, 0),
		BytesRemaining: bytesRemaining,
		Path:           filepath,
	}, nil
}

// creates a new segment file
func (wal *WriteAheadLog) createNewSegment() error {
	wal.Index++
	segmentName := fmt.Sprintf("wal_%05d.log", wal.Index)
	segmentPath := filepath.Join(wal.Path, segmentName)

	file, err := os.Create(segmentPath)
	if err != nil {
		return err
	}
	wal.CurrentFile = file
	wal.BytesRemaining = wal.SegmentSize
	return nil
}

// writes the current state of the buffer to the disk
func (wal *WriteAheadLog) dump() error {
	if wal.CurrentFile == nil {
		wal.createNewSegment()
	}

	if len(wal.Buffer) <= wal.BytesRemaining {
		if _, err := wal.CurrentFile.Write(wal.Buffer); err != nil {
			return err
		}
		wal.BytesRemaining -= len(wal.Buffer)
		wal.Buffer = make([]byte, 0)
		return nil
	}

	// write as many bytes as we can fit
	if _, err := wal.CurrentFile.Write(wal.Buffer[:wal.BytesRemaining]); err != nil {
		return err
	}

	bytesWritten := wal.BytesRemaining
	bytesToWrite := len(wal.Buffer) - wal.BytesRemaining

	cycles := bytesToWrite / wal.SegmentSize

	// close the current file
	if err := wal.CurrentFile.Close(); err != nil {
		return err
	}

	// this loop is used in case we need to write more than one segment
	for i := 0; i < cycles; i++ {
		// create a new segment
		if err := wal.createNewSegment(); err != nil {
			return err
		}

		// write the whole segments
		if _, err := wal.CurrentFile.Write(wal.Buffer[bytesWritten : bytesWritten+wal.SegmentSize]); err != nil {
			return err
		}

		bytesWritten += wal.SegmentSize

		if err := wal.CurrentFile.Close(); err != nil {
			return err
		}
	}

	// write the remaining bytes
	if err := wal.createNewSegment(); err != nil {
		return err
	}

	if _, err := wal.CurrentFile.Write(wal.Buffer[bytesWritten:]); err != nil {
		return err
	}

	wal.BytesRemaining -= len(wal.Buffer) - bytesWritten

	wal.Buffer = make([]byte, 0)
	return nil
}

func (wal *WriteAheadLog) isRdyToDump() bool {
	return len(wal.Buffer) >= wal.SegmentSize
}

// use this method when adding a new entry to the WAL
func (wal *WriteAheadLog) Log(key, value []byte, operation int) error {
	entry, err := NewEntry(key, value, operation)
	if err != nil {
		return err
	}

	wal.Buffer = append(wal.Buffer, entry.Serialize()...)

	if wal.isRdyToDump() {
		err = wal.dump()
	}

	return err
}
