package writeaheadlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
	Index          int // last(current) wal segment
	First          int // first wal segment
	SegmentSize    int // segment size in bytes
	Buffer         WALBuffer
	BytesRemaining int    // remaining bytes from the last segment
	Path           string // contains path to the WAL folder
}

/*
this function will go through the folder with the WAL data, and return the
largest value of a segment
ALL OF THE WAL FILE NAMES MUST BE IN THE FORMAT "wal_00001.log" !!!!!!!!!!
*/
func ScanWALFolder(path string) (int, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return -1, err
	}

	// this regex returns all of the files which match our format
	re := regexp.MustCompile(`^wal_(\d{5})\.log$`)

	maxIndex := 0
	minIndex := 999999999

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()

		// Match the filename against the regex pattern
		matches := re.FindStringSubmatch(filename)

		if len(matches) <= 1 {
			continue
		}

		indexStr := matches[1]
		// Convert the index to an integer
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return 0, err
		}

		// Update the maximum index found
		if index > maxIndex {
			maxIndex = index
		} else if index < minIndex {
			minIndex = index
		}
	}

	return maxIndex, nil
}

// creates the work directory for the WAL if it doesn't exist
func createWorkDir(filepath string) error {
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(filepath, 0755)
	}
	return err
}

func NewWriteAheadLog(filepath string, segmentSize int) (*WriteAheadLog, error) {
	err := createWorkDir(filepath)
	index, err := ScanWALFolder(filepath)

	if err != nil {
		return nil, errors.New("error while reading the wal folder")
	}

	return &WriteAheadLog{
		CurrentFile:    nil,
		Index:          index,
		SegmentSize:    segmentSize,
		Buffer:         *NewWALBuffer(segmentSize),
		BytesRemaining: segmentSize,
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
	return nil
}

// writes the current state of the buffer to the disk
func (wal *WriteAheadLog) dump() error {
	if wal.CurrentFile == nil {
		wal.Index++
		segmentName := fmt.Sprintf("wal_%05d.log", wal.Index)
		segmentPath := filepath.Join(wal.Path, segmentName)

		file, err := os.Create(segmentPath)
		if err != nil {
			return err
		}
		wal.CurrentFile = file
	}

	for i := 0; i < len(wal.Buffer.Data); i++ {
		entry := wal.Buffer.Data[i]

		serializedEntry := entry.Serialize()

		if wal.BytesRemaining >= entry.Size {
			if _, err := wal.CurrentFile.Write(serializedEntry); err != nil {
				return err
			}
			wal.BytesRemaining -= entry.Size
			continue
		}

		// write as many bytes as we can fit
		bytesWritten, err := wal.CurrentFile.Write(serializedEntry[:wal.BytesRemaining])
		if err != nil {
			return err
		}

		// close the current file
		if err := wal.CurrentFile.Close(); err != nil {
			return err
		}

		// create a new segment
		if err := wal.createNewSegment(); err != nil {
			return err
		}

		// write the remaining bytes
		if _, err := wal.CurrentFile.Write(serializedEntry[entry.Size-bytesWritten:]); err != nil {
			return err
		}
		wal.BytesRemaining = wal.SegmentSize - (entry.Size - bytesWritten)
	}

	wal.Buffer = *NewWALBuffer(wal.SegmentSize)
	return nil
}

func (wal *WriteAheadLog) isRdyToDump() bool {
	return wal.Buffer.Size >= wal.SegmentSize
}

// use this method when adding a new entry to the WAL
func (wal *WriteAheadLog) Log(key, value []byte, operation int) error {
	entry, err := NewEntry(key, value, operation)
	if err != nil {
		return err
	}
	wal.Buffer.Add(*entry)

	if wal.isRdyToDump() {
		err = wal.dump()
	}

	return err
}
