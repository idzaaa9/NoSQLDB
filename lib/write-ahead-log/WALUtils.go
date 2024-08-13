package writeaheadlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// helper functions to deserialize the data

func deserializeTimestamp(data []byte) time.Time {
	return time.Unix(int64(binary.BigEndian.Uint64(data)), 0)
}

func deserializeTombstone(data []byte) bool {
	return data[0] == 1
}

func deserializeKeySize(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func deserializeValueSize(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func deserializeCRC(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func deserializeHeader(data []byte) (time.Time, bool, uint64, uint64) {
	timestamp := deserializeTimestamp(data[CRC_SIZE : CRC_SIZE+TIMESTAMP_SIZE])
	tombstone := deserializeTombstone(data[CRC_SIZE+TIMESTAMP_SIZE : CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE])
	keysize := deserializeKeySize(data[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE : CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE])
	valuesize := deserializeValueSize(data[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE : CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE])

	return timestamp, tombstone, keysize, valuesize
}

func deserializeKeyOrValue(data []byte) string {
	return string(data)
}

// util function which opens next segment

func (reader *WALReader) openNextSegment() error {
	// close the current file
	if err := reader.CurrentFile.Close(); err != nil {
		return err
	}
	fmt.Println("segment closed")
	// open the next segment for reading

	if reader.Cursor == reader.SegmentCount {
		return errors.New("no more segments to read")
	}

	reader.Cursor++

	segmentName := fmt.Sprintf("wal_%05d.log", reader.Cursor)
	segmentPath := filepath.Join(reader.Path, segmentName)

	file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)

	if err != nil {
		return err
	}

	fmt.Println("segment ", segmentPath, " opened")

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	fmt.Println("file size: ", fileInfo.Size())

	reader.CurrentFile = file
	reader.BytesRemaining = int(fileInfo.Size())

	return nil
}

// check if the crc is correct

func checkCRC(data []byte) bool {
	return binary.BigEndian.Uint32(data[:4]) == crc32.ChecksumIEEE(data[4:])
}

/*
this function will go through the folder with the WAL data, and return the
largest value of a segment
ALL OF THE WAL FILE NAMES MUST BE IN THE FORMAT "wal_00001.log" !!!!!!!!!!
*/
func ScanWALFolder(path string) (int, int, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return -1, -1, err
	}

	// this regex returns all of the files which match our format
	re := regexp.MustCompile(`^wal_(\d{5})\.log$`)

	maxIndex := 0
	minIndex := 999999999

	isEmpty := true

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

		isEmpty = false

		indexStr := matches[1]
		// Convert the index to an integer
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return -1, -1, err
		}

		// Update the maximum index found
		if index > maxIndex {
			maxIndex = index
		}
		if index < minIndex {
			minIndex = index
		}
	}

	if isEmpty {
		return 0, 0, nil
	}

	return maxIndex, minIndex, nil
}

// creates the work directory for the WAL if it doesn't exist
func createWorkDir(filepath string) error {
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(filepath, 0755)
	}
	return err
}
