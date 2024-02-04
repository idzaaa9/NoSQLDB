package strukture

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type SStable struct {
	LSMLevel     int
	tableNumber  int
	isSingleFile bool
	filepath     string
}

func NewSStable(LSMLevel, tableNumber int, isSingleFile bool, filepath string) (*SStable, error) {
	fileinfo, err := os.Stat(filepath)
	if !fileinfo.IsDir() {
		return nil, errors.New("filepath provided to sstable isnt a directory")
	}
	if err != nil {
		return nil, err
	}

	return &SStable{
		LSMLevel:     LSMLevel,
		tableNumber:  tableNumber,
		isSingleFile: isSingleFile,
		filepath:     filepath,
	}, nil
}

// better name for this function is maybe contains
// since bloom filter can return false positive
func (table *SStable) Contains(key []byte) (bool, error) {
	var serializedFilter []byte
	var err error
	if !table.isSingleFile {
		filename := fmt.Sprintf("filter_%d_%d.db", table.LSMLevel, table.tableNumber)
		filePath := filepath.Join(table.filepath, "filter", filename)

		serializedFilter, err = os.ReadFile(filePath)

		if err != nil {
			return false, errors.New("error while reading serialized bloom filter")
		}
	} else {
		filename := fmt.Sprintf("sstable_%d_%d.db", table.LSMLevel, table.tableNumber)
		filePath := filepath.Join(table.filepath, "sstable", filename)

		file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
		defer file.Close()
		if err != nil {
			return false, errors.New("error while reading sstable file")
		}

		filterLen := make([]byte, 8)
		_, err = file.Read(filterLen)
		if err != nil {
			return false, errors.New("error while reading sstable file")
		}
		serializedFilterLen := binary.BigEndian.Uint64(filterLen)
		serializedFilter := make([]byte, serializedFilterLen)
		_, err = file.Read(serializedFilter)

	}
	tableBF, err := DeserializeBloomFilter(serializedFilter)

	if err != nil {
		return false, errors.New("error while deserializing bloom filter")
	}

	return tableBF.Lookup(string(key)), nil
}

func summaryErr() (int, bool, error) {
	return 0, false, errors.New("error while reading summary file")
}

func (table *SStable) checkSummary(key []byte) (int, bool, error) {
	var file *os.File
	var err error
	if !table.isSingleFile {
		filename := fmt.Sprintf("summary_%d_%d.db", table.LSMLevel, table.tableNumber)
		filepath := filepath.Join(table.filepath, "summary", filename)

		file, err = os.OpenFile(filepath, os.O_RDONLY, 0)
		defer file.Close()
		if err != nil {
			return summaryErr()
		}
	}

	var prevOffset int // offset of the previous key

	firstLenBytes := make([]byte, 4)
	_, err = file.Read(firstLenBytes)
	if err != nil {
		return summaryErr()
	}
	firstLen := binary.BigEndian.Uint32(firstLenBytes)
	firstKeyBytes := make([]byte, firstLen)
	_, err = file.Read(firstKeyBytes)
	if err != nil {
		return summaryErr()
	}
	lastLenBytes := make([]byte, 4)
	_, err = file.Read(lastLenBytes)
	if err != nil {
		return summaryErr()
	}
	lastLen := binary.BigEndian.Uint32(lastLenBytes)
	lastKeyBytes := make([]byte, lastLen)
	_, err = file.Read(lastKeyBytes)
	if err != nil {
		return summaryErr()
	}
	firstKey := string(firstKeyBytes)
	lastKey := string(lastKeyBytes)

	if !(firstKey <= string(key) && lastKey >= string(key)) {
		return 0, false, nil
	}

	// iterate through entries
	for {
		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return summaryErr()
		}
		keyBuf := make([]byte, keyLen)
		if _, err := file.Read(keyBuf); err != nil {
			return summaryErr()
		}
		keyStr := string(keyBuf)
		var indexOffset uint64
		if err := binary.Read(file, binary.BigEndian, &indexOffset); err != nil {
			return summaryErr()
		}

		if keyStr >= string(key) {
			break // break condition
		}

		prevOffset = int(indexOffset)
	}
	return prevOffset, true, nil
}

func (table *SStable) getIndex(prevOffset int, key []byte) (int, error) {
	var file *os.File
	var err error
	if !table.isSingleFile {
		filename := fmt.Sprintf("index_%d_%d.db", table.LSMLevel, table.tableNumber)
		filepath := filepath.Join(table.filepath, "index", filename)

		file, err = os.OpenFile(filepath, os.O_RDONLY, 0)
		defer file.Close()
		if err != nil {
			return 0, errors.New("error while reading index file")
		}
	}

	_, err = file.Seek(int64(prevOffset), 0)
	if err != nil {
		return 0, errors.New("error while seeking index file")
	}

	var lastOffset int // Variable to store the offset of the last smaller key

	// iterate through index file
	for {
		var keyLen uint32
		err := binary.Read(file, binary.BigEndian, &keyLen)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("error reading key length: %w", err)
		}
		keyBuf := make([]byte, keyLen)
		_, err = file.Read(keyBuf)
		if err != nil {
			return 0, fmt.Errorf("error reading key: %w", err)
		}
		var dataOffset uint64
		err = binary.Read(file, binary.BigEndian, &dataOffset)
		if err != nil {
			return 0, fmt.Errorf("error reading data offset: %w", err)
		}

		indexKey := string(keyBuf)
		providedKey := string(key)

		if indexKey < providedKey {
			lastOffset = int(dataOffset)
		} else {
			break // exit condition
		}
	}

	return lastOffset, nil
}

func (table *SStable) getData(prevOffset int, key []byte) (MemtableEntry, bool, error) {
	var file *os.File
	var err error
	if !table.isSingleFile {
		filename := fmt.Sprintf("data_%d_%d.db", table.LSMLevel, table.tableNumber)
		filePath := filepath.Join(table.filepath, "data", filename)

		file, err = os.OpenFile(filePath, os.O_RDONLY, 0)
		defer file.Close()
		if err != nil {
			return MemtableEntry{}, false, errors.New("error while reading data file")
		}

		dictFilename := fmt.Sprintf("dict_%d_%d.db", table.LSMLevel, table.tableNumber)
		dictFilePath := filepath.Join(table.filepath, "dict", dictFilename)
		_, err := os.Stat(dictFilePath)

		if err == nil {
			// TODO: handle dictionary compression
		}
	}
	file.Seek(int64(prevOffset), 1)
	for {
		data := make([]byte, 5) // TODO: fix to work with files
		decodedEntry, _, err := DeserializeMemtableEntry(data)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return MemtableEntry{}, false, nil
			}
			return MemtableEntry{}, false, fmt.Errorf("error while deserializing entry: %w", err)
		}
		if areEqual(decodedEntry.Key, key) {
			return decodedEntry, true, nil
		}
	}
}

func (table *SStable) Get(key []byte) (*MemtableEntry, error) {
	contains, err := table.Contains(key)
	if err != nil {
		return nil, err
	}
	if !contains {
		return nil, nil
	}

	indexOffset, found, err := table.checkSummary(key)
	if !found || err != nil {
		return nil, err
	}

	dataOffset, err := table.getIndex(indexOffset, key)

	entry, found, err := table.getData(dataOffset, key)
	if !found || err != nil {
		return nil, err
	}
	return &entry, nil
}

func areEqual(arr1, arr2 []byte) bool {
	if len(arr1) != len(arr2) {
		return false
	}
	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			return false
		}
	}
	return true
}
