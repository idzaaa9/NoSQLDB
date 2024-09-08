package memtable

import (
	"NoSQLDB/lib/pds"
	"encoding/binary"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type SSReader struct {
	dirPath string
}

func NewSSReader(dirPath string) (*SSReader, error) {
	return &SSReader{
		dirPath: dirPath,
	}, nil
}

func (re *SSReader) Get(key string) (*Entry, error) {
	numberGroups, err := re.groupFilesByNumber()
	if err != nil {
		return nil, err
	}

	sortedNumbers := sortedNumbers(numberGroups)
	for _, number := range sortedNumbers {
		fileNames := make([]string, 0)
		fileNames = append(fileNames, numberGroups[number]...)
		//iterating through tables from newest one
		filterFileName := findFileName(fileNames, "Filter")
		isInFilter, err := checkFilter(filterFileName, key)
		if err != nil {
			return nil, err
		}
		if !isInFilter {
			continue
		} else {
			summaryFileName := findFileName(fileNames, "Summary")
			startOffsetIndex, err := CheckSummaryIndex(summaryFileName, key, 0)
			if err != nil {
				return nil, err
			}

			indexFileName := findFileName(fileNames, "Index")
			startOffsetData, err := CheckSummaryIndex(indexFileName, key, startOffsetIndex)
			if err != nil {
				return nil, err
			}

			dataFileName := findFileName(fileNames, "Data")
			value, err := CheckData(dataFileName, key, startOffsetData)
			if err != nil {
				return nil, err
			}

			if value == nil {
				continue
			}

			entry := NewEntry(key, value, false)
			return entry, nil
		}
	}

	return nil, nil
}

func CheckSummaryIndex(fileName, keyToFind string, startOffset int) (int, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lowerKeyBuf []byte
	var lowerOffsetBuf int

	_, err = file.Seek(int64(startOffset), 0)
	if err != nil {
		return 0, err
	}

	for {
		lowerKey, lowerOffset, err := readSummaryIndexEntry(file)
		if err == io.EOF {
			break
		} else if err != nil {

			return 0, err
		}

		lowerKeyBuf = lowerKey

		if string(lowerKeyBuf) >= keyToFind {
			break
		}

		lowerOffsetBuf = lowerOffset
	}

	return lowerOffsetBuf, nil
}

func CheckData(fileName, keyToFind string, startOffset int) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(int64(startOffset), 0)
	if err != nil {
		return nil, err
	}

	for {
		key, value, err := readDataEntry(file)
		if err == io.EOF {
			return nil, nil
		} else if err != nil {
			return nil, err
		} else if string(key) == keyToFind {
			return value, nil
		}
	}
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func (re *SSReader) groupFilesByNumber() (map[int][]string, error) {
	if !dirExists(re.dirPath) {
		os.Mkdir(re.dirPath, 0755)
		return nil, nil
	}
	groups := make(map[int][]string)

	err := filepath.WalkDir(re.dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			if matched, _ := regexp.MatchString(`usertable-\d+-[^.]+\.txt`, d.Name()); matched {
				parts := strings.Split(d.Name(), "-")
				if len(parts) >= 2 {
					numberStr := parts[1]
					number, err := strconv.Atoi(numberStr)
					if err == nil {
						groups[number] = append(groups[number], path)
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return groups, nil
}

func sortedNumbers(groups map[int][]string) []int {
	var sortedNumbers []int
	for number := range groups {
		sortedNumbers = append(sortedNumbers, number)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sortedNumbers)))
	return sortedNumbers
}

func findFileName(fileNames []string, word string) string {
	for _, s := range fileNames {
		if strings.Contains(s, word) {
			return s
		}
	}
	return ""
}

func checkFilter(filterFileName string, key string) (bool, error) {
	serializedBloomfilter, err := os.ReadFile(filterFileName)
	if err != nil {
		return false, err
	}
	bloomfilter, err := pds.DeserializeFromBytes(serializedBloomfilter)
	if err != nil {
		return false, err
	}

	return bloomfilter.Query(key), nil
}

func readSummaryIndexEntry(file *os.File) ([]byte, int, error) {
	keyLenBuf := make([]byte, KEY_SIZE_SIZE)
	_, err := file.Read(keyLenBuf)
	if err != nil {
		return nil, 0, err
	}

	serializedKeyBuf := make([]byte, int32(binary.BigEndian.Uint32(keyLenBuf)))
	_, err = file.Read(serializedKeyBuf)
	if err != nil {
		return nil, 0, err
	}

	offsetBuf := make([]byte, 4) // sizeof int
	_, err = file.Read(offsetBuf)
	if err != nil {
		return nil, 0, err
	}

	offset := int(binary.BigEndian.Uint32(offsetBuf))

	return serializedKeyBuf, offset, nil
}

func readDataEntry(file *os.File) ([]byte, []byte, error) {
	tombstoneBuf := make([]byte, TOMBSTONE_SIZE)
	_, err := file.Read(tombstoneBuf)
	if err != nil {
		return nil, nil, err
	}

	keyLenBuf := make([]byte, KEY_SIZE_SIZE)
	_, err = file.Read(keyLenBuf)
	if err != nil {
		return nil, nil, err
	}

	serializedKeyBuf := make([]byte, int32(binary.BigEndian.Uint32(keyLenBuf)))
	_, err = file.Read(serializedKeyBuf)
	if err != nil {
		return nil, nil, err
	}

	valueLenBuf := make([]byte, VALUE_SIZE_SIZE)
	_, err = file.Read(valueLenBuf)
	if err != nil {
		return nil, nil, err
	}

	serializedValueBuf := make([]byte, int32(binary.BigEndian.Uint32(valueLenBuf)))
	_, err = file.Read(serializedValueBuf)
	if err != nil {
		return nil, nil, err
	}

	return serializedKeyBuf, serializedValueBuf, nil
}
