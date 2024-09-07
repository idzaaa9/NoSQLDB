package memtable

import (
	"NoSQLDB/lib/pds"
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

func (re *SSReader) Get(key string) ([]byte, error) {
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
			//summaryFileName := findFileName(fileNames, "Summary")
		}
	}

	return nil, nil
}

func (re *SSReader) groupFilesByNumber() (map[int][]string, error) {
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
