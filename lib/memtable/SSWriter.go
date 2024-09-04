package memtable

import (
	"NoSQLDB/lib/merkle-tree"
	"NoSQLDB/lib/pds"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

type SSWriter struct {
	outputDir     string
	tableGen      int
	isSingleFile  bool
	isCompressed  bool
	filter        *pds.BloomFilter
	indexStride   int
	summaryStride int
}

func NewSSWriter(outputDir string, tableGen, indexStride, summaryStride int, isSingleFile, isCompressed bool, filter *pds.BloomFilter) (*SSWriter, error) {
	if isCompressed {
		fileNameDictionary := outputDir + "/dictionary.txt"
		fileDictionary, err := os.Create(fileNameDictionary)
		if err != nil {
			return nil, err
		}
		defer fileDictionary.Close()
	}
	return &SSWriter{
		outputDir:     outputDir,
		tableGen:      tableGen,
		isSingleFile:  isSingleFile,
		isCompressed:  isCompressed,
		filter:        filter,
		indexStride:   indexStride,
		summaryStride: summaryStride * indexStride,
	}, nil
}

func (wr *SSWriter) Flush(mt Memtable) error {
	sortedKeys := mt.SortKeys()
	binaryKeys := make([][]byte, 0)

	fileNameData := fmt.Sprintf("usertable-%02d-Data.txt", wr.tableGen)
	fileNameData = wr.outputDir + "/" + fileNameData
	fileNameIndex := fmt.Sprintf("usertable-%02d-Index.txt", wr.tableGen)
	fileNameIndex = wr.outputDir + "/" + fileNameIndex
	fileNameSummary := fmt.Sprintf("usertable-%02d-Summary.txt", wr.tableGen)
	fileNameSummary = wr.outputDir + "/" + fileNameSummary
	fileNameFilter := fmt.Sprintf("usertable-%02d-Filter.txt", wr.tableGen)
	fileNameFilter = wr.outputDir + "/" + fileNameFilter
	fileNameMetadata := fmt.Sprintf("usertable-%02d-Metadata.txt", wr.tableGen)
	fileNameMetadata = wr.outputDir + "/" + fileNameMetadata

	fileData, err := os.Create(fileNameData)
	if err != nil {
		return err
	}
	defer fileData.Close()

	fileIndex, err := os.Create(fileNameIndex)
	if err != nil {
		return err
	}
	defer fileIndex.Close()

	fileSummary, err := os.Create(fileNameSummary)
	if err != nil {
		return err
	}
	defer fileSummary.Close()

	fileFilter, err := os.Create(fileNameFilter)
	if err != nil {
		return err
	}
	defer fileFilter.Close()

	fileMetadata, err := os.Create(fileNameMetadata)
	if err != nil {
		return err
	}
	defer fileMetadata.Close()

	offsetData := 0
	offsetIndex := 0

	firstSerializedKey := make([]byte, 0)
	lastSerializedKey := make([]byte, 0)
	firstKeyOffsetIndex := 0
	lastKeyOffsetIndex := 0

	compressionCounter := 1

	for i, key := range sortedKeys {
		wr.filter.Add(key)
		entry, err := mt.Get(key)
		if err != nil {
			return err
		}

		if wr.isCompressed {
			keyNumeric, err := wr.keyTransformation(entry.key, &compressionCounter)
			if err != nil {
				return err
			}
			entry.key = strconv.Itoa(keyNumeric)
		}

		serializedEntry := entry.Serialize()
		binaryKeys = append(binaryKeys, serializedEntry)

		_, err = fileData.Write(serializedEntry)
		if err != nil {
			return err
		}

		serializedKey, err := serializeString(entry.Key())
		if err != nil {
			return err
		}

		if i%wr.indexStride == 0 {
			if i == wr.indexStride-1 {
				firstSerializedKey = serializedKey
				firstKeyOffsetIndex = offsetIndex
			} else if i == len(sortedKeys)-1 {
				lastSerializedKey = serializedKey
				lastKeyOffsetIndex = offsetIndex
			}

			_, err := fileIndex.Write(append(serializedKey, intToBinary(offsetData)...))
			if err != nil {
				return err
			}
		}

		if i%wr.summaryStride == 0 {
			_, err := fileSummary.Write(append(serializedKey, intToBinary(offsetIndex)...))
			if err != nil {
				return err
			}
		}

		offsetData += len(serializedEntry)
		if i%wr.indexStride == 0 {
			offsetIndex += len(serializedKey)
			offsetIndex += len(intToBinary(offsetData))
		}
	}

	_, err = fileSummary.Write(append(firstSerializedKey, lastSerializedKey...))
	if err != nil {
		return err
	}
	_, err = fileSummary.Write(append(intToBinary(firstKeyOffsetIndex), intToBinary(lastKeyOffsetIndex)...))
	if err != nil {
		return err
	}

	serializedFilter, err := wr.filter.SerializeToBytes()
	if err != nil {
		return err
	}
	_, err = fileFilter.Write(serializedFilter)
	if err != nil {
		return err
	}

	metadata := merkle.BuildMerkleTree(binaryKeys)
	serializedMetadata := merkle.SerializeMerkleTree(metadata)
	_, err = fileMetadata.Write(serializedMetadata)
	if err != nil {
		return err
	}

	if wr.isSingleFile {
		mergedFileName := fmt.Sprintf("usertable-%02d.txt", wr.tableGen)
		mergedFileName = wr.outputDir + "/" + mergedFileName
		mergedFile, err := os.Create(mergedFileName)
		if err != nil {
			return err
		}
		defer mergedFile.Close()

		segmentOffsetsFileName := wr.outputDir + "/" + "segmentOffsets.txt"
		segmentOffsetsFile, err := os.Create(segmentOffsetsFileName)
		if err != nil {
			return err
		}
		defer segmentOffsetsFile.Close()

		fileData, err := os.OpenFile(fileNameData, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer fileData.Close()

		fileIndex, err := os.OpenFile(fileNameIndex, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer fileIndex.Close()

		fileSummary, err := os.OpenFile(fileNameSummary, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer fileSummary.Close()

		fileFilter, err := os.OpenFile(fileNameFilter, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer fileFilter.Close()

		fileMetadata, err := os.OpenFile(fileNameMetadata, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer fileMetadata.Close()

		segmentOffsetsFile.WriteString("Data: 0\n")
		copyFileContents(fileData, mergedFile)
		currentOffset, err := mergedFile.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		segmentOffsetsFile.WriteString(fmt.Sprint("Index: ", currentOffset, "\n"))
		copyFileContents(fileIndex, mergedFile)
		currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		segmentOffsetsFile.WriteString(fmt.Sprint("Summary: ", currentOffset, "\n"))
		copyFileContents(fileSummary, mergedFile)
		currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		segmentOffsetsFile.WriteString(fmt.Sprint("Filter: ", currentOffset, "\n"))
		copyFileContents(fileFilter, mergedFile)
		currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		segmentOffsetsFile.WriteString(fmt.Sprint("Metadata: ", currentOffset, "\n"))
		copyFileContents(fileMetadata, mergedFile)

		fileData.Close()
		fileIndex.Close()
		fileSummary.Close()
		fileFilter.Close()
		fileMetadata.Close()

		err = os.Remove(fileNameData)
		if err != nil {
			return err
		}
		err = os.Remove(fileNameIndex)
		if err != nil {
			return err
		}
		err = os.Remove(fileNameSummary)
		if err != nil {
			return err
		}
		err = os.Remove(fileNameFilter)
		if err != nil {
			return err
		}
		err = os.Remove(fileNameMetadata)
		if err != nil {
			return err
		}
	}

	return nil
}

func intToBinary(n int) []byte {
	binaryData := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(binaryData, int64(n))
	return binaryData
}

func serializeString(s string) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, uint64(len(s)))
	if err != nil {
		return nil, err
	}
	buf.WriteString(s)
	return buf.Bytes(), nil
}

func copyFileContents(src, dst *os.File) error {
	const bufferSize = 4096

	buffer := make([]byte, bufferSize)
	for {
		n, err := src.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		_, err = dst.Write(buffer[:n])
		if err != nil {
			return err
		}
	}

	return nil
}

func (wr *SSWriter) keyTransformation(key string, counter *int) (int, error) {
	fileNameDictionary := wr.outputDir + "/dictionary.txt"
	fileDict, err := os.OpenFile(fileNameDictionary, os.O_RDWR, 0666)
	if err != nil {
		return 0, err
	}
	defer fileDict.Close()

	exists, offset, err := checkBytesInFile(fileDict, []byte(key))
	if err != nil {
		return 0, err
	}

	if !exists {
		fileDict.Write(intToBinary(len([]byte(key))))
		fileDict.Write([]byte(key))
		fileDict.Write(intToBinary(*counter))
		*counter++
		return *counter - 1, nil
	} else {
		fileDict.Seek(offset, 0)
		numericValue, err := readNumFromDict(fileDict)
		if err != nil {
			return 0, err
		}
		return numericValue, nil
	}
}

func checkBytesInFile(file *os.File, targetBytes []byte) (bool, int64, error) {
	buffer := make([]byte, len(targetBytes))
	var position int64

	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, 0, err
		}

		if n >= len(targetBytes) && bytesEqual(buffer[:len(targetBytes)], targetBytes) {
			return true, position, nil
		}

		position += int64(n)
	}

	return false, 0, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readNumFromDict(fileDict *os.File) (int, error) {
	var keyLen uint64
	if err := binary.Read(fileDict, binary.LittleEndian, &keyLen); err != nil {
		return 0, err
	}

	keyBytes := make([]byte, keyLen)
	if _, err := fileDict.Read(keyBytes); err != nil {
		return 0, err
	}

	var numericValue int
	if err := binary.Read(fileDict, binary.LittleEndian, &numericValue); err != nil {
		return 0, err
	}

	return numericValue, nil
}
