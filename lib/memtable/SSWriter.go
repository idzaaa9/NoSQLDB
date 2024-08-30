package memtable

import (
	"NoSQLDB/lib/pds"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type SSWriter struct {
	outputDir     string
	tableGen      int
	isSingleFile  bool
	isCompressed  bool
	filter        pds.BloomFilter
	indexStride   int
	summaryStride int
}

func NewSSWriter(outputDir string) (*SSWriter, error) {
	return &SSWriter{
		outputDir: outputDir,
	}, nil
}

// if isSingleFile == false
// add variable encoding
func (wr *SSWriter) Flush(mt Memtable) error {
	sortedKeys := mt.SortKeys()

	fileNameData := fmt.Sprintf("usertable-%02d-Data.txt", wr.tableGen)
	fileNameData = wr.outputDir + "/" + fileNameData
	fileNameIndex := fmt.Sprintf("usertable-%02d-Index.txt", wr.tableGen)
	fileNameIndex = wr.outputDir + "/" + fileNameIndex
	fileNameSummary := fmt.Sprintf("usertable-%02d-Summary.txt", wr.tableGen)
	fileNameSummary = wr.outputDir + "/" + fileNameSummary

	fileData, _ := createFile(fileNameData)
	defer fileData.Close()

	fileIndex, _ := createFile(fileNameIndex)
	defer fileIndex.Close()

	fileSummary, _ := createFile(fileNameSummary)
	defer fileSummary.Close()

	offsetData := 0
	offsetIndex := 0

	for i, key := range sortedKeys {
		wr.filter.Add(key)
		entry, _ := mt.Get(key)
		serializedEntry := entry.Serialize()

		writeBytesToFile(serializedEntry, fileData)

		serializedKey, _ := serializeString(entry.Key())

		if i%wr.indexStride == 0 {
			writeBytesToFile(serializedKey, fileIndex)
			writeOffsetToFile(offsetData, fileIndex)
		}

		if i%wr.summaryStride == 0 {
			writeBytesToFile(serializedKey, fileSummary)
			writeOffsetToFile(offsetIndex, fileIndex)
		}

		offsetData += len(serializedEntry)
		if i%wr.indexStride == 0 {
			offsetIndex += len(serializedKey)
			offsetIndex += len(intToBinary(offsetData))
		}
	}

	return nil
}

func createFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func writeBytesToFile(serializedEntry []byte, file *os.File) error {
	_, err := file.Write(serializedEntry)
	if err != nil {
		return err
	}
	return nil
}

func writeOffsetToFile(offset int, file *os.File) error {
	binaryOffset := intToBinary(offset)

	_, err := file.Write(binaryOffset)
	if err != nil {
		return err
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
