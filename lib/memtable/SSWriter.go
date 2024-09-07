package memtable

import (
	"NoSQLDB/lib/pds"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type SSWriter struct {
	outputDir     string
	tableGen      int
	filter        *pds.BloomFilter
	indexStride   int
	summaryStride int
}

func NewSSWriter(outputDir string, tableGen, indexStride, summaryStride int, filter *pds.BloomFilter) (*SSWriter, error) {
	return &SSWriter{
		outputDir:     outputDir,
		tableGen:      tableGen,
		filter:        filter,
		indexStride:   indexStride,
		summaryStride: summaryStride,
	}, nil
}

// Flush writes data from the Memtable to SSTable (data, index, summary, filter, and metadata).
// It takes a Memtable as input and performs the following steps:
// 1. Generates filenames for the required files.
// 2. Creates and opens these files.
// 3. Writes data, index entries, summary data, filter data, and metadata to the respective files.
// 4. If 'isSingleFile' is true, it merges the data from different segments into a single file.
// 5. Records segment offsets in a separate file.
// 6. Closes all files.
// 7. Optionally deletes the intermediate files (if 'isSingleFile' is true).
func (wr *SSWriter) Flush(mt Memtable) error {
	// Generate filenames for data, index, summary, filter, and metadata files
	fileNames := wr.generateFilenames()

	// Create and open the necessary files
	err := wr.generateFiles(fileNames)
	if err != nil {
		return err
	}

	// Write data, index entries, summary data, filter data, and metadata to the files
	err = wr.writeToFiles(mt, fileNames)
	if err != nil {
		return err
	}

	return nil
}

// generateFilenames creates a set of filenames for different components of a sstable.
// It constructs filenames based on the sstable generation number (wr.tableGen) and the output directory.
// The generated filenames include Data, Index, Summary, Filter, and Metadata files.
func (wr *SSWriter) generateFilenames() []string {
	fileNames := make([]string, 0)

	// Construct filenames for various components
	fileNameData := fmt.Sprintf("usertable-%02d-Data.txt", wr.tableGen)
	fileNameData = filepath.Join(wr.outputDir, fileNameData)

	fileNameIndex := fmt.Sprintf("usertable-%02d-Index.txt", wr.tableGen)
	fileNameIndex = filepath.Join(wr.outputDir, fileNameIndex)

	fileNameSummary := fmt.Sprintf("usertable-%02d-Summary.txt", wr.tableGen)
	fileNameSummary = filepath.Join(wr.outputDir, fileNameSummary)

	fileNameFilter := fmt.Sprintf("usertable-%02d-Filter.txt", wr.tableGen)
	fileNameFilter = filepath.Join(wr.outputDir, fileNameFilter)

	// Add the generated filenames to the slice
	fileNames = append(fileNames, fileNameData, fileNameIndex, fileNameSummary, fileNameFilter)

	return fileNames
}

// generateFiles creates empty files with the specified names.
// It takes a slice of filenames as input and creates each file.
// If any error occurs during file creation, it returns that error.
func (wr *SSWriter) generateFiles(fileNames []string) error {
	for _, name := range fileNames {
		// Create a new file with the given name
		file, err := os.Create(name)
		if err != nil {
			return err // Return the error if file creation fails
		}
		file.Close() // Close the file immediately (deferred close)
	}
	return nil // All files created successfully
}

// serializeEntry serializes an Entry (key-value pair) into a byte slice.
// It constructs a binary representation that includes tombstone information,
// key length, key data, value length, and value data.
func (wr *SSWriter) serializeEntry(e Entry) []byte {
	var data []byte
	// Create a tombstone slice (initially all zeros)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	if e.tombstone {
		tombstone[0] = 1
	} else {
		tombstone[0] = 0
	}

	data = tombstone

	// Determine the length of the key
	keyLen := uint32(len(e.key))
	keyLenBytes := make([]byte, KEY_SIZE_SIZE)
	binary.BigEndian.PutUint32(keyLenBytes, keyLen)

	data = append(data, keyLenBytes...)

	// Append the key
	data = append(data, []byte(e.key)...)

	if e.tombstone {
		return data
	}

	// Determine the length of the value
	valueLen := uint32(len(e.value))
	valueLenBytes := make([]byte, VALUE_SIZE_SIZE)
	binary.BigEndian.PutUint32(valueLenBytes, valueLen)

	data = append(data, valueLenBytes...)

	data = append(data, e.value...)

	return data
}

// openFiles opens or creates a set of files with the specified names for writing.
// It takes a slice of filenames as input and returns a slice of file pointers.
// If any error occurs during file opening, it closes any previously opened files
// and returns that error.
func openFiles(fileNames []string) ([]*os.File, error) {
	var files []*os.File

	for _, fileName := range fileNames {
		// Open or create a new file for writing
		file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			// If opening fails, close any previously opened files and return the error
			for _, f := range files {
				f.Close()
			}
			return nil, err
		}
		files = append(files, file) // Add the opened file to the slice
	}

	return files, nil // All files opened successfully
}

// writeToFiles orchestrates the process of writing data, index, summary, filter, and metadata files.
// It takes a Memtable (presumably containing key-value pairs), a slice of file names, and performs the following steps:
// 1. Opens the necessary files (data, index, summary, filter, and metadata).
// 2. Sorts the keys from the Memtable.
// 3. Optionally compresses keys if 'isCompressed' is true.
// 4. Serializes and writes each entry to the data file.
// 5. Writes index entries and summary data at specific intervals.
// 6. Maintains data and index offsets.
// 7. Writes filter data to the filter file.
// 8. Constructs and writes the serialized Merkle tree (metadata) to the metadata file.
// 9. Closes all files when done.
func (wr *SSWriter) writeToFiles(mt Memtable, fileNames []string) error {
	// Open necessary files (data, index, summary, filter, metadata)
	files, err := openFiles(fileNames)
	if err != nil {
		return err
	}

	// Sort keys from the Memtable
	sortedKeys := mt.SortKeys()

	dataFile := files[0]
	indexFile := files[1]
	summaryFile := files[2]
	filterFile := files[3]

	for i, key := range sortedKeys {
		// Add key to the filter
		wr.filter.Add(key)

		// Get entry from the Memtable
		entry, err := mt.Get(key)
		if err != nil {
			return err
		}

		// Serialize the entry and write to the data file
		serializedEntry := wr.serializeEntry(*entry)
		entryLen, err := dataFile.Write(serializedEntry)
		if err != nil {
			return err
		}

		if (i+1)%wr.indexStride == 0 {
			keyLenBuf := make([]byte, KEY_SIZE_SIZE)
			binary.BigEndian.PutUint32(keyLenBuf, uint32(len(key)))
			serializedKey := []byte(key)

			dataFile.Seek(int64(entryLen)*-1, io.SeekCurrent)
			position, err := Tell(dataFile)
			if err != nil {
				return err
			}
			positionBuf := make([]byte, 4) // size of an int
			binary.BigEndian.PutUint32(positionBuf, uint32(position))

			indexEntry := make([]byte, 0)
			indexEntry = append(indexEntry, keyLenBuf...)
			indexEntry = append(indexEntry, serializedKey...)
			indexEntry = append(indexEntry, positionBuf...)

			indexEntrySize, err := indexFile.Write(indexEntry)
			if err != nil {
				return err
			}

			if (i+1)%(wr.summaryStride*wr.indexStride) == 0 {
				indexPos, err := Tell(indexFile)
				if err != nil {
					return err
				}
				indexPos -= indexEntrySize

				indexPosBuf := make([]byte, 4) // sizeof int
				binary.BigEndian.PutUint32(indexPosBuf, uint32(indexPos))

				summaryFile.Write(keyLenBuf)
				summaryFile.Write(serializedKey)
				summaryFile.Write(indexPosBuf)
			}
		}
	}

	serializedFilter, err := wr.filter.SerializeToBytes()
	if err != nil {
		return err
	}
	filterFile.Write(serializedFilter)

	// Close all files
	err = closeFiles(files)
	if err != nil {
		panic(err)
	}

	wr.tableGen++
	return nil
}

func Tell(file *os.File) (int, error) {
	pos, err := file.Seek(0, io.SeekCurrent)
	return int(pos), err
}

// closeFiles closes a set of open files.
// It takes a slice of file pointers and ensures that each file is properly closed.
// If any error occurs during file closing, it returns that error.
func closeFiles(files []*os.File) error {
	for _, file := range files {
		err := file.Close()
		if err != nil {
			return err // Return the error if file closing fails
		}
	}
	return nil // All files closed successfully
}

/*
// generateMergedAndSegmentFiles creates and opens two files: the merged data file and the segment offsets file.
// It constructs filenames based on the table generation number (wr.tableGen) and the output directory.
// The merged data file contains the combined data from various segments.
// The segment offsets file stores information about the offsets of individual segments.
func (wr *SSWriter) generateMergedAndSegmentFiles() (*os.File, *os.File, error) {
	// Construct the filename for the merged data file
	mergedFileName := fmt.Sprintf("usertable-%02d.txt", wr.tableGen)
	mergedFileName = filepath.Join(wr.outputDir, mergedFileName)

	// Construct the filename for the segment offsets file
	segmentOffsetsFileName := filepath.Join(wr.outputDir, "segmentOffsets.txt")

	// Open the merged data file for writing
	mergedFile, err := os.OpenFile(mergedFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, err
	}

	// Open the segment offsets file for writing
	segmentOffsetsFile, err := os.OpenFile(segmentOffsetsFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, err
	}

	return mergedFile, segmentOffsetsFile, nil
}

// openFilesForReading opens a set of files for reading.
// It takes a slice of filenames as input and returns a slice of file pointers.
// If any error occurs during file opening, it closes any previously opened files
// and returns that error.
func openFilesForReading(fileNames []string) ([]*os.File, error) {
	var files []*os.File

	for _, fileName := range fileNames {
		// Open the file for reading
		file, err := os.Open(fileName)
		if err != nil {
			// If opening fails, close any previously opened files and return the error
			for _, f := range files {
				f.Close()
			}
			return nil, err
		}
		files = append(files, file) // Add the opened file to the slice
	}

	return files, nil // All files opened successfully
}

// writeToMergeAndSegments combines data from various segments into a merged file.
// It also records the offsets of different segments in a separate segment offsets file.
// The function takes the following parameters:
// - segmentOffsetsFile: The file to record segment offsets.
// - mergedFile: The merged data file to write combined data.
// - files: A slice of open files (data, index, summary, filter, and metadata).
// The function performs the following steps:
// 1. Writes a placeholder offset for the data segment to the segment offsets file.
// 2. Copies data from the data segment file to the merged data file.
// 3. Records the current offset in the segment offsets file for the index segment.
// 4. Copies data from the index segment file to the merged data file.
// 5. Repeats steps 3 and 4 for the summary, filter, and metadata segments.
// 6. Returns any encountered errors.
func writeToMergeAndSegments(segmentOffsetsFile, mergedFile *os.File, files []*os.File) error {
	// Write a placeholder offset for the data segment in the segment offsets file
	segmentOffsetsFile.WriteString("Data: 0\n")
	// Copy data from the data segment file to the merged data file
	copyFileContents(files[0], mergedFile)
	// Record the current offset for the index segment in the segment offsets file
	currentOffset, err := mergedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	segmentOffsetsFile.WriteString(fmt.Sprint("Index: ", currentOffset, "\n"))
	// Copy data from the index segment file to the merged data file
	copyFileContents(files[1], mergedFile)
	currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	segmentOffsetsFile.WriteString(fmt.Sprint("Summary: ", currentOffset, "\n"))
	copyFileContents(files[2], mergedFile)
	currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	segmentOffsetsFile.WriteString(fmt.Sprint("Filter: ", currentOffset, "\n"))
	copyFileContents(files[3], mergedFile)
	currentOffset, err = mergedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	segmentOffsetsFile.WriteString(fmt.Sprint("Metadata: ", currentOffset, "\n"))
	copyFileContents(files[4], mergedFile)

	return nil
}

// closeMergedAndSegmentFiles closes the merged data file and the segment offsets file.
// It takes two file pointers as input and ensures that both files are properly closed.
// If any error occurs during file closing, it returns that error.
func closeMergedAndSegmentFiles(mergedFile, segmentsFile *os.File) error {
	// Close the merged data file
	err := mergedFile.Close()
	if err != nil {
		return err // Return the error if file closing fails
	}

	// Close the segment offsets file
	err = segmentsFile.Close()
	if err != nil {
		return err // Return the error if file closing fails
	}

	return nil // Both files closed successfully
}

// deleteFiles removes a set of files from the filesystem.
// It takes a slice of filenames as input and attempts to delete each file.
// If any error occurs during file removal, it returns that error.
func deleteFiles(fileNames []string) error {
	for _, fileName := range fileNames {
		err := os.Remove(fileName)
		if err != nil {
			return err // Return the error if file removal fails
		}
	}
	return nil // All files removed successfully
}
*/
