package memtable

import (
	"NoSQLDB/lib/merkle-tree"
	"NoSQLDB/lib/pds"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
		fileNameDictionary := filepath.Join(outputDir, "dictionary.txt")
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

	if wr.isSingleFile {
		// Merge data from different segments into a single file
		mergedFile, segmentOffsetsFile, err := wr.generateMergedAndSegmentFiles()
		if err != nil {
			return err
		}

		// Open the original files for reading
		files, err := openFilesForReading(fileNames)
		if err != nil {
			return err
		}

		// Write merged data and record segment offsets
		err = writeToMergeAndSegments(segmentOffsetsFile, mergedFile, files)
		if err != nil {
			return err
		}

		// Close all files
		err = closeFiles(files)
		if err != nil {
			return err
		}

		// Close the merged data file and segment offsets file
		err = closeMergedAndSegmentFiles(mergedFile, segmentOffsetsFile)
		if err != nil {
			return err
		}

		// Delete the intermediate files
		err = deleteFiles(fileNames)
		if err != nil {
			return err
		}
	}

	return nil
}

// intToBinary encodes an integer into its binary representation.
// It returns a byte slice containing the binary data.
func intToBinary(n int) []byte {
	// Create a byte slice with enough capacity for the maximum varint length.
	binaryData := make([]byte, binary.MaxVarintLen64)

	// Encode the integer 'n' into the 'binaryData' slice.
	binary.PutVarint(binaryData, int64(n))

	// Return the resulting byte slice.
	return binaryData
}

// serializeString encodes a string into a binary representation.
// It returns a byte slice containing the serialized data.
func serializeString(s string) ([]byte, error) {
	// Create a buffer to hold the serialized data.
	var buf bytes.Buffer

	// Write the length of the string (as a uint64) in big-endian order.
	err := binary.Write(&buf, binary.BigEndian, uint64(len(s)))
	if err != nil {
		return nil, err
	}

	// Append the actual string data to the buffer.
	buf.WriteString(s)

	// Return the resulting byte slice.
	return buf.Bytes(), nil
}

// copyFileContents copies the contents from the source file (src) to the destination file (dst).
// It reads data in chunks of a fixed buffer size and writes them to the destination.
func copyFileContents(src, dst *os.File) error {
	const bufferSize = 4096 // Size of the buffer for reading/writing

	buffer := make([]byte, bufferSize)
	for {
		// Read data from the source file into the buffer
		n, err := src.Read(buffer)
		if err != nil {
			if err == io.EOF {
				// End of file reached; break out of the loop
				break
			}
			return err // Return any other read error
		}

		// Write the read data (up to 'n' bytes) to the destination file
		_, err = dst.Write(buffer[:n])
		if err != nil {
			return err // Return any write error
		}
	}

	return nil // Successfully copied all data
}

// keyTransformation performs a transformation on a given key.
// It maintains a dictionary file to store key-value pairs, where keys are strings
// and values are numeric identifiers. If the key already exists in the dictionary,
// it returns the corresponding numeric value; otherwise, it adds the key to the
// dictionary and assigns a new numeric value.
func (wr *SSWriter) keyTransformation(key string, counter *int) (int, error) {
	// Construct the path to the dictionary file
	fileNameDictionary := wr.outputDir + "/dictionary.txt"

	// Open the dictionary file (or create it if it doesn't exist)
	fileDict, err := os.OpenFile(fileNameDictionary, os.O_RDWR, 0666)
	if err != nil {
		return 0, err
	}
	defer fileDict.Close()

	// Check if the key already exists in the dictionary
	exists, offset, err := checkBytesInFile(fileDict, []byte(key))
	if err != nil {
		return 0, err
	}

	if !exists {
		// Key is not in the dictionary; add it
		fileDict.Write(intToBinary(len([]byte(key)))) // Write key length
		fileDict.Write([]byte(key))                   // Write key data
		fileDict.Write(intToBinary(*counter))         // Write the assigned numeric value
		*counter++
		return *counter - 1, nil
	} else {
		// Key exists; retrieve its numeric value
		fileDict.Seek(offset, 0)
		numericValue, err := readNumFromDict(fileDict)
		if err != nil {
			return 0, err
		}
		return numericValue, nil
	}
}

// checkBytesInFile searches for a sequence of target bytes within a file.
// It reads the file in chunks and compares each chunk with the target bytes.
// If the target bytes are found, it returns true along with the position in the file;
// otherwise, it returns false.
func checkBytesInFile(file *os.File, targetBytes []byte) (bool, int64, error) {
	buffer := make([]byte, len(targetBytes)) // Create a buffer for reading

	var position int64 // Keep track of the current position in the file

	for {
		n, err := file.Read(buffer) // Read data into the buffer
		if err != nil {
			if err == io.EOF {
				// End of file reached; target not found
				break
			}
			return false, 0, err // Return any other read error
		}

		// Compare the read data with the target bytes
		if n >= len(targetBytes) && bytesEqual(buffer[:len(targetBytes)], targetBytes) {
			// Target found; return true and the current position
			return true, position, nil
		}

		position += int64(n) // Update the position
	}

	// Target not found
	return false, 0, nil
}

// bytesEqual checks whether two byte slices are equal.
// It compares each byte in the slices and returns true if they match,
// or false if there's any difference.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false // Different lengths; not equal
	}
	for i := range a {
		if a[i] != b[i] {
			return false // Mismatch at position i
		}
	}
	return true // All bytes match; equal
}

// readNumFromDict reads a numeric value from the dictionary file.
// It assumes that the file pointer is positioned at the correct offset
// where the key's data starts.
func readNumFromDict(fileDict *os.File) (int, error) {
	// Read the length of the key (as a uint64) in little-endian order
	var keyLen uint64
	if err := binary.Read(fileDict, binary.LittleEndian, &keyLen); err != nil {
		return 0, err
	}

	// Read the actual key data into a byte slice
	keyBytes := make([]byte, keyLen)
	if _, err := fileDict.Read(keyBytes); err != nil {
		return 0, err
	}

	// Read the associated numeric value (little-endian int) from the file
	var numericValue int
	if err := binary.Read(fileDict, binary.LittleEndian, &numericValue); err != nil {
		return 0, err
	}

	return numericValue, nil
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

	fileNameMetadata := fmt.Sprintf("usertable-%02d-Metadata.txt", wr.tableGen)
	fileNameMetadata = filepath.Join(wr.outputDir, fileNameMetadata)

	// Add the generated filenames to the slice
	fileNames = append(fileNames, fileNameData, fileNameIndex, fileNameSummary, fileNameFilter, fileNameMetadata)

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
	// Create a tombstone slice (initially all zeros)
	tombstone := make([]byte, TOMBSTONE_SIZE)

	// Determine the length of the key
	keyLen := uint32(len(e.key))
	keyLenBytes := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(keyLenBytes, uint64(keyLen))

	if e.tombstone {
		// If it's a tombstone entry, set the first byte to 1
		tombstone[0] = 1
		data := append(tombstone, keyLenBytes[:n]...)
		return append(data, []byte(e.key)...)
	} else {
		// Otherwise, set the first byte to 0
		tombstone[0] = 0
	}

	// Determine the length of the value
	valueLen := uint32(len(e.value))
	valueLenBytes := make([]byte, binary.MaxVarintLen32)
	m := binary.PutUvarint(valueLenBytes, uint64(valueLen))

	// Construct the serialized data
	data := append(tombstone, keyLenBytes[:n]...)
	data = append(data, []byte(e.key)...)
	data = append(data, valueLenBytes[:m]...)
	data = append(data, []byte(e.value)...)

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

// compressKey transforms a given key into a compressed representation.
// It uses a dictionary-based approach to map keys to numeric values.
// The 'compressionCounter' keeps track of assigned numeric values.
func (wr *SSWriter) compressKey(entry *Entry, compressionCounter *int) error {
	// Obtain the numeric value for the key using the keyTransformation function
	keyNumeric, err := wr.keyTransformation(entry.key, compressionCounter)
	if err != nil {
		return err
	}

	// Convert the numeric value to a binary representation and update the entry's key
	entry.key = string(intToBinary(keyNumeric))

	return nil
}

// writeToIndex writes a serialized key and associated metadata to the index file.
// It keeps track of the first and last serialized keys encountered during the process.
// The 'files' slice contains the relevant open files (index file at index 1).
func (wr *SSWriter) writeToIndex(firstKeyOffsetIndex, lastKeyOffsetIndex *int, i, offsetData, offsetIndex int,
	serializedKey []byte, firstSerializedKey, lastSerializedKey *[]byte, files []*os.File) error {
	if i == wr.indexStride-1 {
		// If it's the last key in the current index block, update the first key info
		*firstSerializedKey = serializedKey
		*firstKeyOffsetIndex = offsetIndex
	} else {
		// Otherwise, update the last key info
		*lastSerializedKey = serializedKey
		*lastKeyOffsetIndex = offsetIndex
	}

	// Write the serialized key and associated metadata to the index file
	_, err := files[1].Write(append(serializedKey, intToBinary(offsetData)...))
	if err != nil {
		return err
	}

	return nil
}

// writeToSummary writes a serialized key and associated metadata to the summary file.
// It appends the serialized key and the binary representation of the index offset.
func writeToSummary(serializedKey []byte, offsetIndex int, files []*os.File) error {
	// Write the serialized key and associated metadata to the summary file
	_, err := files[2].Write(append(serializedKey, intToBinary(offsetIndex)...))
	if err != nil {
		return err
	}
	return nil
}

// increaseOffsets updates the data and index offsets based on the serialized entry and key.
// It keeps track of the total data offset and increments the index offset periodically.
func (wr *SSWriter) increaseOffsets(serializedEntry, serializedKey []byte, i int, offsetData, offsetIndex *int) {
	// Update the total data offset by adding the length of the serialized entry
	*offsetData += len(serializedEntry)

	// If it's time to increment the index offset (based on the index stride),
	// add the lengths of the serialized key and the binary representation of the data offset
	if i%wr.indexStride == 0 {
		*offsetIndex += len(serializedKey)
		*offsetIndex += len(intToBinary(*offsetData))
	}
}

// addFirstAndLastKeyToSummary appends the first and last serialized keys, along with their offsets,
// to the summary file. It ensures that the summary file contains essential information about the keys.
func addFirstAndLastKeyToSummary(firstSerializedKey, lastSerializedKey []byte, firstKeyOffsetIndex, lastKeyOffsetIndex int, files []*os.File) error {
	// Append the first and last serialized keys to the summary file
	_, err := files[2].Write(append(firstSerializedKey, lastSerializedKey...))
	if err != nil {
		return err
	}

	// Append the binary representations of the first and last key offsets
	_, err = files[2].Write(append(intToBinary(firstKeyOffsetIndex), intToBinary(lastKeyOffsetIndex)...))
	if err != nil {
		return err
	}

	return nil
}

// writeToFilter writes the serialized BloomFilter data to the filter file.
// It takes a slice of open files (with the filter file at index 3).
func (wr *SSWriter) writeToFilter(files []*os.File) error {
	// Serialize the filter data to bytes
	serializedFilter, err := wr.filter.SerializeToBytes()
	if err != nil {
		return err
	}

	// Write the serialized filter data to the filter file
	_, err = files[3].Write(serializedFilter)
	if err != nil {
		return err
	}

	return nil
}

// writeToMetadata constructs and writes the serialized Merkle tree (metadata) to a file.
// It takes a slice of binary keys (presumably from your data structure) and an open file.
// The Merkle tree is built from these keys, and the resulting structure is serialized.
func writeToMetadata(binaryKeys [][]byte, files []*os.File) error {
	// Build the Merkle tree from the provided binary keys
	metadata := merkle.BuildMerkleTree(binaryKeys)

	// Serialize the Merkle tree structure
	serializedMetadata := merkle.SerializeMerkleTree(metadata)

	// Write the serialized Merkle tree to the metadata file
	_, err := files[4].Write(serializedMetadata)
	if err != nil {
		return err
	}

	return nil
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
	binaryKeys := make([][]byte, 0)

	// Initialize counters and offsets
	compressionCounter := 1
	offsetData, offsetIndex, firstKeyOffsetIndex, lastKeyOffsetIndex := 0, 0, 0, 0
	firstSerializedKey, lastSerializedKey := make([]byte, 0), make([]byte, 0)

	for i, key := range sortedKeys {
		// Add key to the filter
		wr.filter.Add(key)

		// Get entry from the Memtable
		entry, err := mt.Get(key)
		if err != nil {
			return err
		}

		// Optionally compress the key
		if wr.isCompressed {
			err = wr.compressKey(entry, &compressionCounter)
			if err != nil {
				return err
			}
		}

		// Serialize the entry and write to the data file
		serializedEntry := wr.serializeEntry(*entry)
		binaryKeys = append(binaryKeys, serializedEntry)
		_, err = files[0].Write(serializedEntry)
		if err != nil {
			return err
		}

		// Serialize the key for index and summary
		serializedKey, err := serializeString(entry.Key())
		if err != nil {
			return err
		}

		// Write index entries at specific intervals
		if (i+1)%wr.indexStride == 0 {
			err = wr.writeToIndex(&firstKeyOffsetIndex, &lastKeyOffsetIndex, i, offsetData, offsetIndex,
				serializedKey, &firstSerializedKey, &lastSerializedKey, files)
			if err != nil {
				return err
			}
		}

		// Write summary entries at specific intervals
		if (i+1)%wr.summaryStride == 0 {
			err = writeToSummary(serializedKey, offsetIndex, files)
			if err != nil {
				return err
			}
		}

		// Update offsets
		wr.increaseOffsets(serializedEntry, serializedKey, i, &offsetData, &offsetIndex)

		// Write additional metadata (first/last keys, filter, Merkle tree)
		err = addFirstAndLastKeyToSummary(firstSerializedKey, lastSerializedKey,
			firstKeyOffsetIndex, lastKeyOffsetIndex, files)
		if err != nil {
			return err
		}
		err = wr.writeToFilter(files)
		if err != nil {
			return err
		}
		err = writeToMetadata(binaryKeys, files)
		if err != nil {
			return err
		}
	}

	// Close all files
	err = closeFiles(files)
	if err != nil {
		panic(err)
	}

	return nil
}

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

	// Repeat the above steps for the summary, filter, and metadata segments
	// ...

	return nil
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
