package test

import (
	tb "NoSQLDB/lib/write-ahead-log"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const (
	testFilePath    = "./test_wal/"
	testSegmentSize = 1024 // 1 KB segments for simplicity
)

// Helper function to create a temporary directory for WAL tests
func setupWAL(t *testing.T) (*tb.WriteAheadLog, func()) {
	err := os.MkdirAll(testFilePath, 0755)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	wal, err := tb.NewWriteAheadLog(testFilePath, testSegmentSize)
	if err != nil {
		t.Fatalf("failed to create WriteAheadLog: %v", err)
	}

	return wal, func() {
		os.RemoveAll(testFilePath)
	}
}

func TestLogAndDump(t *testing.T) {
	wal, teardown := setupWAL(t)
	defer teardown()

	// Log some entries
	key1 := []byte("key1")
	value1 := []byte("value1")
	if err := wal.Log(key1, value1, 0); err != nil {
		t.Fatalf("failed to log entry: %v", err)
	}

	// Force dump to ensure entries are written to file
	if err := wal.DumpTest(); err != nil {
		t.Fatalf("failed to dump WAL: %v", err)
	}

	// Check the contents of the WAL files
	files, err := filepath.Glob(filepath.Join(testFilePath, "wal_*.log"))
	if err != nil {
		t.Fatalf("failed to list WAL files: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("no WAL files created")
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("failed to read WAL file %s: %v", file, err)
		}
		if len(data) == 0 {
			t.Fatal("WAL file is empty")
		}
	}
}

func TestLogLargeEntry(t *testing.T) {
	wal, teardown := setupWAL(t)
	defer teardown()

	// Log an entry larger than one segment
	key := bytes.Repeat([]byte("k"), testSegmentSize/2)
	value := bytes.Repeat([]byte("v"), testSegmentSize*2)
	if err := wal.Log(key, value, 0); err != nil {
		t.Fatalf("failed to log large entry: %v", err)
	}

	if err := wal.DumpTest(); err != nil {
		t.Fatalf("failed to dump WAL: %v", err)
	}

	// Check the contents of the WAL files
	files, err := filepath.Glob(filepath.Join(testFilePath, "wal_*.log"))
	if err != nil {
		t.Fatalf("failed to list WAL files: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("no WAL files created")
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("failed to read WAL file %s: %v", file, err)
		}
		if len(data) == 0 {
			t.Fatal("WAL file is empty")
		}
	}
}

// Helper function to create a WAL with entries for testing
func setupWALWithEntries(t *testing.T) (*tb.WriteAheadLog, func()) {
	wal, teardown := setupWAL(t)

	// Log some entries
	key1 := []byte("key1")
	value1 := []byte("value1")
	if err := wal.Log(key1, value1, 0); err != nil {
		t.Fatalf("failed to log entry: %v", err)
	}
	if err := wal.DumpTest(); err != nil {
		t.Fatalf("failed to dump WAL: %v", err)
	}

	return wal, teardown
}

func TestWALReader(t *testing.T) {
	wal, teardown := setupWALWithEntries(t)
	defer teardown()

	reader, err := wal.NewWALReader()
	if err != nil {
		t.Fatalf("failed to create WAL reader: %v", err)
	}

	entries, err := reader.Recover()
	if err != nil {
		t.Fatalf("failed to recover entries from WAL: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if string(entry.Key) != "key1" {
		t.Errorf("expected key1, got %s", string(entry.Key))
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value1, got %s", string(entry.Value))
	}
}

func TestWALReaderLargeEntry(t *testing.T) {
	wal, teardown := setupWAL(t)
	defer teardown()

	// Log an entry larger than one segment
	key := bytes.Repeat([]byte("k"), testSegmentSize/2)
	value := bytes.Repeat([]byte("v"), testSegmentSize*2)
	if err := wal.Log(key, value, 0); err != nil {
		t.Fatalf("failed to log large entry: %v", err)
	}
	if err := wal.DumpTest(); err != nil {
		t.Fatalf("failed to dump WAL: %v", err)
	}

	reader, err := wal.NewWALReader()
	if err != nil {
		t.Fatalf("failed to create WAL reader: %v", err)
	}

	entries, err := reader.Recover()
	if err != nil {
		t.Fatalf("failed to recover entries from WAL: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if len(entry.Key) != len(key) {
		t.Errorf("expected key length %d, got %d", len(key), len(entry.Key))
	}
	if len(entry.Value) != len(value) {
		t.Errorf("expected value length %d, got %d", len(value), len(entry.Value))
	}
}
