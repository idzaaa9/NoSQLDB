package strukture

import (
	"errors"
	"os"
)

type Mempool struct {
	tableCount      int
	tables          []*Memtable
	activeTableIdx  int
	outputDirectory string
}

func NewMempool(numTables, memtableSize, skipListDepth, BTreeDegree int, outputDir, memtableType string) (*Mempool, error) {
	memtables := make([]*Memtable, numTables)
	var err error
	for i := 0; i < numTables; i++ {
		memtables[i], err = NewMemtable(memtableSize, skipListDepth, BTreeDegree, memtableType)
	}

	fileInfo, err := os.Stat(outputDir)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, errors.New("provided mempool filepath isnt an directory")
	}

	return &Mempool{
		tableCount:      numTables,
		tables:          memtables,
		activeTableIdx:  0,
		outputDirectory: outputDir,
	}, err
}

func (mp *Mempool) Exists(key []byte) (bool, int) {
	for i := 0; i < mp.tableCount; i++ {
		tableIdx := (mp.activeTableIdx - i + mp.tableCount) % mp.tableCount // the addition makes sure we dont get negative numbers
		if mp.tables[tableIdx].Exists(key) {
			return true, tableIdx
		}
	}
	return false, -1
}

func (mp *Mempool) IsFull() bool {
	for i := 0; i < mp.tableCount; i++ {
		if !mp.tables[i].IsFull() {
			return false
		}
	}
	return true
}

func (mp *Mempool) Get(key []byte, tableIdx int) (*MemtableEntry, error) {
	return mp.tables[tableIdx].Get(key)
}

func (mp *Mempool) Put(entry *MemtableEntry) error {
	exists := mp.tables[mp.activeTableIdx].Exists(entry.Key)
	err := error(nil)
	if exists {
		err = mp.tables[mp.activeTableIdx].Delete(entry.Key)
		if err != nil {
			return err
		}
	}
	err = mp.tables[mp.activeTableIdx].Insert(entry)
	if err != nil {
		return err
	}
	if mp.tables[mp.activeTableIdx].IsFull() {
		nextIdx := (mp.activeTableIdx + 1) % mp.tableCount
		if mp.IsFull() {
			err = mp.tables[nextIdx].Flush()
			return err
		}
		mp.activeTableIdx = nextIdx
	}
	return err
}
