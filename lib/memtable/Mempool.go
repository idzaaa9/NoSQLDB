package memtable

import (
	"errors"
	"os"
)

type Mempool struct {
	tableCount      int
	tables          []Memtable
	activeTableIdx  int
	outputDirectory string
}

func NewMempool(
	numTables, memtableSize, skipListMaxLevel, BTreeMinDegree int, outputDir, memtableType string) (*Mempool, error) {
	memtables := make([]Memtable, numTables)
	var err error
	for i := 0; i < numTables; i++ {
		switch memtableType {
		case USE_BTREE:
			memtables[i] = NewBTreeMemtable(BTreeMinDegree, memtableSize)
		case USE_MAP:
			memtables[i] = NewMapMemtable(memtableSize)
		case USE_SKIP_LIST:
			memtables[i] = NewSkipListMemtable(skipListMaxLevel, memtableSize)
		default:
			return nil, errors.New("invalid memtable type")
		}
	}
	fileInfo, err := os.Stat(outputDir)
	if err != nil {
		err = os.Mkdir(outputDir, 0755)
		if err != nil {
			return nil, err
		}
	}
	if fileInfo == nil || !fileInfo.IsDir() {
		return nil, errors.New("output directory is not a directory")
	}

	return &Mempool{
		tableCount:      numTables,
		tables:          memtables,
		activeTableIdx:  0,
		outputDirectory: outputDir,
	}, err
}

func (mp *Mempool) rotateForward() {
	mp.activeTableIdx = (mp.activeTableIdx + 1) % mp.tableCount
}

func (mp *Mempool) Get(key string) (*Entry, error) {
	for i := 0; i < mp.tableCount; i++ {
		tableIdx := (mp.activeTableIdx - i + mp.tableCount) % mp.tableCount
		entry, err := mp.tables[tableIdx].Get(key)
		if err == nil {
			return entry, nil
		}
	}
	return nil, errors.New("entry not found")
}

/*
	func (mp *Mempool) Exists(key string) (bool, int) {
		for i := 0; i < mp.tableCount; i++ {
			tableIdx := (mp.activeTableIdx - i + mp.tableCount) % mp.tableCount // the addition makes sure we dont get negative numbers
			if mp.tables[tableIdx].Exists(key) {
				return true, tableIdx
			}
		}
		return false, -1
	}

		return true
	}
*/
func (mp *Mempool) shouldFlush() bool {
	for i := 0; i < mp.tableCount; i++ {
		if !mp.tables[i].IsFull() {
			return false
		}
	}
	return true
}

func (mp *Mempool) flushIfNeeded() error {
	if mp.shouldFlush() {
		err := mp.tables[mp.activeTableIdx].Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mp *Mempool) Put(entry *Entry) error {
	err := mp.tables[mp.activeTableIdx].Put(entry.Key(), entry.Value())

	if err != nil {
		return err
	}

	if mp.tables[mp.activeTableIdx].IsFull() {
		err = mp.flushIfNeeded()
		if err != nil {
			return err
		}

		mp.rotateForward()
	}

	return nil
}

// logical delete
func (mp *Mempool) Delete(key string) error {
	err := mp.Put(&Entry{key, nil, true})
	if err != nil {
		return err
	}

	if mp.tables[mp.activeTableIdx].IsFull() {
		err = mp.flushIfNeeded()
		if err != nil {
			return err
		}

		mp.rotateForward()
	}

	return nil
}
