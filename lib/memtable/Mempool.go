package memtable

import (
	sm "NoSQLDB/lib/segment-manager"
	"errors"
)

type Mempool struct {
	writer         SSWriter
	tableCount      int
	tables          []Memtable
	activeTableIdx  int
	outputDirectory string
	segmentManager  *sm.SegmentManager
}

func NewMempool(
	numTables, memtableSize, skipListMaxLevel, BTreeMinDegree int, writer SSWriter, memtableType string) (*Mempool, error) {
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
	if !fileInfo.IsDir() {
		return nil, errors.New("output path is not a directory")
	}

	if fileInfo == nil {
		_, err := os.Create(outputDir)
		if err != nil {
			return nil, err
		}
	}
	return &Mempool{
		writer:         writer,
		tableCount:      numTables,
		tables:          memtables,
		activeTableIdx:  0,
		outputDirectory: outputDir,
		segmentManager:  sm.GetInstance(outputDir, 0),
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

func (mp *Mempool) shouldFlush() bool {
	for i := 0; i < mp.tableCount; i++ {
		if !mp.tables[i].IsFull() {
			return false
		}
	}
	return true
}

func (mp *Mempool) oldestTableIdx() int {
	idx := mp.activeTableIdx - 1
	if idx < 0 {
		idx = mp.tableCount - 1
	}
	return idx
}

func (mp *Mempool) flushIfNeeded() error {
	if mp.shouldFlush() {
		err := mp.tables[mp.oldestTableIdx()].Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mp *Mempool) Put(key string, value []byte) error {
	mp.segmentManager.MemtableIdx = mp.activeTableIdx
	entry := &Entry{key, value, false}
	err := mp.tables[mp.activeTableIdx].Put(entry.Key(), entry.Value())
	mp.segmentManager.AddTableIdx()

	if err != nil {
		return err
	}

	if mp.tables[mp.activeTableIdx].IsFull() {
		mp.rotateForward()

		err = mp.flushIfNeeded()
		if err != nil {
			return err
		}
		mp.segmentManager.RemoveTableIdx(mp.oldestTableIdx())
		mp.segmentManager.DeleteSafeSegments()

		mp.rotateForward()
	}

	return nil
}


// logical delete
func (mp *Mempool) Delete(key string) error {
	mp.segmentManager.MemtableIdx = mp.activeTableIdx
	entry := &Entry{key, nil, true}

	err := mp.tables[mp.activeTableIdx].Put(entry.Key(), entry.Value())
	mp.segmentManager.AddTableIdx()
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

func (mp *Mempool) ActiveTableIdx() int {
	return mp.activeTableIdx
}
