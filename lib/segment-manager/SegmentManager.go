package segmentmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	SEGMENT_PREFIX = "wal_"
	SEGMENT_SUFFIX = ".log"
)

type SegmentManager struct {
	dictionary  map[uint64]map[int]struct{}
	walDir      string
	mu          sync.Mutex
	MemtableIdx int
	SegmentIdx  uint64
}

func NewSegmentManager(walDir string, segmentIdx uint64) *SegmentManager {
	return &SegmentManager{
		dictionary: make(map[uint64]map[int]struct{}),
		walDir:     walDir,
		SegmentIdx: segmentIdx,
	}
}

var (
	instance *SegmentManager
	once     sync.Once
)

func GetInstance(walDir string, segmentIdx uint64) *SegmentManager {
	once.Do(func() {
		instance = NewSegmentManager(walDir, segmentIdx)
	})
	return instance
}

func (sm *SegmentManager) AddTableIdx() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.dictionary[sm.SegmentIdx]; !ok {
		sm.dictionary[sm.SegmentIdx] = make(map[int]struct{})
	}
	sm.dictionary[sm.SegmentIdx][sm.MemtableIdx] = struct{}{}
}

func (sm *SegmentManager) RemoveTableIdx(memtableIndex int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for segmentID, memtableIndexes := range sm.dictionary {
		if _, exists := memtableIndexes[memtableIndex]; exists {
			delete(memtableIndexes, memtableIndex)
			if len(memtableIndexes) == 0 {
				sm.deleteSegment(segmentID)
				delete(sm.dictionary, segmentID)
			}
		}
	}
}

func (sm *SegmentManager) DeleteSafeSegments() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for segmentID, memtableIndexes := range sm.dictionary {
		if len(memtableIndexes) == 0 {
			sm.deleteSegment(segmentID)
			delete(sm.dictionary, segmentID)
		}
	}
}

func (sm *SegmentManager) deleteSegment(segmentID uint64) {
	segmentName := fmt.Sprintf("%s%05d%s", SEGMENT_PREFIX, segmentID, SEGMENT_SUFFIX)
	segmentPath := filepath.Join(sm.walDir, segmentName)
	err := os.Remove(segmentPath)
	if err != nil {
		panic(err)
	}
}
