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
	dictionary map[uint64]map[int]struct{}
	walDir     string
	mu         sync.Mutex
}

func NewSegmentManager(walDir string) *SegmentManager {
	return &SegmentManager{
		dictionary: make(map[uint64]map[int]struct{}),
		walDir:     walDir,
	}
}

var (
	instance *SegmentManager
	once     sync.Once
)

func GetInstance(walDir string) *SegmentManager {
	once.Do(func() {
		instance = NewSegmentManager(walDir)
	})
	return instance
}

func (sm *SegmentManager) AddTableIdx(segmentID uint64, memtableIndex int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.dictionary[segmentID]; !ok {
		sm.dictionary[segmentID] = make(map[int]struct{})
	}
	sm.dictionary[segmentID][memtableIndex] = struct{}{}
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
