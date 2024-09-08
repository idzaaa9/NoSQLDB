package engine

import (
	cache "NoSQLDB/lib/cache"
	cfg "NoSQLDB/lib/config"
	mt "NoSQLDB/lib/memtable"
	tokenbucket "NoSQLDB/lib/token-bucket"
	writeaheadlog "NoSQLDB/lib/write-ahead-log"
	"fmt"
)

type Engine struct {
	WAL         *writeaheadlog.WriteAheadLog
	Mempool     *mt.Mempool
	TokenBucket *tokenbucket.TokenBucket
	Cache       *cache.Cache
	SSReader    *mt.SSReader
}

func NewEngine(config *cfg.Config) (*Engine, error) {

	wal, err := writeaheadlog.NewWriteAheadLog(config.WALDir, config.WALSegmentSize)

	if err != nil {
		fmt.Println("Error creating WriteAheadLog")
		return nil, err
	}

	writer, err := mt.NewSSWriter(config.OutputDir, config.IndexStride, config.SummaryStride, config.BFExpectedElements, config.BFFalsePositiveRate)
	if err != nil {
		fmt.Println("error creating ss writer")
		return nil, err
	}

	mempool, err := mt.NewMempool(
		config.NumTables,
		config.MemtableSize,
		config.SkipListMaxLevel,
		config.BTreeMinDegree,
		writer,
		config.MemtableType)

	if err != nil {
		fmt.Println("Error creating Mempool")
		return nil, err
	}

	tokenBucket := tokenbucket.NewTokenBucket(
		config.TokenBucketSize,
		config.TokenBucketRate,
		config.FillInterval)

	cache := cache.NewCache(config.CacheSize)

	reader, err := mt.NewSSReader(config.OutputDir)

	return &Engine{
		WAL:         wal,
		Mempool:     mempool,
		TokenBucket: tokenBucket,
		Cache:       cache,
		SSReader:    reader,
	}, err
}

func (e Engine) Restore(cfg cfg.Config) error {
	walreader, err := writeaheadlog.NewWALReader(
		cfg.WALDir,
		cfg.WALSegmentSize,
		e.WAL.Index,
		e.WAL.First)

	if err != nil {
		return err
	}

	walEntries, err := walreader.Recover()

	if err != nil {
		return err
	}

	for _, walEntry := range walEntries {
		entry := mt.NewEntry(string(walEntry.Key), walEntry.Value, walEntry.Tombstone)
		e.Mempool.Put(entry)
	}

	return nil
}

func (e Engine) getToken() bool {
	return e.TokenBucket.RemoveToken()
}

func (e *Engine) Put(key string, value []byte) error {
	if !e.getToken() {
		return fmt.Errorf("timed out while putting key %s", key)
	}

	err := e.WAL.Log([]byte(key), value, writeaheadlog.WAL_PUT)

	if err != nil {
		return err
	}

	entry := mt.NewEntry(key, value, false)

	return e.Mempool.Put(entry)
}

func (e *Engine) testPut(key string, value []byte) error {
	err := e.WAL.Log([]byte(key), value, writeaheadlog.WAL_PUT)

	if err != nil {
		return err
	}

	entry := mt.NewEntry(key, value, false)

	return e.Mempool.Put(entry)
}

func (e *Engine) Get(key string) ([]byte, error) {
	if !e.getToken() {
		return nil, fmt.Errorf("timed out while getting key %s", key)
	}
	value, err := e.Mempool.Get(key)

	if value != nil && err == nil {
		return value.Value(), nil
	}

	value = e.Cache.Get(key)
	if value != nil && err == nil {
		return value.Value(), nil
	}

	value, err = e.SSReader.Get(key)

	if value != nil && err == nil {
		return value.Value(), nil
	}

	return nil, err
}

/*
	func (e *Engine) testGet(key string) ([]byte, error) {
		value, err := e.Mempool.Get(key)

		if err == nil {
			return value.Value(), nil
		}

		value = e.Cache.Get(key)
		if value != nil {
			return value.Value(), nil
		}

		value, err = e.SSReader.Get(key)

		if err != nil && value != nil {
			return value.Value(), nil
		}

		return nil, err
	}
*/
func (e *Engine) Delete(key string) error {
	if !e.getToken() {
		return fmt.Errorf("timed out while deleting key %s", key)
	}
	err := e.WAL.Log([]byte(key), nil, writeaheadlog.WAL_DELETE)

	if err != nil {
		return err
	}

	return e.Mempool.Delete(key)
}

/*
func (e *Engine) testDelete(key string) error {
	err := e.WAL.Log([]byte(key), nil, writeaheadlog.WAL_DELETE)

	if err != nil {
		return err
	}

	return e.Mempool.Delete(key)
}
*/
// FillEngine puts hundreds of entries into the engine without triggering the token bucket.
func (e *Engine) FillEngine(numEntries int) {
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))

		// Use the testPut method to add each entry without checking the token bucket
		err := e.testPut(key, value)
		if err != nil {
			panic(err)
		}
	}
}

// Example usage:
// engineInstance := NewEngine(config)
// engineInstance.FillEngine(500) // Populate with 500 entries
