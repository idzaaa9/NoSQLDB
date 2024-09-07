package engine

import (
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
}

func NewEngine(config *cfg.Config) (*Engine, error) {

	wal, err := writeaheadlog.NewWriteAheadLog(config.WALDir, config.WALSegmentSize)

	if err != nil {
		fmt.Println("Error creating WriteAheadLog")
		return nil, err
	}

	mempool, err := mt.NewMempool(
		config.NumTables,
		config.MemtableSize,
		config.SkipListMaxLevel,
		config.BTreeMinDegree,
		config.OutputDir,
		config.MemtableType)

	if err != nil {
		fmt.Println("Error creating Mempool")
		return nil, err
	}

	tokenBucket := tokenbucket.NewTokenBucket(
		config.TokenBucketSize,
		config.TokenBucketRate,
		config.FillInterval)

	return &Engine{
		WAL:         wal,
		Mempool:     mempool,
		TokenBucket: tokenBucket,
	}, err
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

	return e.Mempool.Put(key, value)
}

func (e *Engine) Get(key string) ([]byte, error) {
	if !e.getToken() {
		return nil, fmt.Errorf("timed out while getting key %s", key)
	}
	value, err := e.Mempool.Get(key)

	if err == nil {
		return value.Value(), nil
	}

	return nil, err
}

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
