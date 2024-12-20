package utils

import (
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"time"
)

func Crc32Byte(data []byte) []byte {
	hasher := crc32.NewIEEE()
	hasher.Write(data)
	checksum := hasher.Sum32()
	checksumByte := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumByte, checksum)

	return checksumByte
}

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
