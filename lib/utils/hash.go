package utils

import (
	"encoding/binary"
	"hash/crc32"
)

func Crc32Byte(data []byte) []byte {
	hasher := crc32.NewIEEE()
	hasher.Write(data)
	checksum := hasher.Sum32()
	checksumByte := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumByte, checksum)

	return checksumByte
}
