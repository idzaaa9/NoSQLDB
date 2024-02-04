package strukture

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	size       int32
	tokens     int32
	limit      time.Duration
	lastRefill time.Time
}

func NewTokenBucket(size int32, limit time.Duration) *TokenBucket {
	return &TokenBucket{
		size:       size,
		tokens:     size,
		limit:      limit,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refillBucket() {
	now := time.Now()
	timeDelta := now.Sub(tb.lastRefill)

	if timeDelta >= tb.limit {
		tb.tokens = tb.size
		tb.lastRefill = time.Now()
	}
}

func (tb *TokenBucket) RemoveToken() bool {
	tb.refillBucket()
	if tb.tokens > 0 {
		tb.tokens -= 1
		return true
	}
	return false
}

func (tb *TokenBucket) Serialize() []byte {
	buf := make([]byte, 4*3+8) // tokens, size, limit, lastRefill

	binary.BigEndian.PutUint32(buf[0:4], uint32(tb.tokens))
	binary.BigEndian.PutUint32(buf[4:8], uint32(tb.size))
	binary.BigEndian.PutUint32(buf[8:12], uint32(tb.limit))
	binary.BigEndian.PutUint64(buf[12:20], uint64(tb.lastRefill.Unix()))

	return buf
}

func Deserialize(data []byte) *TokenBucket {
	Tokens := int32(binary.BigEndian.Uint32(data[0:4]))
	Size := int32(binary.BigEndian.Uint32(data[4:8]))
	Limit := time.Duration(binary.BigEndian.Uint32(data[8:12]))
	LastRefillUnix := int64(binary.BigEndian.Uint64(data[12:20]))
	LastRefill := time.Unix(LastRefillUnix, 0)

	return &TokenBucket{
		tokens:     Tokens,
		size:       Size,
		limit:      Limit,
		lastRefill: LastRefill,
	}
}
