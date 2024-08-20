package tokenbucket

import (
	"encoding/binary"
	"sync"
	"time"
)

type TokenBucket struct {
	size       int           // maximum number of tokens
	tokens     int           // current number of tokens
	rate       int           // number of tokens to add per second
	limit      time.Duration // time interval to refill tokens
	lastRefill time.Time     // last time the bucket was refilled
	mu         sync.Mutex    // mutex to protect the bucket
}

func NewTokenBucket(size, rate int) *TokenBucket {
	return &TokenBucket{
		size:       size,
		tokens:     size,
		rate:       rate,
		limit:      time.Second,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refillBucket() {
	now := time.Now()
	timeDelta := now.Sub(tb.lastRefill).Seconds()

	tokensToAdd := int(timeDelta * float64(tb.rate)) // tokens to add since last refill

	if tokensToAdd > 0 {
		tb.tokens += tokensToAdd
		if tb.tokens > tb.size {
			tb.tokens = tb.size
		}
		tb.lastRefill = time.Now()
	}
}

func (tb *TokenBucket) RemoveToken() bool {
	tb.mu.Lock()

	defer tb.mu.Unlock()

	tb.refillBucket()
	if tb.tokens > 0 {
		tb.tokens -= 1
		return true
	}
	return false
}

func (tb *TokenBucket) Serialize() []byte {
	intSize := 4
	buf := make([]byte, 4*intSize+8) // tokens, size, limit, lastRefill

	binary.BigEndian.PutUint32(buf[0:4], uint32(tb.tokens))
	binary.BigEndian.PutUint32(buf[4:8], uint32(tb.size))
	binary.BigEndian.PutUint32(buf[8:12], uint32(tb.rate))
	binary.BigEndian.PutUint32(buf[12:16], uint32(tb.limit))
	binary.BigEndian.PutUint64(buf[16:24], uint64(tb.lastRefill.Unix()))

	return buf
}

func Deserialize(data []byte) *TokenBucket {
	Tokens := int(binary.BigEndian.Uint32(data[0:4]))
	Size := int(binary.BigEndian.Uint32(data[4:8]))
	Rate := int(binary.BigEndian.Uint32(data[8:12]))
	Limit := time.Duration(binary.BigEndian.Uint32(data[12:16]))
	LastRefillUnix := int64(binary.BigEndian.Uint64(data[16:24]))
	LastRefill := time.Unix(LastRefillUnix, 0)

	return &TokenBucket{
		tokens:     Tokens,
		size:       Size,
		rate:       Rate,
		limit:      Limit,
		lastRefill: LastRefill,
	}
}
