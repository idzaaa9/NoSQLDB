package tokenbucket

import (
	"bytes"
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

func NewTokenBucket(size, rate int, limit time.Duration) *TokenBucket {
	return &TokenBucket{
		size:       size,
		tokens:     size,
		rate:       rate,
		limit:      limit,
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
	tb.mu.Lock()
	defer tb.mu.Unlock()

	buf := new(bytes.Buffer)

	// Write integer fields
	binary.Write(buf, binary.BigEndian, int32(tb.tokens))
	binary.Write(buf, binary.BigEndian, int32(tb.size))
	binary.Write(buf, binary.BigEndian, int32(tb.rate))
	binary.Write(buf, binary.BigEndian, int64(tb.limit))

	// Write time field with nanoseconds precision
	binary.Write(buf, binary.BigEndian, tb.lastRefill.UnixNano())

	return buf.Bytes()
}

func Deserialize(data []byte) *TokenBucket {
	buf := bytes.NewReader(data)

	var tokens, size, rate int32
	var limit int64
	var lastRefillNano int64

	// Read integer fields
	binary.Read(buf, binary.BigEndian, &tokens)
	binary.Read(buf, binary.BigEndian, &size)
	binary.Read(buf, binary.BigEndian, &rate)
	binary.Read(buf, binary.BigEndian, &limit)

	// Read time field with nanoseconds precision
	binary.Read(buf, binary.BigEndian, &lastRefillNano)
	lastRefill := time.Unix(0, lastRefillNano) // Reconstruct time with nanoseconds

	return &TokenBucket{
		tokens:     int(tokens),
		size:       int(size),
		rate:       int(rate),
		limit:      time.Duration(limit),
		lastRefill: lastRefill,
	}
}

func (tb *TokenBucket) Tokens() int {
	tb.mu.Lock()

	defer tb.mu.Unlock()

	return tb.tokens
}

func (tb *TokenBucket) RefillTokensTest() {
	tb.mu.Lock()

	defer tb.mu.Unlock()

	tb.refillBucket()
}

func (tb *TokenBucket) CapacityTest() int {
	return tb.size
}

func (tb *TokenBucket) RateTest() int {
	return tb.rate
}

func (tb *TokenBucket) LastRefillTest() time.Time {
	return tb.lastRefill
}
