package tokenbucket_test

import (
	tb "NoSQLDB/lib/token-bucket"
	"sync"
	"testing"
	"time"
)

func TestInitialTokenCount(t *testing.T) {
	bucket := tb.NewTokenBucket(10, 1, 1*time.Second)
	if got := bucket.Tokens(); got != 10 {
		t.Errorf("Initial token count = %v, want 10", got)
	}
}

func TestRemoveToken(t *testing.T) {
	bucket := tb.NewTokenBucket(10, 1, 1*time.Second)

	bucket.RemoveToken() // Try removing 1 token
	if got := bucket.Tokens(); got != 9 {
		t.Errorf("After removing 1 token, token count = %v, want 9", got)
	}

	// Try removing a token when there are none left
	for i := 0; i < 9; i++ {
		bucket.RemoveToken() // Empty the bucket
	}
	if bucket.RemoveToken() {
		t.Errorf("Expected to fail removing a token when none are available")
	}
}

func TestTokenRefill(t *testing.T) {
	bucket := tb.NewTokenBucket(10, 2, 1*time.Second) // 2 tokens per second
	for i := 0; i < 10; i++ {
		bucket.RemoveToken() // Empty the bucket
	}

	time.Sleep(1 * time.Second) // Wait for tokens to refill
	bucket.RefillTokensTest()   // Manually trigger refill

	if got := bucket.Tokens(); got < 2 {
		t.Errorf("After 1 second, token count = %v, want at least 2", got)
	}
}

func TestTokenBucketOverflow(t *testing.T) {
	bucket := tb.NewTokenBucket(10, 2, 1*time.Second) // Capacity of 10, rate of 2 tokens per second
	for i := 0; i < 10; i++ {
		bucket.RemoveToken() // Empty the bucket
	}

	time.Sleep(5 * time.Second) // Wait for tokens to refill
	bucket.RefillTokensTest()   // Manually trigger refill

	if got := bucket.Tokens(); got > 10 {
		t.Errorf("Token count = %v, should not exceed capacity of 10", got)
	}
}

func TestRateLimit(t *testing.T) {
	bucket := tb.NewTokenBucket(5, 1, 1*time.Second) // Capacity of 5 tokens, 1 token per second

	// Initially, the bucket should be full
	if got := bucket.Tokens(); got != 5 {
		t.Errorf("Initial token count = %v, want 5", got)
	}

	// Try to remove tokens faster than they can be refilled
	for i := 0; i < 5; i++ {
		bucket.RemoveToken()
	}

	// Try removing more tokens when none are left
	if bucket.RemoveToken() {
		t.Errorf("Expected to fail removing a token when none are available")
	}

	// Wait and check if tokens are refilled correctly
	time.Sleep(2 * time.Second)
	bucket.RefillTokensTest() // Manually trigger refill

	if got := bucket.Tokens(); got < 2 {
		t.Errorf("After 2 seconds, token count = %v, want at least 2", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	bucket := tb.NewTokenBucket(10, 5, 1*time.Second) // Capacity of 10, rate of 5 tokens per second

	// Create a wait group to wait for concurrent goroutines
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bucket.RemoveToken()
		}()
	}

	wg.Wait() // Wait for all goroutines to finish

	if got := bucket.Tokens(); got < 0 || got > 10 {
		t.Errorf("After concurrent access, token count = %v, should be between 0 and 10", got)
	}
}

func TestSerialization(t *testing.T) {
	originalBucket := tb.NewTokenBucket(10, 2, 1*time.Second)
	originalBucket.RemoveToken() // Modify the bucket to change its state

	// Serialize the bucket
	data := originalBucket.Serialize()
	if data == nil {
		t.Fatal("Failed to serialize the token bucket")
	}

	// Deserialize the bucket
	deserializedBucket := tb.Deserialize(data)
	if deserializedBucket == nil {
		t.Fatal("Failed to deserialize the token bucket")
	}

	// Validate the state of the deserialized bucket
	if got := deserializedBucket.Tokens(); got != originalBucket.Tokens() {
		t.Errorf("Deserialized token count = %v, want %v", got, originalBucket.Tokens())
	}

	if deserializedBucket.CapacityTest() != originalBucket.CapacityTest() {
		t.Errorf("Deserialized bucket capacity = %v, want %v", deserializedBucket.CapacityTest(), originalBucket.CapacityTest())
	}
	if deserializedBucket.RateTest() != originalBucket.RateTest() {
		t.Errorf("Deserialized bucket rate = %v, want %v", deserializedBucket.RateTest(), originalBucket.RateTest())
	}
}

func TestSerializationAfterOperations(t *testing.T) {
	bucket := tb.NewTokenBucket(20, 5, 1*time.Second)
	bucket.RemoveToken() // Perform operations
	time.Sleep(1 * time.Second)
	bucket.RemoveToken() // More operations

	// Serialize the bucket
	data := bucket.Serialize()
	if data == nil {
		t.Fatal("Failed to serialize the token bucket")
	}

	// Simulate some time passing by creating a new bucket and deserializing the old state
	newBucket := tb.NewTokenBucket(20, 5, 1*time.Second) // This is to simulate a new bucket in a different state
	time.Sleep(2 * time.Second)                          // Simulate time passing
	newBucket = tb.Deserialize(data)
	if newBucket == nil {
		t.Fatal("Failed to deserialize the token bucket")
	}

	// Validate the state of the new bucket
	if got := newBucket.Tokens(); got != bucket.Tokens() {
		t.Errorf("Deserialized token count = %v, want %v", got, bucket.Tokens())
	}

	if newBucket.CapacityTest() != bucket.CapacityTest() ||
		newBucket.RateTest() != bucket.RateTest() {
		t.Errorf("Deserialized bucket state does not match original")
	}
}

func TestSerializationEdgeCases(t *testing.T) {
	bucket := tb.NewTokenBucket(0, 1, 1*time.Second) // Edge case: bucket with zero capacity
	bucket.RemoveToken()                             // Should not change anything

	// Serialize and deserialize
	data := bucket.Serialize()
	if data == nil {
		t.Fatal("Failed to serialize the token bucket")
	}

	deserializedBucket := tb.Deserialize(data)
	if deserializedBucket == nil {
		t.Fatal("Failed to deserialize the token bucket")
	}

	// Validate the state of the deserialized bucket
	if got := deserializedBucket.Tokens(); got != bucket.Tokens() {
		t.Errorf("Deserialized token count = %v, want %v", got, bucket.Tokens())
	}

	if deserializedBucket.CapacityTest() != bucket.CapacityTest() ||
		deserializedBucket.RateTest() != bucket.RateTest() {
		t.Errorf("Deserialized bucket state does not match original")
	}
}
