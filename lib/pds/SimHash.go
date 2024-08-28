package pds

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
)

// Helper function to generate hash value as a binary string
func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

// Function to generate SimHash value
func SimHash(tokens []string, weights []int) string {
	var v [128]int

	// Calculate the weighted hash for each token
	for i, token := range tokens {
		hashStr := GetHashAsString([]byte(token))
		for j, char := range hashStr {
			bit := int(char - '0')
			if bit == 1 {
				v[j] += weights[i]
			} else {
				v[j] -= weights[i]
			}
		}
	}

	// Generate the final SimHash fingerprint
	var fingerprint string
	for i := 0; i < 128; i++ {
		if v[i] > 0 {
			fingerprint += "1"
		} else {
			fingerprint += "0"
		}
	}

	return fingerprint
}

// Function to calculate Hamming distance between two fingerprints
func HammingDistance(a, b string) int {
	dist := 0
	for i := range a {
		if a[i] != b[i] {
			dist++
		}
	}
	return dist
}

// SaveFingerprintToBytes serializes the fingerprint and returns a byte slice.
func SaveFingerprintToBytes(fingerprint string) ([]byte, error) {
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// LoadFingerprintFromBytes deserializes the fingerprint from a byte slice.
func LoadFingerprintFromBytes(data []byte) (string, error) {
	var fingerprint string
	err := json.Unmarshal(data, &fingerprint)
	return fingerprint, err
}

// Helper function to check if a slice contains a specific item
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Function to process text and generate SimHash fingerprint
func ProcessText(text string, stopWords []string) (string, error) {
	words := strings.Fields(text)
	var tokens []string
	var weights []int
	for _, word := range words {
		if !contains(stopWords, word) {
			tokens = append(tokens, word)
			weights = append(weights, 1) // Example weight, can be the word frequency
		}
	}
	return SimHash(tokens, weights), nil
}
