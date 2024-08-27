package test

import (
	"NoSQLDB/lib/pds"
	"testing"
)

func TestSimHash(t *testing.T) {
	tokens := []string{"apple", "banana", "cherry"}
	weights := []int{1, 2, 3}
	expectedFingerprint := "11000111001001000000111110110111000110100001101001001110101011011000011011010100111101010001010010111110100001110010000000000000"

	fingerprint := pds.SimHash(tokens, weights)

	if fingerprint != expectedFingerprint {
		t.Errorf("Expected fingerprint: %s, got: %s", expectedFingerprint, fingerprint)
	}
}

func TestHammingDistance(t *testing.T) {
	a := "110010"
	b := "101110"
	expectedDist := 3

	dist := pds.HammingDistance(a, b)

	if dist != expectedDist {
		t.Errorf("Expected Hamming distance: %d, got: %d", expectedDist, dist)
	}
}

func TestSaveAndLoadFingerprint(t *testing.T) {
	fingerprint := "101010101"
	// Save to bytes
	data, err := pds.SaveFingerprintToBytes(fingerprint)
	if err != nil {
		t.Errorf("Error saving fingerprint: %v", err)
	}

	// Load from bytes
	loadedFingerprint, err := pds.LoadFingerprintFromBytes(data)
	if err != nil {
		t.Errorf("Error loading fingerprint: %v", err)
	}

	if loadedFingerprint != fingerprint {
		t.Errorf("Loaded fingerprint does not match original")
	}
}

func TestProcessText(t *testing.T) {
	text := "This is a test example"
	stopWords := []string{"is", "a"}
	expectedFingerprint := "1101110010100010111011001100001110100101001100100100100100100101"

	fingerprint, err := pds.ProcessText(text, stopWords)
	if err != nil {
		t.Errorf("Error processing text: %v", err)
	}

	if fingerprint != expectedFingerprint {
		t.Errorf("Expected fingerprint: %s, got: %s", expectedFingerprint, fingerprint)
	}
}
