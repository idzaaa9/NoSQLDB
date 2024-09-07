package merkle

import (
	"crypto/sha1"
	"encoding/hex"
)

// MerkleTree represents a Merkle tree structure.
type MerkleTree struct {
	root *Node
}

// Node represents a node in the Merkle tree.
type Node struct {
	data  []byte
	left  *Node
	right *Node
}

// String returns the hexadecimal representation of the node's data.
func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

// Hash computes the SHA-1 hash of the given data.
func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// BuildMerkleTree constructs a Merkle tree from the provided data.
func BuildMerkleTree(data [][]byte) *MerkleTree {
	if len(data) == 0 {
		return nil
	}
	nodes := make([]*Node, len(data))
	for i, d := range data {
		nodes[i] = &Node{data: d}
	}
	for len(nodes) > 1 {
		var nextLevel []*Node
		for i := 0; i < len(nodes); i += 2 {
			var left, right *Node
			if i < len(nodes)-1 {
				left, right = nodes[i], nodes[i+1]
			} else {
				left, right = nodes[i], nodes[i]
			}
			parentData := append(left.data, right.data...)
			parentHash := Hash(parentData)
			nextLevel = append(nextLevel, &Node{data: parentHash[:], left: left, right: right})
		}
		nodes = nextLevel
	}
	return &MerkleTree{root: nodes[0]}
}

// SerializeMerkleTree serializes the Merkle tree into a byte slice.
func SerializeMerkleTree(tree *MerkleTree) []byte {
	if tree == nil || tree.root == nil {
		return nil
	}
	var serializedData []byte
	var serializeNode func(node *Node)
	serializeNode = func(node *Node) {
		if node == nil {
			serializedData = append(serializedData, []byte("empty")...)
		} else {
			serializedData = append(serializedData, node.data...)
			serializeNode(node.left)
			serializeNode(node.right)
		}
	}
	serializeNode(tree.root)
	return serializedData
}

// DeserializeMerkleTree deserializes a byte slice into a Merkle tree.
func DeserializeMerkleTree(data []byte) *MerkleTree {
	if len(data) == 0 {
		return nil
	}
	var index int
	var deserializeNode func() *Node
	deserializeNode = func() *Node {
		if index >= len(data) {
			return nil
		}
		if string(data[index:index+5]) == "empty" {
			index += 5
			return nil
		}
		hash := data[index : index+20]
		index += 20
		left := deserializeNode()
		right := deserializeNode()
		return &Node{data: hash, left: left, right: right}
	}
	root := deserializeNode()
	return &MerkleTree{root: root}
}
