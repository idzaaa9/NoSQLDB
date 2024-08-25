package skiplist

import (
	"fmt"
	"math/rand"
)

// Node represents a node in the skip list.
type Node struct {
	key       string
	value     []byte
	tombstone bool
	forward   []*Node
}

func (n *Node) Key() string {
	return n.key
}

func (n *Node) Value() []byte {
	return n.value
}

func (n *Node) Tombstone() bool {
	return n.tombstone
}

type SkipList struct {
	maxLevel int
	head     *Node
	level    int
}

// NewSkipList creates a new SkipList with the specified maximum level
func NewSkipList(maxLevel int) *SkipList {
	head := &Node{
		forward: make([]*Node, maxLevel+1),
	}
	return &SkipList{
		maxLevel: maxLevel,
		head:     head,
		level:    0,
	}
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for rand.Int31n(2) == 1 && level < s.maxLevel {
		level++
	}
	return level
}

// Put inserts a key-value pair into the skip list.
func (sl *SkipList) Put(key string, value []byte) {
	update := make([]*Node, sl.maxLevel+1)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && current.key == key {
		if !current.tombstone {
			current.value = value
		} else {
			current.value = value
			current.tombstone = false
		}
		return
	}

	newLevel := sl.roll()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &Node{
		key:       key,
		value:     value,
		tombstone: false,
		forward:   make([]*Node, newLevel+1),
	}

	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// Get retrieves the value associated with the key.
func (sl *SkipList) Get(key string) (*Node, bool) {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && current.key == key && !current.tombstone {
		return current, true
	}

	return nil, false
}

// LogicallyDelete marks the node with the given key as logically deleted.
func (sl *SkipList) LogicallyDelete(key string) bool {
	update := make([]*Node, sl.maxLevel+1)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && current.key == key {
		if !current.tombstone {
			current.tombstone = true
			return true
		}
	}

	return false
}

// PrintList prints the skip list for debugging purposes.
func (sl *SkipList) Print() {
	for i := sl.level; i >= 0; i-- {
		fmt.Printf("Level %d: ", i)
		node := sl.head.forward[i]
		for node != nil {
			if node.tombstone {
				fmt.Printf("{%s: <deleted>} -> ", node.key)
			} else {
				fmt.Printf("{%s: %s} -> ", node.key, node.value)
			}
			node = node.forward[i]
		}
		fmt.Println("nil")
	}
}

func (sl *SkipList) Size() int {
	current := sl.head
	size := 0
	for current.forward[0] != nil {
		size++
		current = current.forward[0]
	}
	return size
}
