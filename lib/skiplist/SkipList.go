package skiplist

import (
	"fmt"
	"math/rand"
)

type SkipListNode struct {
	key       string          // key is a string
	value     []byte          // value is a byte slice
	tombstone bool            // tombstone is a boolean
	next      []*SkipListNode // next is a slice of pointers to the next node
}

func NewSkipListNode(key string, value []byte, height int) *SkipListNode {
	return &SkipListNode{
		key:       key,
		value:     value,
		tombstone: false,
		next:      make([]*SkipListNode, height),
	}
}

func NewSkipListNodeTombstone(key string, height int) *SkipListNode {
	return &SkipListNode{
		key:       key,
		tombstone: true,
		next:      make([]*SkipListNode, height),
	}
}

type SkipList struct {
	maxHeight int
	head      *SkipListNode
	height    int
}

func NewSkipList(maxHeight int) *SkipList {
	return &SkipList{
		maxHeight: maxHeight,
		head:      NewSkipListNode("", nil, maxHeight),
		height:    0,
	}
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func (s *SkipList) find(key string) (*SkipListNode, []*SkipListNode) {
	var next *SkipListNode
	var path []*SkipListNode

	prev := s.head
	for i := s.height - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && next.key < key {
			prev = next
			next = next.next[i]
		}
		path = append(path, prev)
	}

	if next != nil && next.key == key {
		return next, path
	}
	return nil, path
}

func (s *SkipList) Get(key string) (*SkipListNode, error) {
	value, _ := s.find(key)
	if value == nil {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

func (s *SkipList) Put(key string, value []byte) {
	node, path := s.find(key)
	if node != nil {
		node.value = value
		return
	}

	level := s.roll()
	if level > s.height {
		s.height = level
	}

	newNode := NewSkipListNode(key, value, level)
	for i := 0; i < level; i++ {
		newNode.next[i] = path[i].next[i]
		path[i].next[i] = newNode
	}
}

func (s *SkipList) PutLogicallyDeleted(key string) {
	node, path := s.find(key)
	if node != nil {
		node.tombstone = true
		return
	}

	level := s.roll()
	if level > s.height {
		s.height = level
	}

	newNode := NewSkipListNodeTombstone(key, level)
	for i := 0; i < level; i++ {
		newNode.next[i] = path[i].next[i]
		path[i].next[i] = newNode
	}
}

func (s *SkipList) Delete(key string) {
	node, path := s.find(key)
	if node == nil {
		return
	}

	for i := 0; i < len(node.next); i++ {
		path[i].next[i] = node.next[i]
	}
}

func (s *SkipList) LogicallyDelete(key string) {
	node, _ := s.find(key)
	if node == nil {
		s.PutLogicallyDeleted(key)
	}

	node.tombstone = true
}
