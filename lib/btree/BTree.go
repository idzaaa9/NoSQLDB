package btree

import "encoding/binary"

type Entry struct {
	key       string
	value     []byte
	tombstone bool
}

func (e *Entry) Key() string {
	return e.key
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) Tombstone() bool {
	return e.tombstone
}

type Node struct {
	keys     []string
	values   []*Entry
	children []*Node
	isLeaf   bool
}

func (n *Node) Keys() []string {
	return n.keys
}

func (n *Node) Values() []*Entry {
	return n.values
}

func (n *Node) Children() []*Node {
	return n.children
}

func (n *Node) IsLeaf() bool {
	return n.isLeaf
}

func NewNode(minDegree int, isLeaf bool) *Node {
	return &Node{
		keys:     make([]string, 0, 2*minDegree-1),
		values:   make([]*Entry, 0, 2*minDegree-1),
		children: make([]*Node, 0, 2*minDegree),
		isLeaf:   isLeaf,
	}
}

type BTree struct {
	root      *Node
	minDegree int
	size      int
}

func (bt *BTree) Root() *Node {
	return bt.root
}

func NewBTree(minDegree int) *BTree {
	return &BTree{
		root:      NewNode(minDegree, true),
		minDegree: minDegree,
		size:      0,
	}
}

func (b *BTree) Get(key string, node *Node) (*Entry, int) {
	if node == nil {
		node = b.root
	}

	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if i < len(node.keys) && key == node.keys[i] {
		return node.values[i], i
	}

	if node.isLeaf {
		return nil, -1
	}

	return b.Get(key, node.children[i])
}

func (b *BTree) Update(key string, value []byte, tombstone bool) bool {
	entry, _ := b.Get(key, nil)
	if entry != nil {
		entry.value = value
		entry.tombstone = tombstone
		return true
	}
	return false
}

func (b *BTree) splitChild(parent *Node, i int) {
	// i is the index of the child to split, which is full
	child := parent.children[i]

	newNode := NewNode(b.minDegree, child.isLeaf)
	parent.children = append(parent.children, newNode)

	// Move the median key and value of child to parent
	parent.keys = append(parent.keys, child.keys[b.minDegree-1])
	parent.values = append(parent.values, child.values[b.minDegree-1])

	// split child's keys and values
	newNode.keys = append(newNode.keys, child.keys[b.minDegree:(2*b.minDegree)-1]...)
	newNode.values = append(newNode.values, child.values[b.minDegree:(2*b.minDegree)-1]...)

	child.keys = child.keys[:b.minDegree-1]
	child.values = child.values[:b.minDegree-1]

	// if newNode is not a leaf, move child's children to newNode
	if !child.isLeaf {
		newNode.children = append(newNode.children, child.children[b.minDegree:2*b.minDegree]...)
		child.children = child.children[:b.minDegree-1]
	}
}

func (b *BTree) Put(key string, value []byte, tombstone bool) {
	if b.Update(key, value, tombstone) {
		return
	}

	if !(len(b.root.keys) == (2*b.minDegree - 1)) {
		b.size++
		b.putNotFull(b.root, key, value, tombstone)
		return
	}
	newRoot := NewNode(b.minDegree, false)
	newRoot.children = append(newRoot.children, b.root)

	b.splitChild(newRoot, 0)
	b.root = newRoot

	b.size++
	b.putNotFull(b.root, key, value, tombstone)
}

func (b *BTree) putNotFull(node *Node, key string, value []byte, tombstone bool) {
	i := len(node.keys) - 1

	if node.isLeaf {
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		node.keys = append(node.keys[:i], append([]string{key}, node.keys[i:]...)...)
		node.values = append(node.values[:i], append([]*Entry{{key, value, tombstone}}, node.values[i:]...)...)
	} else {
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		if len(node.children[i].keys) == (2*b.minDegree - 1) {
			b.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		b.putNotFull(node.children[i], key, value, tombstone)
	}
}

func (b *BTree) Size() int {
	return b.size
}

func (e *Entry) Serialize() []byte {
	tombstone := make([]byte, TOMBSTONE_SIZE)

	keysize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint32(keysize, uint32(len(e.key)))

	if e.tombstone {
		tombstone[0] = 1
		data := append(tombstone, keysize...)
		return append(data, []byte(e.key)...)
	} else {
		tombstone[0] = 0
	}

	valuesize := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint32(valuesize, uint32(len(e.value)))

	data := append(tombstone, keysize...)
	data = append(data, []byte(e.key)...)

	data = append(data, valuesize...)
	return append(data, []byte(e.value)...)
}
