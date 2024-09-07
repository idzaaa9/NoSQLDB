package cache

import "NoSQLDB/lib/memtable"

// Cache is a struct that manages cached data using the LRU algorithm
type Cache struct {
	capacity int // The maximum number of items that can be stored in the Cache
	items    map[string]*Page
	head     *Page
	tail     *Page
}

// Page is a struct that represents an entry with pointers to the previous and next entries
type Page struct {
	entry *memtable.Entry
	prev  *Page
	next  *Page
}

func NewCache(capacity int) *Cache {
	return &Cache{
		capacity: capacity,
		items:    make(map[string]*Page),
	}
}

// Get returns the value of the key if it exists in the cache, otherwise it returns nil
func (c *Cache) Get(key string) *Page {
	if entry, ok := c.items[key]; ok {
		c.moveToHead(entry)
		return entry
	}
	return nil
}

func (c *Cache) moveToHead(page *Page) {
	if page.prev != nil {
		page.prev.next = page.next
	}
	if page.next != nil {
		page.next.prev = page.prev
	}
	page.prev = nil
	page.next = c.head
	c.head.prev = page
	c.head = page
}

// Put adds a new key-value pair to the cache. If the cache is full, it removes the least recently used item
func (c *Cache) Put(entry *memtable.Entry) {
	if page, ok := c.items[entry.Key()]; ok {
		page.entry = entry
		c.moveToHead(page)
		return
	}
	if len(c.items) >= c.capacity {
		delete(c.items, c.tail.entry.Key())
		c.removeTail()
	}
	page := &Page{entry: entry}
	c.items[entry.Key()] = page
	if c.head == nil {
		c.head = page
		c.tail = page
	} else {
		page.next = c.head
		c.head.prev = page
		c.head = page
	}
}

func (c *Cache) removeTail() {
	if c.tail.prev != nil {
		c.tail.prev.next = nil
	} else {
		c.head = nil
	}
	c.tail = c.tail.prev
}
