package main

import (
	"NoSQLDB/lib/memtable"
	"NoSQLDB/lib/pds"
)

func main() {
	myMemtable := memtable.NewMapMemtable(100)

	myMemtable.Put("apple", []byte("red"))
	myMemtable.Put("orange", []byte("orange"))
	myMemtable.Put("banana", []byte("yellow"))

	filter := pds.NewBloomFilter(10, 3)

	writer, err := memtable.NewSSWriter("tmp", 1, 3, 5, true, true, filter)
	if err != nil {
		panic(err)
	}

	writer.Flush(myMemtable)
}
