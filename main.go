package main

import (
	"NoSQLDB/lib/memtable"
	"NoSQLDB/lib/pds"
)

func main() {
	myMemtable := memtable.NewMapMemtable(100)

	myMemtable.Put("picko", []byte("jfsd"))
	myMemtable.Put("brt", []byte("jfsd"))
	myMemtable.Put("alo", []byte("jfsd"))

	filter := pds.NewBloomFilter(10, 3)

	writer, err := memtable.NewSSWriter("tmp", 1, 3, 5, true, false, filter)
	if err != nil {
		panic(err)
	}

	writer.Flush(myMemtable)
}
