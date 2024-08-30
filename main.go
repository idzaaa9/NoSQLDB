package main

import (
	"NoSQLDB/lib/memtable"
)

func main() {
	myMemtable := memtable.NewMapMemtable(100)

	myMemtable.Put("picko", []byte("jfsd"))
	myMemtable.Put("brt", []byte("jfsd"))
	myMemtable.Put("alo", []byte("jfsd"))

	myMemtable.Flush()
}
