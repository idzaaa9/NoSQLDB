package main

import (
	"NoSQLDB/lib/memtable"
	"NoSQLDB/lib/pds"
	"fmt"
	"os"
)

func main() {
	mt := memtable.NewMapMemtable(100)

	mt.Put("apple", []byte("red"))
	mt.Put("orange", []byte("orange"))
	mt.Put("banana", []byte("yellow"))

	filter := pds.NewBloomFilter(10, 3)

	wr, err := memtable.NewSSWriter("tmp", 1, 3, 3, filter)
	if err != nil {
		panic(err)
	}

	err = wr.Flush(mt)
	if err != nil {
		panic(err)
	}

	contentData, err := os.ReadFile("tmp/usertable-01-Data.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Data:", err)
	} else {
		fmt.Println("Data:", contentData)
	}

	contentIndex, err := os.ReadFile("tmp/usertable-01-Index.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Index:", err)
	} else {
		fmt.Println("Index:", contentIndex)
	}

	contentSummary, err := os.ReadFile("tmp/usertable-01-Summary.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Summary:", err)
	} else {
		fmt.Println("Summary:", contentSummary)
	}

	contentFilter, err := os.ReadFile("tmp/usertable-01-Filter.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Filter:", err)
	} else {
		fmt.Println("Filter: ", contentFilter)
	}
}
