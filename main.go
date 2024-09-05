package main

import (
	"NoSQLDB/lib/memtable"
	"NoSQLDB/lib/pds"
	"fmt"
	"io/ioutil"
)

func main() {
	mt := memtable.NewMapMemtable(100)

	mt.Put("apple", []byte("red"))
	mt.Put("orange", []byte("orange"))
	mt.Put("banana", []byte("yellow"))

	filter := pds.NewBloomFilter(10, 3)

	wr, err := memtable.NewSSWriter("tmp", 1, 3, 5, true, true, filter)
	if err != nil {
		panic(err)
	}

	err = wr.Flush(mt)
	if err != nil {
		panic(err)
	}

	contentData, err := ioutil.ReadFile("tmp/usertable-01-Data.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Data:", err)
	} else {
		fmt.Println("Data:", contentData)
	}

	contentIndex, err := ioutil.ReadFile("tmp/usertable-01-Index.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Index:", err)
	} else {
		fmt.Println("Index:", contentIndex)
	}

	contentSummary, err := ioutil.ReadFile("tmp/usertable-01-Summary.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Summary:", err)
	} else {
		fmt.Println("Summary:", contentSummary)
	}

	contentFilter, err := ioutil.ReadFile("tmp/usertable-01-Filter.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Filter:", err)
	} else {
		fmt.Println("Filter: ", contentFilter)
	}

	contentMetadata, err := ioutil.ReadFile("tmp/usertable-01-Metadata.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Metadata:", err)
	} else {
		fmt.Println("Metadata: ", contentMetadata)
	}

	contentDictionary, err := ioutil.ReadFile("tmp/dictionary.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Dictionary:", err)
	} else {
		fmt.Println("Dictionary: ", contentDictionary)
	}

	contentMerged, err := ioutil.ReadFile("tmp/usertable-01.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Merged:", err)
	} else {
		fmt.Println("Merged: ", contentMerged)
	}

	contentOffsets, err := ioutil.ReadFile("tmp/segmentOffsets.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Offsets:", err)
	} else {
		fmt.Println("Offsets: ", contentOffsets)
	}

}
