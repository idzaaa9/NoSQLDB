package main

import (
	"NoSQLDB/lib/memtable"
	"NoSQLDB/lib/pds"
	"fmt"
	"os"
)

func main() {
	mt := memtable.NewMapMemtable(100)

	mt.Put("orange", []byte("orange"))
	mt.Put("orange", []byte("orange"))
	mt.Put("banana", []byte("yellow"))
	mt.Put("kora", []byte("red"))
	mt.Put("cep", []byte("sadsa"))
	mt.Put("sok", []byte("yellow"))
	mt.Put("car", []byte("red"))

	mt2 := memtable.NewBTreeMemtable(4, 6)

	mt2.Put("pavle", []byte("red"))
	mt2.Put("ognjen", []byte("orange"))
	mt2.Put("orange", []byte("yellow"))
	mt2.Put("krusevac", []byte("red"))
	mt2.Put("mili", []byte("orange"))
	mt2.Put("alen", []byte("yellow"))
	mt2.Put("petar pan", []byte("red"))

	mt3 := memtable.NewBTreeMemtable(4, 6)

	mt3.Put("pavle", []byte("red"))
	mt3.Put("orange", []byte("tumbston"))
	mt3.Put("das", []byte("yellow"))
	mt3.Put("krusevac", []byte("red"))
	mt3.Put("orangen", []byte("red"))
	mt3.Put("alen", []byte("orange"))
	mt3.Put("goran", []byte("orange"))

	filter := pds.NewBloomFilter(10, 3)

	wr, err := memtable.NewSSWriter("tmp", 1, 3, 2, filter)
	if err != nil {
		panic(err)
	}

	err = wr.Flush(mt)
	if err != nil {
		panic(err)
	}

	err = wr.Flush(mt2)
	if err != nil {
		panic(err)
	}

	err = wr.Flush(mt3)
	if err != nil {
		panic(err)
	}

	contentData, err := os.ReadFile("tmp/usertable-01-Data.txt")
	if err != nil {
		fmt.Println("Greška pri čitanju fajla Data:", err)
	} else {
		fmt.Println("Data:", contentData)
		fmt.Println("Data od 72", contentData[72:])
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

	// contentFilter, err := os.ReadFile("tmp/usertable-01-Filter.txt")
	// if err != nil {
	// 	fmt.Println("Greška pri čitanju fajla Filter:", err)
	// } else {
	// 	fmt.Println("Filter: ", contentFilter)
	// }

	reader, err := memtable.NewSSReader("tmp")
	if err != nil {
		panic(err)
	}

	fmt.Println("key to find ", []byte("orange"))

	valOrange, err := reader.Get("orange")
	if err != nil {
		panic(err)
	}
	fmt.Println("jagoda: ", string(valOrange))

	// valKamion, err := reader.Get("kamion")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("kamion: ", string(valKamion))

}
