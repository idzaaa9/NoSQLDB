package main

import (
	"NoSQLDB/lib/memtable"
	"fmt"
	"os"
)

func main() {
	btm := memtable.NewBTreeMemtable(2, 10)

	btm.Put("key1", []byte("value1"))
	btm.Put("key2", []byte("value2"))

	err := btm.Flush()
	if err != nil {
		fmt.Printf("Greška pri pozivanju Flush(): %v\n", err)
		os.Exit(1)
	}

}
