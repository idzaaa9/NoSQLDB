package cli

import (
	"fmt"
	"runtime"
)

func ClearConsole() {
	switch runtime.GOOS {
	case "windows":
		fmt.Print("\x1b[H\x1b[2J")
	default:
		fmt.Print("\033[H\033[2J")
	}
}

func PutMenu() (string, string) {
	var key, value string

	fmt.Println("Put")
	fmt.Print("Enter key: ")
	fmt.Scanln(&key)
	fmt.Print("Enter value: ")
	fmt.Scanln(&value)

	return key, value
}

func HandleError(err error) {
	if err == nil {
		fmt.Println("Success")
	} else {
		fmt.Println("Error:", err)
	}
	fmt.Scanln()
}

func GetMenu() string {
	var key string

	fmt.Println("Get")
	fmt.Print("Enter key: ")
	fmt.Scanln(&key)

	return key
}

func DeleteMenu() string {
	var key string

	fmt.Println("Delete")
	fmt.Print("Enter key: ")
	fmt.Scanln(&key)

	return key
}

func PDSMenu() {
	fmt.Println("Probabilistic data structures")
	fmt.Println("1. Bloom filter")
	fmt.Println("2. Count-min sketch")
	fmt.Println("3. HyperLogLog")
	fmt.Println("4. Exit")
}
