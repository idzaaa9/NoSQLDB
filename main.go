package main

import (
	"NoSQLDB/lib/cli"
	cfg "NoSQLDB/lib/config"
	"NoSQLDB/lib/engine"
	"fmt"
)

func main() {
	config, err := cfg.LoadConfig("config.json")

	if err != nil {
		config = cfg.GetDefaultConfig()
	}

	engine, err := engine.NewEngine(config)

	if err != nil {
		panic(err)
	}

	cli.ClearConsole()
	fmt.Println("do you want to restore data from the log? (Y/n): ")
	var choice byte
	fmt.Scanln(&choice)
	fmt.Scanln(choice)
	if choice != 'n' {
		engine.Restore(*config)
		fmt.Println("Data restored")
		fmt.Scanln()
	}

	for {
		cli.ClearConsole()
		fmt.Println("key-value store")
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. Probabilistic data structures")
		fmt.Println("5. Test")
		fmt.Println("6. Exit")

		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			cli.ClearConsole()
			key, value := cli.PutMenu()
			err := engine.Put(key, []byte(value))
			cli.HandleError(err)
		case 2:
			cli.ClearConsole()
			key := cli.GetMenu()
			value, err := engine.Get(key)
			if err == nil {
				if string(value) == "" || value == nil {
					fmt.Println("entry not existing or deleted")
				} else {
					fmt.Println("Value:", string(value))
				}
			} else {
				fmt.Println("Error:", err)
			}
			fmt.Scanln()
		case 3:
			cli.ClearConsole()
			key := cli.DeleteMenu()
			err := engine.Delete(key)
			cli.HandleError(err)
		case 5:
			cli.ClearConsole()
			fmt.Println("Filling DB with test data...")
			engine.FillEngine(500)
			fmt.Println("DB filled with test data")
			fmt.Scanln()
		case 6:
			return
		}
	}
}
