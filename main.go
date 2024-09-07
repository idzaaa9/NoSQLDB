package main

import (
	"NoSQLDB/lib/cli"
	"NoSQLDB/lib/config"
	"NoSQLDB/lib/engine"
	"fmt"
)

func fillDB(engine *engine.Engine) {
	for l := 0; l < 4; l++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key_%d_%d", l, i)
			value := fmt.Sprintf("value%d", i)
			err := engine.Put(key, []byte(value))
			if err != nil {
				fmt.Println("Error:", err)
			}
		}
	}
}

func main() {
	config, err := config.LoadConfig("config.json")

	if err != nil {
		panic(err)
	}

	engine, err := engine.NewEngine(config)

	if err != nil {
		panic(err)
	}

	for true {
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
				fmt.Println("Value:", string(value))
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
			fillDB(engine)
			fmt.Println("DB filled with test data")
			fmt.Scanln()
		case 6:
			return
		}
	}
}
