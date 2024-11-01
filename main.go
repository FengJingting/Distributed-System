package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	initConfig()
	// Start background processes
	go listenAndReply("8080")
	go detect_failure_n(5)

	// Command interface
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "list_mem":
			list_mem()
		case "list_self":
			list_self()
		case "join":
			join()
		default:
			fmt.Println("Unknown command")
		}
	}
}
