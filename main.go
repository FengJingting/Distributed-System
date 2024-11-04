package main

import (
	// "mp3/cassandra"
	"mp3/memberlist"
	"mp3/utils"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	// load configs
	utils.InitConfig()
	// Start background processes
	go memberlist.ListenAndReply("8080") //启动8080端口，监听各个vm发来的ping
	// go detect_failure_n(5)

	// Command interface
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		// memberlist operation
		case "list_mem_ids":
			memberlist.List_mem_ids()
		case "list_self":
			memberlist.List_self()
		case "join":
			memberlist.Join()
			// 打印哈希环中的节点及其前驱和后继
			fmt.Println("Current nodes in the ring:")
			for _, hash := range utils.Ring.SortedHashes {
				node := utils.Ring.Nodes[hash]
				fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
				if node.Predecessor != nil {
					fmt.Printf("  Predecessor: ID=%d\n", node.Predecessor.ID)
				}
				if node.Successor != nil {
					fmt.Printf("  Successor: ID=%d\n", node.Successor.ID)
				}
			}
		// basic file operation
		// case "create":
		// 	create()
		// case "get":
		// 	get()
		// case "append":
		// 	append()
		// case "merge":
		// 	merge()
		// case "multiappend":
		// 	multiappend()
		// display operation
		// case "is":
		// 	is()
		// case "store":
		// 	store()
		// case "getfromreplica":
		// 	getfromreplica()
		default:
			fmt.Println("Unknown command")
		}
	}
}
