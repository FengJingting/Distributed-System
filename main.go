package main

import (
	"mp3/cassandra"
	"mp3/memberlist"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	// load configs
	cassandra.InitConfig()
	// 判断是否为 Introducer 节点
    if cassandra.Introducer == cassandra.Domain {
        fmt.Println("This node is the Introducer. Adding itself to the member list.")
        memberlist.AddNode(cassandra.Domain, cassandra.Port)  // 调用 addNode 函数将自己添加到成员列表
    }
	// Start background processes
	go memberlist.ListenAndReply("8080") //启动8080端口，监听各个vm发来的ping
	go memberlist.Detect_failure_n(5)

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
