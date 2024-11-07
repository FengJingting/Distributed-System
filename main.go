package main

import (
	"bufio"
	"fmt"
	"mp3/cassandra"
	"mp3/file"
	"mp3/memberlist"
	"os"
	"strings"
)

func main() {
	// load configs
	cassandra.InitConfig()
	// 判断是否为 Introducer 节点
	if cassandra.Introducer == cassandra.Domain {
		fmt.Println("This node is the Introducer. Adding itself to the member list.")
		memberlist.AddNode(cassandra.Domain, cassandra.Port) // 调用 addNode 函数将自己添加到成员列表
	}
	// Start background processes
	go memberlist.ListenAndReply("8080") //启动8080端口，监听各个vm发来的ping
	go memberlist.Detect_failure_n(50)

	// Command interface
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// 将输入拆分为命令和参数
		fields := strings.Fields(input)
		if len(fields) == 0 {
			continue
		}

		// 获取命令名称
		command := fields[0]

		switch command {
		// memberlist operation
		case "list_mem_ids":
			memberlist.List_mem_ids()
		case "list_self":
			memberlist.List_self()
		case "join":
			memberlist.Join()
		// basic file operation
		case "create":
			if len(fields) < 3 {
				fmt.Println("Usage: create <localFilename> <hyDFSFilename>")
				continue
			}
			file.Create(fields[1], fields[2]) // 传递两个参数

		case "get":
			if len(fields) < 2 {
				fmt.Println("Usage: get <filename>")
				continue
			}
			file.Get(fields[1], fields[2]) // 传递一个参数

		case "append":
			if len(fields) < 3 {
				fmt.Println("Usage: append <filename> <content>")
				continue
			}
			file.Append(fields[1], fields[2]) // 传递两个参数
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
