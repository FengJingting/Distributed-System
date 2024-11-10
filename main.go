package main

import (
	"bufio"
	"fmt"
	"log"
	"mp3/cassandra"
	"mp3/file"
	"mp3/memberlist"
	"net"
	"os"
	"strings"
)

func main() {
	// load configs
	cassandra.InitConfig()
	file.Init()
	// 判断是否为 Introducer 节点
	if cassandra.Introducer == cassandra.Domain {
		fmt.Println("This node is the Introducer. Adding itself to the member list.")
		memberlist.AddNode(cassandra.Domain, cassandra.MemberPort)// 调用 addNode 函数将自己添加到成员列表
	}
	// Start background processes
	go memberlist.ListenAndReply(cassandra.MemberPort) //启动8080端口，监听各个vm发来的ping
	go memberlist.Detect_failure_n(5)

	// 启动文件操作服务器 9090
	go startFileOperationServer()

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
		case "multiappend":
			if len(fields) < 4 {
				fmt.Println("Usage: multiappend <filename> <vmAddresses> <localFilenames>")
				continue
			}

			// filename 是第一个参数
			filename := fields[1]
			fmt.Println("filename:",filename)

			// 将虚拟机地址和本地文件名转换为切片
			vmAddresses := strings.Split(fields[2], ",")
			fmt.Println("vmAddresses:",vmAddresses)
			localFilenames := strings.Split(fields[3], ",")
			fmt.Println("localFilenames:",localFilenames)

			// 检查是否地址和文件名数量匹配
			if len(vmAddresses) != len(localFilenames) {
				fmt.Println("The number of VM addresses and local filenames must match.")
				continue
			}

			// 调用 MultiAppend 函数
			err := file.MultiAppend(filename, vmAddresses, localFilenames)
			if err != nil {
				fmt.Printf("MultiAppend failed: %v\n", err)
			} else {
				fmt.Println("Multi-append operation completed successfully.")
			}
			if len(fields) < 4 {
				fmt.Println("Usage: multiappend <filename> <vmAddresses> <localFilenames>")
				continue
			}
		// display operation
		case "is":
			file.Is()
		case "store":
			file.Store()
		case "getfromreplica":
			file.Getfromreplica()

		default:
			fmt.Println("Unknown command")
		}
	}
}

// 启动文件操作服务器函数
func startFileOperationServer() {
	listener, err := net.Listen("tcp", ":"+cassandra.FilePort) // 在9090端口上启动监听
	if err != nil {
		log.Fatalf("Error starting file operation server: %v", err)
	}
	defer listener.Close()

	fmt.Printf("File operation server listening on port: %s", cassandra.FilePort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// 在 goroutine 中处理每个连接
		go func(c net.Conn) {
			defer c.Close()
			if err := file.HandleFileOperation(c); err != nil {
				log.Printf("Error handling file operation: %v", err)
			}
		}(conn)
	}
}
