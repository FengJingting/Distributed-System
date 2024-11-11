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
	// Load configurations
	cassandra.InitConfig()
	file.Init()
	// Check if this node is the Introducer
	if cassandra.Introducer == cassandra.Domain {
		fmt.Println("This node is the Introducer. Adding itself to the member list.")
		memberlist.AddNode(cassandra.Domain, cassandra.MemberPort) // Call addNode to add itself to the member list
	}
	// Start background processes
	go memberlist.ListenAndReply(cassandra.MemberPort) // Start on port 8080, listen for pings from other VMs
	go memberlist.Detect_failure_n(20)

	// Start the file operation server on port 9090
	go startFileOperationServer()

	// Command interface
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// Split input into command and arguments
		fields := strings.Fields(input)
		if len(fields) == 0 {
			continue
		}

		// Get command name
		command := fields[0]

		switch command {
		// Member list operations
		case "list_mem_ids":
			memberlist.List_mem_ids()
		case "list_self":
			memberlist.List_self()
		case "join":
			memberlist.Join()
		// Basic file operations
		case "create":
			if len(fields) < 3 {
				fmt.Println("Usage: create <localFilename> <hyDFSFilename>")
				continue
			}
			file.Create(fields[1], fields[2], true) // Default: continue after quorum

		case "get":
			fmt.Println(fields)
			if len(fields) < 3 {
				fmt.Println("Usage: get <dfs> <local>")
				continue
			}
			file.Get(fields[1], fields[2])

		case "append":
			if len(fields) < 3 {
				fmt.Println("Usage: append <filename> <content>")
				continue
			}
			file.Append(fields[1], fields[2], true) // Default: continue after quorum
		// case "merge":
		// 	merge()
		case "multiappend":
			if len(fields) < 4 {
				fmt.Println("Usage: multiappend <filename> <vmAddresses> <localFilenames>")
				continue
			}

			// filename is the first argument
			filename := fields[1]
			fmt.Println("filename:", filename)

			// Convert VM addresses and local filenames into slices
			vmAddresses := strings.Split(fields[2], ",")
			fmt.Println("vmAddresses:", vmAddresses)
			localFilenames := strings.Split(fields[3], ",")
			fmt.Println("localFilenames:", localFilenames)

			// Check if the number of addresses matches the number of filenames
			if len(vmAddresses) != len(localFilenames) {
				fmt.Println("The number of VM addresses and local filenames must match.")
				continue
			}

			// Call the MultiAppend function
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
		// Display operations
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

// Start file operation server function
func startFileOperationServer() {
	listener, err := net.Listen("tcp", ":"+cassandra.FilePort) // Start listening on port 9090
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

		// Handle each connection in a goroutine
		go func(c net.Conn) {
			defer c.Close()
			if err := file.HandleFileOperation(c); err != nil {
				log.Printf("Error handling file operation: %v", err)
			}
		}(conn)
	}
}
