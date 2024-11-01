package main

import (
	"fmt"
	"strings"
)

type Memberlist map[string][][]string

func list_mem() {
	// Iterate through and print each key-value pair in the memberlist
	for status, nodes := range memberlist {
		fmt.Printf("%s nodes:\n", status)
		if len(nodes) == 0 {
			fmt.Println("No nodes")
		} else {
			for _, node := range nodes {
				fmt.Printf("  %s\n", node)
			}
		}
	}
}

func list_self() {
	fmt.Println("list_self function called")
	// Iterate through nodes and find self by domain
	fmt.Println("list_self function called")

	// Iterate through the nodes in all states in the memberlist, looking for entries that match the domain
	found := false
	for state, nodes := range memberlist {
		for _, node := range nodes {
			if node[0] == domain {
				// Join all elements in the node into a single string
				joined := strings.Join(node, ", ")
				fmt.Printf("Found matching node in state '%s': %s\n", state, joined)
				found = true
			}
		}
	}

	// If no matching node is found
	if !found {
		fmt.Println("No matching node found in any state")
	}
}


func join() {
	fmt.Println("join function called")
	// Send a join message to the Introducer
	message := "join+" + domain
	send("fa24-cs425-2901.cs.illinois.edu", "8080", message)
}

func changeStatus(status, NodeIP, port, timestamp, initial string) {
	var nodeToMove []string // To store the found node's information
	var found bool

	// Traverse memberlist[initial], find the matching node
	aliveNodes := memberlist[initial]
	for i, node := range aliveNodes {
		// Match NodeIP, port
		if node[0] == NodeIP && node[1] == port && node[2] == timestamp {
			// Node found, save its information
			nodeToMove = node

			// Remove the node from alive list
			countMutex.Lock()
			memberlist[initial] = append(aliveNodes[:i], aliveNodes[i+1:]...)
			countMutex.Unlock()

			found = true
			//fmt.Printf("Node %s moved from alive\n", NodeIP)
			break
		}
	}

	// If the node is found, move it to the target status list
	if found {
		countMutex.Lock()
		memberlist[status] = append(memberlist[status], nodeToMove)
		countMutex.Unlock()
		//fmt.Printf("Node %s added to %s\n", NodeIP, status)
		list_mem() // For debugging, print the current state of the memberlist
		Write_to_log()
	} else {
		fmt.Printf("Node %s with port %s not found in alive\n", NodeIP, port)
	}
}