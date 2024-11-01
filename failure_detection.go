package main

import (
	"fmt"
	"time"
)

func detect_failure(t float64) {
	// Retrieve the global list of alive nodes
	aliveNodes := memberlist["alive"]
	// Iterate through each alive node to perform ping
	for i := 0; i < len(aliveNodes); i++ {
		node := aliveNodes[i]
		ip := node[0]   // IP
		port := node[1] // Port

		// skip itself
		if ip == domain {
			continue
		}

		// send ping
		if !send_ping(ip, port, t) {
			fmt.Printf("Node %s failed to respond\n", ip)
			//process depend on suspiction mode
			// If ifSus is false, mark the node as failed and broadcast the status
			fmt.Printf("Marking node %s as failed and broadcasting failure\n", ip)
			changeStatus("failed", node[0], node[1], node[2], "alive")

		}
	}
}

func detect_failure_n(t float64) {
	// Run continuous failure detection
	for {
		// Shuffle the order of the alive list
		Shuffle(memberlist["alive"])

		// Call the detect_failure function
		detect_failure(t)
		time.Sleep(1 * time.Second)
	}
}
