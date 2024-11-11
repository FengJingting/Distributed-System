package memberlist 

import (
	"fmt"
	"time"
	"mp3/cassandra"
)

func isNodeInSuspect(node cassandra.Node) bool {
	for _, suspectNode := range cassandra.Memberlist["suspect"] {
		// 假设每个节点通过其 IP 唯一标识
		if node.ID == suspectNode.ID {
			return true
		}
	}
	return false
}

func detect_failure(t float64) {
	// Retrieve the global list of alive nodes
	aliveNodes := cassandra.Memberlist["alive"]
	aliveNodes = append(aliveNodes, cassandra.Memberlist["suspect"]...)
	// Iterate through each alive node to perform ping
	for i := 0; i < len(aliveNodes); i++ {
		node := aliveNodes[i]
		ip := node.IP     // IP
		port := node.Port // Port

		// Skip itself
		if ip == cassandra.Domain {
			continue
		}
		// Send ping
		if !send_ping(ip, port, t) {
			fmt.Printf("Node %s failed to respond\n", ip)
			// Process based on suspicion mode
			// If ifSus is false, mark the node as failed and broadcast the status
			fmt.Printf("Marking node %s as suspect and broadcasting suspicion\n", ip)
			changeStatus("suspect", fmt.Sprint(node.ID))
			time.Sleep(5 * time.Second)
			if isNodeInSuspect(node) {
				changeStatus("failed", fmt.Sprint(node.ID))
				send_update(fmt.Sprint(node.ID), "failed", cassandra.Domain)
			}
		}
	}
}

func Detect_failure_n(t float64) {
	// Run continuous failure detection
	for {
		// Shuffle the order of the alive list
		cassandra.Shuffle(cassandra.Memberlist["alive"])

		// Call the detect_failure function
		detect_failure(t)
		time.Sleep(2 * time.Second)
	}
}
