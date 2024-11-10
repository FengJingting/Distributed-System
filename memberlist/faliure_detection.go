package memberlist

import (
	"fmt"
	"time"

	// "mp3/utils"
	"mp3/cassandra"
)

func detect_failure(t float64) {
	// Retrieve the global list of alive nodes
	aliveNodes := cassandra.Memberlist["alive"]
	// Iterate through each alive node to perform ping
	for i := 0; i < len(aliveNodes); i++ {
		node := aliveNodes[i]
		ip := node.IP     // IP
		port := node.Port // Port

		// skip itself
		if ip == cassandra.Domain {
			continue
		}

		// send ping
		if !send_ping(ip, port, t) {
			fmt.Printf("Node %s failed to respond\n", ip)
			//process depend on suspiction mode
			// If ifSus is false, mark the node as failed and broadcast the status
			fmt.Printf("Marking node %s as failed and broadcasting failure\n", ip)
			changeStatus("failed", fmt.Sprint(node.ID))
			send_update( fmt.Sprint(node.ID),"failed",cassandra.Domain)

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
		time.Sleep(1 * time.Second)
	}
}
