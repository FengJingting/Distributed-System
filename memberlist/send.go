package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
	"mp3/cassandra"
)

func send_status(status string, selfIP string) {
	// Status options: alive/suspect/fail/leave
	message := status
	// Send to all servers except itself
	for _, node := range cassandra.Memberlist["alive"] {
		serverIP := node.IP  // Use node.IP instead of node[0]
		if serverIP == selfIP {
			continue
		}
		port := node.Port  // Use node.Port instead of node[1]
		send(serverIP, port, message)
	}
}

func send_update_whole(status string, selfIP string) {
	// Status option: update
	// Form message with memberlist and ring

	// 1. Serialize memberlist
	jsonMemberlist, err := json.Marshal(cassandra.Memberlist)
	if err != nil {
		fmt.Println("Error encoding memberlist JSON:", err)
		return
	}

	// 2. Create a copy of the ring with zeroed PredecessorID and SuccessorID
	ringCopy := &cassandra.ConsistentHashRing{
		Nodes:        make(map[uint64]*cassandra.Node),
		SortedHashes: cassandra.Ring.SortedHashes,
	}
	for id, node := range cassandra.Ring.Nodes {
		// Make a shallow copy of each node with PredecessorID and SuccessorID set to zero
		nodeCopy := *node
		ringCopy.Nodes[id] = &nodeCopy
	}

	// 3. Serialize ring copy
	jsonRing, err := json.Marshal(ringCopy)
	if err != nil {
		fmt.Println("Error encoding ring JSON:", err)
		return
	}

	// 4. Send message with serialized memberlist and ring
	message := fmt.Sprintf("update+%s+%s", string(jsonMemberlist), string(jsonRing))
	for _, node := range cassandra.Memberlist["alive"] {
		if node.IP != selfIP {
			send(node.IP, node.Port, message)
		}
	}
	return
}

func send_ping(ip, port string, t float64) bool {
	// Send a ping message to a specified node
	message := "ping"
	serverAddr, err := net.ResolveUDPAddr("udp", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return false
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing UDP:", err)
		return false
	}
	defer conn.Close()

	// Send ping message
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending ping:", err)
		return false
	}
	// Set a read deadline for the response
	conn.SetReadDeadline(time.Now().Add(time.Duration(t * float64(time.Second))))

	// Receive the response
	buffer := make([]byte, 1024)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Println("Error receiving response or timeout:", err)
		return false
	}

	return string(buffer[:n]) == "ack"
}

func send(ip string, port string, message string) {
	fmt.Printf("Attempting to send message to IP: %s, Port: %s\n", ip, port)
	fmt.Printf("Message content: %s\n", message)

	serverAddr, err := net.ResolveUDPAddr("udp", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing UDP:", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	// Only attempt to read response once to avoid blocking
	buffer := make([]byte, 8192)
	//conn.SetReadDeadline(time.Now().Add(2 * time.Second))  // Set a short timeout
	n, remoteAddr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Println("Error receiving response or timeout:", err)
		return
	}
	fmt.Printf("Received response from %s: %s\n", remoteAddr, string(buffer[:n]))
}

// Update (one string)
func send_update(content string, status string, selfIP string) {
	// Status options: alive/suspect/failed/leave
	message := status + "+" + content
	for _, node := range cassandra.Memberlist["alive"] {
		serverIP := node.IP
		if serverIP == selfIP {
			continue
		}
		port := cassandra.MemberPort
		send(serverIP, port, message)
	}
}
