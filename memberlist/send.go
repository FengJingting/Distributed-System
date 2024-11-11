package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
	"mp3/cassandra"
)

func send_status(status string, selfIP string) {
	// status option: alive/suspect/fail/leave
	message := status
	// send to all servers except itself
	for _, node := range cassandra.Memberlist["alive"] {
		serverIP := node.IP  // 使用 node.IP 而不是 node[0]
		if serverIP == selfIP {
			continue
		}
		port := node.Port  // 使用 node.Port 而不是 node[1]
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

	// 2. Temporarily remove PredecessorID and SuccessorID to avoid cyclic references
	originalPredecessorIDs := make(map[uint64]uint64)
	originalSuccessorIDs := make(map[uint64]uint64)

	for id, node := range cassandra.Ring.Nodes {
		originalPredecessorIDs[id] = node.PredecessorID
		originalSuccessorIDs[id] = node.SuccessorID
		node.PredecessorID = 0
		node.SuccessorID = 0
	}

	// 3. Serialize ring
	jsonRing, err := json.Marshal(cassandra.Ring)
	if err != nil {
		fmt.Println("Error encoding ring JSON:", err)
		return
	}

	// 4. Restore PredecessorID and SuccessorID after serialization
	for id, node := range cassandra.Ring.Nodes {
		node.PredecessorID = originalPredecessorIDs[id]
		node.SuccessorID = originalSuccessorIDs[id]
	}

	// 5. Send message with serialized memberlist and ring
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
	//currentTime := time.Now().Format("2006-01-02 15:04:05")
	//fmt.Println("Ping sent to", ip, currentTime)

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



// update (one string)
func send_update(content string, status string, selfIP string) {
	// status option: alive/suspect/failed/leave
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