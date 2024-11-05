package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
	// "mp3/utils"
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

	// 2. Remove cyclic references in ring
	for _, node := range cassandra.Ring.Nodes {
		node.Predecessor = nil
		node.Successor = nil
	}

	// 3. Serialize ring
	jsonRing, err := json.Marshal(cassandra.Ring)
	if err != nil {
		fmt.Println("Error encoding ring JSON:", err)
		return
	}

	// 4. Restore Predecessor and Successor pointers for all nodes in the ring
	cassandra.Ring.UpdatePredecessorsAndSuccessors() // 调用恢复前驱和后继的方法

	// 5. Send message with memberlist and ring
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
	serverAddr, err := net.ResolveTCPAddr("tcp", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return false
	}

	conn, err := net.DialTCP("tcp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing TCP:", err)
		return false
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending ping:", err)
		return false
	}
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println("Ping sent to", ip, currentTime)

	conn.SetReadDeadline(time.Now().Add(time.Duration(t * float64(time.Second))))

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
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
	buffer := make([]byte, 1024)
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
		port := node.Port
		send(serverIP, port, message)
	}
}