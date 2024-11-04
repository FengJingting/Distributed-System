package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
	"mp3/utils"
)

func send_status(status string, selfIP string) {
	// status option: alive/suspect/fail/leave
	message := status
	// send to all servers except itself
	for _, node := range utils.Memberlist["alive"] {
		serverIP := node.IP  // 使用 node.IP 而不是 node[0]
		if serverIP == selfIP {
			continue
		}
		port := node.Port  // 使用 node.Port 而不是 node[1]
		send(serverIP, port, message)
	}
}

// update (the whole list)
func send_update_whole(status string, selfIP string) {
	// status option: update
	// form message
	jsonData, err := json.Marshal(utils.Memberlist)
	if err != nil {
		fmt.Println("Error encoding memberlist JSON:", err)
		return
	}
	message := status + "+" + string(jsonData)
	// send to all servers except itself
	for _, node := range utils.Memberlist["alive"] {
		serverIP := node.IP
		if serverIP == selfIP {
			continue
		}
		port := node.Port
		send(serverIP, port, message)
	}
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
	// Resolve the TCP address
	serverAddr, err := net.ResolveTCPAddr("tcp", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	// Dial a TCP connection
	conn, err := net.DialTCP("tcp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing TCP:", err)
		return
	}
	defer conn.Close()

	// Send the message
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	// Buffer to receive the response
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}
	fmt.Printf("Received from server: %s\n", string(buffer[:n]))
}


// update (one string)
func send_update(content string, status string, selfIP string) {
	// status option: alive/suspect/failed/leave
	message := status + "+" + content
	for _, node := range utils.Memberlist["alive"] {
		serverIP := node.IP
		if serverIP == selfIP {
			continue
		}
		port := node.Port
		send(serverIP, port, message)
	}
}
