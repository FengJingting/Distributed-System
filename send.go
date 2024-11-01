package main
import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

func send_status(status string, selfIP string) {
	//status option: alive/suspect/fail/leave
	message := status
	//send to all server except itself
	for _, node := range memberlist["alive"] {
		serverIP := node[0]
		if serverIP == selfIP {
			continue
		}
		port := node[1]
		send(serverIP, port, message)
	}
}

// update (the whole list)
func send_update_whole(status string, selfIP string) {
	//status option: update
	//form message
	jsonData, err := json.Marshal(memberlist)
	if err != nil {
		fmt.Println("Error encoding memberlist JSON:", err)
		return
	}
	message := status + "+" + string(jsonData)
	//send to all server except itself
	for _, node := range memberlist["alive"] {
		serverIP := node[0]
		if serverIP == selfIP {
			continue
		}
		port := node[1]
		send(serverIP, port, message)
	}
}

func send_ping(ip, port string, t float64) bool {
	// Send a ping message to a specified node
	// construct message
	message := "ping"
	//  Parse IP and port
	serverAddr, err := net.ResolveTCPAddr("TCP", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return false
	}

	// build TCP connection
	conn, err := net.DialTCP("TCP", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing TCP:", err)
		return false
	}
	defer conn.Close()

	// send ping message
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending ping:", err)
		return false
	}
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println("Ping sent to", ip, currentTime)

	// Set timeout
	conn.SetReadDeadline(time.Now().Add(time.Duration(t * float64(time.Second))))

	// Prepare to receive ack
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)

	if err != nil {
		// If a timeout or error occurs during receivin
		fmt.Println("Error receiving response or timeout:", err)
		return false
	}
	// receive correct response
	if string(buffer[:n]) == "ack" {
		return true
	}

	// receive non ack
	return false
}

func send(ip string, port string, message string) {
	// Send a generic message to a specified node
		/*
		Send the message only once
		Exit if there's an error resolving the IP address or unable to establish a TCP connection
		Exit if TCP message cannot be sent or if no response is received
	*/
	serverAddr, err := net.ResolveTCPAddr("TCP", ip+":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.DialTCP("TCP", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing TCP:", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	// Receive the response
	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}
}

// update (one string)
func send_update(content string, status string, selfIP string) {
	//status option: alive/suspect/failed/leave
	//form message
	message := status + "+" + content
	//send to all server except itself
	for _, node := range memberlist["alive"] {
		serverIP := node[0]
		if serverIP == selfIP {
			continue
		}
		port := node[1]
		send(serverIP, port, message)
	}
}