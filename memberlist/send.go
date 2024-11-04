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

// update (the whole list)
func send_update_whole(status string, selfIP string) {
	// status option: update
	// form message
	jsonData, err := json.Marshal(cassandra.Memberlist)
	if err != nil {
		fmt.Println("Error encoding memberlist JSON:", err)
		return
	}
	message := status + "+" + string(jsonData)
	// send to all servers except itself
	for _, node := range cassandra.Memberlist["alive"] {
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

	buffer := make([]byte, 1024)
	n, remoteAddr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}
	fmt.Printf("Received from %s: %s\n", remoteAddr, string(buffer[:n]))
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


// 广播当前环信息到所有节点
func broadcastRingUpdate() {
    cassandra.CountMutex.Lock()
    defer cassandra.CountMutex.Unlock()

    // 序列化 Memberlist 和 Ring
    memberlistData, err := json.Marshal(cassandra.Memberlist)
    if err != nil {
        fmt.Println("Error encoding Memberlist:", err)
        return
    }

    ringData, err := json.Marshal(cassandra.Ring)
    if err != nil {
        fmt.Println("Error encoding Ring:", err)
        return
    }

    // 构建广播消息
    message := fmt.Sprintf("update+%s+%s", string(memberlistData), string(ringData))

    // 遍历所有节点并发送消息
    for _, node := range cassandra.Memberlist["alive"] {
        if node.IP != cassandra.Domain {  // 跳过自身
            send(node.IP, node.Port, message)
        }
    }
}