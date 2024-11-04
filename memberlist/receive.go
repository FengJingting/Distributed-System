package memberlist

import (
	// "encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
	"mp3/utils"
)

func ListenAndReply(port string) {
	addr, err := net.ResolveUDPAddr("udp", ":"+port)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening on UDP:", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		// Receive data
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			continue
		}

		receivedMessage := string(buf[:n])
		fmt.Printf("Received message from %s: %s\n", remoteAddr.String(), receivedMessage)
		// Parse the received message into an array (split by "+")
		messageParts := strings.Split(receivedMessage, "+")

		// Handle different message types
		if len(messageParts) == 1 && messageParts[0] == "ping" {
			// Set packet loss rate
			// If the array length is 1 and the first element is "ping", return "ack"
			// _, err = conn.WriteToUDP([]byte("ack"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }
			//fmt.Printf("Sent ack to %s\n", remoteAddr.String())

		} else if len(messageParts) == 5 && messageParts[0] == "failed" {
			// Handle failed node
			fmt.Printf("Node failed\n")
			// changeStatus(messageParts[0], messageParts[1], messageParts[2], messageParts[3], "alive")
			// // Reply
			// _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }

		} else if len(messageParts) == 2 && messageParts[0] == "join" {
			fmt.Printf("Node Added\n")
			// timestamp := time.Now().Format(time.RFC3339)
			// Create new node [IP, Port, Timestamp]
			// newNode := []string{messageParts[1], "8080", timestamp}

			// Add new node to memberlist["alive"]
			// utils.CountMutex.Lock()
			// newNode := []string{messageParts[1], "8080", timestamp}
			// utils.Memberlist["alive"] = append(utils.Memberlist["alive"], newNode)
			// utils.CountMutex.Unlock()
			// list_mem()
			// Reply
			// _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }
			// send_update_whole("update", utils.Domain)
			// utils.Write_to_log()

		} else if len(messageParts) == 2 && messageParts[0] == "update" {
			// Node update
			fmt.Printf("Node Updated:\n")
			// var newmemberlist Memberlist
			// err := json.Unmarshal([]byte(messageParts[1]), &newmemberlist)
			// if err != nil {
			// 	fmt.Println("Error decoding memberlist JSON:", err)
			// 	return
			// }
			// utils.CountMutex.Lock()
			// memberlist = newmemberlist
			// utils.CountMutex.Unlock()
			// list_mem()
			// _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }
			// utils.Write_to_log()

		} else {
			fmt.Println(messageParts)
		}
	}
}

// addNode 初始化并添加新节点到 alive 列表中
func addNode(ip string, port string) {
	ID := utils.Hash(ip+port)
    node := &utils.Node{
        ID:        ID,
        IP:        ip,
        Port:      port,
        Timestamp: int(time.Now().Unix()),
    }
    // 将新节点添加到 memberlist 的 alive 列表中
    utils.CountMutex.Lock()
    defer utils.CountMutex.Unlock()

    utils.Memberlist["alive"] = append(utils.Memberlist["alive"], *node)
    fmt.Printf("Node added to 'alive': ID=%d, IP=%s, Port=%s, Timestamp=%d\n", node.ID, node.IP, node.Port, node.Timestamp)
    
	utils.Ring.AddRing(node)

}
