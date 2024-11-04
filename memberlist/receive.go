package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
	"mp3/utils"
	"mp3/cassandra"
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
			// newNode := []string{messageParts[1], "8080", timestamp}

			// Add new node to memberlist["alive"]
			// cassandra.CountMutex.Lock()
			// newNode := []string{messageParts[1], "8080", timestamp}
			// cassandra.Memberlist["alive"] = append(cassandra.Memberlist["alive"], newNode)
			// cassandra.CountMutex.Unlock()
			// list_mem()
			// Reply
			// _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }
			// send_update_whole("update", cassandra.Domain)
			

			// 解析消息并调用 addNode 函数添加节点
			// 调用 addNode 添加节点
			newIP := messageParts[1]
			newPort := "8080"  // 使用接收到的端口信息
			AddNode(newIP, newPort)

			// 将 memberlist 和 ring 编码为 JSON 并发送回给请求者
			send_update_whole("update", newIP)
			// TODO: 写入日志
			// cassandra.Write_to_log()

		} else if len(messageParts) == 2 && messageParts[0] == "update" {
			// Node update
			fmt.Printf("Node Updated:\n")
			// var newmemberlist Memberlist
			// err := json.Unmarshal([]byte(messageParts[1]), &newmemberlist)
			// if err != nil {
			// 	fmt.Println("Error decoding memberlist JSON:", err)
			// 	return
			// }
			// cassandra.CountMutex.Lock()
			// memberlist = newmemberlist
			// cassandra.CountMutex.Unlock()
			// list_mem()
			// _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			// if err != nil {
			// 	fmt.Println("Error sending ack:", err)
			// 	continue
			// }
			// cassandra.Write_to_log()
			 // 解析 `memberlist` JSON 数据
			 var newMemberlist map[string][]cassandra.Node
			 err := json.Unmarshal([]byte(messageParts[1]), &newMemberlist)
			 if err != nil {
				 fmt.Println("Error decoding memberlist JSON:", err)
				 return
			 }
		 
			 // 解析 `ring` JSON 数据
			 var newRing cassandra.ConsistentHashRing
			 err = json.Unmarshal([]byte(messageParts[2]), &newRing)
			 if err != nil {
				 fmt.Println("Error decoding ring JSON:", err)
				 return
			 }
		 
			 // 使用锁确保对全局 `memberlist` 和 `ring` 的线程安全更新
			 cassandra.CountMutex.Lock()
			 cassandra.Memberlist = newMemberlist
			 *cassandra.Ring = newRing
			 cassandra.CountMutex.Unlock()
		 
			 // 输出更新后的 `memberlist` 和 `ring`
			 fmt.Println("Updated Memberlist and Ring:")
			 List_mem_ids()
			 fmt.Println("Updated Ring:")
			 for _, hash := range cassandra.Ring.SortedHashes {
				 node := cassandra.Ring.Nodes[hash]
				 fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
			 }
		 
			 // 发送 `ack` 确认消息
			 _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			 if err != nil {
				 fmt.Println("Error sending ack:", err)
				 return
			 }

		} else {
			fmt.Println(messageParts)
		}
	}
}

// addNode 初始化并添加新节点到 alive 列表中
func AddNode(ip string, port string) {
	ID := utils.Hash(ip+port)
    node := &cassandra.Node{
        ID:        ID,
        IP:        ip,
        Port:      port,
        Timestamp: int(time.Now().Unix()),
    }
    // 将新节点添加到 memberlist 的 alive 列表中
    cassandra.CountMutex.Lock()
    defer cassandra.CountMutex.Unlock()

    cassandra.Memberlist["alive"] = append(cassandra.Memberlist["alive"], *node)
    fmt.Printf("Node added to 'alive': ID=%d, IP=%s, Port=%s, Timestamp=%d\n", node.ID, node.IP, node.Port, node.Timestamp)
    
	cassandra.Ring.AddRing(node)

}
