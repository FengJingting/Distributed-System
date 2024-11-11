package memberlist

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
	"mp3/utils"
	"mp3/cassandra"
	"strconv"
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

	buf := make([]byte, 8192)

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
			// If the array length is 1 and the first element is "ping", return "ack"
			_, err = conn.WriteToUDP([]byte("ack"), remoteAddr)
			if err != nil {
				fmt.Println("Error sending ack:", err)
				continue
			}
			//fmt.Printf("Sent ack to %s\n", remoteAddr.String())

		} else if len(messageParts) == 2 && messageParts[0] == "failed" {
			// Handle failed node
			fmt.Printf("Node failed\n")
			// Retrieve the IP and port of the failed node
			failedID := messageParts[1]
			// Confirm receipt of the "failed" message to the sender
			_, err := conn.WriteToUDP([]byte("received"), remoteAddr)
			if err != nil {
				fmt.Println("Error sending ack for failed message:", err)
				return
			}
			
			// Find and remove the failed node
			cassandra.CountMutex.Lock()
			defer cassandra.CountMutex.Unlock()
			changeStatus("failed", failedID)
		} else if len(messageParts) == 2 && messageParts[0] == "join" {
			fmt.Printf("Node Added\n")
			// Parse the message and call the addNode function to add the node
			newIP := messageParts[1]
			newPort := "8080"  // Use the received port information
			AddNode(newIP, newPort)
			_, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			if err != nil {
				fmt.Println("Error sending ack:", err)
				continue
			}
			// Encode the memberlist and ring as JSON and send it back to the requester
			send_update_whole("update", cassandra.Domain)
			// TODO: Write to log
			// cassandra.Write_to_log()

		} else if len(messageParts) == 3 && messageParts[0] == "update" {
			// Node update
			fmt.Printf("Node Updated:\n")
			 var newMemberlist map[string][]cassandra.Node
			 err := json.Unmarshal([]byte(messageParts[1]), &newMemberlist)
			 if err != nil {
				 fmt.Println("Error decoding memberlist JSON:", err)
				 return
			 }
		 
			 // Parse `ring` JSON data
			 var newRing cassandra.ConsistentHashRing
			 err = json.Unmarshal([]byte(messageParts[2]), &newRing)
			 if err != nil {
				 fmt.Println("Error decoding ring JSON:", err)
				 return
			 }
		 
			 // Use a lock to ensure thread-safe updates to the global `memberlist` and `ring`
			 cassandra.CountMutex.Lock()
			 cassandra.Memberlist = newMemberlist
			 *cassandra.Ring = newRing
			 cassandra.CountMutex.Unlock()
			 
			 // Send `ack` confirmation message
			 _, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			 if err != nil {
				 fmt.Println("Error sending ack:", err)
				 return
			 }
			 
			 // Output the updated `memberlist` and `ring`
			 fmt.Println("Updated Memberlist and Ring:")
			 List_mem_ids()
			 fmt.Println("Updated Ring:")
			 for _, hash := range cassandra.Ring.SortedHashes {
				 node := cassandra.Ring.Nodes[hash]
				 fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
			 }
		} else if len(messageParts) == 3 && messageParts[1] == "suspect" {
			// Handle node suspect
			fmt.Printf("Suspect Node JSON: %s\n", messageParts[2])
			
			// 将 messageParts[2] 的 JSON 字符串解析为 []Node 类型
			var suspect []cassandra.Node
			err := json.Unmarshal([]byte(messageParts[2]), &suspect)
			if err != nil {
				fmt.Println("Error decoding suspect list JSON:", err)
				return
			}

			// 遍历解析出来的 suspect 节点列表
			for _, node := range suspect {
				fmt.Println("suspect node:", node)
				if node.IP == cassandra.Domain {
					// 如果节点是自身，则标记 SelfSuspected 为 true
					cassandra.SelfSuspected = true
				} else {
					// 否则，将节点状态更改为 suspect
					nodeIDStr := strconv.FormatUint(node.ID, 10)  // 假设 node.ID 是 uint64
					changeStatus("suspect", nodeIDStr)
				}
			}
			// 发送确认响应
			_, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			if err != nil {
				fmt.Println("Error sending ack:", err)
			}
		} else if len(messageParts) == 3 && messageParts[1] == "alive" {
			// Handle node alive message
			fmt.Printf("Node is alive: %s\n", messageParts[1])
			var alive []string
			err := json.Unmarshal([]byte(messageParts[2]), &alive)
			if err != nil {
				fmt.Println("Error decoding memberlist JSON:", err)
				return
			}
			for _, node := range cassandra.Memberlist["suspect"] {
				// Use && for logical AND check
				if node.IP == cassandra.Domain {
					nodeIDStr := strconv.FormatUint(node.ID, 10)
					changeStatus("alive", nodeIDStr)
				}
			}
			_, err = conn.WriteToUDP([]byte("received"), remoteAddr)
			if err != nil {
				fmt.Println("Error sending ack:", err)
				continue
			}
		}else {
			fmt.Println(len(messageParts))
		}
	}
}

// addNode initializes and adds a new node to the alive list
func AddNode(ip string, port string) {
	fmt.Println("_________Add Node___________")
	ID := utils.Hash(ip+port)
    node := &cassandra.Node{
        ID:        ID,
        IP:        ip,
        Port:      port,
        Timestamp: int(time.Now().Unix()),
    }
    // Add the new node to the alive list in the memberlist
    cassandra.CountMutex.Lock()
    defer cassandra.CountMutex.Unlock()
	fmt.Println("cassandra.Memberlist", cassandra.Memberlist)
    cassandra.Memberlist["alive"] = append(cassandra.Memberlist["alive"], *node)
    fmt.Printf("Node added to 'alive': ID=%d, IP=%s, Port=%s, Timestamp=%d\n", node.ID, node.IP, node.Port, node.Timestamp)
    
	cassandra.Ring.AddRing(node)
}

