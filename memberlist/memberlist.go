package memberlist

import (
    "fmt"
    // "strings"
    // "mp3/utils"
	"mp3/cassandra"
	"strconv"
	"os"
	"time"
)


func findNodeDetailsByIP(ip string) (string, string, string, bool) {
	// 遍历 memberlist["alive"] 列表中的每个节点
	for _, node := range cassandra.Memberlist["alive"] {
	   if node.IP == ip {
		   // 返回端口、ID、时间戳（转换为字符串），最后一个参数表示是否找到
		   return node.Port, strconv.FormatUint(node.ID, 10), strconv.Itoa(node.Timestamp), true
	   }
   }
   // 如果没有找到，返回空字符串和 false
   return "", "", "", false
}

func List_mem_ids() {
    fmt.Println("list_mem_ids")

    // 加锁以确保线程安全
    cassandra.Ring.Mutex.Lock()
    defer cassandra.Ring.Mutex.Unlock()

    // 遍历 SortedHashes 获取排序好的节点
    for _, nodeHash := range cassandra.Ring.SortedHashes {
        node := cassandra.Ring.Nodes[nodeHash]

        // 找到节点的状态
        var nodeStatus string
        for status, nodes := range cassandra.Memberlist {
            found := false
            for _, n := range nodes {
                if n.ID == node.ID {
                    nodeStatus = status
                    found = true
                    break
                }
            }
            if found {
                break
            }
        }

        // 打印节点的状态、ID、IP 和端口
        fmt.Printf("Status: %s, Node ID: %d, IP: %s, Port: %s\n", 
                    nodeStatus, nodeHash, node.IP, node.Port)
    }
}

func List_self() {
    fmt.Println("list_self function called")

    // 加锁以确保安全访问
    cassandra.Ring.Mutex.Lock()
    defer cassandra.Ring.Mutex.Unlock()

    found := false
    for state, nodes := range cassandra.Memberlist {
        for _, node := range nodes {
            // 检查当前节点是否匹配本节点的 IP 地址
            if node.IP == cassandra.Domain {
                fmt.Printf("Found matching node in state '%s':\n", state)
                fmt.Printf("  Node ID: %d\n  IP: %s\n  Port: %s\n  Timestamp: %d\n", 
                           node.ID, node.IP, node.Port, node.Timestamp)

                // 获取前驱节点和后继节点信息
                ringNode := cassandra.Ring.Nodes[node.ID]
                if ringNode != nil {
                    if ringNode.Predecessor != nil {
                        fmt.Printf("  Predecessor: ID=%d, IP=%s, Port=%s\n", 
                                   ringNode.Predecessor.ID, ringNode.Predecessor.IP, ringNode.Predecessor.Port)
                    } else {
                        fmt.Println("  Predecessor: None")
                    }

                    if ringNode.Successor != nil {
                        fmt.Printf("  Successor: ID=%d, IP=%s, Port=%s\n", 
                                   ringNode.Successor.ID, ringNode.Successor.IP, ringNode.Successor.Port)
                    } else {
                        fmt.Println("  Successor: None")
                    }
                } else {
                    fmt.Println("  Node information not found in the ring.")
                }

                found = true
                break
            }
        }
        if found {
            break
        }
    }

    if !found {
        fmt.Println("No matching node found in any state")
    }
}


func Join() {
    fmt.Println("join function called")
    // 发送加入消息给 Introducer
    message := "join+" + cassandra.Domain
    send(cassandra.Introducer, cassandra.MemberPort, message)
}

// func changeStatus(status, nodeIP, port, timestamp, initial string) {
//     var nodeToMove cassandra.Node
//     var found bool

//     // 遍历 memberlist[initial]，找到匹配的节点
//     aliveNodes := cassandra.Memberlist[initial]
//     for i, node := range aliveNodes {
//         // 匹配节点 IP 和端口
//         if node.IP == nodeIP && node.Port == port && fmt.Sprint(node.Timestamp) == timestamp {
//             // 找到节点
//             nodeToMove = node

//             // 从 initial 列表中删除节点
//             cassandra.CountMutex.Lock()
//             cassandra.Memberlist[initial] = append(aliveNodes[:i], aliveNodes[i+1:]...)
//             cassandra.CountMutex.Unlock()

//             found = true
//             break
//         }
//     }

//     // 如果找到节点，将其移动到目标状态列表
//     if found {
//         cassandra.CountMutex.Lock()
//         cassandra.Memberlist[status] = append(cassandra.Memberlist[status], nodeToMove)
//         cassandra.CountMutex.Unlock()

//         List_mem_ids() // 调试用，打印当前 memberlist 的状态
//         Write_to_log()
//     } else {
//         fmt.Printf("Node %s with port %s not found in alive\n", nodeIP, port)
//     }
// }
func changeStatus(newStatus, nodeID string) {
    var nodeToMove cassandra.Node
    var found bool

    // 加锁以安全访问 `Memberlist`
    cassandra.CountMutex.Lock()
    defer cassandra.CountMutex.Unlock()

    // 在 `Memberlist` 中找到节点的当前状态并记录其状态
    for status, nodes := range cassandra.Memberlist {
        for i, node := range nodes {
            if fmt.Sprint(node.ID) == nodeID {
                nodeToMove = node

                // 从当前状态列表中删除节点
                cassandra.Memberlist[status] = append(nodes[:i], nodes[i+1:]...)
                found = true
                break
            }
        }
        if found {
            break
        }
    }

    // 如果找到节点，则将其添加到新的状态列表并从哈希环中删除
    if found {
        // 将节点添加到 `newStatus` 列表中
        cassandra.Memberlist[newStatus] = append(cassandra.Memberlist[newStatus], nodeToMove)

        // 从一致性哈希环中删除节点
        cassandra.Ring.RemoveNode(nodeToMove.ID) // 假设 `RemoveNode` 方法已在 `Ring` 中实现

        // 打印更新后的 `Memberlist` 以供调试
        List_mem_ids() // 调试用，打印当前 `Memberlist` 的状态
        // Write_to_log() // 将更新后的信息写入日志
    } else {
        fmt.Printf("Node with ID %s not found in current memberlist.\n", nodeID)
    }
}

func Write_to_log() {
	// 打开或创建日志文件，以追加模式写入
	logFile, err := os.OpenFile("memberlist.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening/creating log file:", err)
		return
	}
	defer logFile.Close()

	// 获取当前时间作为日志条目的时间戳
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// 写入日志条目的时间戳
	_, err = fmt.Fprintf(logFile, "Log Timestamp: %s\n", currentTime)
	if err != nil {
		fmt.Println("Error writing timestamp to log file:", err)
		return
	}

	// 写入 `memberlist` 数据
	_, err = fmt.Fprintln(logFile, "Memberlist:")
	if err != nil {
		fmt.Println("Error writing memberlist header to log file:", err)
		return
	}
	for status, nodes := range cassandra.Memberlist {
		for _, node := range nodes {
			_, err := fmt.Fprintf(logFile, "[%s] Status: %s, Node ID: %d, IP: %s, Port: %s, Timestamp: %d\n",
				currentTime, status, node.ID, node.IP, node.Port, node.Timestamp)
			if err != nil {
				fmt.Println("Error writing node status to log file:", err)
				return
			}
		}
	}

	// 写入 `ring` 数据
	_, err = fmt.Fprintln(logFile, "Ring:")
	if err != nil {
		fmt.Println("Error writing ring header to log file:", err)
		return
	}
	cassandra.Ring.Mutex.Lock()
	defer cassandra.Ring.Mutex.Unlock()

	for _, hash := range cassandra.Ring.SortedHashes {
		node := cassandra.Ring.Nodes[hash]
		_, err = fmt.Fprintf(logFile, "Node ID: %d, IP: %s, Port: %s, ", node.ID, node.IP, node.Port)
		if err != nil {
			fmt.Println("Error writing ring node data to log file:", err)
			return
		}
		// 写入前驱和后继节点信息
		if node.Predecessor != nil {
			_, err = fmt.Fprintf(logFile, "Predecessor ID: %d, ", node.Predecessor.ID)
			if err != nil {
				fmt.Println("Error writing predecessor to log file:", err)
				return
			}
		} else {
			_, err = fmt.Fprint(logFile, "Predecessor ID: nil, ")
		}
		if node.Successor != nil {
			_, err = fmt.Fprintf(logFile, "Successor ID: %d\n", node.Successor.ID)
			if err != nil {
				fmt.Println("Error writing successor to log file:", err)
				return
			}
		} else {
			_, err = fmt.Fprintln(logFile, "Successor ID: nil")
		}
	}

	// 添加分隔线来区分不同时间的日志
	_, err = fmt.Fprintln(logFile, "---------------------------")
	if err != nil {
		fmt.Println("Error writing separator to log file:", err)
	}

	fmt.Println("Memberlist and Ring successfully written to log.")
}
