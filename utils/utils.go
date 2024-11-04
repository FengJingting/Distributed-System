package utils
import (
	"math/rand"
	"time"
	"os"
	"fmt"
	"hash/fnv"
	"strconv"
	"sort"
)

// Shuffle 函数用于随机打乱节点列表
func Shuffle(nodes []Node) {
    rand.Seed(time.Now().UnixNano())
    rand.Shuffle(len(nodes), func(i, j int) {
        nodes[i], nodes[j] = nodes[j], nodes[i]
    })
}

func findNodeDetailsByIP(ip string) (string, string, string, bool) {
	 // 遍历 memberlist["alive"] 列表中的每个节点
	 for _, node := range Memberlist["alive"] {
        if node.IP == ip {
            // 返回端口、ID、时间戳（转换为字符串），最后一个参数表示是否找到
            return node.Port, strconv.FormatUint(node.ID, 10), strconv.Itoa(node.Timestamp), true
        }
    }
    // 如果没有找到，返回空字符串和 false
    return "", "", "", false
}

func Write_to_log() {
	// Log current memberlist to a file
	// Open or create the log file in append mode
	logFile, err := os.OpenFile("memberlist.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening/creating log file:", err)
		return
	}
	defer logFile.Close()

	// Get the current time as a timestamp for the log entry
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// Write the overall timestamp of the log entry
	_, err = fmt.Fprintf(logFile, "Log Timestamp: %s\n", currentTime)
	if err != nil {
		fmt.Println("Error writing timestamp to log file:", err)
		return
	}

	// Write the memberlist data to the log file, recording the timestamp for each node
	for status, nodes := range Memberlist {
		for _, node := range nodes {
			_, err := fmt.Fprintf(logFile, "[%s] %s: %v\n", currentTime, status, node)
			if err != nil {
				fmt.Println("Error writing node status to log file:", err)
				return
			}
		}
	}

	// Add a separator line to distinguish logs at different times
	_, err = fmt.Fprintln(logFile, "---------------------------")
	if err != nil {
		fmt.Println("Error writing separator to log file:", err)
	}

	fmt.Println("Memberlist successfully written to log.")
}

// 导出 hash 函数为 Hash
func Hash(filename string) uint64 {
    hasher := fnv.New64a()
    hasher.Write([]byte(filename))
    return hasher.Sum64()
}

// updatePredecessorsAndSuccessors 更新所有节点的前驱和后继
func (ring *ConsistentHashRing) updatePredecessorsAndSuccessors() {
    n := len(ring.SortedHashes)
    for i, hash := range ring.SortedHashes {
        node := ring.Nodes[hash]

        // 前驱节点：如果 i == 0，则前驱是最后一个节点，否则前驱是前一个节点
        if i == 0 {
            node.Predecessor = ring.Nodes[ring.SortedHashes[n-1]]
        } else {
            node.Predecessor = ring.Nodes[ring.SortedHashes[i-1]]
        }

        // 后继节点：如果 i == n-1，则后继是第一个节点，否则后继是下一个节点
        if i == n-1 {
            node.Successor = ring.Nodes[ring.SortedHashes[0]]
        } else {
            node.Successor = ring.Nodes[ring.SortedHashes[i+1]]
        }
    }
}

// AddRing 将新节点添加到一致性哈希环中
func (ring *ConsistentHashRing) AddRing(node *Node) {
    ring.Mutex.Lock()
    defer ring.Mutex.Unlock()

    // 使用节点的 IP 和端口生成唯一的哈希值作为节点位置
    nodeKey := fmt.Sprintf("%s:%s", node.IP, node.Port)
    nodeHash := Hash(nodeKey)

    // 将哈希值和节点指针添加到 Nodes 中
    ring.Nodes[nodeHash] = node

    // 将哈希值插入到 SortedHashes 中并保持排序
    ring.SortedHashes = append(ring.SortedHashes, nodeHash)
    sort.Slice(ring.SortedHashes, func(i, j int) bool { return ring.SortedHashes[i] < ring.SortedHashes[j] })

    // 更新前驱和后继
    ring.updatePredecessorsAndSuccessors()
} 