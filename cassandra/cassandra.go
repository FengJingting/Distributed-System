package cassandra

import (
	"fmt"
	// "mp3/memberlist"
	// "hash/fnv"
	"io/ioutil"
	// "net"
	"log"
	"time"

	// "mp3/utils"
	"encoding/json"
	"math/rand"
	"sort"
)

// initConfig 读取并解析 JSON 配置文件
func InitConfig() {
	var config Config
	data, err := ioutil.ReadFile("./config.json")
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error parsing config JSON: %v", err)
	}

	// 将配置值赋给全局变量
	Domain = config.Domain
	FilePort = config.FilePort
	MemberPort = config.MemberPort
	Introducer = config.Introducer
	// fmt.Print(Domain, Port, Introducer)

	// 初始化哈希环
	Ring = NewConsistentHashRing()
}

// Shuffle 函数用于随机打乱节点列表
func Shuffle(nodes []Node) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

// 初始化一致性哈希环
func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		Nodes:        make(map[uint64]*Node), // 使用 `uint64` 和 `*Node`
		SortedHashes: []uint64{},             // 使用 `uint64`
	}
}

// updatePredecessorsAndSuccessors 更新所有节点的前驱和后继
func (ring *ConsistentHashRing) UpdatePredecessorsAndSuccessors() {
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

func (ring *ConsistentHashRing) AddRing(node *Node) {
	ring.Mutex.Lock()
	defer ring.Mutex.Unlock()

	// 使用 IP 和端口生成的哈希值（Node ID）作为 key
	nodeID := node.ID // 假设 Node ID 已经通过 IP 和端口的哈希生成
	ring.Nodes[nodeID] = node

	// 更新 SortedHashes 列表并保持排序
	ring.SortedHashes = append(ring.SortedHashes, nodeID)
	sort.Slice(ring.SortedHashes, func(i, j int) bool { return ring.SortedHashes[i] < ring.SortedHashes[j] })

	// 更新前驱和后继关系（确保环结构的完整性）
	ring.UpdatePredecessorsAndSuccessors()
	// 打印哈希环中的节点及其前驱和后继
	fmt.Println("Current nodes in the ring:")
	for _, hash := range Ring.SortedHashes {
		node := Ring.Nodes[hash]
		fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
		if node.Predecessor != nil {
			fmt.Printf("  Predecessor: ID=%d\n", node.Predecessor.ID)
		}
		if node.Successor != nil {
			fmt.Printf("  Successor: ID=%d\n", node.Successor.ID)
		}
	}
}

// RemoveNode 从一致性哈希环中移除节点
func (ring *ConsistentHashRing) RemoveNode(nodeID uint64) {
	ring.Mutex.Lock()
	defer ring.Mutex.Unlock()

	// 检查节点是否存在
	_, exists := ring.Nodes[nodeID]
	if !exists {
		fmt.Printf("Node with ID %d not found in the ring.\n", nodeID)
		return
	}

	// 从 Nodes 映射中删除节点
	delete(ring.Nodes, nodeID)

	// 在 SortedHashes 列表中找到并移除节点的哈希值
	for i, hash := range ring.SortedHashes {
		if hash == nodeID {
			ring.SortedHashes = append(ring.SortedHashes[:i], ring.SortedHashes[i+1:]...)
			break
		}
	}

	// 更新所有节点的前驱和后继
	ring.UpdatePredecessorsAndSuccessors()
	fmt.Printf("Node with ID %d removed from the ring.\n", nodeID)
}

// ---------------------------Basic file operations---------------------
// Create
// func create(localFilename, hyDFSFilename string) error {
// 	content, err := ioutil.ReadFile(localFilename)
// 	if err != nil {
// 		return fmt.Errorf("error reading local file: %v", err)
// 	}
// 	server := getTargetServer(hyDFSFilename)
// 	send1 := sendFile(*server, hyDFSFilename, content)
// 	send2 := sendFile(*server.Successor, hyDFSFilename, content)
// 	send3 := sendFile(*server.Successor.Successor, hyDFSFilename, content)
// 	if send1 != nil || send2 != nil || send3 != nil {
// 		return fmt.Errorf("sendFile errors: send1: %v, send2: %v, send3: %v", send1, send2, send3)
// 	} else {
// 		return nil
// 	}
// }

// Get (fetch)
// func get(hyDFSFilename, localFilename string) error {
// 	server := getTargetServer(hyDFSFilename)
// 	content, err := fetchFile(*server, hyDFSFilename)
// 	if err != nil {
// 		return fmt.Errorf("error fetching file: %v", err)
// 	}
// 	return ioutil.WriteFile(localFilename, content, 0644)
// }

// Append
// func appendFile(localFilename, hyDFSFilename string) error {
// 	content, err := ioutil.ReadFile(localFilename)
// 	if err != nil {
// 		return fmt.Errorf("error reading local file: %v", err)
// 	}
// 	server := getTargetServer(hyDFSFilename)
// 	append1 := sendAppend(*server, hyDFSFilename, content)
// 	append2 := sendAppend(*server.Successor, hyDFSFilename, content)
// 	append3 := sendAppend(*server.Successor.Successor, hyDFSFilename, content)
// 	if append1 != nil || append2 != nil || append3 != nil {
// 		return fmt.Errorf("sendFile errors: append1: %v, append2: %v, append3: %v", append1, append2, append3)
// 	} else {
// 		return nil
// 	}
// }
