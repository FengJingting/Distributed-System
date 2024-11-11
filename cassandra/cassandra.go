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

		// 前驱节点：如果 i == 0，则前驱是最后一个节点的 ID，否则前驱是前一个节点的 ID
		if i == 0 {
			node.PredecessorID = ring.Nodes[ring.SortedHashes[n-1]].ID
		} else {
			node.PredecessorID = ring.Nodes[ring.SortedHashes[i-1]].ID
		}

		// 后继节点：如果 i == n-1，则后继是第一个节点的 ID，否则后继是下一个节点的 ID
		if i == n-1 {
			node.SuccessorID = ring.Nodes[ring.SortedHashes[0]].ID
		} else {
			node.SuccessorID = ring.Nodes[ring.SortedHashes[i+1]].ID
		}

		// 更新节点在环中的位置
		ring.Nodes[hash] = node
	}
}

func (ring *ConsistentHashRing) AddRing(node *Node) {
	ring.Mutex.Lock()
	defer ring.Mutex.Unlock()

	// 使用节点的 ID 作为 key 添加到哈希环中
	nodeID := node.ID // 假设 Node ID 已经生成
	ring.Nodes[nodeID] = node

	// 更新 SortedHashes 列表并保持排序
	ring.SortedHashes = append(ring.SortedHashes, nodeID)
	sort.Slice(ring.SortedHashes, func(i, j int) bool { return ring.SortedHashes[i] < ring.SortedHashes[j] })

	// 更新前驱和后继关系（确保环结构的完整性）
	ring.UpdatePredecessorsAndSuccessors()

	// 打印哈希环中的节点及其前驱和后继 ID
	fmt.Println("Current nodes in the ring:")
	for _, hash := range ring.SortedHashes {
		node := ring.Nodes[hash]
		fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
		if node.PredecessorID != 0 {
			fmt.Printf("  Predecessor ID=%d\n", node.PredecessorID)
		} else {
			fmt.Println("  Predecessor: None")
		}
		if node.SuccessorID != 0 {
			fmt.Printf("  Successor ID=%d\n", node.SuccessorID)
		} else {
			fmt.Println("  Successor: None")
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

	// TODO: 更新文件
	// 查找后继节点中哪些文件的后继属于自己的前驱到自己这一区间

	// 像后继节点发起文件请求，把对应的文件保存到自己的hydfs中，
}