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
	"sort"
	"math/rand"
	"encoding/json"
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
    Port = config.Port
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

// // AddRing 将新节点添加到一致性哈希环中
// func (ring *ConsistentHashRing) AddRing(node *Node) {
//     ring.Mutex.Lock()
//     defer ring.Mutex.Unlock()

//     // 使用节点的 IP 和端口生成唯一的哈希值作为节点位置
//     nodeKey := fmt.Sprintf("%s:%s", node.IP, node.Port)
//     nodeHash := utils.Hash(nodeKey)

//     // 将哈希值和节点指针添加到 Nodes 中
//     ring.Nodes[nodeHash] = node

//     // 将哈希值插入到 SortedHashes 中并保持排序
//     ring.SortedHashes = append(ring.SortedHashes, nodeHash)
//     sort.Slice(ring.SortedHashes, func(i, j int) bool { return ring.SortedHashes[i] < ring.SortedHashes[j] })

//     // 更新前驱和后继
//     ring.updatePredecessorsAndSuccessors()
// 	// 打印哈希环中的节点及其前驱和后继
// 	fmt.Println("Current nodes in the ring:")
// 	for _, hash := range Ring.SortedHashes {
// 		node := Ring.Nodes[hash]
// 		fmt.Printf("Node ID=%d, IP=%s, Port=%s\n", node.ID, node.IP, node.Port)
// 		if node.Predecessor != nil {
// 			fmt.Printf("  Predecessor: ID=%d\n", node.Predecessor.ID)
// 		}
// 		if node.Successor != nil {
// 			fmt.Printf("  Successor: ID=%d\n", node.Successor.ID)
// 		}
// 	}
// 	// broadcastRingUpdate()
// } 

func (ring *ConsistentHashRing) AddRing(node *Node) {
    ring.Mutex.Lock()
    defer ring.Mutex.Unlock()

    // 使用 IP 和端口生成的哈希值（Node ID）作为 key
    nodeID := node.ID  // 假设 Node ID 已经通过 IP 和端口的哈希生成
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

func merge() {
	fmt.Println("merge function called")
    // fmt.Print("请输入文件名: ")
    // reader := bufio.NewReader(os.Stdin)
    // filename, _ := reader.ReadString('\n')
    // filename = strings.TrimSpace(filename) // 去除换行符和空白

    // // 输出或处理输入的文件名
    // fmt.Printf("你输入的文件名是: %s\n", filename)
	// TODO: merge操作
}

// multiappend 函数：提示用户输入文件名、虚拟机地址和本地文件名，并进行并发的追加操作
func multiappend() {
	fmt.Println("multiappend function called")
    // reader := bufio.NewReader(os.Stdin)

    // // 读取目标文件名
    // fmt.Print("请输入目标文件名: ")
    // filename, _ := reader.ReadString('\n')
    // filename = strings.TrimSpace(filename)

    // // 读取虚拟机地址
    // fmt.Print("请输入虚拟机地址 (用空格隔开): ")
    // vmInput, _ := reader.ReadString('\n')
    // vmAddresses := strings.Fields(strings.TrimSpace(vmInput))

    // // 读取本地文件名
    // fmt.Print("请输入本地文件名 (用空格隔开): ")
    // fileInput, _ := reader.ReadString('\n')
    // localFilenames := strings.Fields(strings.TrimSpace(fileInput))

    // // 检查虚拟机地址和本地文件名的数量是否匹配
    // if len(vmAddresses) != len(localFilenames) {
    //     fmt.Println("虚拟机地址与本地文件名数量不匹配。")
    //     return
    // }

   // TODO：完成并发追加
}
