package cassandra

import (
    // "encoding/json"
    // "fmt"
    // "io/ioutil"
    // "log"
    "sync"
)

var Ring *ConsistentHashRing
// Config 结构体
type Config struct {
    Domain     string                   `json:"domain"`
    FilePort   string                   `json:"fileport"`
    MemberPort string                   `json:"memberport"`
    Introducer string                   `json:"introducer"`
}


// Node 结构体
type Node struct {
	ID          uint64 `json:"id"`
	IP          string `json:"domain"`
	Port        string `json:"port"`
	SuccessorID uint64 `json:"successor,omitempty"`  // 改为存储后继节点的 ID
	PredecessorID uint64 `json:"predecessor,omitempty"`  // 改为存储前驱节点的 ID
	Timestamp   int    `json:"timestamp"`
}


// 定义一个结构体来表示一致性哈希环
type ConsistentHashRing struct {
    Nodes        map[uint64]*Node // 使用 `uint64` 作为键类型，与 `hash` 函数的返回类型一致
    SortedHashes []uint64         // 使用 `uint64` 类型
    Mutex        sync.Mutex
}

// 全局变量
var (
    Introducer  string
    Domain      string
    MemberPort  string
    FilePort    string
    CountMutex  sync.Mutex
    Memberlist  = map[string][]Node{
        "alive":   {},
        "failed":  {},
        "suspect": {},
        "leave":   {},
    }
)