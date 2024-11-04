package utils

import (
    "encoding/json"
    // "fmt"
    "io/ioutil"
    "log"
    "sync"
)

// Node 结构体
type Node struct {
	ID          uint64     `json:"id"`
	IP          string  `json:"domain"`  
	Port        string  `json:"port"`  
	Successor   *Node   `json:"successor,omitempty"`
	Predecessor *Node   `json:"predecessor,omitempty"`
	Timestamp   int     `json:"timestamp"`
}

// Config 结构体
type Config struct {
    Domain     string                   `json:"domain"`
    Port       string                   `json:"port"`
    Introducer string                   `json:"introducer"`
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
    Port        string
    CountMutex  sync.Mutex
    Memberlist  = map[string][]Node{
        "alive":   {},
        "failed":  {},
        "suspect": {},
        "leave":   {},
    }
)

var Ring *ConsistentHashRing

// 初始化一致性哈希环
func NewConsistentHashRing() *ConsistentHashRing {
    return &ConsistentHashRing{
        Nodes:        make(map[uint64]*Node), // 使用 `uint64` 和 `*Node`
        SortedHashes: []uint64{},             // 使用 `uint64`
    }
}

// initConfig 读取并解析 JSON 配置文件
func InitConfig() {
    var config Config
    data, err := ioutil.ReadFile("config.json")
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
