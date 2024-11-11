package cassandra

import (
    // "encoding/json"
    // "fmt"
    // "io/ioutil"
    // "log"
    "sync"
)

var Ring *ConsistentHashRing

// Config struct
type Config struct {
    Domain     string                   `json:"domain"`
    FilePort   string                   `json:"fileport"`
    MemberPort string                   `json:"memberport"`
    Introducer string                   `json:"introducer"`
}

// Node struct
type Node struct {
	ID          uint64 `json:"id"`
	IP          string `json:"domain"`
	Port        string `json:"port"`
	SuccessorID uint64 `json:"successor"`  // Store the successor node's ID
	PredecessorID uint64 `json:"predecessor"`  // Store the predecessor node's ID
	Timestamp   int    `json:"timestamp"`
}

// Define a struct to represent the consistent hash ring
type ConsistentHashRing struct {
    Nodes        map[uint64]*Node // Use `uint64` as the key type, consistent with the return type of the `hash` function
    SortedHashes []uint64         // Use `uint64` type
    Mutex        sync.Mutex
}

// Global variables
var (
    Introducer  string
    Domain      string
    MemberPort  string
    FilePort    string
    CountMutex  sync.Mutex
    SelfSuspected bool = false
    Memberlist  = map[string][]Node{
        "alive":   {},
        "failed":  {},
        "suspect": {},
        "leave":   {},
    }
)
