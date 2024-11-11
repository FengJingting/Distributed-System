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

// InitConfig reads and parses the JSON configuration file
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

	// Assign configuration values to global variables
	Domain = config.Domain
	FilePort = config.FilePort
	MemberPort = config.MemberPort
	Introducer = config.Introducer
	// fmt.Print(Domain, Port, Introducer)

	// Initialize the hash ring
	Ring = NewConsistentHashRing()
}

// Shuffle function randomly shuffles the list of nodes
func Shuffle(nodes []Node) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

// Initialize consistent hash ring
func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		Nodes:        make(map[uint64]*Node), // Use `uint64` and `*Node`
		SortedHashes: []uint64{},             // Use `uint64`
	}
}

// UpdatePredecessorsAndSuccessors updates all nodes' predecessors and successors
func (ring *ConsistentHashRing) UpdatePredecessorsAndSuccessors() {
	n := len(ring.SortedHashes)
	for i, hash := range ring.SortedHashes {
		node := ring.Nodes[hash]

		// Predecessor node: if i == 0, the predecessor is the last node's ID; otherwise, the predecessor is the previous node's ID
		if i == 0 {
			node.PredecessorID = ring.Nodes[ring.SortedHashes[n-1]].ID
		} else {
			node.PredecessorID = ring.Nodes[ring.SortedHashes[i-1]].ID
		}

		// Successor node: if i == n-1, the successor is the first node's ID; otherwise, the successor is the next node's ID
		if i == n-1 {
			node.SuccessorID = ring.Nodes[ring.SortedHashes[0]].ID
		} else {
			node.SuccessorID = ring.Nodes[ring.SortedHashes[i+1]].ID
		}

		// Update node's position in the ring
		ring.Nodes[hash] = node
	}
}

func (ring *ConsistentHashRing) AddRing(node *Node) {
	ring.Mutex.Lock()
	defer ring.Mutex.Unlock()

	// Use the node's ID as a key to add it to the hash ring
	nodeID := node.ID // Assuming Node ID is already generated
	ring.Nodes[nodeID] = node

	// Update SortedHashes list and keep it sorted
	ring.SortedHashes = append(ring.SortedHashes, nodeID)
	sort.Slice(ring.SortedHashes, func(i, j int) bool { return ring.SortedHashes[i] < ring.SortedHashes[j] })

	// Update predecessors and successors (ensuring ring structure integrity)
	ring.UpdatePredecessorsAndSuccessors()

	// Print the nodes in the hash ring and their predecessor and successor IDs
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

// RemoveNode removes a node from the consistent hash ring
func (ring *ConsistentHashRing) RemoveNode(nodeID uint64) {
	ring.Mutex.Lock()
	defer ring.Mutex.Unlock()

	// Check if the node exists
	_, exists := ring.Nodes[nodeID]
	if !exists {
		fmt.Printf("Node with ID %d not found in the ring.\n", nodeID)
		return
	}

	// Delete the node from the Nodes map
	delete(ring.Nodes, nodeID)

	// Find and remove the node's hash value from the SortedHashes list
	for i, hash := range ring.SortedHashes {
		if hash == nodeID {
			ring.SortedHashes = append(ring.SortedHashes[:i], ring.SortedHashes[i+1:]...)
			break
		}
	}

	// Update all nodes' predecessors and successors
	ring.UpdatePredecessorsAndSuccessors()
	fmt.Printf("Node with ID %d removed from the ring.\n", nodeID)

	// TODO: Update files
	// Find which files' successors in the successor node belong to the interval from its predecessor to itself

	// Initiate a file request to the successor node and save the corresponding files in its hydfs.
}
