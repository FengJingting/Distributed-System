package memberlist

import (
    "fmt"
    // "strings"
    "mp3/utils"
    "io/ioutil"
    "mp3/cassandra"
    "mp3/file"
    "strconv"
    "os"
    "time"
    "sync"
)

func findNodeDetailsByIP(ip string) (string, string, string, bool) {
    // Iterate through each node in the memberlist["alive"] list
    for _, node := range cassandra.Memberlist["alive"] {
        if node.IP == ip {
            // Return port, ID, timestamp (converted to string), last parameter indicates if found
            return node.Port, strconv.FormatUint(node.ID, 10), strconv.Itoa(node.Timestamp), true
        }
    }
    // If not found, return empty strings and false
    return "", "", "", false
}

func List_mem_ids() {
    fmt.Println("list_mem_ids")

    // Lock to ensure thread safety
    cassandra.Ring.Mutex.Lock()
    defer cassandra.Ring.Mutex.Unlock()

    // Iterate through SortedHashes to get sorted nodes
    for _, nodeHash := range cassandra.Ring.SortedHashes {
        node := cassandra.Ring.Nodes[nodeHash]

        // Find the status of the node
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

        // Print the status, ID, IP, and port of the node
        fmt.Printf("Status: %s, Node ID: %d, IP: %s, Port: %s\n", nodeStatus, nodeHash, node.IP, node.Port)

        // Retrieve and print predecessor node information
        if predecessorNode, ok := cassandra.Ring.Nodes[node.PredecessorID]; ok && node.PredecessorID != 0 {
            fmt.Printf("  Predecessor: ID=%d, IP=%s, Port=%s\n", predecessorNode.ID, predecessorNode.IP, predecessorNode.Port)
        } else {
            fmt.Println("  Predecessor: None")
        }

        // Retrieve and print successor node information
        if successorNode, ok := cassandra.Ring.Nodes[node.SuccessorID]; ok && node.SuccessorID != 0 {
            fmt.Printf("  Successor: ID=%d, IP=%s, Port=%s\n", successorNode.ID, successorNode.IP, successorNode.Port)
        } else {
            fmt.Println("  Successor: None")
        }
    }
}

func List_self() {
    fmt.Println("list_self function called")

    // Ensure safe access by locking
    cassandra.Ring.Mutex.Lock()
    defer cassandra.Ring.Mutex.Unlock()

    found := false
    for state, nodes := range cassandra.Memberlist {
        for _, node := range nodes {
            // Check if the current node matches the IP address of this node
            if node.IP == cassandra.Domain {
                fmt.Printf("Found matching node in state '%s':\n", state)
                fmt.Printf("  Node ID: %d\n  IP: %s\n  Port: %s\n  Timestamp: %d\n", 
                           node.ID, node.IP, node.Port, node.Timestamp)

                // Retrieve ringNode based on node's ID
                ringNode := cassandra.Ring.Nodes[node.ID]
                if ringNode != nil {
                    // Retrieve and print Predecessor information
                    if pred, ok := cassandra.Ring.Nodes[ringNode.PredecessorID]; ok && ringNode.PredecessorID != 0 {
                        fmt.Printf("  Predecessor: ID=%d, IP=%s, Port=%s\n", 
                                   pred.ID, pred.IP, pred.Port)
                    } else {
                        fmt.Println("  Predecessor: None")
                    }

                    // Retrieve and print Successor information
                    if succ, ok := cassandra.Ring.Nodes[ringNode.SuccessorID]; ok && ringNode.SuccessorID != 0 {
                        fmt.Printf("  Successor: ID=%d, IP=%s, Port=%s\n", 
                                   succ.ID, succ.IP, succ.Port)
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
    // Clear all files in the `hydfs` directory
    directory := "./files/hydfs/"
    files, err := ioutil.ReadDir(directory)
    if err != nil {
        fmt.Println("Error reading hydfs directory:", err)
    } else {
        for _, file := range files {
            if !file.IsDir() {
                filePath := directory + file.Name()
                err := os.Remove(filePath)
                if err != nil {
                    fmt.Printf("Error deleting file %s: %v\n", file.Name(), err)
                } else {
                    fmt.Printf("Cleared file: %s\n", file.Name())
                }
            }
        }
    }
    // Send join message to the Introducer
    message := "join+" + cassandra.Domain
    send(cassandra.Introducer, cassandra.MemberPort, message)
}

// Add a global mutex in the cassandra package
var changeStatusMutex sync.Mutex

// Modify changeStatus function
func changeStatus(newStatus, nodeID string) {
    // Use a global mutex to ensure atomicity of the change operation
    changeStatusMutex.Lock()
    defer changeStatusMutex.Unlock()

    var nodeToMove cassandra.Node
    var found bool

    // Find the current status of the node in `Memberlist` and record its status
    for status, nodes := range cassandra.Memberlist {
        for i, node := range nodes {
            if fmt.Sprint(node.ID) == nodeID {
                nodeToMove = node

                // Remove the node from the current status list
                cassandra.Memberlist[status] = append(nodes[:i], nodes[i+1:]...)
                found = true
                break
            }
        }
        if found {
            break
        }
    }

    // If the node is found, add it to the new status list and remove it from the hash ring
    if found {
        cassandra.Memberlist[newStatus] = append(cassandra.Memberlist[newStatus], nodeToMove)
        cassandra.Ring.RemoveNode(nodeToMove.ID)

        // If newStatus is "failed," perform replica check
        if newStatus == "failed" {
            performReplicaCheck()
        }

        // Print the updated `Memberlist` for debugging
        List_mem_ids() // For debugging, print the current state of `Memberlist`
    } else {
        fmt.Printf("Node with ID %s not found in current memberlist.\n", nodeID)
    }
}

func performReplicaCheck() {
    fmt.Println("Performing replica check on successor nodes...")

    // List files from DfsDir and perform the replica check on each file
    files, err := ioutil.ReadDir(file.DfsDir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
        return
    }

    for _, fileEntry := range files {
        if !fileEntry.IsDir() { // Only process files
            fileName := fileEntry.Name()
            replicaCount := countReplicas(fileName)
            fmt.Println("replicaCount", replicaCount)
            if replicaCount < 3 {
                fmt.Printf("Insufficient replicas for file %s: found %d replicas, expected 3\n", fileName, replicaCount)
                // Replicate the file to maintain three replicas
                startTime := time.Now()
                replicateFileToSuccessors(fileName, 3-replicaCount)
                duration := time.Since(startTime)
                fmt.Printf("Time taken to replicate file %s: %v\n", fileName, duration)
            }
        }
    }
}

func countReplicas(fileName string) int {
    count := 0

    // Get the starting node based on the current domain and port hash
    current, ok := cassandra.Ring.Nodes[utils.Hash(cassandra.Domain+cassandra.MemberPort)]
    if !ok {
        fmt.Println("Error: Domain not found in ring nodes")
        return 0
    }
    fmt.Println("Domain found", current)

    // Check up to 3 replicas in the ring
    for i := 0; i < 3; i++ {
        if current == nil {
            break
        }
        fmt.Println(current)
        
        if fileExistsOnNode(current, fileName) {
            fmt.Println("count ++")
            count++
        }

        // Move to the next successor using SuccessorID
        if next, ok := cassandra.Ring.Nodes[current.SuccessorID]; ok && current.SuccessorID != 0 {
            current = next
        } else {
            // No more successors to check
            break
        }
    }
    return count
}

// Helper function to check if a file exists on a given node
func fileExistsOnNode(node *cassandra.Node, fileName string) bool {
    content, err := file.FetchFileReplica(*node, fileName)
    return err == nil && len(content) > 0
}

func replicateFileToSuccessors(fileName string, replicasNeeded int) {
    content, err := ioutil.ReadFile(file.DfsDir + fileName)
    if err != nil {
        fmt.Println("Error reading local file:", err)
        return
    }

    // Get the starting node based on the current domain and port hash
    current, ok := cassandra.Ring.Nodes[utils.Hash(cassandra.Domain+cassandra.MemberPort)]
    if !ok {
        fmt.Println("Error: Domain not found in ring nodes")
        return
    }

    // Loop to replicate to successors
    for replicasNeeded > 0 && current != nil {
        if !fileExistsOnNode(current, fileName) {
            // Attempt to replicate the file
            err := file.SendFile(*current, fileName, content)
            if err != nil {
                fmt.Printf("Error replicating file %s to node %d: %v\n", fileName, current.ID, err)
            } else {
                replicasNeeded--
                fmt.Printf("File %s replicated to node %d\n", fileName, current.ID)
            }
        }

        // Move to the next successor using SuccessorID
        if next, ok := cassandra.Ring.Nodes[current.SuccessorID]; ok && current.SuccessorID != 0 {
            current = next
        } else {
            // No more successors to check
            break
        }
    }

    if replicasNeeded > 0 {
        fmt.Printf("Warning: Only %d replicas created for file %s, not enough nodes available.\n", 3-replicasNeeded, fileName)
    }
}

func Write_to_log() {
    fmt.Println("Memberlist and Ring successfully written to log.")
}
