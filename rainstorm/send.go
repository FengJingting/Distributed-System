package rainstorm

import (
	"fmt"
	"mp3/cassandra"
	"mp3/file"
	"net"
	"sort"
	"strconv"
	"strings"
)

// Create a dictionary Tasks to store server IPs and corresponding task counts
var Tasks = make(map[string]int)
var TasksList = make(map[string][]string)

// Initialize the global variable Tasks from  Memberlist["alive"] 
func InitializeTasks() error {
	// Get active nodes from  Memberlist["alive"] 
	aliveNodes := cassandra.Memberlist["alive"]
	if len(aliveNodes) == 0 {
		return fmt.Errorf("no alive nodes available to initialize tasks")
	}

	// Update the global variable  Tasks  with each node's IP and task count
	for _, node := range aliveNodes {
		Tasks[node.IP] = 0
	}

	// Print the  Tasks  dictionary (optional, for debugging purposes)
	// fmt.Println("Updated Tasks dictionary:", Tasks)

	return nil
}

//  SelectTopNodes  retrieves the top  numNodes  active nodes with the fewest tasks from  Tasks 
func SelectTopNodes(numNodes int) ([]cassandra.Node, error) {
	// Return an error if  Tasks  is empty
	if len(Tasks) == 0 {
		return nil, fmt.Errorf("no tasks available to select nodes")
	}

	// Convert the  Tasks  dictionary into a slice
	var nodes []cassandra.Node
	for ip, numTasks := range Tasks {
		// Find the corresponding node by its IP
		for _, node := range cassandra.Memberlist["alive"] {
			if node.IP == ip {
				// Update the node's  NumTasks  with the value from the dictionary
				node.NumTasks = numTasks
				nodes = append(nodes, node)
				break
			}
		}
	}

	// Return an error if no matching nodes are found
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no alive nodes found in Tasks")
	}

	// Sort the nodes by their  NumTasks  in ascending order
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NumTasks < nodes[j].NumTasks
	})

	// Adjust the number of returned nodes if there are fewer than  numNodes 
	if len(nodes) < numNodes {
		numNodes = len(nodes)
	}

	// Return the top  numNodes  nodes with the fewest tasks
	return nodes[:numNodes], nil
}

//  sendTransform  sends a task message to a specified server using TCP
func sendTransform(sourceFile string, startLine int, endLine int, serverIP string) error {
	// Add the "transform" prefix to the message
	fullMessage := "transform" + "+" + sourceFile + "+" + strconv.Itoa(startLine) + "+" + strconv.Itoa(endLine)
	fmt.Println("Message:", fullMessage)

	// Construct the target address using a fixed  StreamPort 
	address := fmt.Sprintf("%s:%s", serverIP, cassandra.StreamPort)
	// fmt.Printf("Sending task to server %s\n", address)

	// Establish a TCP connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to server %s: %v", address, err)
	}
	defer conn.Close()

	// Send the message
	_, err = conn.Write([]byte(fullMessage))
	if err != nil {
		return fmt.Errorf("failed to send message to server %s: %v", address, err)
	}

	// fmt.Printf("Message successfully sent to server %s\n", address)
	return nil
}

//  sendFilterRequest  sends a  Filter1  request
func sendFilterRequest(prefix string, targetIP string, targetPort string, filename string, lineNumber int, line string, sign string) error {
	// Build the message dynamically using the prefix
	message := prefix + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + line + "+" + sign
	//fmt.Println("send", message)

	// Construct the target address
	address := fmt.Sprintf("%s:%s", targetIP, cassandra.StreamPort)

	// Establish a TCP connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", address, err)
	}
	defer conn.Close()

	// Send the message
	_, err = conn.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("failed to send %s message to %s: %v", prefix, address, err)
	}
	Tasks[targetIP] += 1
	TasksList[targetIP] = append(TasksList[targetIP], filename+"+"+strconv.Itoa(lineNumber)+"+"+prefix+"+"+line+"+"+sign)
	// Save the result to the log file
	logString, _ := ConvertToLogString(TasksList)
	if err := AppendResultToLog(logString, logFilename); err != nil {
		fmt.Printf("Failed to append result to log file: %v\n", err)
	}

	// fmt.Printf("%s request sent to %s successfully: %s\n", prefix, address, message)
	return nil
}

//  AppendLog  appends the provided content directly
func AppendLog(content []byte, hyDFSFilename string, continueAfterQuorum bool) error {
	// fmt.Println("------------send_append_log-------------")
	hyDFSFilename = strings.TrimSpace(hyDFSFilename)

	// Retrieve the target server for the file
	server := file.GetTargetServer(hyDFSFilename)
	if server == nil {
		return fmt.Errorf("error finding target server for filename: %v", hyDFSFilename)
	}

	// Define servers for the append operation
	servers := []*cassandra.Node{server}

	// Retrieve successors using  SuccessorID  fields
	if successor1, ok := cassandra.Ring.Nodes[server.SuccessorID]; ok {
		servers = append(servers, successor1)
		if successor2, ok := cassandra.Ring.Nodes[successor1.SuccessorID]; ok {
			servers = append(servers, successor2)
		}
	}
	// fmt.Println("Servers for appending:", servers)

	// Initialize success count and iterate over servers
	successCount := 0
	for _, srv := range servers {
		if srv == nil {
			continue
		}
		// Attempt to append the content to the current server
		// fmt.Println("content", hyDFSFilename)
		if err := file.SendAppend(*srv, hyDFSFilename, content); err == nil {
			successCount++
		}

		// Exit early if quorum is reached and  continueAfterQuorum  is false
		if successCount >= file.W && !continueAfterQuorum {
			// fmt.Println("Append quorum reached")
			return nil
		}
	}

	// Perform a final quorum check
	if successCount >= file.W {
		// fmt.Println("Append quorum reached after writing all nodes")
		return nil
	}
	return fmt.Errorf("append quorum not reached, only %d nodes succeeded", successCount)
}