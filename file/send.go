package file

//_________________________cache__________________________________
import (
	"fmt"
	"net"

	lru "github.com/hashicorp/golang-lru" // LRU Cache library

	//"time"
	"bufio"
	"io/ioutil"
	"log"
	"mp3/cassandra"
	"mp3/utils"
	"strconv"
	"strings"
	"sync"
	// "net"
)

// super parematers
const (
	RingLength uint64 = 1 << 32
	DfsDir            = "./files/hydfs/"
	LocalDir          = "./files/local/"
	CacheSize         = 100 // Set the cache size
	N                 = 3   // 总副本数
	W                 = 2   // 写 Quorum
	R                 = 2   // 读 Quorum
)

// Cache for storing file contents
var fileCache *lru.Cache

func Init() {
	// Initialize the LRU cache with the specified size
	cache, err := lru.New(CacheSize)
	if err != nil {
		fmt.Println("Error initializing LRU cache:", err)
		return
	}
	fileCache = cache
}

// Function to print contents of the cache
func PrintCacheContents() {
	fmt.Println("Cache contents:")

	// Get keys and print each key-value pair in the cache
	for _, key := range fileCache.Keys() {
		if value, found := fileCache.Get(key); found {
			fmt.Printf("Key: %v, Value: %v\n", key, value)
		}
	}
	fmt.Println("End of cache contents.")
}

// ___________________________cache________________________________
// ----------------------node---------------------
// Node operator
func AddNode(node cassandra.Node) {
	// 加锁
	cassandra.CountMutex.Lock()
	defer cassandra.CountMutex.Unlock()

	// 向 "alive" 状态的列表中添加节点
	cassandra.Memberlist["alive"] = append(cassandra.Memberlist["alive"], node)
}

func RemoveNode(nodeID uint64) {
	// 加锁
	cassandra.CountMutex.Lock()
	defer cassandra.CountMutex.Unlock()

	// 遍历 "alive" 状态列表，寻找并删除指定节点
	for i, node := range cassandra.Memberlist["alive"] {
		if node.ID == nodeID {
			// 找到节点，移除它
			cassandra.Memberlist["alive"] = append(cassandra.Memberlist["alive"][:i], cassandra.Memberlist["alive"][i+1:]...)
			break
		}
	}
}

// ----------------Helper functions------------------
// Find target node based on consistent hashing
func getTargetServer(filename string) *cassandra.Node {
	hashValue := utils.Hash(filename) % RingLength

	// List of nodes sorted by their ID (hashes in the ring)
	nodes := cassandra.Memberlist["alive"]

	// Check if only one node is alive, in which case it handles all requests
	if len(nodes) == 1 {
		return &nodes[0]
	}

	for _, node := range nodes {
		// Case 1: Normal range where hash falls between predecessor and current node
		if node.Predecessor != nil &&
			((uint64(node.Predecessor.ID) < hashValue && uint64(node.ID) >= hashValue) ||
				(uint64(node.Predecessor.ID) > uint64(node.ID) && (hashValue >= uint64(node.Predecessor.ID) || hashValue < uint64(node.ID)))) {
			return cassandra.Ring.Nodes[node.ID]
		}
	}

	// Wraparound case: If hash does not fit between any predecessors and IDs,
	// it belongs to the first node in the sorted list
	return cassandra.Ring.Nodes[nodes[0].ID]
}

func FetchFileWithTimestamp(node cassandra.Node, filename string) ([]byte, int64, error) {
	address := node.IP + ":" + cassandra.FilePort
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, 0, fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// 发送 GET 请求
	message := fmt.Sprintf("GET %s", filename)
	_, err = conn.Write([]byte(message + "\n"))
	if err != nil {
		return nil, 0, fmt.Errorf("error sending request: %v", err)
	}

	// 接收内容和时间戳
	reader := bufio.NewReader(conn)
	timestampStr, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, fmt.Errorf("error reading timestamp: %v", err)
	}
	timestampStr = strings.TrimSpace(timestampStr)
	timestamp, _ := strconv.ParseInt(timestampStr, 10, 64)

	content, err := ioutil.ReadAll(reader)
	return content, timestamp, err
}

// Send a file to a node (for create and append)
func SendFile(node cassandra.Node, filename string, content []byte) error {
	fmt.Println("-----------send_SendFile-------------")
	address := node.IP + ":" + cassandra.FilePort
	fmt.Println("address:",address)
	conn, err := net.Dial("tcp", address)
	fmt.Println("conn",conn)
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// 构造并发送 CREATE 请求
	fileSize := len(content)
	message := fmt.Sprintf("CREATE %s\n%d\n%s", filename, fileSize, content)
	fmt.Println("message",message)
	_, err = conn.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("error sending file: %v", err)
	}

	// 读取确认消息
	reader := bufio.NewReader(conn)
	ack, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(ack) != "OK" {
		return fmt.Errorf("error confirming file creation on %s: %v", address, err)
	}

	fmt.Printf("File %s created successfully on %s\n", filename, address)
	return nil
}

// Fetch a file from a server
func FetchFile(node cassandra.Node, filename string) ([]byte, error) {
	//_________________________cache__________________________________
	// Check if the file is already in cache
	// PrintCacheContents()
	if content, found := fileCache.Get(filename); found {
		fmt.Println("File found in cache")
		return content.([]byte), nil
	}
	//___________________________cache________________________________
	address := string(node.IP) + ":" + cassandra.FilePort //":9090"
	fmt.Println("address:", address)
	conn, err := net.Dial("tcp", address)
	fmt.Println("conn:", conn)
	if err != nil {
		return nil, fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// Protocol: "GET filename"
	message := fmt.Sprintf("GET %s\n", filename)
	fmt.Println("message:", message)
	_, err = conn.Write([]byte(message))
	fmt.Println("write")
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}

	response, err := ioutil.ReadAll(conn)
	//__________________________add to cache________________________________
	// Add fetched file content to cache
	fileCache.Add(filename, response)
	fmt.Println("File added to cache:", filename)
	//__________________________end add to cache____________________________
	return response, err
}

// Send append content to a server
func sendAppend(node cassandra.Node, filename string, content []byte) error {
	address := node.IP + ":" + cassandra.FilePort
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// 构造并发送 APPEND 请求
	fileSize := len(content)
	message := fmt.Sprintf("APPEND %s\n%d\n%s", filename, fileSize, content)
	_, err = conn.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("error sending append request: %v", err)
	}

	// 读取确认消息
	reader := bufio.NewReader(conn)
	ack, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(ack) != "OK" {
		return fmt.Errorf("error confirming append on %s: %v", address, err)
	}

	fmt.Printf("Content appended to file %s on %s successfully\n", filename, address)
	return nil
}

// ---------------------------Basic file operations---------------------
// Create
func Create(localFilename, hyDFSFilename string, continueAfterQuorum bool) error {
	fmt.Println("------------send_create-------------")
	localFilepath := LocalDir + localFilename
	content, err := ioutil.ReadFile(localFilepath)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)
	fmt.Println("server",server)
	successCount := 0
	servers := []*cassandra.Node{server, server.Successor, server.Successor.Successor}
	fmt.Println(servers)
	for _, srv := range servers {
		if srv == nil {
			continue
		}
		if err := SendFile(*srv, hyDFSFilename, content); err == nil {
			successCount++
		}

		// 如果达到 Quorum，且不需要继续写入剩余副本，则退出循环
		if successCount >= W && !continueAfterQuorum {
			fmt.Println("Write quorum reached")
			return nil
		}
	}

	if successCount >= W {
		fmt.Println("Write quorum reached after writing all nodes")
		return nil
	}
	return fmt.Errorf("write quorum not reached, only %d nodes succeeded", successCount)
}

// func Create(localFilename, hyDFSFilename string) error {
// 	fmt.Println("------------send_create-------------")
// 	localFilepath := LocalDir + localFilename
// 	content, err := ioutil.ReadFile(localFilepath)
// 	if err != nil {
// 		return fmt.Errorf("error reading local file: %v", err)
// 	}
// 	server := getTargetServer(hyDFSFilename)

// 	successCount := 0
// 	servers := []*cassandra.Node{server, server.Successor, server.Successor.Successor}

// 	for _, srv := range servers {
// 		if srv == nil {
// 			continue
// 		}
// 		if err := SendFile(*srv, hyDFSFilename, content); err == nil {
// 			successCount++
// 		}
// 		if successCount >= W {
// 			fmt.Println("Write quorum reached")
// 			return nil
// 		}
// 	}
// 	return fmt.Errorf("write quorum not reached, only %d nodes succeeded", successCount)
// }

// Get (fetch)
func Get(hyDFSFilename, localFilename string) error {
	fmt.Println("------------send_get-------------")
	server := getTargetServer(hyDFSFilename)

	var latestContent []byte
	var latestTimestamp int64
	successCount := 0

	servers := []*cassandra.Node{server, server.Successor, server.Successor.Successor}

	for _, srv := range servers {
		if srv == nil {
			continue
		}
		content, timestamp, err := FetchFileWithTimestamp(*srv, hyDFSFilename)
		if err == nil {
			successCount++
			if timestamp > latestTimestamp {
				latestContent = content
				latestTimestamp = timestamp
			}
		}
		if successCount >= R {
			fmt.Println("Read quorum reached")
			localFilepath := LocalDir + localFilename
			return ioutil.WriteFile(localFilepath, latestContent, 0644)
		}
	}
	return fmt.Errorf("read quorum not reached, only %d nodes succeeded", successCount)
}

// Append
func Append(localFilename, hyDFSFilename string, continueAfterQuorum bool) error {
	fmt.Println("------------send_append-------------")
	localFilepath := LocalDir + localFilename
	content, err := ioutil.ReadFile(localFilepath)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)

	successCount := 0
	servers := []*cassandra.Node{server, server.Successor, server.Successor.Successor}

	for _, srv := range servers {
		if srv == nil {
			continue
		}
		if err := sendAppend(*srv, hyDFSFilename, content); err == nil {
			successCount++
		}

		// 如果达到 Quorum 且不需要继续写入剩余副本，则退出循环
		if successCount >= W && !continueAfterQuorum {
			fmt.Println("Append quorum reached")
			return nil
		}
	}

	// 确认 Quorum 达成或写入完成
	if successCount >= W {
		fmt.Println("Append quorum reached after writing all nodes")
		return nil
	}
	return fmt.Errorf("append quorum not reached, only %d nodes succeeded", successCount)
}

// func Append(localFilename, hyDFSFilename string) error {
// 	fmt.Println("------------send_append-------------")
// 	localFilepath := LocalDir + localFilename
// 	content, err := ioutil.ReadFile(localFilepath)
// 	if err != nil {
// 		return fmt.Errorf("error reading local file: %v", err)
// 	}
// 	server := getTargetServer(hyDFSFilename)

// 	successCount := 0
// 	servers := []*cassandra.Node{server, server.Successor, server.Successor.Successor}

// 	for _, srv := range servers {
// 		if srv == nil {
// 			continue
// 		}
// 		if err := sendAppend(*srv, hyDFSFilename, content); err == nil {
// 			successCount++
// 		}
// 		if successCount >= W {
// 			fmt.Println("Write quorum reached")
// 			return nil
// 		}
// 	}
// 	return fmt.Errorf("write quorum not reached, only %d nodes succeeded", successCount)
// }

func Merge() {
	fmt.Println("------------send_merge-------------")
	// fmt.Print("请输入文件名: ")
	// reader := bufio.NewReader(os.Stdin)
	// filename, _ := reader.ReadString('\n')
	// filename = strings.TrimSpace(filename) // 去除换行符和空白

	// // 输出或处理输入的文件名
	// fmt.Printf("你输入的文件名是: %s\n", filename)
	// TODO: merge操作
}

// multiappend 函数：提示用户输入文件名、虚拟机地址和本地文件名，并进行并发的追加操作
func MultiAppend(filename string, vmAddresses []string, localFilenames []string) error {
	// 检查输入是否匹配
	if len(vmAddresses) != len(localFilenames) {
		return fmt.Errorf("number of VM addresses and local filenames must be the same")
	}

	// 创建等待组，用于等待所有并发操作完成
	var wg sync.WaitGroup

	// 遍历虚拟机地址和对应的本地文件名
	for i, addr := range vmAddresses {
		localFilename := LocalDir + localFilenames[i]
		fmt.Println("hello", localFilename)

		// 读取本地文件内容
		content, err := ioutil.ReadFile(localFilename)
		if err != nil {
			log.Printf("Error reading file %s: %v", localFilename, err)
			continue
		}

		// 创建节点信息
		node := cassandra.Node{IP: addr} // 假设所有节点使用相同端口

		// 增加等待组计数器
		wg.Add(1)

		filepath := filename
		// 并发执行 append 操作
		go func(n cassandra.Node, f string, c []byte) {
			defer wg.Done()
			err := sendAppend(n, f, c)
			if err != nil {
				fmt.Printf("Failed to append to %s on %s: %v\n", f, n.IP, err)
			} else {
				fmt.Printf("Successfully appended to %s on %s\n", f, n.IP)
			}
		}(node, filepath, content)
	}

	// 等待所有并发任务完成
	wg.Wait()
	fmt.Println("Multi-append operation completed.")
	return nil
}
