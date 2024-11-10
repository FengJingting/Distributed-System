package file
//_________________________cache__________________________________
import (
	"fmt"
	"net"
	"github.com/hashicorp/golang-lru" // LRU Cache library
	//"time"
	"io/ioutil"
	"mp3/cassandra"
	"mp3/utils"
	// "net"
)

// super parematers
const (
	RingLength uint64 = 1 << 32
	DfsDir            = "./files/hydfs/"
	LocalDir          = "./files/local/"
	CacheSize         = 100 // Set the cache size
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

//___________________________cache________________________________
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
	hashValue := utils.Hash(filename)
	// control region of hash value
	hashValue = hashValue % RingLength
	// fmt.Println(hashValue)
	// find
	for _, node := range cassandra.Memberlist["alive"] {
		if len(cassandra.Memberlist["alive"]) == 1 {
			return &node
		}
		if node.Predecessor != nil && uint64(node.Predecessor.ID) < hashValue && uint64(node.ID) > hashValue {
			return &node
		}
	}
	return nil
}

// Send a file to a node (for create and append)
func sendFile(node cassandra.Node, filename string, content []byte) error {
	fmt.Println("---------send_sendFile-----------")
	address := string(node.IP) + ":" + cassandra.FilePort //":9090"
	fmt.Println("sendFile_address: ", address)
	conn, err := net.Dial("tcp", address)
	fmt.Println("conn:", conn)
	if err != nil {
		return fmt.Errorf("connection error: %s, %v", address, err)
	}
	defer conn.Close()
	fileSize := len(content)
	message := fmt.Sprintf("CREATE %s\n%d\n%s", filename, fileSize, content)
	fmt.Println("message: ", message)
	_, err = conn.Write([]byte(message))
	return err
}

// Fetch a file from a server
func fetchFile(node cassandra.Node, filename string) ([]byte, error) {
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
	address := string(node.IP) + ":" + cassandra.FilePort //":9090"
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// Protocol: "APPEND filename content"
	fileSize := len(content)
	message := fmt.Sprintf("APPEND %s\n%d\n%s", filename, fileSize, content)
	_, err = conn.Write([]byte(message))
	return err
}

// ---------------------------Basic file operations---------------------
// Create
func Create(localFilename, hyDFSFilename string) error {
	fmt.Println("------------send_create-------------")
	localFilepath := LocalDir + localFilename
	fmt.Println("localFilepath: ", localFilepath)
	content, err := ioutil.ReadFile(localFilepath)
	fmt.Println("content: ", content)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)
	fmt.Println("server: ", server)
	send1 := sendFile(*server, hyDFSFilename, content)
	fmt.Println("send1")
	if send1 != nil {
		return fmt.Errorf("sendFile errors: send1: %v", send1)
	} else {
		return nil
	}
	// send2 := sendFile(*server.Successor, hyDFSFilename, content)
	// send3 := sendFile(*server.Successor.Successor, hyDFSFilename, content)
	// if send1 != nil || send2 != nil || send3 != nil {
	// 	return fmt.Errorf("sendFile errors: send1: %v, send2: %v, send3: %v", send1, send2, send3)
	// } else {
	// 	return nil
	// }
}

// Get (fetch)
func Get(hyDFSFilename, localFilename string) error {
	fmt.Println("------------send_get-------------")
	hyDFSFilepath := DfsDir + hyDFSFilename
	fmt.Println("hyDFSFilepath:", hyDFSFilepath)
	server := getTargetServer(hyDFSFilepath)
	fmt.Println("server:", server)
	content, err := fetchFile(*server, hyDFSFilename)
	fmt.Println("content:", content)
	if err != nil {
		return fmt.Errorf("error fetching file: %v", err)
	}
	localFilepath := LocalDir + localFilename
	fmt.Println("localFilepath:", localFilepath)
	return ioutil.WriteFile(localFilepath, content, 0644)
}

// Append
func Append(localFilename, hyDFSFilename string) error {
	fmt.Println("------------send_append-------------")
	localFilepath := LocalDir + localFilename
	fmt.Println("localFilepath:", localFilepath)
	content, err := ioutil.ReadFile(localFilepath)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)
	append1 := sendAppend(*server, hyDFSFilename, content)
	fmt.Println("append1")
	if append1 != nil {
		return fmt.Errorf("sendFile errors: append1: %v", append1)
	} else {
		return nil
	}
	// append2 := sendAppend(*server.Successor, hyDFSFilename, content)
	// append3 := sendAppend(*server.Successor.Successor, hyDFSFilename, content)
	// if append1 != nil || append2 != nil || append3 != nil {
	// 	return fmt.Errorf("sendFile errors: append1: %v, append2: %v, append3: %v", append1, append2, append3)
	// } else {
	// 	return nil
	// }
}

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
func MultiAppend() {
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
