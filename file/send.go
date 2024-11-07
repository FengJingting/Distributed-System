package file

import (
	"fmt"
	"net"

	//"time"
	"io/ioutil"
	"mp3/cassandra"
	"mp3/utils"
)

// super parematers
const (
	RingLength uint64 = 1 << 32
	DfsDir            = "./files/hydfs"
	LocalDir          = "./files/local"
)

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
	// find
	for _, node := range cassandra.Memberlist["alive"] {
		if node.Predecessor != nil && uint64(node.Predecessor.ID) < hashValue && uint64(node.ID) > hashValue {
			return &node
		}
	}
	return nil
}

// Send a file to a node (for create and append)
func sendFile(node cassandra.Node, filename string, content []byte) error {
	address := string(node.IP) + ":" + node.Port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("connection error: %s, %v", address, err)
	}
	defer conn.Close()

	// 待完成：以什么格式传输要写的content，本地收到之后应该如何创建
	message := fmt.Sprintf("CREATE %s %s", filename, content)
	_, err = conn.Write([]byte(message))
	return err
}

// Fetch a file from a server
func fetchFile(node cassandra.Node, filename string) ([]byte, error) {
	address := string(node.IP) + ":" + node.Port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// Protocol: "GET filename"
	message := fmt.Sprintf("GET %s", filename)
	_, err = conn.Write([]byte(message))
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}

	response, err := ioutil.ReadAll(conn)
	return response, err
}

// Send append content to a server
func sendAppend(node cassandra.Node, filename string, content []byte) error {
	address := string(node.IP) + ":" + node.Port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}
	defer conn.Close()

	// Protocol: "APPEND filename content"
	message := fmt.Sprintf("APPEND %s %s", filename, content)
	_, err = conn.Write([]byte(message))
	return err
}

// ---------------------------Basic file operations---------------------
// Create
func Create(localFilename, hyDFSFilename string) error {
	content, err := ioutil.ReadFile(localFilename)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)
	send1 := sendFile(*server, hyDFSFilename, content)
	send2 := sendFile(*server.Successor, hyDFSFilename, content)
	send3 := sendFile(*server.Successor.Successor, hyDFSFilename, content)
	if send1 != nil || send2 != nil || send3 != nil {
		return fmt.Errorf("sendFile errors: send1: %v, send2: %v, send3: %v", send1, send2, send3)
	} else {
		return nil
	}
}

// Get (fetch)
func Get(hyDFSFilename, localFilename string) error {
	server := getTargetServer(hyDFSFilename)
	content, err := fetchFile(*server, hyDFSFilename)
	if err != nil {
		return fmt.Errorf("error fetching file: %v", err)
	}
	localFilepath := LocalDir + localFilename
	return ioutil.WriteFile(localFilepath, content, 0644)
}

// Append
func Append(localFilename, hyDFSFilename string) error {
	content, err := ioutil.ReadFile(localFilename)
	if err != nil {
		return fmt.Errorf("error reading local file: %v", err)
	}
	server := getTargetServer(hyDFSFilename)
	append1 := sendAppend(*server, hyDFSFilename, content)
	append2 := sendAppend(*server.Successor, hyDFSFilename, content)
	append3 := sendAppend(*server.Successor.Successor, hyDFSFilename, content)
	if append1 != nil || append2 != nil || append3 != nil {
		return fmt.Errorf("sendFile errors: append1: %v, append2: %v, append3: %v", append1, append2, append3)
	} else {
		return nil
	}
}

func Merge() {
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
