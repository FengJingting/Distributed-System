package cassandra

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net"
	//"time"
)

// ---------------------------Basic file operations---------------------
// Create
func create(localFilename, hyDFSFilename string) error {
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
func get(hyDFSFilename, localFilename string) error {
	server := getTargetServer(hyDFSFilename)
	content, err := fetchFile(*server, hyDFSFilename)
	if err != nil {
		return fmt.Errorf("error fetching file: %v", err)
	}
	return ioutil.WriteFile(localFilename, content, 0644)
}

// Append
func appendFile(localFilename, hyDFSFilename string) error {
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
