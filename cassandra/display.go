package cassandra

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// 命令行输入is，后续的处理函数
// TODO: 测试lsHyDFSfilename函数调用是否正确，规范文件名输入是否需要后缀
func is() {
	fmt.Print("is_ function called: ")
	// fmt.Print("请输入文件名: ")
	// reader := bufio.NewReader(os.Stdin)
	// filename, _ := reader.ReadString('\n')
	// filename = filename[:len(filename)-1] // 去除换行符
	// lsHyDFSfilename(filename)
}

// 命令行输入store，后续的处理函数

func store() {
	fmt.Print("store function called: ")
	// TODO:  list the set of file names (along with their IDs)
	// that are replicated (stored) on HyDFS at this (local) process/VM.
	// This should NOT include files stored on the local file system.
	// Also, print the process/VM’s ID on the ring.
}

// getfromreplica VMaddress HyDFSfilename localfilename:
func getfromreplica() {
	fmt.Print("请输入 VMaddress, HyDFSfilename 和 localfilename（用空格分隔）: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	// 使用空格分割输入内容
	parts := strings.Split(input, " ")
	if len(parts) != 3 {
		fmt.Println("输入格式不正确，请确保输入三个参数并用空格分隔。")
		return
	}

	VMaddress := parts[0]
	HyDFSfilename := parts[1]
	localfilename := parts[2]
	fmt.Printf("VM Address: %s, HyDFS Filename: %s, Local Filename: %s\n", VMaddress, HyDFSfilename, localfilename)

	// TODO: 调用get函数
	// variant of get file to get the file from a particular replica.
	// This should simply fetch the file and not perform any further actions on the file.
	// We will use this on all replicas to check if the files are identical after a merge.
	//Get(hyDFSFilename, localfilename);
}
