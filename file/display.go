package file

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "mp3/utils"
    "mp3/cassandra"
)

// is function for handling lsHyDFSfilename command
func Is() {
    fmt.Print("is_ function called: ")
    fmt.Print("请输入文件名: ")
    reader := bufio.NewReader(os.Stdin)
    filename, _ := reader.ReadString('\n')
    filename = strings.TrimSpace(filename) // Remove newline and trim
    lsHyDFSfilename(filename) 
}

func lsHyDFSfilename(filename string) []string {
    var result []string
    server := getTargetServer(filename)

    for i := 0; i < 3; i++ {
        if server == nil {
            fmt.Println("Server not found.")
            break
        }

        serverAddr := server.IP
        serverID := fmt.Sprintf("%d", server.ID)
        // 这一步卡住了
        content, err := fetchFile(*server, filename)
        if err == nil && content != nil {
            result = append(result, fmt.Sprintf("VM Address: %s, VM ID: %s", serverAddr, serverID))
        } else {
            fmt.Printf("File not found on server: %s (ID: %s)\n", serverAddr, serverID)
        }

        server = server.Successor
    }
    
    fmt.Println("Get_server")
    return result
}

// 测试通过
func Store() {
    fmt.Print("store function called: ")
    // Retrieve local node information based on Domain/IP
    var localNodeID uint64
    for id, node := range cassandra.Ring.Nodes {
        if node.IP == cassandra.Domain {
            localNodeID = id
            break
        }
    }
    fmt.Printf("VM ID on the ring: %d\n", localNodeID)

    directory := "files/hydfs"

    files, err := os.ReadDir(directory)
    if err != nil {
        fmt.Println("Error reading directory:", err)
        return
    }

    for _, file := range files {
        if file.IsDir() {
            continue
        }

        fileName := file.Name()
        fileID := utils.Hash(fileName)

        fmt.Printf("File Name: %s, File ID: %d\n", fileName, fileID)
    }
}

func getServerFromAddress(VMaddress string) (*cassandra.Node, error) {
    for _, node := range cassandra.Ring.Nodes {
        if node.IP == VMaddress {
            return node, nil
        }
    }
    return nil, fmt.Errorf("Server with address %s not found", VMaddress)
}

func Getfromreplica() {
    fmt.Print("请输入 VMaddress, HyDFSfilename 和 localfilename（用空格分隔）: ")
    reader := bufio.NewReader(os.Stdin)
    input, _ := reader.ReadString('\n')
    input = strings.TrimSpace(input)

    parts := strings.Split(input, " ")
    if len(parts) != 3 {
        fmt.Println("输入格式不正确，请确保输入三个参数并用空格分隔。")
        return 
    }

    VMaddress := parts[0]
    HyDFSfilename := parts[1]
    localfilename := parts[2]
    fmt.Printf("VM Address: %s, HyDFS Filename: %s, Local Filename: %s\n", VMaddress, HyDFSfilename, localfilename)

    server, err := getServerFromAddress(VMaddress)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    // 这一步卡住了
    content, err := fetchFile(*server, HyDFSfilename)
    if err != nil {
        fmt.Printf("error fetching file: %v\n", err)
        return
    }
    localFilepath := filepath.Join("localDir", localfilename)
    if err := os.WriteFile(localFilepath, content, 0644); err != nil {
        fmt.Printf("Error writing to local file: %v\n", err)
    }
}
