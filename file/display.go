package file

import (
    "bufio"
    "fmt"
    "os"
    // "path/filepath"
    "strings"
    "mp3/utils"
    "mp3/cassandra"
    "io/ioutil"
)

// is function for handling lsHyDFSfilename command
func Is() {
    fmt.Print("is_ function called: ")
    fmt.Print("请输入文件名: ")
    reader := bufio.NewReader(os.Stdin)
    filename, _ := reader.ReadString('\n')
    filename = strings.TrimSpace(filename) // Remove newline and trim
    // Get the list of VM addresses and IDs that store the file
    result := lsHyDFSfilename(filename)
    
    // Print each VM address and ID in the result
    if len(result) > 0 {
        fmt.Println("存有该文件的虚拟机信息：")
        for _, entry := range result {
            fmt.Println(entry)
        }
    } else {
        fmt.Println("没有找到存有该文件的虚拟机。")
    }
}

// Helper function to get server by ID
func getServerByID(serverID uint64) *cassandra.Node {
    for _, node := range cassandra.Ring.Nodes {
        if node.ID == serverID {
            return node
        }
    }
    return nil
}

func lsHyDFSfilename(filename string) []string {
    var result []string
    server := getTargetServer(filename)
    fmt.Printf("server",server)
    for i := 0; i < 3; i++ {
        if server == nil {
            fmt.Println("Server not found.")
            break
        }

        serverAddr := server.IP
        serverID := fmt.Sprintf("%d", server.ID)
        content, err := fetchFile(*server, filename)
        if err == nil && content != nil {
            result = append(result, fmt.Sprintf("VM Address: %s, VM ID: %s", serverAddr, serverID))
        } else {
            fmt.Printf("File not found on server: %s (ID: %s)\n", serverAddr, serverID)
        }

        // Fetch the next server using server.Successor.ID
        if server.Successor != nil {
            server = getServerByID(server.Successor.ID)
        } else {
            break
        }
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
    fmt.Println(parts)
    if len(parts) != 3 {
        fmt.Println("输入格式不正确，请确保输入三个参数并用空格分隔。")
        return
    }

    VMaddress := parts[0]
    hyDFSfilename := parts[1]
    localfilename := parts[2]
    fmt.Printf("VM Address: %s, HyDFS Filename: %s, Local Filename: %s\n", VMaddress, hyDFSfilename, localfilename)

    // Get the server information based on the VM address
    server, err := getServerFromAddress(VMaddress)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    // Fetch the file from the server
    content, err := fetchFile(*server, hyDFSfilename)
    if err != nil {
        fmt.Printf("Error fetching file: %v\n", err)
        return
    }
    fmt.Println("Content fetched:", content)

    // Save the content to the specified local file path
    localFilepath := LocalDir + "/" + localfilename
    err = ioutil.WriteFile(localFilepath, content, 0644)
    if err != nil {
        fmt.Printf("Error writing to local file: %v\n", err)
        return
    }
    fmt.Println("File saved at:", localFilepath)
}
