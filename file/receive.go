package file

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
)

// 处理接收到的文件操作命令
func HandleFileOperation(conn net.Conn) error {
	data, err := ioutil.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("error reading from connection: %v", err)
	}

	message := string(data)
	parts := strings.SplitN(message, " ", 3)
	if len(parts) < 2 {
		return fmt.Errorf("invalid message format")
	}

	operation := parts[0]
	filename := parts[1]

	switch operation {
	case "GET":
		fmt.Println("------------receive_get-------------")
		// 读取文件内容并返回
		filepath := DfsDir + filename
		_, err := os.Stat(filepath)
		if os.IsNotExist(err) {
			fmt.Println("File Not Exist")
			return nil
		}
		content, err := ioutil.ReadFile(filepath)
		if err != nil {
			return fmt.Errorf("error reading file %s: %v", filename, err)
		}
		conn.Write(content) // 将文件内容发送回客户端
		fmt.Printf("File %s read and sent back successfully\n", filename)

	case "CREATE":
		fmt.Println("------------receive_create-------------")
		if len(parts) < 3 {
			return fmt.Errorf("missing content for CREATE operation")
		}
		content := parts[2]
		filepath := DfsDir + filename
		err = ioutil.WriteFile(filepath, []byte(content), 0644)
		if err != nil {
			return fmt.Errorf("error creating file %s: %v", filename, err)
		}
		fmt.Printf("File %s created successfully in local directory\n", filename)

	case "APPEND":
		fmt.Println("------------receive_append-------------")
		if len(parts) < 3 {
			return fmt.Errorf("missing content for APPEND operation")
		}
		content := parts[2]
		filepath := DfsDir + filename
		file, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", filename, err)
		}
		defer file.Close()

		_, err = file.WriteString(content)
		if err != nil {
			return fmt.Errorf("error appending to file %s: %v", filename, err)
		}
		fmt.Printf("Content appended to file %s successfully\n", filename)

	default:
		return fmt.Errorf("unknown operation: %s", operation)
	}

	return nil
}
