package file

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
)

func HandleFileOperation(conn net.Conn) error {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// 读取操作和文件名
	header, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("error reading from connection: %v", err)
	}
	header = strings.TrimSpace(header)
	parts := strings.SplitN(header, " ", 2)
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
		
			// 构建文件路径
			filepath := DfsDir + filename
		
			// 检查本地文件是否已存在
			if _, err := os.Stat(filepath); err == nil {
				// 文件存在，无需写入，直接返回成功消息
				fmt.Printf("File %s already exists in local directory\n", filename)
				conn.Write([]byte("OK\n")) // 发送成功消息
				return nil
			}
		
			// 读取文件大小
			var fileSize int64
			_, err := fmt.Fscanf(reader, "%d\n", &fileSize)
			if err != nil {
				conn.Write([]byte("ERROR reading file size\n")) // 发送错误消息
				return fmt.Errorf("error reading file size: %v", err)
			}
		
			// 读取文件内容
			content := make([]byte, fileSize)
			_, err = io.ReadFull(reader, content)
			if err != nil {
				conn.Write([]byte("ERROR reading file content\n")) // 发送错误消息
				return fmt.Errorf("error reading file content: %v", err)
			}
		
			// 写入文件
			err = ioutil.WriteFile(filepath, content, 0644)
			if err != nil {
				conn.Write([]byte("ERROR writing file\n")) // 发送错误消息
				return fmt.Errorf("error creating file %s: %v", filename, err)
			}
		
			fmt.Printf("File %s created successfully in local directory\n", filename)
			conn.Write([]byte("OK\n")) // 发送成功消息

	case "APPEND":
		fmt.Println("------------receive_append-------------")
		var fileSize int64
		_, err := fmt.Fscanf(reader, "%d\n", &fileSize)
		if err != nil {
			conn.Write([]byte("ERROR reading file size\n")) // 发送错误消息
			return fmt.Errorf("error reading file size: %v", err)
		}

		content := make([]byte, fileSize)
		_, err = io.ReadFull(reader, content)
		if err != nil {
			conn.Write([]byte("ERROR reading file content\n")) // 发送错误消息
			return fmt.Errorf("error reading file content: %v", err)
		}

		filepath := DfsDir + filename
		file, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			conn.Write([]byte("ERROR opening file\n")) // 发送错误消息
			return fmt.Errorf("error opening file %s: %v", filename, err)
		}
		defer file.Close()

		_, err = file.Write(content)
		if err != nil {
			conn.Write([]byte("ERROR appending to file\n")) // 发送错误消息
			return fmt.Errorf("error appending to file %s: %v", filename, err)
		}
		fmt.Printf("Content appended to file %s successfully\n", filename)
		conn.Write([]byte("OK\n")) // 发送成功消息

	default:
		conn.Write([]byte("ERROR unknown operation\n")) // 发送错误消息
		return fmt.Errorf("unknown operation: %s", operation)
	}

	return nil
}
