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

	// Read operation and filename
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
		// Read file content and return it
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
		conn.Write(content) // Send file content back to the client
		fmt.Println(content)
		fmt.Printf("File %s read and sent back successfully\n", filename)

	case "CREATE": 
		fmt.Println("------------receive_create-------------")
	
		// Construct file path
		filepath := DfsDir + filename
	
		// Check if the file already exists locally
		if _, err := os.Stat(filepath); err == nil {
			// File exists, no need to write, directly return success message
			fmt.Printf("File %s already exists in local directory\n", filename)
			conn.Write([]byte("OK\n")) // Send success message
			return nil
		}
	
		// Read file size
		var fileSize int64
		_, err := fmt.Fscanf(reader, "%d\n", &fileSize)
		if err != nil {
			conn.Write([]byte("ERROR reading file size\n")) // Send error message
			return fmt.Errorf("error reading file size: %v", err)
		}
	
		// Read file content
		content := make([]byte, fileSize)
		_, err = io.ReadFull(reader, content)
		if err != nil {
			conn.Write([]byte("ERROR reading file content\n")) // Send error message
			return fmt.Errorf("error reading file content: %v", err)
		}
	
		// Write to file
		err = ioutil.WriteFile(filepath, content, 0644)
		if err != nil {
			conn.Write([]byte("ERROR writing file\n")) // Send error message
			return fmt.Errorf("error creating file %s: %v", filename, err)
		}
	
		fmt.Printf("File %s created successfully in local directory\n", filename)
		conn.Write([]byte("OK\n")) // Send success message

	case "APPEND":
		fmt.Println("------------receive_append-------------")
		var fileSize int64
		_, err := fmt.Fscanf(reader, "%d\n", &fileSize)
		if err != nil {
			conn.Write([]byte("ERROR reading file size\n")) // Send error message
			return fmt.Errorf("error reading file size: %v", err)
		}

		content := make([]byte, fileSize)
		_, err = io.ReadFull(reader, content)
		if err != nil {
			conn.Write([]byte("ERROR reading file content\n")) // Send error message
			return fmt.Errorf("error reading file content: %v", err)
		}

		filepath := DfsDir + filename
		file, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			conn.Write([]byte("ERROR opening file\n")) // Send error message
			return fmt.Errorf("error opening file %s: %v", filename, err)
		}
		defer file.Close()

		_, err = file.Write(content)
		if err != nil {
			conn.Write([]byte("ERROR appending to file\n")) // Send error message
			return fmt.Errorf("error appending to file %s: %v", filename, err)
		}
		fmt.Printf("Content appended to file %s successfully\n", filename)
		conn.Write([]byte("OK\n")) // Send success message

	default:
		conn.Write([]byte("ERROR unknown operation\n")) // Send error message
		return fmt.Errorf("unknown operation: %s", operation)
	}

	return nil
}
