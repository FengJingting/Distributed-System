package rainstorm

import (
	"encoding/json"
	"fmt"
	"mp3/cassandra"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	mu sync.Mutex // define lock
)

// receiveTask from certain port
func ReceiveTask(port string) {
	//create listener
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Failed to start server on port %s: %v\n", port, err)
		return
	}
	defer listener.Close()

	// fmt.Printf("Server listening on port %s\n", port)

	for {
		//connect
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		//process
		go handleConnection(conn)
	}
}

func removeValueFromMapKey(m map[string][]string, key, substring string) {
	// if key exist
	if values, exists := m[key]; exists {
		newValues := []string{}
		for _, v := range values {
			// not contain
			if !strings.Contains(v, substring) {
				newValues = append(newValues, v)
			}
		}
		// update key
		if len(newValues) > 0 {
			m[key] = newValues
		} else {
			// del key if val == 0
			delete(m, key)
		}
	}
}

func ConvertToLogString(tasksList map[string][]string) (string, error) {
	//to JSON
	jsonString, err := json.Marshal(tasksList)
	if err != nil {
		return "", fmt.Errorf("failed to convert tasksList to JSON: %v", err)
	}
	return string(jsonString), nil
}

// AppendResultToLog
func AppendResultToLog(result, logFilename string) error {
	// Use AppendLog
	result = result + "\n"
	err := AppendLog([]byte(result), logFilename, false)
	if err != nil {
		return fmt.Errorf("failed to append result to log file: %v", err)
	}

	// fmt.Printf("Result successfully appended to log file: %s\n", logFilename)
	return nil
}

// handleConnection
func handleConnection(conn net.Conn) {
	defer conn.Close()
	// read message
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Printf("Failed to read message: %v\n", err)
		return
	}
	receivedData := string(buffer[:n])
	//split using "+"
	parts := strings.Split(receivedData, "+")
	//choose by prefix
	prefix := parts[0]
	switch prefix {
	case "transform":
		filename := parts[1]
		startLine, err1 := strconv.Atoi(parts[2])
		endLine, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			fmt.Printf("Invalid transform line numbers: %s\n", parts[1])
			return
		}
		//Transform Func
		Transform(filename, startLine, endLine)
	case "transform_ack":
		ip := parts[1]
		filename := parts[2]
		line, err := strconv.Atoi(parts[3])
		if err != nil {
			fmt.Printf("Invalid line number: %s, error: %v\n", parts[3], err)
			return
		}
		lineContent := parts[4]
		//schedule func
		if app == "app1" {
			err = sendFilterRequest("filter1", ip, cassandra.StreamPort, filename, line, lineContent, paramX)
		} else if app == "app2" {
			err = sendFilterRequest("filter2", ip, cassandra.StreamPort, filename, line, lineContent, paramX)
		} else {
			fmt.Printf("Unknown application: %s\n", app)
			return
		}

		if err != nil {
			fmt.Printf("Failed to send Filter request for %s: %v\n", app, err)
		} else {
			// fmt.Printf("Filter request for %s successfully sent to IP: %s\n", app, ip)
		}
		// fmt.Printf("Acknowledged filter1 for IP: %s\n", ip)
		// Tasks
		removeValueFromMapKey(TasksList, ip, filename+"+"+strconv.Itoa(line)+"+"+"transform")
		Tasks[ip] = Tasks[ip] - 1
		//log
		logString, err := ConvertToLogString(TasksList)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if err := AppendResultToLog(logString, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}

	case "filter1":
		filename := parts[1]
		line, err1 := strconv.Atoi(parts[2])
		lineContent := parts[3]
		sign := parts[4]
		if err1 != nil {
			fmt.Printf("Invalid line number in filter1 message: %v\n", parts[2])
			return
		}
		// Filter1 func
		Filter1(filename, line, lineContent, sign)
	case "filter1_ack":
		ip := parts[1]
		filename := parts[2]
		line, err := strconv.Atoi(parts[3])
		if err != nil {
			fmt.Printf("Invalid line number: %s, error: %v\n", parts[3], err)
			return
		}
		lineContent := parts[4]
		servers, err := SelectTopNodes(1)
		for _, server := range servers {
			// fmt.Println("Server:", server)
			err = sendFilterRequest("aggregate1", server.IP, cassandra.StreamPort, filename, line, lineContent, "")
			if err != nil {
				fmt.Printf("Failed to send Filter1_ack request: %v\n", err)
			}
		}
		// fmt.Printf("Acknowledged filter1 for IP: %s\n", ip)
		// Tasks
		removeValueFromMapKey(TasksList, ip, filename+"+"+strconv.Itoa(line)+"+"+"filter")
		Tasks[ip] = Tasks[ip] - 1
		// log
		logString, err := ConvertToLogString(TasksList)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if err := AppendResultToLog(logString, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
	case "filter2":
		if len(parts) < 5 {
			fmt.Printf("Invalid filter2 message format: %v\n", parts)
			return
		}
		filename := parts[1]
		line, err1 := strconv.Atoi(parts[2])
		lineContent := parts[3]
		sign := parts[4]
		if err1 != nil {
			fmt.Printf("Invalid line number in filter2 message: %v\n", parts[2])
			return
		}
		//Filter2
		Filter2(filename, line, lineContent, sign)
	case "filter2_ack":
		ip := parts[1]
		filename := parts[2]
		line, err := strconv.Atoi(parts[3])
		if err != nil {
			fmt.Printf("Invalid line number: %s, error: %v\n", parts[3], err)
			return
		}
		lineContent := parts[4]
		//updateNumTasks(ip, -1)
		// Filter1
		servers, err := SelectTopNodes(1)
		for _, server := range servers {
			//fmt.Println("Server:", server)
			err = sendFilterRequest("aggregate2", server.IP, cassandra.StreamPort, filename, line, lineContent, "")
			if err != nil {
				fmt.Printf("Failed to send Filter1 request: %v\n", err)
			}
		}
		//fmt.Printf("Acknowledged filter2_ack for IP: %s\n", ip)
		// Tasks
		removeValueFromMapKey(TasksList, ip, filename+"+"+strconv.Itoa(line)+"+"+"filter")
		Tasks[ip] = Tasks[ip] - 1
		// log
		logString, err := ConvertToLogString(TasksList)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if err := AppendResultToLog(logString, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
	case "aggregate1":
		filename := parts[1]
		lineNumber, err1 := strconv.Atoi(parts[2]) //to int
		if err1 != nil {
			fmt.Printf("Invalid line number in aggregate1 message: %s\n", parts[2])
			return
		}
		lineContent := parts[3]

		// Aggregate1
		//print("linecontent", lineContent)
		if lineContent != "None" {
			Aggregate1(filename, lineNumber, lineContent)
		}
	case "aggregate1_ack":
		//output
		result := parts[4]
		fmt.Printf("app1 result: %s\n", result)
		ip := parts[1]
		filename := parts[2]
		linenumber, _ := strconv.Atoi(parts[3])
		// Tasks
		removeValueFromMapKey(TasksList, ip, filename+"+"+strconv.Itoa(linenumber)+"+"+"filter")
		Tasks[ip] = Tasks[ip] - 1
		// log
		if err := AppendResultToLog(result, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
		// fmt.Println("destFile", destFile)
		// to dest file
		if err := AppendResultToLog(result, destFile); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
	case "aggregate2":
		filename := parts[1]
		lineNumber, err1 := strconv.Atoi(parts[2]) //to int
		if err1 != nil {
			fmt.Printf("Invalid line number in aggregate1 message: %s\n", parts[2])
			return
		}
		lineContent := parts[3]
		
		if lineContent != "None" {
			Aggregate2(filename, lineNumber, lineContent)
		}
	case "aggregate2_ack":
		mu.Lock()

		ip := parts[1]
		filename := parts[2]
		linenumber, _ := strconv.Atoi(parts[3])
		category := parts[4]
		if _, exists := count[category]; !exists {
			count[category] = 0 //initialize
		}
		count[category] += 1 //add		
		fmt.Printf("app2 result: %d\n", count)
		// Tasks
		removeValueFromMapKey(TasksList, ip, filename+"+"+strconv.Itoa(linenumber)+"+"+"filter")
		Tasks[ip] = Tasks[ip] - 1
		// log
		// res := strconv.Itoa(count)
		res := fmt.Sprintf("%v", count)
		if err := AppendResultToLog(res, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
		//to dest file
		if err := AppendResultToLog(res, destFile); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
		mu.Unlock()

	default:
		fmt.Printf("Unknown message prefix: %s\n", prefix)
	}
}
