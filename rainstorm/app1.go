package rainstorm

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mp3/cassandra"
	"mp3/file"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// const LocalDir = "./files/local"
const configFilePath = "./config.json"

// Transform function
func Transform(filename string, startLine int, endLine int) error {
	fmt.Println("_______________Transform_________________")
	cfg, err := cassandra.LoadConfig(configFilePath)
	if err != nil {
		return fmt.Errorf("Failed to load config: %v", err)
	}
	localFilePath := filepath.Join(LocalDir, filename)
	// Check if the file exists locally
	if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
		// If it doesn't exist, use file.Get to copy the file from the DFS system to the local directory
		err := file.Get(filename, filename)
		if err != nil {
			return fmt.Errorf("Cannot get file: %v", err)
		}
	}

	// Open the CSV file
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("Cannot open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow inconsistent number of fields per line

	// Read the headers
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("Get graph head Error: %v", err)
	}

	lineNumber := 1 // Current line number, including the header

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Read Line Error: %v", err)
		}
		lineNumber++

		if lineNumber < startLine+1 { // Since lineNumber includes the header, add 1
			continue
		}
		if lineNumber > endLine+1 {
			break
		}

		// Convert the record into a "header: content" format
		LineMap := make(map[string]string)
		for i, header := range headers {
			if i < len(record) {
				LineMap[header] = record[i]
			} else {
				LineMap[header] = ""
			}
		}

		// Convert the map to JSON
		LineJson, err := json.Marshal(LineMap)
		if err != nil {
			return fmt.Errorf("Map2Json Error: %v", err)
		}

		// Line message
		LineMessage := "transform_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + string(LineJson)
		AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
	}

	return nil
}

// Filter function
func Filter1(filename string, lineNumber int, line string, sign string) { // sign: "X"
	fmt.Println("_______________Filter1_________________")
	cfg, err := cassandra.LoadConfig(configFilePath)
	if err != nil {
		fmt.Printf("Failed to load config: %v", err)
		return
	}
	// Check if the line contains "X"
	if strings.Contains(line, sign) {
		// If it contains "X", return the original tuple
		// fmt.Println(line)
		LineMessage := "filter1_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + line
		AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
		return
	} else {
		// If it does not contain "X", return <filename:linenumber, "None">
		LineMessage := "filter1_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + "None"
		AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
		return
	}
}

// Aggregate function
func Aggregate1(filename string, lineNumber int, line string) {
	fmt.Println("_______________Aggregate1_________________")
	// fmt.Println("Aggregate1 called")
	cfg, err := cassandra.LoadConfig(configFilePath)
	if err != nil {
		fmt.Printf("Failed to load config: %v", err)
		return
	}
	// Extract the contents of the "OBJECTID" and "Sign_Type" fields
	var LineMap map[string]string
	// fmt.Println("line", line)
	err = json.Unmarshal([]byte(line), &LineMap)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// Print the parsed map
	// fmt.Println(LineMap)
	LineOut := "OBJECTID: " + LineMap["OBJECTID"] + ", Sign_Type: " + LineMap["Sign_Type"]
	LineMessage := "aggregate1_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + LineOut
	AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
}