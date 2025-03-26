package rainstorm

import (
	"encoding/json"
	"fmt"
	"mp3/cassandra"
	"strconv"
)

// filter Func
func Filter2(filename string, lineNumber int, line string, sign string) {
	fmt.Println("_______________Filter2_________________")
	cfg, err := cassandra.LoadConfig(configFilePath)
	if err != nil {
		fmt.Printf("Failed to load config: %v", err)
		return
	}
	var LineMap map[string]string
	err = json.Unmarshal([]byte(line), &LineMap)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// check if the line contains X
	if LineMap["Sign_Post"] == sign { //strings.Contains(LineMap["Sign_Post"], sign) { //"Punched Telespar") || strings.Contains(LineMap["Sign_Post"], "Unpunched Telespar") || strings.Contains(LineMap["Sign_Post"], "Streetlight") {
		// Contain X Return original line 
		LineMessage := "filter2_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + line
		AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
		return
	} else {
		//Not contain X, return <filename:linenumber, “None”>
		LineMessage := "filter2_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + "None"
		AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
		return
	}
}

// aggregate Func
func Aggregate2(filename string, lineNumber int, line string) {
	fmt.Println("_______________Aggregate2_________________")
	cfg, err := cassandra.LoadConfig(configFilePath)
	if err != nil {
		fmt.Printf("Failed to load config: %v", err)
		return
	}
	// Get “Category"
	var LineMap map[string]string
	err = json.Unmarshal([]byte(line), &LineMap)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	LineMessage := "aggregate2_ack" + "+" + cfg.Domain + "+" + filename + "+" + strconv.Itoa(lineNumber) + "+" + LineMap["Category"]
	AppSend(cfg.Introducer, cfg.StreamPort, LineMessage)
}
