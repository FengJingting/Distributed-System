package main

import (
	"math/rand"
	"time"
	"os"
	"fmt"
)

func Shuffle(slice [][]string) {
	rand.Seed(time.Now().UnixNano())
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func findNodeDetailsByIP(ip string) (string, string, string, bool) {
	// Iterate through the alive list in the memberlist to find the node matching the given IP
	for _, node := range memberlist["alive"] {
		if node[0] == ip {
			// Return the port, version number, and the last parameter
			return node[1], node[2], node[3], true
		}
	}
	// If not found, return empty strings and false
	return "", "", "", false
}

func Write_to_log() {
	// Log current memberlist to a file
	// Open or create the log file in append mode
	logFile, err := os.OpenFile("memberlist.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening/creating log file:", err)
		return
	}
	defer logFile.Close()

	// Get the current time as a timestamp for the log entry
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// Write the overall timestamp of the log entry
	_, err = fmt.Fprintf(logFile, "Log Timestamp: %s\n", currentTime)
	if err != nil {
		fmt.Println("Error writing timestamp to log file:", err)
		return
	}

	// Write the memberlist data to the log file, recording the timestamp for each node
	for status, nodes := range memberlist {
		for _, node := range nodes {
			_, err := fmt.Fprintf(logFile, "[%s] %s: %v\n", currentTime, status, node)
			if err != nil {
				fmt.Println("Error writing node status to log file:", err)
				return
			}
		}
	}

	// Add a separator line to distinguish logs at different times
	_, err = fmt.Fprintln(logFile, "---------------------------")
	if err != nil {
		fmt.Println("Error writing separator to log file:", err)
	}

	fmt.Println("Memberlist successfully written to log.")
}
