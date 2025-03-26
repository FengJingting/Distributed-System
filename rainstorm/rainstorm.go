package rainstorm

import (
	"bufio"
	"fmt"
	"mp3/cassandra"
	"mp3/file"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// super parameters
const (
	LocalDir    = "./files/local/"
	logFilename = "log.log"
)

var (
	paramX       string
	numTasks     int
	destFilePath string
	destFile     string
	app          string
	count        map[string]int
)

func Rainstorm() {
	InitializeTasks()
	// TODO: Prompt user for app, op1, op2, source file, destination file, and number of tasks
	// Request user input for required parameters
	reader := bufio.NewReader(os.Stdin)

	// Select application name
	// app = "app1"
	for {
		fmt.Print("Enter application name (app1 or app2): ")
		appInput, _ := reader.ReadString('\n')
		appInput = strings.TrimSpace(appInput)

		if appInput == "app1" || appInput == "app2" {
			app = appInput
			if appInput == "app2" {
				count = map[string]int{
					"Punched Telespar":    0,
					"Unpunched Telespar":  0,
					"Streetlight":         0,
					"Streetname":          0,
					"Warning":             0,
					"Parking":             0,
					"Regulatory":          0,
					"School":              0,
					"Guide":               0,
					"Custom":              0,
					"Object Marker":       0,
					"MTD":                 0,
				}
				fmt.Println("count is", count)
			}
			break
		} else {
			fmt.Println("Error: Invalid application name. Please choose 'app1' or 'app2'.")
		}
	}

	// Input source file
	fmt.Print("Enter source file name in HydFS: ")
	sourceFile, _ := reader.ReadString('\n')
	sourceFile = strings.TrimSpace(sourceFile)

	// Input destination file
	fmt.Print("Enter destination file name in HydFS (e.g., hydfs_dest_file): ")
	destFile, _ = reader.ReadString('\n')
	destFile = strings.TrimSpace(destFile)
	// Save results to the destination file
	if err := AppendResultToLog("result:", destFile); err != nil {
		fmt.Printf("Failed to append result to log file: %v\n", err)
	}

	// Input number of tasks
	numTasks = 1
	for {
		fmt.Print("Enter the number of tasks (e.g., 5): ")
		numTasksInput, _ := reader.ReadString('\n')
		numTasksInput = strings.TrimSpace(numTasksInput)

		// Attempt to convert input to integer
		n, err := strconv.Atoi(numTasksInput)
		if err != nil || n <= 0 {
			fmt.Println("Error: Invalid number of tasks. Please enter a positive integer.")
		} else {
			numTasks = n
			break
		}
	}

	// Input parameter x
	for {
		fmt.Print("Enter a value for parameter x (e.g., task_group): ")
		paramXInput, _ := reader.ReadString('\n')
		paramXInput = strings.TrimSpace(paramXInput)

		// Validate non-empty string
		if paramXInput == "" {
			fmt.Println("Error: Parameter x cannot be empty. Please enter a valid string.")
		} else {
			paramX = paramXInput
			break
		}
	}

	// Print entered parameters
	fmt.Println("\nParameters:")
	fmt.Printf("Application: %s\n", app)
	fmt.Printf("Source File: %s\n", sourceFile)
	fmt.Printf("Destination File: %s\n", destFile)
	fmt.Printf("Number of Tasks: %d\n", numTasks)
	fmt.Printf("Parameter x: %s\n", paramX)

	// Call the schedule function
	schedule(app, sourceFile, destFile, numTasks)
}

// checkAndFetchFile checks if the file exists locally and fetches it from DFS if not
func checkAndFetchFile(fileName string, localDir string) (string, error) {
	localFilePath := filepath.Join(localDir, fileName)

	// Check if the file exists locally
	if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
		fmt.Printf("File %s not found locally. Fetching from DFS...\n", fileName)
		// Fetch file from DFS if not found locally
		err := file.Get(fileName, fileName)
		if err != nil {
			return "", fmt.Errorf("failed to fetch file %s from DFS: %v", fileName, err)
		}
	}

	return localFilePath, nil
}

// getTotalLines opens the file and counts the total number of lines
func getTotalLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	totalLines := 0
	for scanner.Scan() {
		totalLines++
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading file %s: %v", filePath, err)
	}

	return totalLines, nil
}

func schedule(app string, sourceFile string, destFile string, numTasks int) {
	// Schedule tasks
	sourceFilePath, err := checkAndFetchFile(sourceFile, LocalDir)
	if err != nil {
		fmt.Printf("Error with sourceFile: %v\n", err)
		return
	}

	destFilePath, err = checkAndFetchFile(destFile, LocalDir)
	if err != nil {
		fmt.Printf("Error with destFile: %v\n", err)
		return
	}

	// Download sourceFile to local, get file name and total length
	totalLines, err := getTotalLines(sourceFilePath)
	if err != nil {
		fmt.Printf("Error getting total lines from source file: %v\n", err)
		return
	}

	// Divide file length into numTasks parts
	linesPerTask := (totalLines + numTasks - 1) / numTasks // Ensure even distribution
	servers, err := SelectTopNodes(numTasks)

	// Distribute tasks
	for i := 0; i < numTasks; i++ {
		startLine := i*linesPerTask + 1
		endLine := (i + 1) * linesPerTask
		if endLine > totalLines {
			endLine = totalLines
		}

		server := servers[i].IP
		fmt.Println("servers is", server)
		err := sendTransform(sourceFile, startLine, endLine, server)
		if err != nil {
			fmt.Printf("Failed to send task: %v\n", err)
		} else {
			fmt.Printf("Task sent successfully for lines %d to %d\n", startLine, endLine)
		}

		// Update TasksList
		for i := startLine; i <= endLine; i++ {
			TasksList[server] = append(TasksList[server], sourceFile+"+"+strconv.Itoa(i)+"+"+"transform")
		}

		// Update Tasks
		Tasks[server] = Tasks[server] + (endLine - startLine + 1)

		// Save results to log file
		logString, _ := ConvertToLogString(TasksList)
		if err := AppendResultToLog(logString, logFilename); err != nil {
			fmt.Printf("Failed to append result to log file: %v\n", err)
		}
	}
}