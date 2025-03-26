package rainstorm

import (
	//"bufio"
	"fmt"
	"net"
	//"strings"
)

// Send Func
func AppSend(ip string, port string, message string) error {
	address := fmt.Sprintf("%s:%s", ip, port)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", address, err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	// fmt.Printf("Message sent to %s:%d: %s\n", ip, port, message)
	return nil
}
