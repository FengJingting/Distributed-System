package memberlist

import (
    "fmt"
    // "strings"
    "mp3/utils"
)

func List_mem_ids() {
    fmt.Println("list_mem_ids")
    // TODO: augment list_mem from MP2 to also print the ID on the ring which each node in the membership list maps to.
    // You may want to sort this by ID (useful for tests).
}

func List_mem() {
    // Iterate through and print each key-value pair in the memberlist
    for status, nodes := range utils.Memberlist {
        fmt.Printf("%s nodes:\n", status)
        if len(nodes) == 0 {
            fmt.Println("No nodes")
        } else {
            for _, node := range nodes {
                fmt.Printf("  ID: %d, IP: %s, Port: %s, Timestamp: %d\n", node.ID, node.IP, node.Port, node.Timestamp)
            }
        }
    }
}

func List_self() {
    fmt.Println("list_self function called")

    found := false
    for state, nodes := range utils.Memberlist {
        for _, node := range nodes {
            if node.IP == utils.Domain {
                // 输出节点信息
                fmt.Printf("Found matching node in state '%s': ID: %d, IP: %s, Port: %s, Timestamp: %d\n", state, node.ID, node.IP, node.Port, node.Timestamp)
                found = true
            }
        }
    }

    // 如果没有找到匹配的节点
    if !found {
        fmt.Println("No matching node found in any state")
    }
}

func Join() {
    fmt.Println("join function called")
    // 发送加入消息给 Introducer
    message := "join+" + utils.Domain
    send(utils.Introducer, utils.Port, message)
}

func changeStatus(status, nodeIP, port, timestamp, initial string) {
    var nodeToMove utils.Node
    var found bool

    // 遍历 memberlist[initial]，找到匹配的节点
    aliveNodes := utils.Memberlist[initial]
    for i, node := range aliveNodes {
        // 匹配节点 IP 和端口
        if node.IP == nodeIP && node.Port == port && fmt.Sprint(node.Timestamp) == timestamp {
            // 找到节点
            nodeToMove = node

            // 从 initial 列表中删除节点
            utils.CountMutex.Lock()
            utils.Memberlist[initial] = append(aliveNodes[:i], aliveNodes[i+1:]...)
            utils.CountMutex.Unlock()

            found = true
            break
        }
    }

    // 如果找到节点，将其移动到目标状态列表
    if found {
        utils.CountMutex.Lock()
        utils.Memberlist[status] = append(utils.Memberlist[status], nodeToMove)
        utils.CountMutex.Unlock()

        List_mem() // 调试用，打印当前 memberlist 的状态
        utils.Write_to_log()
    } else {
        fmt.Printf("Node %s with port %s not found in alive\n", nodeIP, port)
    }
}
