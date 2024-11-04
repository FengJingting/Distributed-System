package utils
import (
	// "time"
	// "os"
	// "fmt"
	"hash/fnv"
	// "strconv"
	// "encoding/json"
)

// 导出 hash 函数为 Hash
func Hash(filename string) uint64 {
    hasher := fnv.New64a()
    hasher.Write([]byte(filename))
    return hasher.Sum64()
}



