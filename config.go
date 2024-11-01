package main

import (
    "encoding/json"
    "io/ioutil"
    "log"
    "sync"
)

type Config struct {
    domain     string                   `json:"domain"`
    memberlist map[string][][]string    `json:"memberlist"`
}

var (
    countMutex sync.Mutex
    config     Config
	domain     string                  
    memberlist map[string][][]string    
)

// initConfig 读取并解析 JSON 配置文件
func initConfig() {
	var config Config
    data, err := ioutil.ReadFile("config.json")
    if err != nil {
        log.Fatalf("Error reading config file: %v", err)
    }

    err = json.Unmarshal(data, &config)
    if err != nil {
        log.Fatalf("Error parsing config JSON: %v", err)
    }

    // 将配置值赋给全局变量
    domain = config.domain
    memberlist = config.memberlist
}
