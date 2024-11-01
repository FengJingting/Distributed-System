package main

import "sync"

const (
	domain  = "fa24-cs425-2901.cs.illinois.edu"
	loserate = 0.0
)

var (
	countMutex    sync.Mutex
	memberlist = map[string][][]string{
		"alive":   {{"fa24-cs425-2901.cs.illinois.edu", "8080", "v0", "-1"}},
		"failed":  {},
	}
)