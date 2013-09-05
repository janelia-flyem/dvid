/*
	Thie file implements a monitor for chunk handling.  It exposes two channels
	that handlers can use to signal progress.  The monitoring data can be
	retrieved via ChunkLoadJSON().
*/

package datastore

import (
	"encoding/json"
	"sync"
	"time"
)

// Track requested/completed block ops
type loadStruct struct {
	Requests  int
	Completed int
}
type loadMap map[DataString]loadStruct

var (
	DoneChannel    chan DataString
	RequestChannel chan DataString

	loadLastSec loadMap
	loadAccess  sync.RWMutex
)

func init() {
	loadLastSec = make(loadMap)
	DoneChannel = make(chan DataString)
	RequestChannel = make(chan DataString)
	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	requests := make(map[DataString]int)
	completed := make(map[DataString]int)
	for {
		select {
		case name := <-DoneChannel:
			completed[name]++
		case name := <-RequestChannel:
			requests[name]++
		case <-secondTick:
			loadAccess.RLock()
			for name, _ := range loadLastSec {
				loadLastSec[name] = loadStruct{
					Requests:  requests[name],
					Completed: completed[name],
				}
				requests[name] = 0
				completed[name] = 0
			}
			loadAccess.RUnlock()
		}
	}
}

// StartMonitor starts to monitor chunk handlers for the given data.
func StartMonitor(name DataString) {
	loadAccess.Lock()
	loadLastSec[name] = loadStruct{}
	loadAccess.Unlock()
}

// StopMonitor stops monitoring of chunk handlers for the given data.
func StopMonitor(name DataString) {
	loadAccess.Lock()
	delete(loadLastSec, name)
	loadAccess.Unlock()
}

// ChunkLoadJSON returns a JSON description of the chunk op requests for each data.
func ChunkLoadJSON() (jsonStr string, err error) {
	loadAccess.RLock()
	m, err := json.Marshal(loadLastSec)
	loadAccess.RUnlock()
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}
