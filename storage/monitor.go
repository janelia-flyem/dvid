/*
	Thie file implements a monitor for various operations.  It exposes two channels
	that handlers can use to signal progress.
*/

package storage

import (
	"sync"
	"time"
)

const MonitorBuffer = 10000

var (
	// Number of bytes read in last second.
	BytesReadPerSec int

	// Number of bytes written in last second.
	BytesWrittenPerSec int

	// Number of key-value GET calls in last second.
	GetsPerSec int

	// Number of key-value PUT calls in last second.
	PutsPerSec int

	bytesRead    chan int
	bytesWritten chan int
)

func init() {
	bytesRead = make(chan int, MonitorBuffer)
	bytesWritten = make(chan int, MonitorBuffer)

	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	var access sync.Mutex
	for {
		select {
		case b := <-bytesRead:
			BytesReadPerSec += b
			GetsPerSec++
		case b := <-bytesWritten:
			BytesWrittenPerSec += b
			PutsPerSec++
		case <-secondTick:
			access.Lock()
			BytesReadPerSec = 0
			BytesWrittenPerSec = 0
			GetsPerSec = 0
			PutsPerSec = 0
			access.Unlock()
		}
	}
}
