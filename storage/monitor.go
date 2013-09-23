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

	// Current tallies up to a second.
	curBytesReadPerSec    int
	curBytesWrittenPerSec int
	curGetsPerSec         int
	curPutsPerSec         int
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
			curBytesReadPerSec += b
			curGetsPerSec++
		case b := <-bytesWritten:
			curBytesWrittenPerSec += b
			curPutsPerSec++
		case <-secondTick:
			access.Lock()
			BytesReadPerSec = curBytesReadPerSec
			BytesWrittenPerSec = curBytesWrittenPerSec
			curBytesReadPerSec = 0
			curBytesWrittenPerSec = 0
			GetsPerSec = curGetsPerSec
			PutsPerSec = curPutsPerSec
			curGetsPerSec = 0
			curPutsPerSec = 0
			access.Unlock()
		}
	}
}
