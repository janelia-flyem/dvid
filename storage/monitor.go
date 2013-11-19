/*
	Thie file implements a monitor for various operations.  It exposes channels
	that can be used to track I/O bandwidth.
*/

package storage

import (
	"sync"
	"time"
)

const MonitorBuffer = 10000

var (
	// Number of bytes read in last second from storage engine.
	StoreBytesReadPerSec int

	// Number of bytes written in last second to storage engine.
	StoreBytesWrittenPerSec int

	// Number of bytes read in last second from file system.
	FileBytesReadPerSec int

	// Number of bytes written in last second to file system.
	FileBytesWrittenPerSec int

	// Number of key-value GET calls in last second.
	GetsPerSec int

	// Number of key-value PUT calls in last second.
	PutsPerSec int

	// Channel to notify bytes read from a storage engine.
	StoreBytesRead chan int

	// Channel to notify bytes written to a storage engine.
	StoreBytesWritten chan int

	// Channel to notify bytes read from file system.
	FileBytesRead chan int

	// Channel to notify bytes written to file system.
	FileBytesWritten chan int

	// Current tallies up to a second.
	storeBytesReadPerSec    int
	storeBytesWrittenPerSec int
	fileBytesReadPerSec     int
	fileBytesWrittenPerSec  int
	getsPerSec              int
	putsPerSec              int
)

func init() {
	StoreBytesRead = make(chan int, MonitorBuffer)
	StoreBytesWritten = make(chan int, MonitorBuffer)
	FileBytesRead = make(chan int, MonitorBuffer)
	FileBytesWritten = make(chan int, MonitorBuffer)

	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	var access sync.Mutex
	for {
		select {
		case b := <-StoreBytesRead:
			storeBytesReadPerSec += b
			getsPerSec++
		case b := <-StoreBytesWritten:
			storeBytesWrittenPerSec += b
			putsPerSec++
		case b := <-FileBytesRead:
			fileBytesReadPerSec += b
		case b := <-FileBytesWritten:
			fileBytesWrittenPerSec += b
		case <-secondTick:
			access.Lock()

			FileBytesReadPerSec = fileBytesReadPerSec
			FileBytesWrittenPerSec = fileBytesWrittenPerSec
			fileBytesReadPerSec = 0
			fileBytesWrittenPerSec = 0

			StoreBytesReadPerSec = storeBytesReadPerSec
			StoreBytesWrittenPerSec = storeBytesWrittenPerSec
			storeBytesReadPerSec = 0
			storeBytesWrittenPerSec = 0

			GetsPerSec = getsPerSec
			PutsPerSec = putsPerSec
			getsPerSec = 0
			putsPerSec = 0

			access.Unlock()
		}
	}
}
