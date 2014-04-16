/*
	Thie file implements a monitor for various operations.  It exposes channels
	that can be used to track I/O bandwidth.
*/

package storage

import "time"

const MonitorBuffer = 10000

var (
	// Number of bytes read in last second from storage engine.
	StoreKeyBytesReadPerSec int

	// Number of bytes written in last second to storage engine.
	StoreKeyBytesWrittenPerSec int

	// Number of bytes read in last second from storage engine.
	StoreValueBytesReadPerSec int

	// Number of bytes written in last second to storage engine.
	StoreValueBytesWrittenPerSec int

	// Number of bytes read in last second from file system.
	FileBytesReadPerSec int

	// Number of bytes written in last second to file system.
	FileBytesWrittenPerSec int

	// Number of key-value GET calls in last second.
	GetsPerSec int

	// Number of key-value PUT calls in last second.
	PutsPerSec int

	// Channel to notify bytes read from a storage engine.
	StoreKeyBytesRead chan int

	// Channel to notify bytes written to a storage engine.
	StoreKeyBytesWritten chan int

	// Channel to notify bytes read from a storage engine.
	StoreValueBytesRead chan int

	// Channel to notify bytes written to a storage engine.
	StoreValueBytesWritten chan int

	// Channel to notify bytes read from file system.
	FileBytesRead chan int

	// Channel to notify bytes written to file system.
	FileBytesWritten chan int

	// Current tallies up to a second.
	storeKeyBytesReadPerSec      int
	storeKeyBytesWrittenPerSec   int
	storeValueBytesReadPerSec    int
	storeValueBytesWrittenPerSec int
	fileBytesReadPerSec          int
	fileBytesWrittenPerSec       int
	getsPerSec                   int
	putsPerSec                   int
)

func init() {
	StoreKeyBytesRead = make(chan int, MonitorBuffer)
	StoreKeyBytesWritten = make(chan int, MonitorBuffer)
	StoreValueBytesRead = make(chan int, MonitorBuffer)
	StoreValueBytesWritten = make(chan int, MonitorBuffer)
	FileBytesRead = make(chan int, MonitorBuffer)
	FileBytesWritten = make(chan int, MonitorBuffer)

	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	for {
		select {
		case b := <-StoreKeyBytesRead:
			storeKeyBytesReadPerSec += b
		case b := <-StoreKeyBytesWritten:
			storeKeyBytesWrittenPerSec += b
		case b := <-StoreValueBytesRead:
			storeValueBytesReadPerSec += b
			getsPerSec++
		case b := <-StoreValueBytesWritten:
			storeValueBytesWrittenPerSec += b
			putsPerSec++
		case b := <-FileBytesRead:
			fileBytesReadPerSec += b
		case b := <-FileBytesWritten:
			fileBytesWrittenPerSec += b
		case <-secondTick:
			FileBytesReadPerSec = fileBytesReadPerSec
			FileBytesWrittenPerSec = fileBytesWrittenPerSec
			fileBytesReadPerSec = 0
			fileBytesWrittenPerSec = 0

			StoreKeyBytesReadPerSec = storeKeyBytesReadPerSec
			StoreKeyBytesWrittenPerSec = storeKeyBytesWrittenPerSec
			storeKeyBytesReadPerSec = 0
			storeKeyBytesWrittenPerSec = 0

			StoreValueBytesReadPerSec = storeValueBytesReadPerSec
			StoreValueBytesWrittenPerSec = storeValueBytesWrittenPerSec
			storeValueBytesReadPerSec = 0
			storeValueBytesWrittenPerSec = 0

			GetsPerSec = getsPerSec
			PutsPerSec = putsPerSec
			getsPerSec = 0
			putsPerSec = 0
		}
	}
}
