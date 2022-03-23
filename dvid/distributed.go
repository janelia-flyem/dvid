/*
	This file supports DVID distributed systems and goroutines.
*/

package dvid

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	cgoNumActive uint64
	startCgo     sync.Mutex
)

// BlockOnActiveCgo will block until all active writing cgo routines have been finished.
// This requires cgo routines to be bracketed by:
//    dvid.StartCgo()
//    /* Some cgo code that's writing */
//    dvid.StopCgo()
func BlockOnActiveCgo() {
	startCgo.Lock()
	defer startCgo.Unlock()

	Infof("Checking for any active cgo routines...\n")
	waits := 0
	for {
		if cgoNumActive == 0 || waits >= 10 {
			return
		}
		Infof("Waited %d seconds for %d active cgo routines ...\n", waits, cgoNumActive)
		waits++
		time.Sleep(1 * time.Second)
	}
}

// StartCgo is used to mark the beginning of a cgo routine and blocks if we have requested
// a BlockOnActiveCgo().  This is used for graceful shutdowns and monitoring.
func StartCgo() {
	startCgo.Lock()
	atomic.AddUint64(&cgoNumActive, 1)
	startCgo.Unlock()
}

func StopCgo() {
	atomic.AddUint64(&cgoNumActive, ^uint64(0)) // decrements by 1
}

// NumberActiveCGo returns the number of active CGo routines, typically involved with database operations.
func NumberActiveCGo() uint64 {
	return cgoNumActive
}
