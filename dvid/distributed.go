/*
	This file supports DVID distributed systems and goroutines.
*/

package dvid

import (
	"sync"
	"time"
)

type cgoStatus int

const (
	MaxNumberConcurrentCgo = 10000

	cgoStarted cgoStatus = 1
	cgoStopped cgoStatus = -1
)

var (
	// CgoActive is a buffered channel for signaling cgo routines that are active.
	cgoActive    chan cgoStatus
	cgoActive_mu sync.RWMutex

	cgoNumActive int
	startCgo     sync.Mutex
)

func init() {
	// Create CgoActive channel to keep tract of the # of active cgo routines.
	// This is useful for graceful shutdown of DVID when there are outstanding
	// goroutines using cgo.
	cgoActive = make(chan cgoStatus, MaxNumberConcurrentCgo)
	go func() {
		for {
			switch <-cgoActive {
			case cgoStarted:
				cgoActive_mu.Lock()
				cgoNumActive++
				cgoActive_mu.Unlock()
			case cgoStopped:
				cgoActive_mu.Lock()
				cgoNumActive--
				cgoActive_mu.Unlock()
			}
		}
	}()
}

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
		if (cgoNumActive == 0 && len(cgoActive) == 0) || waits >= 10 {
			return
		}
		Infof("Waited %d seconds for %d active cgo routines (%d messages to be processed)...\n",
			waits, cgoNumActive, len(cgoActive))
		waits++
		time.Sleep(1 * time.Second)
	}
}

// StartCgo is used to mark the beginning of a cgo routine and blocks if we have requested
// a BlockOnActiveCgo().  This is used for graceful shutdowns and monitoring.
func StartCgo() {
	startCgo.Lock()
	cgoActive <- cgoStarted
	startCgo.Unlock()
}

func StopCgo() {
	cgoActive <- cgoStopped
}

// NumberActiveCGo returns the number of active CGo routines, typically involved with database operations.
func NumberActiveCGo() int {
	cgoActive_mu.RLock()
	defer cgoActive_mu.RUnlock()

	return cgoNumActive
}
