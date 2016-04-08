/*
	This file supports DVID distributed systems and goroutines.
*/

package dvid

import (
	"strings"
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

// Filter is a string specification of type-specific filters to apply to key-value pairs
// before sending them to a remote DVID.  For example, a Filter could look like:
//
//    roi:seven_column,3f8a/tile:xy,xz
//
// The above specifies two filters joined by a forward slash.  The first is an "roi" filters
// that lists a ROI data instance name ("seven_column") and its version as a partial, unique
// UUID.  The second is a "tile" filter that specifies two types of tile plane: xy and xz.
type Filter string

// GetFilter parses a Filter and returns the value of the given filter type.
// If no filter of ftype is available, the second argument is false.
func (f Filter) GetFilter(ftype string) (value string, found bool) {
	// Separate filters.
	fs := strings.Split(string(f), "/")
	if len(fs) == 0 {
		return
	}

	// Scan each filter to see if its the given ftype.
	for _, filter := range fs {
		parts := strings.Split(filter, ":")
		if parts[0] == ftype {
			if len(parts) != 2 {
				return
			}
			return parts[1], true
		}
	}
	return
}

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

// BlockOnActiveCgo will block until all active cgo routines have been finished or
// queued for starting.  This requires cgo routines to be bracketed by:
//    dvid.StartCgo()
//    /* Some cgo code */
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
