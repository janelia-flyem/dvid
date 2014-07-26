package server

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	// Repos provides high-level repository management for DVID and is initialized
	// on start.
	Repos datastore.RepoManager

	// ActiveHandlers is maximum number of active handlers over last second.
	ActiveHandlers int

	// Running tally of active handlers up to the last second
	curActiveHandlers int

	// MaxChunkHandlers sets the maximum number of chunk handlers (goroutines) that
	// can be multiplexed onto available cores.  (See -numcpu setting in dvid.go)
	MaxChunkHandlers = runtime.NumCPU()

	// HandlerToken is buffered channel to limit spawning of goroutines.
	// See ProcessChunk() in datatype/voxels for example.
	HandlerToken = make(chan int, MaxChunkHandlers)

	// Throttle allows server-wide throttling of operations.  This is used for voxels-based
	// compute-intensive operations on constrained servers.
	// TODO: This should be replaced with message queue mechanism for prioritized requests.
	Throttle = make(chan int, MaxThrottledOps)

	// SpawnGoroutineMutex is a global lock for compute-intense processes that want to
	// spawn goroutines that consume handler tokens.  This lets processes capture most
	// if not all available handler tokens in a FIFO basis rather than have multiple
	// concurrent requests launch a few goroutines each.
	SpawnGoroutineMutex sync.Mutex

	// Timeout in seconds for waiting to open a datastore for exclusive access.
	TimeoutSecs int

	// Keep track of the startup time for uptime.
	startupTime time.Time = time.Now()
)

type storageManager struct {
	// True if SetupEngines() has been called
	setup bool

	// Cached type-asserted interfaces
	kvEngine    Engine
	kvDB        OrderedKeyValueDB
	kvSetter    OrderedKeyValueSetter
	kvGetter    OrderedKeyValueGetter
	graphEngine Engine
	graphDB     GraphDB
	graphSetter GraphSetter
	graphGetter GraphGetter
}

func init() {
	// Initialize the number of throttled ops available.
	for i := 0; i < MaxThrottledOps; i++ {
		Throttle <- 1
	}

	// Initialize the number of handler tokens available.
	for i := 0; i < MaxChunkHandlers; i++ {
		HandlerToken <- 1
	}

	// Monitor the handler token load, resetting every second.
	loadCheckTimer := time.Tick(10 * time.Millisecond)
	ticks := 0
	go func() {
		for {
			<-loadCheckTimer
			ticks = (ticks + 1) % 100
			if ticks == 0 {
				ActiveHandlers = curActiveHandlers
				curActiveHandlers = 0
			}
			numHandlers := MaxChunkHandlers - len(HandlerToken)
			if numHandlers > curActiveHandlers {
				curActiveHandlers = numHandlers
			}
		}
	}()

}

// Shutdown handles graceful cleanup of server functions before exiting DVID.
// This may not be so graceful if the chunk handler uses cgo since the interrupt
// may be caught during cgo execution.
func Shutdown() {
	waits := 0
	for {
		active := MaxChunkHandlers - len(HandlerToken)
		if waits >= 20 {
			log.Printf("Already waited for 20 seconds.  Continuing with shutdown...")
			break
		} else if active > 0 {
			log.Printf("Waiting for %d chunk handlers to finish...\n", active)
			waits++
		} else {
			log.Println("No chunk handlers active...")
			break
		}
		time.Sleep(1 * time.Second)
	}
	storage.Shutdown()
	Repos.
		dvid.BlockOnActiveCgo()
}
