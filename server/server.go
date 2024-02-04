/*
Package server configures and launches http/rpc server and storage engines specific
to the type of DVID platform: local (e.g., running on MacBook Pro), clustered, or
using cloud-based services like Google Cloud.

Datatypes can use any of the three tiers of storage (MetaData, Mutable, Immutable)
that provide a layer of storage semantics (latency, mutability, etc) on top of
underlying storage engines.

The DVID web client is also managed from this package.	For a DVID web console, see the
repo:

https://github.com/janelia-flyem/dvid-console

The goal of a DVID web console is to provide a GUI for monitoring and performing
a subset of operations in a nicely formatted view.

DVID command line interaction occurs via the rpc interface to a running server.
Please see the main DVID documentation:

http://godoc.org/github.com/janelia-flyem/dvid
*/
package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	// MaxDataRequest sets the limit on the amount of data that could be returned for a request
	MaxDataRequest = int64(3) * dvid.Giga

	// InteractiveOpsPer2Min gives the number of interactive-level requests
	// received over the last 2 minutes.  This is useful for throttling "batch"
	// operations on a single DVID server.  Note that this metric is an lower
	// bound on the number of interactive requests over the last minute since
	// we do non-blocking reports.
	InteractiveOpsPer2Min int

	// Channel to track the # of interactive requests.
	interactiveOpsCh = make(chan bool)

	// Current tally of interactive requests over last 5 minutes in bins of 5 seconds,
	// where index 0 = current time bin.
	interactiveOps = make([]int, 60)

	// ActiveHandlers is maximum number of active handlers over last second.
	ActiveHandlers int

	// Running tally of active handlers up to the last second
	curActiveHandlers int

	// MaxChunkHandlers sets the maximum number of chunk handlers (goroutines) that
	// can be multiplexed onto available cores.  (See -numcpu setting in dvid.go)
	MaxChunkHandlers = 10 * runtime.NumCPU()

	// HandlerToken is buffered channel to limit spawning of goroutines.
	// See ProcessChunk() in datatype/imageblk for example.
	HandlerToken = make(chan int, MaxChunkHandlers)

	// LargeMutationMutex is a global lock for compute-intense processes that want to
	// spawn goroutines that consume handler tokens.  This lets processes capture most
	// if not all available handler tokens in a FIFO basis rather than have multiple
	// concurrent requests launch a few goroutines each.
	LargeMutationMutex sync.Mutex

	// TimeoutSecs specifies the seconds waiting to open a datastore for exclusive access.
	TimeoutSecs int

	// maxThrottledOps sets the maximum number of concurrent CPU-heavy ops that can be
	// performed on this server when requests are submitted using "throttled=true" query
	// strings.  See imageblk and labelblk 3d GET/POST voxel requests.
	maxThrottledOps = 1

	// curThrottleOps is the current number of CPU-heavy ops being performed on the server.
	curThrottledOps int
	curThrottleMu   sync.Mutex

	// Keep track of the startup time for uptime.
	startupTime = time.Now()

	// Read-only mode ignores all HTTP requests but GET and HEAD
	readonly bool

	// Full write mode allows writes to all nodes, even committed ones.
	fullwrite bool

	// Monitor mode writes load data to debug log if chunk handlers are active.
	monitor bool

	// gitVersion is a git-derived string that allows recovery of the exact source code
	// used for this version of the DVID server.  This package variable is exposed through
	// the read-only Version() function and is set by build-time code generation using
	// the "git describe" command.  See the CMakeLists.txt file for the code generation.
	gitVersion string

	// kafka topic to publish activity for this server
	kafkaActivityTopic string

	initialized bool

	// signals when we should shutdown server.
	shutdownCh chan struct{}
)

const defaultGCPercent = 400

func init() {
	shutdownCh = make(chan struct{})

	// Set the GC closer to old Go 1.4 setting
	// old := debug.SetGCPercent(defaultGCPercent)

	// Initialize the number of handler tokens available.
	for i := 0; i < MaxChunkHandlers; i++ {
		HandlerToken <- 1
	}

	// Monitor the handler token load, resetting every second and possibly writing stats to log.
	loadCheckTimer := time.Tick(10 * time.Millisecond)
	ticks := 0
	go func() {
		for {
			<-loadCheckTimer
			ticks = (ticks + 1) % 100
			if ticks == 0 {
				ActiveHandlers = curActiveHandlers
				curActiveHandlers = 0
				if monitor {
					go monitorLoad()
				}
			}
			numHandlers := MaxChunkHandlers - len(HandlerToken)
			if numHandlers > curActiveHandlers {
				curActiveHandlers = numHandlers
			}
		}
	}()

	// Monitor the # of interactive requests over last 2 minutes.
	reqCheckTimer := time.Tick(5 * time.Second)
	go func() {
		for {
			select {
			case <-interactiveOpsCh:
				interactiveOps[0]++
			case <-reqCheckTimer:
				newCount := InteractiveOpsPer2Min - interactiveOps[23] + interactiveOps[0]
				InteractiveOpsPer2Min = newCount
				copy(interactiveOps[1:], interactiveOps[:23])
				interactiveOps[0] = 0
			}
		}
	}()
}

func monitorLoad() {
	if ActiveHandlers > 0 || storage.GetsPerSec > 0 || storage.PutsPerSec > 0 {
		dvid.Debugf("Load: handlers=%d, GETs=%d, PUT=%d, keyR=%d, keyW=%d, valR=%d, valW=%d\n",
			ActiveHandlers, storage.GetsPerSec, storage.PutsPerSec,
			storage.StoreKeyBytesReadPerSec, storage.StoreKeyBytesWrittenPerSec,
			storage.StoreValueBytesReadPerSec, storage.StoreValueBytesWrittenPerSec)
	}
}

func CheckChunkThrottling() {
	<-HandlerToken
}

// if a pidfile is specified in the [server] config TOML, make sure it doesn't
// contain a working process, then write the current pid.
func writePidFile() error {
	if tc.Server.PidFile == "" {
		return nil
	}
	pidbytes, err := ioutil.ReadFile(tc.Server.PidFile)
	if os.IsExist(err) {
		pid, err := strconv.Atoi(string(pidbytes))
		if err != nil {
			return err
		}
		if _, err := os.FindProcess(pid); err == nil {
			return fmt.Errorf("pid %d found in Pid-File %q is still running; aborting", pid, tc.Server.PidFile)
		}
	}
	dirPath := filepath.Dir(tc.Server.PidFile)
	if dirPath != "" {
		_, err := os.Stat(dirPath)
		if os.IsNotExist(err) {
			err := os.MkdirAll(dirPath, 0755)
			if err != nil {
				return err
			}
		}
	}
	pid := os.Getpid()
	pidbytes = []byte(strconv.Itoa(pid))
	return ioutil.WriteFile(tc.Server.PidFile, pidbytes, 0664)
}

func deletePidFile() error {
	if tc.Server.PidFile == "" {
		return nil
	}
	return os.Remove(tc.Server.PidFile)
}

func setMaxThrottleOps(maxOps int) {
	curThrottleMu.Lock()
	maxThrottledOps = maxOps
	curThrottleMu.Unlock()
}

// GitVersion returns a git-derived string that allows recovery of the exact source code
// used for this DVID server.
func GitVersion() string {
	return gitVersion
}

// GotInteractiveRequest can be called to track the # of interactive requests that
// require some amount of computation.  Don't use this to track simple polling APIs.
// This routine will not block.
func GotInteractiveRequest() {
	select {
	case interactiveOpsCh <- true:
	default:
	}
}

// BlockOnInteractiveRequests will block this goroutine until the number of interactive
// requests dips below InteractiveOpsBeforeBlock in server configuration.
func BlockOnInteractiveRequests(caller ...string) {
	if tc.Server.InteractiveOpsBeforeBlock == 0 {
		return
	}
	for {
		if InteractiveOpsPer2Min < tc.Server.InteractiveOpsBeforeBlock {
			return
		}
		if len(caller) != 0 {
			dvid.Infof("Routine %q paused due to %d interactive requests over last 2 min.\n",
				caller[0], InteractiveOpsPer2Min)
		}
		time.Sleep(10 * time.Second)
	}
}

// SetReadOnly can put the server in a read-only mode.
func SetReadOnly(on bool) {
	readonly = on
	fullwrite = !on
}

// SetFullWrite allows mutations on any version.
func SetFullWrite(on bool) {
	fullwrite = on
	readonly = !on
}

// SetMonitor can put server in monitor mode (writes load stats to debug if activity).
func SetMonitor(on bool) {
	monitor = on
}

// SetAdminToken sets what can be passed in query string under "admintoken" to
// get admin (fullwrite) privileges.
func SetAdminToken(token string) {
	adminToken = token
}

// AboutJSON returns a JSON string describing the properties of this server.
func AboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"Cores":             fmt.Sprintf("%d", dvid.NumCPU),
		"Maximum Cores":     fmt.Sprintf("%d", runtime.NumCPU()),
		"Datastore Version": datastore.Version,
		"DVID Version":      gitVersion,
		"Storage backend":   storage.EnginesAvailable(),
		"Server time":       time.Now().String(),
		"Server uptime":     time.Since(startupTime).String(),
		"RPC Address":       RPCAddress(),
		"Host":              Host(),
		"Note":              Note(),
		"TOML location":     ConfigLocation(),
	}
	if readonly {
		data["Mode"] = "read only"
	}
	if fullwrite {
		data["Mode"] = "allow writes on committed nodes"
	}
	kservers := KafkaServers()
	if len(kservers) > 0 {
		data["Kafka Servers"] = strings.Join(kservers, ",")
		data["Kafka Activity Topic"] = KafkaActivityTopic()
	}
	if KafkaPrefixTopic() != "" {
		data["Kafka Topic Prefix"] = KafkaPrefixTopic()
	}

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		dvid.Errorf("Unable to get max # files from syscall: %v\n", err)
	} else {
		data["Max Open Files"] = strconv.FormatUint(limit.Cur, 10)
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// About returns a chart of version identifiers for the DVID source code, DVID datastore, and
// all component data types for this executable.
func About() string {
	var text = "\nCompile-time version information for this DVID executable:\n\n"
	writeLine := func(name dvid.TypeString, version string) {
		text += fmt.Sprintf("%-20s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID Version", gitVersion)
	writeLine("Datastore Version", datastore.Version)
	text += "\n"
	writeLine("Storage engines", storage.EnginesAvailable())
	writeLine("Max chunk handlers", fmt.Sprintf("%d", MaxChunkHandlers))
	for _, t := range datastore.Compiled {
		writeLine(t.GetTypeName(), t.GetTypeVersion())
	}
	return text
}

// Shutdown handles graceful cleanup of server functions before exiting DVID.
// This may not be so graceful if the chunk handler uses cgo since the interrupt
// may be caught during cgo execution.
func Shutdown() {
	// Stop accepting requests.
	dvid.DenyRequests()

	// Wait for chunk handlers.
	waits := 0
	for {
		active := MaxChunkHandlers - len(HandlerToken)
		if waits >= 20 {
			dvid.Infof("Already waited for 20 seconds.  Continuing with shutdown...")
			break
		} else if active > 0 {
			dvid.Infof("Waiting for %d chunk handlers to finish...\n", active)
			waits++
		} else {
			dvid.Infof("No chunk handlers active. Proceeding...\n")
			break
		}
		time.Sleep(1 * time.Second)
	}
	if tc.Server.ShutdownDelay > 0 {
		dvid.Infof("Waiting %d seconds for any HTTP requests to drain...\n", tc.Server.ShutdownDelay)
		time.Sleep(time.Duration(tc.Server.ShutdownDelay) * time.Second)
	}
	datastore.Shutdown()
	dvid.BlockOnActiveCgo()
	storage.Shutdown()
	rpc.Shutdown()
	dvid.Shutdown()
	shutdownCh <- struct{}{}
	deletePidFile()
}
