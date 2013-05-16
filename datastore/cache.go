/*
	This file holds caching and buffering for datastore operation.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	"log"
	_ "os"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Constants that allow tuning of DVID for a particular target computer.
const (
	// Default size of LRU cache for DVID data blocks in MB.  The larger this number,
	// the fewer disk accesses you'll need.
	DefaultCacheMBytes = 512

	// Default number of block handlers to use per data type. 
	DefaultNumBlockHandlers = 8

	// NumBlockHandlers sets the number of processors we have for our "map" operation.
	NumBlockHandlers = 8

	// Number of block write requests that can be buffered on each block handler
	// before sender is blocked.  
	// This constant * sizeof(request struct) * NumBlockHandlers
	// should be a reasonable number for the target DVID computer.
	// The request struct contains two pointers.
	BlockHandlerBufferSize = 100000
)

type OpType uint8

const (
	GetOp OpType = iota
	PutOp
)

func (op OpType) String() string {
	switch op {
	case GetOp:
		return "GET"
	case PutOp:
		return "PUT"
	}
	return fmt.Sprintf("Illegal Op (%d)", op)
}

type OpResult string

// Block is the unit of get/put for each data type.  We typically decompose a
// larger structure (DataStruct) into Blocks, process each Block separately
// by a handler assigned for each spatial index, and then let the requestor 
// know when all the processing is done via a sync.WaitGroup.
//
// TODO -- Generalize "Op" since we don't care what Op is as long as the type
// can do it.  So want pass along from type's parsing of request to block handler.
type BlockRequest struct {
	// The larger data structure that we're going to fill in using blocks.
	// This may be a slice and thinner than the blocks it intersects.
	DataStruct

	// Block holds the data for a block, a small rectangular volume of voxels.
	Block storage.Value

	// Parameters for this particular block
	Op       OpType
	IndexKey Index
	BlockKey storage.Key

	// Let's us notify requestor when all blocks are done.
	Wait *sync.WaitGroup

	DB storage.DataHandler
}

// Each data type has a pool of channels to communicate with block handlers. 
type BlockChannels map[DataSetString]([]chan *BlockRequest)

// Track requested/completed block ops
type loadStruct struct {
	Requests  int
	Completed int
}
type loadMap map[DataSetString]loadStruct

var (
	// HandlerChannels are map from data type names to a pool of block handler
	// goroutines.  See the function ReserveBlockHandlers.
	HandlerChannels BlockChannels

	// DiskAccess is a mutex to make sure we don't have goroutines simultaneously trying
	// to access the key-value database on disk.
	// TODO: Reexamine this in the context of parallel disk drives during cluster use.
	DiskAccess sync.Mutex

	// Monitor the requested and completed block ops
	loadLastSec    loadMap
	loadAccess     sync.RWMutex
	doneChannel    chan DataSetString
	requestChannel chan DataSetString
)

func init() {
	HandlerChannels = make(BlockChannels)
	loadLastSec = make(loadMap)
	doneChannel = make(chan DataSetString)
	requestChannel = make(chan DataSetString)
	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	requests := make(map[DataSetString]int)
	completed := make(map[DataSetString]int)
	for {
		select {
		case name := <-doneChannel:
			completed[name]++
		case name := <-requestChannel:
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

// ReserveBlockHandlers makes sure we have block handler goroutines for each
// data set.  Blocks are routed to the same handler each time, so concurrent
// access to a block by multiple requests are funneled sequentially into a 
// channel reserved for that block's handler.
func ReserveBlockHandlers(name DataSetString, t TypeService) {
	loadAccess.Lock()
	loadLastSec[name] = loadStruct{}
	loadAccess.Unlock()

	var channelMapAccess sync.Mutex
	channelMapAccess.Lock()
	// Do we have channels and handlers for this type and image version?
	_, found := HandlerChannels[name]
	if !found {
		log.Printf("Starting %d block handlers for data set '%s' (%s)...\n",
			t.NumBlockHandlers(), name, t.TypeName())
		channels := make([]chan *BlockRequest, 0, t.NumBlockHandlers())
		for i := 0; i < t.NumBlockHandlers(); i++ {
			channel := make(chan *BlockRequest, BlockHandlerBufferSize)
			channels = append(channels, channel)
			go func(i int, c chan *BlockRequest) {
				dvid.Log(dvid.Debug, "Starting block handler %d for %s...",
					i+1, name)
				for {
					block := <-c
					if block == nil {
						log.Fatalln("Received nil block in block handler!")
					}
					//dvid.Fmt(dvid.Debug, "Running handler on block %x...\n", block.IndexKey)
					block.DataStruct.BlockHandler(block)
					doneChannel <- name
				}
			}(i, channel)
			// TODO -- keep stats on # of handlers
		}
		HandlerChannels[name] = channels
	}
	channelMapAccess.Unlock()
}

// BlockLoadJSON returns a JSON description of the block requests for each dataset.
func BlockLoadJSON() (jsonStr string, err error) {
	loadAccess.RLock()
	m, err := json.Marshal(loadLastSec)
	loadAccess.RUnlock()
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}
