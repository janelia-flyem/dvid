/*
	Mapper is a component for map/reduce operations that you can add to 
	data types to implement block-level concurrency.
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

// Operation provides an abstract interface to datatype-specific operations.
// Each data type will implement operations that are mostly opaque at this level.
type Operation interface {
	TypeService
	Data() interface{}
	DatasetName() DatasetString
	WaitGroup() *sync.WaitGroup
}

// Mapper administrates map/reduce operations for data types.
type Mapper struct {
	op         Operation
	blockChans []chan *BlockRequest
}

// Constants that allow tuning of DVID for a particular target computer.
const (
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

// Mapper can delegate

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
type BlockChannels map[DatasetString]([]chan *BlockRequest)

// Track requested/completed block ops
type loadStruct struct {
	Requests  int
	Completed int
}
type loadMap map[DatasetString]loadStruct

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
	doneChannel    chan DatasetString
	requestChannel chan DatasetString
)

func init() {
	HandlerChannels = make(BlockChannels)
	loadLastSec = make(loadMap)
	doneChannel = make(chan DatasetString)
	requestChannel = make(chan DatasetString)
	go loadMonitor()
}

// Monitors the # of requests/done on block handlers per data set.
func loadMonitor() {
	secondTick := time.Tick(1 * time.Second)
	requests := make(map[DatasetString]int)
	completed := make(map[DatasetString]int)
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
func ReserveBlockHandlers(t TypeService, name DatasetString, numHandlers int) {
	loadAccess.Lock()
	loadLastSec[name] = loadStruct{}
	loadAccess.Unlock()

	var channelMapAccess sync.Mutex
	channelMapAccess.Lock()
	// Do we have channels and handlers for this type and image version?
	_, found := HandlerChannels[name]
	if !found {
		log.Printf("Starting %d block handlers for data set '%s' (%s)...\n",
			t.NumBlockHandlers(), name, t.DatatypeName())
		channels := make([]chan *BlockRequest, 0, numHandlers)
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

// MapBlocks breaks down a DataStruct into a sequence of blocks that can be
// handled concurrently by datatype-specific block handers.  Each block handler
// is assigned the same blocks based on its index.
//
// Phase 1: Time leveldb built-in LRU cache and write buffer. (current)
// Phase 2: Minimize leveldb built-in LRU cache and use DVID LRU cache with
//   periodic and on-demand writes. 
// TODO -- Examine possible interleaving of block-level requests across MapBlocks()
//   calls and its impact on GET requests fulfilled while some blocks are still being
//   modified.
func (op *Operation) MapBlocks(data DataStruct) error {

	// Get compressed dataset index
	datasetBytes, err := vs.DataIndexBytes(data.DatasetName())
	if err != nil {
		return err
	}

	// Make sure we have Block Handlers for this data type.
	channels, found := HandlerChannels[data.DatasetName()]
	if !found {
		return fmt.Errorf("Error in reserving block handlers in MapBlocks() for %s!",
			data.DatasetName())
	}

	// Traverse blocks, get key/values if not in cache, and put block in queue for handler.
	ro := keyvalue.NewReadOptions()
	kvIterator, err := vs.kvdb.NewIterator(ro)
	defer kvIterator.Close()
	if err != nil {
		return err
	}
	indexIterator := NewZYXIterator(data)
	seek := true

	//dvid.Fmt(dvid.Debug, "Mapping blocks for %s\n", data)
	DiskAccess.Lock()
	for {
		index := indexIterator()
		if index == nil {
			break
		}
		key := storage.Key{
			DatasetKey: data.DatasetName(),
			VersionKey: op.VersionBytes(),
			CompressedDataset: datasetBytes,
		}
		// blockKey := BlockKey(uuidBytes, indexBytes, datatypeBytes, data.IsolatedKeys())

		// Pull from the datastore
		if seek || (kvIterator.Valid() && string(kvIterator.Key()) != key.Bytes()) {
			kvIterator.Seek(key)
			seek = false
		}
		var value keyvalue.Value
		if kvIterator.Valid() && string(kvIterator.Key()) == key.Bytes() {
			value = kvIterator.Value()
			kvIterator.Next()
		} else {
			seek = true
			if op.LoopOnNoKey {
				continue
			}
		}

		// Initialize the block request
		req := &BlockRequest{
			DataStruct: data,
			Block:      value,
			Op:         op,
			IndexKey:   Index(indexBytes),
			BlockKey:   blockKey,
			Wait:       wg,
			DB:         vs.kvdb,
			//WriteBatch: writeBatch,
		}

		// Try to spread sequential block keys among different block handlers to get 
		// most out of our concurrent processing.
		if op.wg != nil {
			op.wg.Add(1)
		}
		channelNum := req.IndexKey.Hash(data, len(channels))
		//dvid.Fmt(dvid.Debug, "Sending %s block %s request %s down channel %d\n",
		//	op, Index(indexBytes).BlockCoord(data), data, channelNum)
		channels[channelNum] <- req
		requestChannel <- data.DatasetName()
	}
	DiskAccess.Unlock()
	return nil
}
