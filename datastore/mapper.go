/*
	Mapper is a component for map/reduce operations that you can add to
	data types to implement block-level concurrency.
*/

package datastore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Operation provides an abstract interface to datatype-specific operations.
// Each data type will implement operations that are mostly opaque at this level.
type Operation interface {
	DatasetService

	DatastoreService() *Service

	VersionLocalID() dvid.LocalID

	// Does this operation only read data?
	IsReadOnly() bool

	// Returns an IndexIterator that steps through all chunks for this operation.
	IndexIterator() IndexIterator

	// Map traverses chunks in an operation and maps it to dataset-specific chunk handlers.
	// Types that implement the Operation interface can use datastore.Map() to fulfill
	// this method or implement their own mapping scheme.
	Map()

	// WaitAdd increments an internal WaitGroup
	WaitAdd()

	// Wait blocks until the operation is complete
	Wait()
}

// Constants that allow tuning of DVID for a particular target computer.
const (
	// Default number of block handlers to use per dataset.
	DefaultNumChunkHandlers = 8

	// Number of chunk write requests that can be buffered on each block handler
	// before sender is blocked.
	ChunkHandlerBufferSize = 100000
)

type ChunkOp struct {
	Operation
	Chunk    []byte
	Key      storage.Key
	shutdown bool
}

// Each dataset has a pool of channels to communicate with gouroutine handlers.
type ChunkChannels map[DatasetString]([]chan *ChunkOp)

// Track requested/completed block ops
type loadStruct struct {
	Requests  int
	Completed int
}
type loadMap map[DatasetString]loadStruct

var (
	chunkChannels ChunkChannels

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
	chunkChannels = make(ChunkChannels)
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

// StartChunkHandlers starts goroutines that handle chunks for every dataset in
// the service.
func (s *Service) StartChunkHandlers() {
	for _, dataset := range s.Datasets {
		startChunkHandlers(dataset)
	}
}

// startChunkHandler makes sure we have chunk handler goroutines for each
// data set.  Chunks are routed to the same handler each time, so concurrent
// access to a chunk by multiple requests are funneled sequentially into a
// channel reserved for that chunk's handler.
func startChunkHandlers(dataset DatasetService) {
	datasetName := dataset.DatasetName()

	loadAccess.Lock()
	loadLastSec[datasetName] = loadStruct{}
	loadAccess.Unlock()

	var channelMapAccess sync.Mutex
	channelMapAccess.Lock()
	// Do we have channels and handlers for this type and image version?
	_, found := chunkChannels[datasetName]
	if !found {
		log.Printf("Starting %d block handlers for data set '%s' (%s)...\n",
			dataset.NumChunkHandlers(), datasetName, dataset.DatatypeName)
		channels := make([]chan *ChunkOp, 0, dataset.NumChunkHandlers())
		for i := 0; i < dataset.NumChunkHandlers(); i++ {
			channel := make(chan *ChunkOp, ChunkHandlerBufferSize)
			channels = append(channels, channel)
			go func(i int, c chan *ChunkOp) {
				dvid.Log(dvid.Debug, "Starting chunk handler %d for %s...",
					i+1, datasetName)
				for {
					chunkOp := <-c
					if chunkOp == nil {
						log.Fatalln("Received nil chunk in chunk handler!")
					}
					if chunkOp.shutdown {
						dvid.Log(dvid.Debug, "Shutting down chunk handler %d for %s...",
							i+1, datasetName)
						break
					}
					//dvid.Fmt(dvid.Debug, "Running handler on block %x...\n", block.IndexKey)
					dataset.ChunkHandler(chunkOp)
					doneChannel <- datasetName
				}
			}(i, channel)
			// TODO -- keep stats on # of handlers
		}
		chunkChannels[datasetName] = channels
	}
	channelMapAccess.Unlock()
}

func stopChunkHandlers(dataset DatasetService) {
	// Remove load stat cache
	loadAccess.Lock()
	delete(loadLastSec, dataset.DatasetName())
	loadAccess.Unlock()

	// Send shutdown op to all handlers for this dataset
	var haltOp ChunkOp
	haltOp.shutdown = true
	for _, c := range chunkChannels[dataset.DatasetName()] {
		c <- &haltOp
	}
}

// ChunkLoadJSON returns a JSON description of the chunk op requests for each dataset.
func ChunkLoadJSON() (jsonStr string, err error) {
	loadAccess.RLock()
	m, err := json.Marshal(loadLastSec)
	loadAccess.RUnlock()
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// Map performs any sequential read, then breaks down an operation by sending chunks
// across a range of datatype-specific handlers that concurrently process the data.
// Each handler is assigned the same chunks based on its index.
func Map(op Operation) error {

	datasetID := op.DatasetLocalID()
	versionID := op.VersionLocalID()

	// Make sure our backend database can handle necessary interfaces
	service := op.DatastoreService()
	itermaker, err := service.IteratorMaker()
	if err != nil {
		return err
	}

	// Make sure we have Chunk Handlers for this dataset.
	channels, found := chunkChannels[op.DatasetName()]
	if !found {
		return fmt.Errorf("Error in reserving block handlers in MapBlocks() for %s!",
			op.DatasetName())
	}

	// Traverse chunks, get key/values if not in cache, and put chunk op in queue for handler.
	kvIterator, err := itermaker.NewIterator()
	defer kvIterator.Close()
	if err != nil {
		return err
	}
	indexIterator := op.IndexIterator()
	seek := true

	//dvid.Fmt(dvid.Debug, "Mapping blocks for %s\n", data)
	DiskAccess.Lock()
	for {
		index := indexIterator()
		if index == nil {
			break
		}
		key := storage.Key{datasetID, versionID, index.Bytes()}

		// Pull from the datastore
		if seek || (kvIterator.Valid() && !bytes.Equal(kvIterator.Key(), key.Bytes())) {
			kvIterator.Seek(key)
			seek = false
		}
		var value []byte
		if kvIterator.Valid() && bytes.Equal(kvIterator.Key(), key.Bytes()) {
			value = kvIterator.Value()
			kvIterator.Next()
		} else {
			seek = true
			// If this is a put, don't continue since we need to write this key.
			// If this is a get, we have no value for this key so continue.
			if op.IsReadOnly() {
				continue // Simply uses zero value
			}
		}

		// Try to spread sequential block keys among different block handlers to get
		// most out of our concurrent processing.
		op.WaitAdd()
		channelNum := index.Hash(len(channels))

		//dvid.Fmt(dvid.Debug, "Sending %s block %s request %s down channel %d\n",
		//	op, Index(indexBytes).BlockCoord(data), data, channelNum)

		channels[channelNum] <- &ChunkOp{op, value, key, false}
		requestChannel <- op.DatasetName()
	}
	DiskAccess.Unlock()
	return nil
}
