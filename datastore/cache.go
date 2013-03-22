/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

import (
	"fmt"
	"log"
	_ "os"
	"sync"

	"github.com/janelia-flyem/dvid/cache"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
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

type uuidData struct {
	// The default version of the datastore
	Head UUID

	// Holds all UUIDs in open datastore.  When we construct keys, use the smaller
	// unique int per datastore instead of the full 16 byte value.  This can save
	// 14 bytes per key.
	Uuids map[string]int16
}

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
type BlockRequest struct {
	// The larger data structure that we're going to fill in using blocks.
	// This may be a slice and thinner than the blocks it intersects.
	DataStruct

	// Block holds the data for a block, a small rectangular volume of voxels.
	Block keyvalue.Value

	// Parameters for this particular block
	Op         OpType
	SpatialKey SpatialIndex
	BlockKey   keyvalue.Key

	// Let's us notify requestor when all blocks are done.
	Wait *sync.WaitGroup

	DB keyvalue.KeyValueDB

	// Include a WriteBatch so PUT ops can be batched
	//WriteBatch keyvalue.WriteBatch
}

// Each data type has a pool of channels to communicate with block handlers. 
type BlockChannels map[string]([]chan *BlockRequest)

// DiskAccess is a mutex to make sure we don't have goroutines simultaneously trying
// to access the key-value database on disk.
// TODO: Reexamine this in the context of parallel disk drives during cluster use.
var DiskAccess sync.Mutex

// Global variable that holds the LRU cache for DVID instance. 
var dataCache *cache.LRUCache

// Initialize the LRU cache
func InitDataCache(numBytes uint64) {
	dataCache = cache.NewLRUCache(numBytes)
}

// GetCachedBlock returns data for a given DVID Block if it's in the cache.
// Since the block key includes a UUID, this function can be used to access
// any image versions cached blocks. 
func GetCachedBlock(blockKey keyvalue.Key) (value keyvalue.Value, found bool) {
	data, found := dataCache.Get(string(blockKey))
	value = data.(keyvalue.Value)
	// TODO -- keep statistics on hits for web client
	return
}

// SetCachedBlock places a key/value pair into the DVID data cache. 
func SetCachedBlock(blockKey keyvalue.Key, data keyvalue.Value) {
	dataCache.Set(string(blockKey), data)
	// TODO -- keep stats on sets
}

// ReserveBlockHandlers makes sure we have block handler goroutines for each
// data type and each image version.  This makes sure that operations on a
// specific block can only be performed by the same handler. 
func (vs *VersionService) ReserveBlockHandlers(t TypeService) {
	// Do we have channels and handlers for this type and image version?
	channels, found := vs.channels[t.TypeName()]
	if !found {
		// Create channels and handlers
		channels := make([]chan *BlockRequest, 0, t.NumBlockHandlers())
		for i := 0; i < t.NumBlockHandlers(); i++ {
			channel := make(chan *BlockRequest, BlockHandlerBufferSize)
			channels = append(channels, channel)
			go func(i int, c chan *BlockRequest) {
				dvid.Log(dvid.Debug, "Starting block handler %d for %s...",
					i+1, t.TypeName())
				for {
					block := <-c
					if block == nil {
						log.Fatalln("Received nil block in block handler!")
					}
					//dvid.Fmt(dvid.Debug, "Running handler on block %x...\n", block.SpatialKey)
					block.DataStruct.BlockHandler(block)
				}
			}(i, channel)
			// TODO -- keep stats on # of handlers
		}
		vs.channels[t.TypeName()] = channels
	} else {
		dvid.Log(dvid.Debug, "Found %d block handlers for %s.", len(channels), t.TypeName())
	}
}

// MapBlocks breaks down a DataStruct into a sequence of blocks that can be
// efficiently read from the key-value database.
// Phase 1: Time leveldb built-in LRU cache and write buffer. (current)
// Phase 2: Minimize leveldb built-in LRU cache and use DVID LRU cache with
//   periodic and on-demand writes. 
// TODO -- Examine possible interleaving of block-level requests across MapBlocks()
//   calls and its impact on GET requests fulfilled while some blocks are still being
//   modified.
func (vs *VersionService) MapBlocks(op OpType, data DataStruct, wg *sync.WaitGroup) error {

	// Get components of the block key
	uuidBytes := vs.UuidBytes()
	datatypeBytes := vs.DataIndexBytes(data.TypeName())

	// Make sure we have Block Handlers for this data type.
	vs.ReserveBlockHandlers(data)
	channels, found := vs.channels[data.TypeName()]
	if !found {
		return fmt.Errorf("Error in reserving block handlers in MapBlocks() for %s!",
			data.TypeName())
	}

	// Traverse blocks, get key/values if not in cache, and put block in queue for handler.
	ro := keyvalue.NewReadOptions()
	db_it, err := vs.kvdb.NewIterator(ro)
	defer db_it.Close()
	if err != nil {
		return err
	}
	spatial_it := NewSpatialIterator(data)
	start := true
	var value keyvalue.Value

	dvid.Fmt(dvid.Debug, "Mapping blocks for %s\n", data)
	DiskAccess.Lock()
	for {
		spatialBytes := spatial_it()
		if spatialBytes == nil {
			break
		}
		blockKey := BlockKey(uuidBytes, spatialBytes, datatypeBytes, data.IsolatedKeys())
		dvid.Fmt(dvid.Debug, "Iterating on block key %s\n", blockKey)

		// Phase 2: Is this block in the cache?
		// value, found := GetCachedBlock(blockKey)

		// Pull from the datastore
		if start || db_it.Valid() && string(db_it.Key()) != string(blockKey) {
			if start {
				dvid.Fmt(dvid.Debug, "Seeking to key %s...\n", blockKey)
			} else {
				dvid.Fmt(dvid.Debug, "Seeking to key %s from %s...\n", blockKey,
					string(db_it.Key()))
			}
			db_it.Seek(blockKey)
			start = false
		}
		if db_it.Valid() {
			value = db_it.Value()
			dvid.Fmt(dvid.Debug, "Found valid value with length %d bytes\n", len(value))
			// Advance the database iterator
			db_it.Next()
			//SetCachedBlock(blockKey, value)
		} else {
			if op == GetOp {
				dvid.Fmt(dvid.Debug, "Invalid iterator on GET: Skipping\n")
				continue
			} else {
				dvid.Fmt(dvid.Debug, "Invalid iterator on PUT: Allocating data for block\n")
				value = make(keyvalue.Value, data.BlockBytes(), data.BlockBytes())
			}
		}

		// Initialize the block request
		req := &BlockRequest{
			DataStruct: data,
			Block:      value,
			Op:         op,
			SpatialKey: SpatialIndex(spatialBytes),
			BlockKey:   blockKey,
			Wait:       wg,
			DB:         vs.kvdb,
			//WriteBatch: writeBatch,
		}

		// Try to spread sequential block keys among different block handlers to get 
		// most out of our concurrent processing.
		wg.Add(1)
		channelNum := req.SpatialKey.Hash(data, len(channels))
		dvid.Fmt(dvid.Debug, "Sending block request %s down channel %d\n",
			data, channelNum)
		channels[channelNum] <- req
	}
	DiskAccess.Unlock()
	return nil
}
