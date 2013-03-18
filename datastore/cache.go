/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

type OpResult string

// Block is an interface for data that is held within DVID blocks, which are the
// fundamental value in each key/value pair.  A Block is usually voxel-oriented data
// but can contain graph or other kinds of data as well.  
type Block interface {
}

// Each data type has a pool of channels to communicate with block handlers. 
type BlockChannels map[string]([]chan Block)

// Global variable that holds the LRU cache for DVID instance. 
var dataCache *cache.LRUCache

// Initialize the LRU cache
func InitDataCache(numBytes int) {
	dataCache = cache.NewLRUCache(numBytes)
}

// GetCachedBlock returns data for a given DVID Block if it's in the cache.
// Since the block key includes a UUID, this function can be used to access
// any image versions cached blocks. 
func GetCachedBlock(blockKey keyvalue.Key) (data keyvalue.Value, found bool) {
	data, found = dataCache.Get(string(blockKey))
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
func (vs *VersionService) ReserveBlockHandlers(typeName string, numHandlers int) {
	// Do we have channels and handlers for this type and image version?
	_, found := vs.channels[typeName]
	if !found {
		// Create channels and handlers
		channels := make([]chan Block, 0, numHandlers)
		for i := 0; i < numHandlers; i++ {
			channel := make(chan Block, BlockHandlerBufferSize)
			channels = append(channels, channel)
			go vs.blockHandler(channel)
			// TODO -- keep stats on # of handlers
		}
		vs.channels[typeName] = channels
	}
}

// ProcessBlock sends each block through a channel to its handler.  Since channel
// assignment is determined solely by the block key, the same block will always
// be sent to the same handler.
func (vs *VersionService) ProcessBlock(block Block) {
	// Make sure we have channels to handlers for this block's data type.
	channels, found := vs.channels[block.TypeName()]
	if !found {
		vs.ReserveBlockHandlers(block.TypeName(), DefaultNumBlockHandlers)
		channels, found = vs.channels[block.TypeName()]
	}

	// Try to spread sequential block keys out among different block handlers to get 
	// most out of our concurrent processing.
	channelNum := block.spatialKey.Hash(block.TypeService, len(channels))

	// Send the block
	channels[channelNum] <- block
}

// blockHandler is a goroutine that fulfills block-level requests sent on a channel.
func (vs *VersionService) blockHandler(channel chan Block) {
	for {
		block := <-channel

		// Get min and max voxel coordinates of this block
		si := vs.SpatialIndex(req.br.coord)
		minBlockVoxel := vs.OffsetToBlock(si)
		maxBlockVoxel := minBlockVoxel.AddSize(vs.BlockMax)

		// Get min and max voxel coordinates of the entire subvolume
		subvol := req.br.subvol
		minSubvolVoxel := subvol.Offset
		maxSubvolVoxel := minSubvolVoxel.AddSize(subvol.Size)

		// Bound the start and end voxel coordinates of the subvolume by the block limits.
		start := minSubvolVoxel.BoundMin(minBlockVoxel)
		end := maxSubvolVoxel.BoundMax(maxBlockVoxel)

		// Traverse the data from start to end voxel coordinates and write to the block.
		// TODO -- Optimize the inner loop and see if we actually get faster :)  Currently,
		// the code tries to make it very clear what transformations are happening.
		for z := start[2]; z <= end[2]; z++ {
			for y := start[1]; y <= end[1]; y++ {
				for x := start[0]; x <= end[0]; x++ {
					voxelCoord := dvid.VoxelCoord{x, y, z}
					i := subvol.VoxelCoordToDataIndex(voxelCoord)
					b := vs.VoxelCoordToBlockIndex(voxelCoord)
					bI := b * subvol.BytesPerVoxel // index into block.data
					switch req.br.op {
					case GetOp:
						for n := 0; n < subvol.BytesPerVoxel; n++ {
							subvol.Data[i+n] = data[bI+n]
						}
					case PutOp:
						for n := 0; n < subvol.BytesPerVoxel; n++ {
							data[bI+n] = subvol.Data[i+n]
						}
					}
				}
			}
		}

		switch req.br.op {
		case GetOp:
		case PutOp:
			// PUT the block
			err = vs.kvdb.putBytes(req.br.blockKey, data)
			if err != nil {
				dvid.Error("ERROR blockHandler(%d): unable to write block!\n", channelNum)
			}
		}

		// Notify the requestor that this block is done.
		if req.br.wg != nil {
			req.br.wg.Done()
		}

	} // request loop
}
