/*
	This file holds caching and buffering for datastore operation.  It includes caches for
	UUIDs and block write buffers common to data types.
*/

package datastore

import (
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
)

type cachedData struct {
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

// BlockRequest encapsulates the essential data needed to process blocks in the datastore.
// Blocks are the units of Map/Reduce operations on a DVID volume.
// Each block handler receives BlockRequests through a channel.
type BlockRequest struct {
	op       OpType
	coord    dvid.BlockCoord
	blockKey Key
	subvol   *dvid.Subvolume

	// Let's us notify requestor when all blocks are done.
	wg *sync.WaitGroup
}

// NewBlockRequest manufactures a BlockRequest for use by datastore block processors.
func NewBlockRequest(op OpType, coord dvid.BlockCoord, blockKey Key, subvol *dvid.Subvolume,
	wg *sync.WaitGroup) *BlockRequest {

	return &BlockRequest{
		op:       op,
		coord:    coord,
		blockKey: blockKey,
		subvol:   subvol,
		wg:       wg,
	}
}

type request struct {
	vs *VersionService
	br *BlockRequest
}

// NumBlockHandlers sets the number of processors we have for our "map" operation.
const NumBlockHandlers = 1

// Number of block write requests that can be buffered on each block handler
// before sender is blocked.
const BlockHandlerBufferSize = 100000

type BlockChannels []chan request

var blockChannels BlockChannels

func init() {
	blockChannels = make(BlockChannels, NumBlockHandlers, NumBlockHandlers)
	for channelNum, _ := range blockChannels {
		blockChannels[channelNum] = make(chan request, BlockHandlerBufferSize)
		go blockHandler(channelNum)
	}
}

var NumSent, NumReceived int

// WriteBlock accepts all requests to process blocks using data from a given subvolume,
// and sends the request to a block handler goroutine.
func (vs *VersionService) ProcessBlock(br *BlockRequest) error {
	// Try to spread block coordinates out among block handlers to get most out of
	// our concurrent processing.  However, we may want a handler to receive similar
	// spatial indices and be better able to batch them for sequential writes.
	channelN := (br.coord[0]*2 + br.coord[1]*3 + br.coord[2]*5) % NumBlockHandlers

	// Package the write block data and send it down the chosen channel
	blockChannels[channelN] <- request{vs, br}
	return nil
}

// blockHandler is a goroutine that fulfills block write requests sent on the given
// channel number.
func blockHandler(channelNum int) {
	for {
		req := <-blockChannels[channelNum]
		vs := req.vs
		si := vs.SpatialIndex(req.br.coord)
		subvol := req.br.subvol

		// Get the block data.  If we have error, like key not present,
		// just use empty values for block.
		data := make([]byte, vs.BlockNumVoxels(), vs.BlockNumVoxels())
		blockBytes, err := vs.kvdb.getBytes(req.br.blockKey)
		if err == nil {
			copy(data, blockBytes)
		}

		dataBytes := vs.BlockNumVoxels() * subvol.BytesPerVoxel
		if dataBytes != len(data) {
			dvid.Error("ERROR blockHandler(%d): retrieved block has %d bytes not %d bytes\n",
				channelNum, len(data), dataBytes)
			continue
		}

		// Get min and max voxel coordinates of this block
		minBlockVoxel := vs.OffsetToBlock(si)
		maxBlockVoxel := minBlockVoxel.AddSize(vs.BlockMax)

		// Get min and max voxel coordinates of the entire subvolume
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
