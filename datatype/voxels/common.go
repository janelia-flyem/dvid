/*
	This file contains interfaces and functions common to voxel-type data types.
*/

package voxels

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// Operation holds Voxel-specific data for processing chunks.
type Operation struct {
	ExtData
	OpType
	*ROI
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
)

func (o OpType) String() string {
	switch o {
	case GetOp:
		return "Get Op"
	case PutOp:
		return "Put Op"
	default:
		return "Illegal Op"
	}
}

// Block is the basic key-value for the voxel type.
// The value is a slice of bytes corresponding to data within a block.
type Block storage.KeyValue

// Blocks is a slice of Block.
type Blocks []Block

// ROI encapsulates a request-specific ROI check with a given scaling
// for voxels outside the ROI.
type ROI struct {
	Iter        *roi.Iterator
	attenuation uint8
}

// IntData implementations handle internal DVID voxel representations, knowing how
// to break data into chunks (blocks for voxels).  Typically, each voxels-oriented
// package has a Data type that fulfills the IntData interface.
type IntData interface {
	BaseData() dvid.Data

	NewExtHandler(dvid.Geometry, interface{}) (ExtData, error)

	Compression() dvid.Compression

	Checksum() dvid.Checksum

	Values() dvid.DataValues

	BlockSize() dvid.Point

	Extents() *Extents

	ProcessChunk(*storage.Chunk)
}

// ExtData provides the shape, location (indexing), and data of a set of voxels
// connected with external usage. It is the type used for I/O from DVID to clients,
// e.g., 2d images, 3d subvolumes, etc.  These user-facing data must be converted to
// and from internal DVID representations using key-value pairs where the value is a
// block of data, and the key contains some spatial indexing.
//
// We can read/write different external formats through the following steps:
//   1) Create a data type package (e.g., datatype/labels64) and define a ExtData type
//      where the data layout (i.e., the values in a voxel) is identical to
//      the targeted DVID IntData.
//   2) Do I/O for external format (e.g., Raveler's superpixel PNG images with implicit Z)
//      and convert external data to the ExtData instance.
//   3) Pass ExtData to voxels package-level functions.
//
type ExtData interface {
	VoxelHandler

	NewChunkIndex() dvid.ChunkIndexer

	Index(p dvid.ChunkPoint) dvid.Index

	IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error)

	// DownRes reduces the image data by the integer scaling for each dimension.
	DownRes(downmag dvid.Point) error

	// Returns a 2d image suitable for external DVID use
	GetImage2d() (*dvid.Image, error)
}

// VoxelHandlers can get and set n-D voxels.
type VoxelHandler interface {
	VoxelGetter
	VoxelSetter
}

type VoxelGetter interface {
	dvid.Geometry

	Values() dvid.DataValues

	Stride() int32

	ByteOrder() binary.ByteOrder

	Data() []byte

	Interpolable() bool
}

type VoxelSetter interface {
	SetGeometry(geom dvid.Geometry)

	SetValues(values dvid.DataValues)

	SetStride(stride int32)

	SetByteOrder(order binary.ByteOrder)

	SetData(data []byte)
}

// GetImage retrieves a 2d image from a version node given a geometry of voxels.
func GetImage(ctx storage.Context, i IntData, e ExtData, r *ROI) (*dvid.Image, error) {
	if err := GetVoxels(ctx, i, e, r); err != nil {
		return nil, err
	}
	return e.GetImage2d()
}

// GetVolume retrieves a n-d volume from a version node given a geometry of voxels.
func GetVolume(ctx storage.Context, i IntData, e ExtData, r *ROI) ([]byte, error) {
	if err := GetVoxels(ctx, i, e, r); err != nil {
		return nil, err
	}
	return e.Data(), nil
}

// GetVoxels copies voxels from an IntData for a version to an ExtData, e.g.,
// a requested subvolume or 2d image.
func GetVoxels(ctx storage.Context, i IntData, e ExtData, r *ROI) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&Operation{e, GetOp, r}, wg}

	server.SpawnGoroutineMutex.Lock()
	for it, err := e.IndexIterator(i.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			server.SpawnGoroutineMutex.Unlock()
			return err
		}

		// Send the entire range of key-value pairs to chunk processor
		err = db.ProcessRange(ctx, indexBeg.Bytes(), indexEnd.Bytes(), chunkOp, i.ProcessChunk)
		if err != nil {
			server.SpawnGoroutineMutex.Unlock()
			return fmt.Errorf("Unable to GET data %s: %s", ctx, err.Error())
		}
	}
	server.SpawnGoroutineMutex.Unlock()
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

// PutVoxels copies voxels from an ExtData (e.g., subvolume or 2d image) into an IntData
// for a version.   Since chunk sizes can be larger than the PUT data, this also requires
// integrating the PUT data into current chunks before writing the result.  There are two passes:
//   Pass one: Retrieve all available key/values within the PUT space.
//   Pass two: Merge PUT data into those key/values and store them.
func PutVoxels(ctx storage.Context, i IntData, e ExtData) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&Operation{e, PutOp, nil}, wg}

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	versionID := ctx.VersionID()
	putMutex := ctx.Mutex()
	putMutex.Lock()
	defer putMutex.Unlock()

	// Get UUID
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		return err
	}

	// Keep track of changing extents and mark repo as dirty if changed.
	var extentChanged bool
	defer func() {
		if extentChanged {
			err := datastore.SaveRepo(uuid)
			if err != nil {
				dvid.Infof("Error in trying to save repo on change: %s\n", err.Error())
			}
		}
	}()

	// Track point extents
	extents := i.Extents()
	if extents.AdjustPoints(e.StartPoint(), e.EndPoint()) {
		extentChanged = true
	}

	// Iterate through index space for this data.
	for it, err := e.IndexIterator(i.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		i0, i1, err := it.IndexSpan()
		if err != nil {
			return err
		}
		ptBeg := i0.Duplicate().(dvid.ChunkIndexer)
		ptEnd := i1.Duplicate().(dvid.ChunkIndexer)

		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)

		if extents.AdjustIndices(ptBeg, ptEnd) {
			extentChanged = true
		}

		// GET all the key-value pairs for this range.
		keyvalues, err := db.GetRange(ctx, ptBeg.Bytes(), ptEnd.Bytes())
		if err != nil {
			return fmt.Errorf("Error in reading data during PUT: %s", err.Error())
		}

		// Send all data to chunk handlers for this range.
		var kv, oldkv *storage.KeyValue
		numOldkv := len(keyvalues)
		oldI := 0
		if numOldkv > 0 {
			oldkv = keyvalues[oldI] // Start with the first old kv we have in this range.
		}
		wg.Add(int(endX-begX) + 1)
		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndexBytes := e.Index(c).Bytes()
			// Check for this index among old key-value pairs and if so,
			// send the old value into chunk handler.  Else we are just sending
			// keys with no value.
			if oldkv != nil && oldkv.K != nil {
				oldIndexBytes, err := ctx.IndexFromKey(oldkv.K)
				if err != nil {
					return err
				}
				if bytes.Compare(curIndexBytes, oldIndexBytes) == 0 {
					kv = oldkv
					oldI++
					if oldI < numOldkv {
						oldkv = keyvalues[oldI]
					} else {
						oldkv.K = nil
					}
				} else {
					kv = &storage.KeyValue{K: ctx.ConstructKey(curIndexBytes)}
				}
			} else {
				kv = &storage.KeyValue{K: ctx.ConstructKey(curIndexBytes)}
			}
			// TODO -- Pass batch write via chunkOp and group all PUTs
			// together at once.  Should increase write speed, particularly
			// since the PUTs are using mostly sequential keys.
			i.ProcessChunk(&storage.Chunk{chunkOp, kv})
		}
	}

	wg.Wait()
	return nil
}
