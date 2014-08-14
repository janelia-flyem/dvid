/*
	This file collects types and functions usable from both labelmap and labels64 datatypes.
*/

package labels64

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type KeyType byte

// Label indexing is handled through a variety of key spaces that optimize
// throughput for access patterns required by our API.  For dcumentation purposes,
// consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
//   v: # of voxels for a label
const (
	// KeyInverseMap have keys of form 'b+a'
	KeyInverseMap KeyType = iota

	// KeyForwardMap have keys of form 'a+b'
	// For superpixel->body maps, this key would be superpixel+body.
	KeyForwardMap

	// KeySpatialMap have keys of form 's+a+b'
	// They are useful for composing label maps for a spatial index.
	KeySpatialMap

	// KeyLabelSpatialMap have keys of form 'b+s' and have a sparse volume
	// encoding for its value. They are useful for returning all blocks
	// intersected by a label.
	KeyLabelSpatialMap

	// KeyLabelSizes have keys of form 'v+b'.
	// They allow rapid size range queries.
	KeyLabelSizes

	// KeyLabelSurface have keys of form 'b' and have the label's sparse volume
	// for its value.
	KeyLabelSurface
)

var (
	zeroLabelBytes = make([]byte, 8, 8)
)

// ZeroBytes returns a slice of bytes that represents the zero label.
func ZeroBytes() []byte {
	return zeroLabelBytes
}

func (t KeyType) String() string {
	switch t {
	case KeyInverseMap:
		return "Inverse Label Map"
	case KeyForwardMap:
		return "Forward Label Map"
	case KeySpatialMap:
		return "Spatial Index to Labels Map"
	case KeyLabelSpatialMap:
		return "Forward Label to Spatial Index Map"
	case KeyLabelSizes:
		return "Forward Label sorted by volume"
	case KeyLabelSurface:
		return "Forward Label Surface"
	default:
		return "Unknown Key Type"
	}
}

// NewLabelSpatialMapIndex returns an identifier for storing a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func NewLabelSpatialMapIndex(label uint64, block dvid.IndexZYX) []byte {
	index := make([]byte, 1+8+dvid.IndexZYXSize)
	index[0] = byte(KeyLabelSpatialMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:9+dvid.IndexZYXSize], block.Bytes())
	return dvid.IndexBytes(index)
}

// NewLabelSizesIndex returns an identifier for storing a "size + mapped label".
func NewLabelSizesIndex(size, label uint64) []byte {
	index := make([]byte, 17)
	index[0] = byte(KeyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return dvid.IndexBytes(index)
}

func labelFromLabelSizesIndex(key []byte) (uint64, error) {
	indexBytes, err := storage.DataContextIndex(key)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(indexBytes[9:17]), nil
}

// NewForwardMapIndex returns an index that encodes a "label + mapping", where
// the label and mapping are both uint64.
func NewForwardMapIndex(label []byte, mapping uint64) []byte {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], label)
	binary.BigEndian.PutUint64(index[9:17], mapping)
	return dvid.IndexBytes(index)
}

// NewSpatialMapIndex returns an index that encodes a "spatial index + label + mapping".
func NewSpatialMapIndex(blockIndex dvid.Index, label []byte, mapping uint64) []byte {

	index := make([]byte, 1+dvid.IndexZYXSize+8+8) // s + a + b
	index[0] = byte(KeySpatialMap)
	i := 1 + dvid.IndexZYXSize
	copy(index[1:i], blockIndex.Bytes())
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mapping)
	return dvid.IndexBytes(index)
}

// NewLabelSurfaceIndex returns an identifier for a given label's surface.
func NewLabelSurfaceIndex(label uint64) []byte {
	index := make([]byte, 1+8)
	index[0] = byte(KeyLabelSurface)
	binary.BigEndian.PutUint64(index[1:9], label)
	return dvid.IndexBytes(index)
}

// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
func StoreKeyLabelSpatialMap(versionID dvid.VersionID, data dvid.Data, batcher storage.KeyValueBatcher,
	zyxBytes []byte, labelRLEs map[uint64]dvid.RLEs) {

	ctx := storage.NewDataContext(data, versionID)
	batch := batcher.NewBatch(ctx)
	defer func() {
		if err := batch.Commit(); err != nil {
			dvid.Infof("Error on batch PUT of KeyLabelSpatialMap: %s\n", err.Error())
		}
	}()
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(KeyLabelSpatialMap)
	copy(bsIndex[9:9+dvid.IndexZYXSize], zyxBytes)
	for b, rles := range labelRLEs {
		binary.BigEndian.PutUint64(bsIndex[1:9], b)
		key := dvid.IndexBytes(bsIndex)
		runsBytes, err := rles.MarshalBinary()
		if err != nil {
			dvid.Infof("Error encoding KeyLabelSpatialMap keys for mapped label %d: %s\n", b, err.Error())
			return
		}
		batch.Put(key, runsBytes)
	}
}

// ComputeSurface computes and stores a label surface.
// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func ComputeSurface(ctx storage.Context, data *Data, ch chan *storage.Chunk, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		server.HandlerToken <- 1
	}()

	// Sequentially process all the sparse volume data for each label coming down channel.
	var curVol dvid.SparseVol
	var curLabel uint64
	notFirst := false
	for {
		chunk := <-ch
		if chunk == nil {
			if notFirst {
				if err := data.computeAndSaveSurface(ctx, &curVol); err != nil {
					dvid.Infof("Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)
		if label != curLabel || label == 0 {
			if notFirst {
				if err := data.computeAndSaveSurface(ctx, &curVol); err != nil {
					dvid.Infof("Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			curVol.Clear()
			curVol.SetLabel(label)
		}

		if err := curVol.AddRLEs(chunk.V); err != nil {
			dvid.Infof("Error adding RLE for label %d: %s\n", label, err.Error())
			return
		}
		curLabel = label
		notFirst = true
	}
}

func (d *Data) computeAndSaveSurface(ctx storage.Context, vol *dvid.SparseVol) error {
	surfaceBytes, err := vol.SurfaceSerialization(d.BlockSize().Value(2), d.Resolution.VoxelSize)
	if err != nil {
		return err
	}
	store, err := storage.BigDataStore()
	if err != nil {
		return err
	}

	// Surface blobs are always stored using gzip with best compression, trading off time
	// during the store for speed during interactive GETs.
	compression, _ := dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
	serialization, err := dvid.SerializeData(surfaceBytes, compression, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("Unable to serialize data in surface computation: %s\n", err.Error())
	}
	key := NewLabelSurfaceIndex(vol.Label())
	return store.Put(ctx, key, serialization)
}

// GetSurface returns a gzipped byte array with # voxels and float32 arrays for vertices and
// normals.
func GetSurface(ctx storage.Context, label uint64) ([]byte, bool, error) {
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return nil, false, fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}

	// Retrieve the precomputed surface or that it's not available.
	data, err := bigdata.Get(ctx, NewLabelSurfaceIndex(label))
	if err != nil {
		return nil, false, fmt.Errorf("Error in retrieving surface for label %d: %s", label, err.Error())
	}
	if data == nil {
		return []byte{}, false, nil
	}
	uncompress := false
	surfaceBytes, _, err := dvid.DeserializeData(data, uncompress)
	if err != nil {
		return nil, false, fmt.Errorf("Unable to deserialize surface for label %d: %s\n", label, err.Error())
	}
	return surfaceBytes, true, nil
}

type sparseOp struct {
	versionID dvid.VersionID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// GetSparseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    # Voxels
//    uint32    # Spans
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//		  ...
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func GetSparseVol(ctx storage.Context, label uint64) ([]byte, error) {
	bigdata, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))
	binary.Write(buf, binary.LittleEndian, byte(0))
	buf.WriteByte(byte(0))
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	// Get the start/end indices for this body's KeyLabelSpatialMap (b + s) keys.
	begIndex := NewLabelSpatialMapIndex(label, dvid.MinIndexZYX)
	endIndex := NewLabelSpatialMapIndex(label, dvid.MaxIndexZYX)

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	wg := new(sync.WaitGroup)
	op := &sparseOp{versionID: ctx.VersionID(), encoding: buf.Bytes()}
	err = bigdata.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{op, wg}, func(chunk *storage.Chunk) {
		op := chunk.Op.(*sparseOp)
		op.numBlocks++
		op.encoding = append(op.encoding, chunk.V...)
		op.numRuns += uint32(len(chunk.V) / 16)
		chunk.Wg.Done()
	})
	if err != nil {
		return nil, err
	}
	wg.Wait()

	binary.LittleEndian.PutUint32(op.encoding[8:12], op.numRuns)

	dvid.Debugf("[%s] label %d: found %d blocks, %d runs\n", ctx, label, op.numBlocks, op.numRuns)
	return op.encoding, nil
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func ComputeSizes(ctx storage.Context, sizeCh chan *storage.Chunk, smalldata storage.SmallDataStorer,
	wg *sync.WaitGroup) {

	// Make sure our small data store can do batching.
	batcher, ok := smalldata.(storage.KeyValueBatcher)
	if !ok {
		dvid.Criticalf("Unable to compute label sizes: small data store can't do batching!")
		return
	}

	const BATCH_SIZE = 10000
	batch := batcher.NewBatch(ctx)

	defer func() {
		wg.Done()
	}()

	// Sequentially process all the sparse volume data for each label
	var curLabel, curSize uint64
	putsInBatch := 0
	notFirst := false
	for {
		chunk := <-sizeCh
		if chunk == nil {
			key := NewLabelSizesIndex(curSize, curLabel)
			batch.Put(key, dvid.EmptyValue())
			if err := batch.Commit(); err != nil {
				dvid.Infof("Error on batch PUT of label sizes: %s\n", err.Error())
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)

		// Compute the size
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(chunk.V); err != nil {
			dvid.Infof("Error deserializing RLEs: %s\n", err.Error())
			return
		}
		numVoxels, _ := rles.Stats()

		// If we are a new label, store size
		if notFirst && label != curLabel {
			key := NewLabelSizesIndex(curSize, curLabel)
			curSize = 0
			batch.Put(key, dvid.EmptyValue())
			putsInBatch++
			if putsInBatch%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Infof("Error on batch PUT of label sizes: %s\n", err.Error())
					return
				}
				batch = batcher.NewBatch(ctx)
			}
		}
		curLabel = label
		curSize += uint64(numVoxels)
		notFirst = true
	}
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
// If maxSize is 0, all mapped labels are returned >= minSize.
func GetSizeRange(data dvid.Data, versionID dvid.VersionID, minSize, maxSize uint64) (string, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return "{}", err
	}
	ctx := storage.NewDataContext(data, versionID)

	// Get the start/end keys for the size range.
	firstKey := NewLabelSizesIndex(minSize, 0)
	var upperBound uint64
	if maxSize != 0 {
		upperBound = maxSize
	} else {
		upperBound = math.MaxUint64
	}
	lastKey := NewLabelSizesIndex(upperBound, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	keys, err := store.KeysInRange(ctx, firstKey, lastKey)
	if err != nil {
		return "{}", err
	}

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		labels[i], err = labelFromLabelSizesIndex(key)
		if err != nil {
			return "{}", err
		}
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}
