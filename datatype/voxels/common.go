/*
	This file contains interfaces and functions common to voxel-type data types.
*/

package voxels

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
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
	blocksInROI map[string]bool
	attenuation uint8
	denormChan  BlockChannel
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

// Block3d encodes a 3d block coordinate and data.
type Block3d struct {
	Index *dvid.IndexZYX
	Data  []byte
}

// BlockChannel is a channel of voxel blocks.
type BlockChannel chan Block3d

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

	BackgroundBlock() []byte

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

	// Only do one request at a time, although each request can start many goroutines.
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

	wg := new(sync.WaitGroup)
	for it, err := e.IndexIterator(i.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		blockBeg := NewVoxelBlockIndex(indexBeg)
		blockEnd := NewVoxelBlockIndex(indexEnd)

		// Get set of blocks in ROI if ROI provided
		var chunkOp *storage.ChunkOp
		if r != nil && r.Iter != nil {
			ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
			ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)
			begX := ptBeg.Value(0)
			endX := ptEnd.Value(0)

			blocksInROI := make(map[string]bool, (endX - begX + 1))
			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				curIndex := dvid.IndexZYX(c)
				if r.Iter.InsideFast(curIndex) {
					indexString := string(curIndex.Bytes())
					blocksInROI[indexString] = true
				}
			}
			chunkOp = &storage.ChunkOp{&Operation{e, GetOp, blocksInROI, r.attenuation, nil}, wg}
		} else {
			chunkOp = &storage.ChunkOp{&Operation{e, GetOp, nil, 0, nil}, wg}
		}

		// Send the entire range of key-value pairs to chunk processor
		err = db.ProcessRange(ctx, blockBeg, blockEnd, chunkOp, i.ProcessChunk)
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %s", ctx, err.Error())
		}
	}
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func GetBlocks(ctx storage.Context, uncompressed bool, start dvid.ChunkPoint3d, span int) ([]byte, error) {
	bigdata, err := storage.BigDataStore()
	if err != nil {
		return nil, fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
	}

	indexBeg := dvid.IndexZYX(start)
	end := start
	end[0] += int32(span - 1)
	indexEnd := dvid.IndexZYX(end)
	voxelBlockBeg := NewVoxelBlockIndex(&indexBeg)
	voxelBlockEnd := NewVoxelBlockIndex(&indexEnd)

	keyvalues, err := bigdata.GetRange(ctx, voxelBlockBeg, voxelBlockEnd)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	// Save the # of keyvalues actually obtained.
	numkv := len(keyvalues)
	binary.Write(&buf, binary.LittleEndian, int32(numkv))

	// Write the block indices in XYZ little-endian format + the size of each block
	for _, kv := range keyvalues {
		indexZYX, err := DecodeVoxelBlockKey(kv.K)
		if err != nil {
			return nil, err
		}
		indexBytes, err := indexZYX.MarshalBinary()
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(indexBytes)
		if err != nil {
			return nil, err
		}
		if !uncompressed {
			binary.Write(&buf, binary.LittleEndian, int32(len(kv.V)))
		}
	}

	// Write the actual data
	for _, kv := range keyvalues {
		block, _, err := dvid.DeserializeData(kv.V, uncompressed)
		if err != nil {
			return nil, fmt.Errorf("Unable to deserialize block, %s (%v): %s", ctx, kv.K, err.Error())
		}
		_, err = buf.Write(block)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

type OpOptions struct {
	roi      *ROI
	modsChan BlockChannel
}

func (opts *OpOptions) SetROI(roi *ROI) {
	opts.roi = roi
}

func (opts *OpOptions) SetModsChannel(modsChan BlockChannel) {
	opts.modsChan = modsChan
}

// PutVoxels copies voxels from an ExtData (e.g., subvolume or 2d image) into an IntData
// for a version.   Since chunk sizes can be larger than the PUT data, this also requires
// integrating the PUT data into current chunks before writing the result.  There are two passes:
//   Pass one: Retrieve all available key/values within the PUT space.
//   Pass two: Merge PUT data into those key/values and store them.
func PutVoxels(ctx storage.Context, i IntData, e ExtData, options OpOptions) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	chunkOp := &storage.ChunkOp{&Operation{e, PutOp, nil, 0, options.modsChan}, wg}

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

	// Only do one request at a time, although each request can start many goroutines.
	server.SpawnGoroutineMutex.Lock()
	defer server.SpawnGoroutineMutex.Unlock()

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

		indexBeg := NewVoxelBlockIndex(ptBeg)
		indexEnd := NewVoxelBlockIndex(ptEnd)

		// GET all the key-value pairs for this range.
		keyvalues, err := db.GetRange(ctx, indexBeg, indexEnd)
		if err != nil {
			return fmt.Errorf("Error in reading data during PUT: %s", err.Error())
		}

		// Send all data to chunk handlers for this range.
		var kv *storage.KeyValue
		numOldkv := len(keyvalues)
		var oldI int // index into old key-value pairs
		if numOldkv == 0 {
			oldI = -1
		}
		wg.Add(int(endX-begX) + 1)
		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndex := dvid.IndexZYX(c)
			curIndexBytes := NewVoxelBlockIndex(&curIndex)

			// Check for this index among old key-value pairs and if so,
			// send the old value into chunk handler.  Else we are just sending
			// keys with no value.
			if oldI >= 0 {
				oldIndexBytes, err := ctx.IndexFromKey(keyvalues[oldI].K)
				if err != nil {
					return err
				}
				if bytes.Compare(curIndexBytes, oldIndexBytes) == 0 {
					kv = keyvalues[oldI]
					oldI++
					if oldI >= numOldkv {
						oldI = -1
					}
				} else {
					kv = &storage.KeyValue{K: ctx.ConstructKey(curIndexBytes)}
				}
			} else {
				kv = &storage.KeyValue{K: ctx.ConstructKey(curIndexBytes)}
			}

			// Don't PUT if this index is outside a specified ROI
			if options.roi != nil && options.roi.Iter != nil && !options.roi.Iter.InsideFast(curIndex) {
				wg.Done()
				continue
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

func loadHDF(i IntData, load *bulkLoadInfo) error {
	return fmt.Errorf("DVID currently does not support HDF5 image import.")
	// TODO: Use a DVID-specific HDF5 loader that works off HDF5 C library.
	/*
			for _, filename := range load.filenames {
				f, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
				if err != nil {
					return err
				}
				defer f.Close()

				fmt.Printf("Opened HDF5 file: %s\n", filename)
				numobj, err := f.NumObjects()
				fmt.Printf("Number of objects: %d\n", numobj)
				for n := uint(0); n < numobj; n++ {
					name, err := f.ObjectNameByIndex(n)
					if err != nil {
						return err
					}
					fmt.Printf("Object name %d: %s\n", n, name)
					repo, err := f.OpenRepo(name)
					if err != nil {
						return err
					}
					dtype, err := repo.Datatype()
					if err != nil {
						return err
					}
					fmt.Printf("Type size: %d\n", dtype.Size())
					dataspace := repo.Space()
					dims, maxdims, err := dataspace.SimpleExtentDims()
					if err != nil {
						return err
					}
					fmt.Printf("Dims: %s\n", dims)
					fmt.Printf("Maxdims: %s\n", maxdims)
					data := make([]uint8, dims[0]*dims[1]*dims[2])
					err = repo.Read(&data)
					if err != nil {
						return err
					}
					fmt.Printf("Read %d bytes\n", len(data))
				}
			}
		return nil
	*/
}

// Optimized bulk loading of XY images by loading all slices for a block before processing.
// Trades off memory for speed.
func loadXYImages(i IntData, load *bulkLoadInfo) error {
	fmt.Println("Reading XY images...")

	// Construct a storage.Context for this data and version
	ctx := datastore.NewVersionedContext(i.BaseData(), load.versionID)

	// Load first slice, get dimensions, allocate blocks for whole slice.
	// Note: We don't need to lock the block slices because goroutines do NOT
	// access the same elements of a slice.
	const numLayers = 2
	var numBlocks int
	var blocks [numLayers]Blocks
	var layerTransferred, layerWritten [numLayers]sync.WaitGroup
	var waitForWrites sync.WaitGroup

	curBlocks := 0
	blockSize := i.BlockSize()
	blockBytes := blockSize.Prod() * int64(i.Values().BytesPerElement())

	// Iterate through XY slices batched into the Z length of blocks.
	fileNum := 1
	for _, filename := range load.filenames {
		server.BlockOnInteractiveRequests("voxels.loadXYImages")

		timedLog := dvid.NewTimeLog()

		zInBlock := load.offset.Value(2) % blockSize.Value(2)
		firstSlice := fileNum == 1
		lastSlice := fileNum == len(load.filenames)
		firstSliceInBlock := firstSlice || zInBlock == 0
		lastSliceInBlock := lastSlice || zInBlock == blockSize.Value(2)-1
		lastBlocks := fileNum+int(blockSize.Value(2)) > len(load.filenames)

		// Load images synchronously
		e, err := loadXYImage(i, filename, load.offset)
		if err != nil {
			return err
		}

		// Allocate blocks and/or load old block data if first/last XY blocks.
		// Note: Slices are only zeroed out on first and last slice with assumption
		// that ExtData is packed in XY footprint (values cover full extent).
		// If that is NOT the case, we need to zero out blocks for each block layer.
		if fileNum == 1 || (lastBlocks && firstSliceInBlock) {
			numBlocks = dvid.GetNumBlocks(e, blockSize)
			if fileNum == 1 {
				for layer := 0; layer < numLayers; layer++ {
					blocks[layer] = make(Blocks, numBlocks, numBlocks)
					for b := 0; b < numBlocks; b++ {
						blocks[layer][b].V = i.BackgroundBlock()
					}
				}
				var bufSize uint64 = uint64(blockBytes) * uint64(numBlocks) * uint64(numLayers) / 1000000
				dvid.Debugf("Allocated %d MB for buffers.\n", bufSize)
			} else {
				blocks[curBlocks] = make(Blocks, numBlocks, numBlocks)
				for b := 0; b < numBlocks; b++ {
					blocks[curBlocks][b].V = i.BackgroundBlock()
				}
			}
			err = loadOldBlocks(load.versionID, i, e, blocks[curBlocks])
			if err != nil {
				return err
			}
		}

		// Transfer data between external<->internal blocks asynchronously
		layerTransferred[curBlocks].Add(1)
		go func(ext ExtData, curBlocks int) {
			// Track point extents
			if i.Extents().AdjustPoints(e.StartPoint(), e.EndPoint()) {
				load.extentChanged.SetTrue()
			}

			// Process an XY image (slice).
			changed, err := writeXYImage(load.versionID, i, ext, blocks[curBlocks])
			if err != nil {
				dvid.Infof("Error writing XY image: %s\n", err.Error())
			}
			if changed {
				load.extentChanged.SetTrue()
			}
			layerTransferred[curBlocks].Done()
		}(e, curBlocks)

		// If this is the end of a block (or filenames), wait until all goroutines complete,
		// then asynchronously write blocks.
		if lastSliceInBlock {
			waitForWrites.Add(1)
			layerWritten[curBlocks].Add(1)
			go func(curBlocks int) {
				layerTransferred[curBlocks].Wait()
				dvid.Debugf("Writing block buffer %d using %s and %s...\n",
					curBlocks, i.Compression(), i.Checksum())
				err := writeBlocks(ctx, i.Compression(), i.Checksum(), blocks[curBlocks],
					&layerWritten[curBlocks], &waitForWrites)
				if err != nil {
					dvid.Errorf("Error in async write of voxel blocks: %s", err.Error())
				}
			}(curBlocks)
			// We can't move to buffer X until all blocks from buffer X have already been written.
			curBlocks = (curBlocks + 1) % numLayers
			dvid.Debugf("Waiting for layer %d to be written before reusing layer %d blocks\n",
				curBlocks, curBlocks)
			layerWritten[curBlocks].Wait()
			dvid.Debugf("Using layer %d...\n", curBlocks)
		}

		fileNum++
		load.offset = load.offset.Add(dvid.Point3d{0, 0, 1})
		timedLog.Infof("Loaded %s slice %s", i, e)
	}
	waitForWrites.Wait()
	return nil
}

// KVWriteSize is the # of key-value pairs we will write as one atomic batch write.
const KVWriteSize = 500

// writeBlocks writes blocks of voxel data asynchronously using batch writes.
func writeBlocks(ctx storage.Context, compress dvid.Compression, checksum dvid.Checksum, blocks Blocks, wg1, wg2 *sync.WaitGroup) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}

	preCompress, postCompress := 0, 0

	<-server.HandlerToken
	go func() {
		defer func() {
			wg1.Done()
			wg2.Done()
			dvid.Debugf("Wrote voxel blocks.  Before %s: %d bytes.  After: %d bytes\n",
				compress, preCompress, postCompress)
			server.HandlerToken <- 1
		}()
		// If we can do write batches, use it, else do put ranges.
		// With write batches, we write the byte slices immediately.
		// The put range approach can lead to duplicated memory.
		batcher, ok := db.(storage.KeyValueBatcher)
		if ok {
			batch := batcher.NewBatch(ctx)
			for i, block := range blocks {
				serialization, err := dvid.SerializeData(block.V, compress, checksum)
				preCompress += len(block.V)
				postCompress += len(serialization)
				if err != nil {
					dvid.Errorf("Unable to serialize block: %s\n", err.Error())
					return
				}
				indexBytes, err := ctx.IndexFromKey(block.K)
				if err != nil {
					dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
					return
				}
				batch.Put(indexBytes, serialization)
				if i%KVWriteSize == KVWriteSize-1 {
					if err := batch.Commit(); err != nil {
						dvid.Errorf("Error on trying to write batch: %s\n", err.Error())
						return
					}
					batch = batcher.NewBatch(ctx)
				}
			}
			if err := batch.Commit(); err != nil {
				dvid.Errorf("Error on trying to write batch: %s\n", err.Error())
				return
			}
		} else {
			// Serialize and compress the blocks.
			keyvalues := make(storage.KeyValues, len(blocks))
			for i, block := range blocks {
				serialization, err := dvid.SerializeData(block.V, compress, checksum)
				preCompress += len(block.V)
				postCompress += len(serialization)
				if err != nil {
					dvid.Errorf("Unable to serialize block: %s\n", err.Error())
					return
				}
				indexBytes, err := ctx.IndexFromKey(block.K)
				if err != nil {
					dvid.Errorf("Unable to recover index from block key: %v\n", block.K)
					return
				}
				keyvalues[i] = storage.KeyValue{
					K: indexBytes,
					V: serialization,
				}
			}

			// Write them in one swoop.
			err := db.PutRange(ctx, keyvalues)
			if err != nil {
				dvid.Errorf("Unable to write slice blocks: %s\n", err.Error())
			}
		}

	}()
	return nil
}

// Loads a XY oriented image at given offset, returning an ExtData.
func loadXYImage(i IntData, filename string, offset dvid.Point) (ExtData, error) {
	img, _, err := dvid.GoImageFromFile(filename)
	if err != nil {
		return nil, err
	}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, dvid.RectSize(img.Bounds()))
	if err != nil {
		return nil, fmt.Errorf("Unable to determine slice: %s", err.Error())
	}
	e, err := i.NewExtHandler(slice, img)
	if err != nil {
		return nil, err
	}
	storage.FileBytesRead <- len(e.Data())
	return e, nil
}

// LoadImages bulk loads images using different techniques if it is a multidimensional
// file like HDF5 or a sequence of PNG/JPG/TIF images.
func LoadImages(versionID dvid.VersionID, i IntData, offset dvid.Point, filenames []string) error {
	if len(filenames) == 0 {
		return nil
	}
	timedLog := dvid.NewTimeLog()

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	ctx := storage.NewDataContext(i.BaseData(), versionID)
	loadMutex := ctx.Mutex()
	loadMutex.Lock()

	// Handle cleanup given multiple goroutines still writing data.
	load := &bulkLoadInfo{filenames: filenames, versionID: versionID, offset: offset}
	defer func() {
		loadMutex.Unlock()

		if load.extentChanged.Value() {
			err := datastore.SaveRepoByVersionID(versionID)
			if err != nil {
				dvid.Errorf("Error in trying to save repo for voxel extent change: %s\n", err.Error())
			}
		}
	}()

	// Use different loading techniques if we have a potentially multidimensional HDF5 file
	// or many 2d images.
	if dvid.Filename(filenames[0]).HasExtensionPrefix("hdf", "h5") {
		loadHDF(i, load)
	} else {
		loadXYImages(i, load)
	}

	timedLog.Infof("RPC load of %d files completed", len(filenames))
	return nil
}

// Loads blocks with old data if they exist.
func loadOldBlocks(versionID dvid.VersionID, i IntData, e ExtData, blocks Blocks) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	ctx := datastore.NewVersionedContext(i.BaseData(), versionID)

	// Create a map of old blocks indexed by the index
	oldBlocks := map[string]([]byte){}

	// Iterate through index space for this data using ZYX ordering.
	blockSize := i.BlockSize()
	blockNum := 0
	for it, err := e.IndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		begBytes := NewVoxelBlockIndex(indexBeg)
		endBytes := NewVoxelBlockIndex(indexEnd)

		// Get previous data.
		keyvalues, err := db.GetRange(ctx, begBytes, endBytes)
		if err != nil {
			return err
		}
		for _, kv := range keyvalues {
			indexBytes, err := ctx.IndexFromKey(kv.K)
			if err != nil {
				return err
			}
			block, _, err := dvid.DeserializeData(kv.V, true)
			if err != nil {
				return fmt.Errorf("Unable to deserialize block, %s: %s", ctx, err.Error())
			}
			oldBlocks[string(indexBytes)] = block
		}

		// Load previous data into blocks
		ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
		ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)
		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)
		c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
		for x := begX; x <= endX; x++ {
			c[0] = x
			curIndex := dvid.IndexZYX(c)
			curIndexBytes := NewVoxelBlockIndex(&curIndex)
			blocks[blockNum].K = ctx.ConstructKey(curIndexBytes)
			block, ok := oldBlocks[string(curIndexBytes)]
			if ok {
				copy(blocks[blockNum].V, block)
			}
			blockNum++
		}
	}
	return nil
}

// Writes a XY image (the ExtData) into the blocks that intersect it.
// This function assumes the blocks have been allocated and if necessary, filled
// with old data.
func writeXYImage(versionID dvid.VersionID, i IntData, e ExtData, blocks Blocks) (extentChanged bool, err error) {

	// Setup concurrency in image -> block transfers.
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	// Iterate through index space for this data using ZYX ordering.
	ctx := datastore.NewVersionedContext(i.BaseData(), versionID)
	blockSize := i.BlockSize()
	var startingBlock int32

	for it, err := e.IndexIterator(blockSize); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return extentChanged, err
		}

		ptBeg := indexBeg.Duplicate().(dvid.ChunkIndexer)
		ptEnd := indexEnd.Duplicate().(dvid.ChunkIndexer)

		// Track point extents
		if i.Extents().AdjustIndices(ptBeg, ptEnd) {
			extentChanged = true
		}

		// Do image -> block transfers in concurrent goroutines.
		begX := ptBeg.Value(0)
		endX := ptEnd.Value(0)

		<-server.HandlerToken
		wg.Add(1)
		go func(blockNum int32) {
			c := dvid.ChunkPoint3d{begX, ptBeg.Value(1), ptBeg.Value(2)}
			for x := begX; x <= endX; x++ {
				c[0] = x
				curIndex := dvid.IndexZYX(c)
				curIndexBytes := NewVoxelBlockIndex(&curIndex)
				blocks[blockNum].K = ctx.ConstructKey(curIndexBytes)

				// Write this slice data into the block.
				WriteToBlock(e, &(blocks[blockNum]), blockSize)
				blockNum++
			}
			server.HandlerToken <- 1
			wg.Done()
		}(startingBlock)

		startingBlock += (endX - begX + 1)
	}
	return
}

// ComputeTransform determines the block coordinate and beginning + ending voxel points
// for the data corresponding to the given Block.
func ComputeTransform(v ExtData, block *Block, blockSize dvid.Point) (blockBeg, dataBeg, dataEnd dvid.Point, err error) {
	ptIndex := v.NewChunkIndex()

	var indexBytes []byte
	ctx := &storage.DataContext{}
	indexBytes, err = ctx.IndexFromKey(block.K)
	if err != nil {
		return
	}
	if indexBytes[0] != byte(KeyVoxelBlock) {
		err = fmt.Errorf("Block key (%v) has non-VoxelBlock index", block.K)
	}
	if err = ptIndex.IndexFromBytes(indexBytes[1:]); err != nil {
		return
	}

	// Get the bounding voxel coordinates for this block.
	minBlockVoxel := ptIndex.MinPoint(blockSize)
	maxBlockVoxel := ptIndex.MaxPoint(blockSize)

	// Compute the boundary voxel coordinates for the ExtData and adjust
	// to our block bounds.
	minDataVoxel := v.StartPoint()
	maxDataVoxel := v.EndPoint()
	begVolCoord, _ := minDataVoxel.Max(minBlockVoxel)
	endVolCoord, _ := maxDataVoxel.Min(maxBlockVoxel)

	// Adjust the DVID volume voxel coordinates for the data so that (0,0,0)
	// is where we expect this slice/subvolume's data to begin.
	dataBeg = begVolCoord.Sub(v.StartPoint())
	dataEnd = endVolCoord.Sub(v.StartPoint())

	// Compute block coord matching dataBeg
	blockBeg = begVolCoord.Sub(minBlockVoxel)

	return
}

func ReadFromBlock(v ExtData, block *Block, blockSize dvid.Point, attenuation uint8) error {
	if attenuation != 0 {
		return readScaledBlock(v, block, blockSize, attenuation)
	}
	return transferBlock(GetOp, v, block, blockSize)
}

func WriteToBlock(v ExtData, block *Block, blockSize dvid.Point) error {
	return transferBlock(PutOp, v, block, blockSize)
}

func readScaledBlock(v ExtData, block *Block, blockSize dvid.Point, attenuation uint8) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := ComputeTransform(v, block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()
	bytesPerVoxel := int64(v.Values().BytesPerElement())
	if bytesPerVoxel != 1 {
		return fmt.Errorf("Can only scale non-ROI blocks with 1 byte voxels")
	}

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * bytesPerVoxel
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	// Get the block beginning coordinates.
	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
			blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
			dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				data[dataI] = (block.V[blockI] >> attenuation)
				blockI += bX
				dataI += bytesPerVoxel
			}
			bz++
		}

	case v.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		return fmt.Errorf("DVID currently does not support 2d in n-d space.")

	case v.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBegX * bytesPerVoxel
		dX := int64(v.Size().Value(0)) * bytesPerVoxel
		dY := int64(v.Size().Value(1)) * dX
		dataOffset := int64(dataBeg.Value(0)) * bytesPerVoxel
		blockZ := blockBegZ

		for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
			blockY := blockBegY
			for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
				blockI := blockZ*bY + blockY*bX + blockOffset
				dataI := int64(dataZ)*dY + int64(dataY)*dX + dataOffset
				for x := int64(dataBeg.Value(0)); x <= int64(dataEnd.Value(0)); x++ {
					data[dataI+x] = (block.V[blockI+x] >> attenuation)
				}
				blockY++
			}
			blockZ++
		}

	default:
		return fmt.Errorf("Cannot ReadFromBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

func transferBlock(op OpType, v ExtData, block *Block, blockSize dvid.Point) error {
	if blockSize.NumDims() > 3 {
		return fmt.Errorf("DVID voxel blocks currently only supports up to 3d, not 4+ dimensions")
	}
	blockBeg, dataBeg, dataEnd, err := ComputeTransform(v, block, blockSize)
	if err != nil {
		return err
	}
	data := v.Data()
	bytesPerVoxel := int64(v.Values().BytesPerElement())

	// Compute the strides (in bytes)
	bX := int64(blockSize.Value(0)) * bytesPerVoxel
	bY := int64(blockSize.Value(1)) * bX
	dX := int64(v.Stride())

	blockBegX := int64(blockBeg.Value(0))
	blockBegY := int64(blockBeg.Value(1))
	blockBegZ := int64(blockBeg.Value(2))

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		dataI := int64(dataBeg.Value(1))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		switch op {
		case GetOp:
			for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
				copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
				blockI += bX
				dataI += dX
			}
		case PutOp:
			for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
				copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
				blockI += bX
				dataI += dX
			}
		}

	case v.DataShape().Equals(dvid.XZ):
		dataI := int64(dataBeg.Value(2))*dX + int64(dataBeg.Value(0))*bytesPerVoxel
		blockI := blockBegZ*bY + blockBegY*bX + blockBegX*bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		switch op {
		case GetOp:
			for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
				copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
				blockI += bY
				dataI += dX
			}
		case PutOp:
			for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
				copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
				blockI += bY
				dataI += dX
			}
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBegZ
		switch op {
		case GetOp:
			for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
				dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
				blockI := bz*bY + blockBegY*bX + blockBegX*bytesPerVoxel
				for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
					copy(data[dataI:dataI+bytesPerVoxel], block.V[blockI:blockI+bytesPerVoxel])
					blockI += bX
					dataI += bytesPerVoxel
				}
				bz++
			}
		case PutOp:
			for y := int64(dataBeg.Value(2)); y <= int64(dataEnd.Value(2)); y++ {
				dataI := y*dX + int64(dataBeg.Value(1))*bytesPerVoxel
				blockI := bz*bY + blockBegY*bX + blockBegX*bytesPerVoxel
				for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
					copy(block.V[blockI:blockI+bytesPerVoxel], data[dataI:dataI+bytesPerVoxel])
					blockI += bX
					dataI += bytesPerVoxel
				}
				bz++
			}
		}

	case v.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtData in n-d space.
		return fmt.Errorf("DVID currently does not support 2d in n-d space.")

	case v.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBegX * bytesPerVoxel
		dX := int64(v.Size().Value(0)) * bytesPerVoxel
		dY := int64(v.Size().Value(1)) * dX
		dataOffset := int64(dataBeg.Value(0)) * bytesPerVoxel
		bytes := int64(dataEnd.Value(0)-dataBeg.Value(0)+1) * bytesPerVoxel
		blockZ := blockBegZ

		switch op {
		case GetOp:
			for dataZ := int64(dataBeg.Value(2)); dataZ <= int64(dataEnd.Value(2)); dataZ++ {
				blockY := blockBegY
				for dataY := int64(dataBeg.Value(1)); dataY <= int64(dataEnd.Value(1)); dataY++ {
					blockI := blockZ*bY + blockY*bX + blockOffset
					dataI := dataZ*dY + dataY*dX + dataOffset
					copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
					blockY++
				}
				blockZ++
			}
		case PutOp:
			for dataZ := int64(dataBeg.Value(2)); dataZ <= int64(dataEnd.Value(2)); dataZ++ {
				blockY := blockBegY
				for dataY := int64(dataBeg.Value(1)); dataY <= int64(dataEnd.Value(1)); dataY++ {
					dataI := dataZ*dY + dataY*dX + dataOffset
					blockI := blockZ*bY + blockY*bX + blockOffset
					copy(block.V[blockI:blockI+bytes], data[dataI:dataI+bytes])
					blockY++
				}
				blockZ++
			}
		}

	default:
		return fmt.Errorf("Cannot ReadFromBlock() unsupported voxels data shape %s", v.DataShape())
	}
	return nil
}

// BackgroundBlock returns a block buffer that has been preinitialized to the background value.
func (d *Data) BackgroundBlock() []byte {
	numElements := d.BlockSize().Prod()
	bytesPerElement := int64(d.Values().BytesPerElement())
	blockData := make([]byte, numElements*bytesPerElement)
	if d.Background != 0 && bytesPerElement == 1 {
		background := byte(d.Background)
		for i := range blockData {
			blockData[i] = background
		}
	}
	return blockData
}

// ProcessChunk processes a chunk of data as part of a mapped operation.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc).
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) ProcessChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.processChunk(chunk)
}

func (d *Data) processChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op, ok := chunk.Op.(*Operation)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}

	// Make sure our received chunk is valid.
	if chunk == nil {
		dvid.Errorf("Received nil chunk in ProcessChunk.  Ignoring chunk.\n")
		return
	}
	if chunk.K == nil {
		dvid.Errorf("Received nil chunk key in ProcessChunk.  Ignoring chunk.\n")
		return
	}

	// If there's an ROI, if outside ROI, use blank buffer or allow scaling via attenuation.
	var zeroOut bool
	var attenuation uint8
	indexZYX, err := DecodeVoxelBlockKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error processing voxel block: %s\n", err.Error())
		return
	}
	if op.blocksInROI != nil {
		indexString := string(indexZYX.Bytes())
		_, insideROI := op.blocksInROI[indexString]
		if !insideROI {
			if op.attenuation == 0 {
				zeroOut = true
			}
			attenuation = op.attenuation
		}
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var blockData []byte
	if zeroOut || chunk.V == nil {
		blockData = d.BackgroundBlock()
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
			return
		}
	}

	// Perform the operation.
	block := &Block{K: chunk.K, V: blockData}
	switch op.OpType {
	case GetOp:
		if err = ReadFromBlock(op.ExtData, block, d.BlockSize(), attenuation); err != nil {
			dvid.Errorf("Unable to ReadFromBlock() in %q: %s\n", d.DataName(), err.Error())
			return
		}
	case PutOp:
		if err = WriteToBlock(op.ExtData, block, d.BlockSize()); err != nil {
			dvid.Errorf("Unable to WriteToBlock() in %q: %s\n", d.DataName(), err.Error())
			return
		}
		if op.denormChan != nil {
			op.denormChan <- Block3d{indexZYX, blockData}
		}
		bigdata, err := storage.BigDataStore()
		if err != nil {
			dvid.Errorf("Unable to obtain BigData store in %q: %s\n", d.DataName(), err.Error())
			return
		}
		serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
		if err != nil {
			dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
			return
		}
		if err := bigdata.Put(nil, chunk.K, serialization); err != nil {
			dvid.Errorf("Unable to PUT voxel data for key %v: %s\n", chunk.K, err.Error())
			return
		}
	}
}

// Handler conversion of little to big endian for voxels larger than 1 byte.
func littleToBigEndian(v ExtData, data []uint8) (bigendian []uint8, err error) {
	bytesPerVoxel := v.Values().BytesPerElement()
	if v.ByteOrder() == nil || v.ByteOrder() == binary.BigEndian || bytesPerVoxel == 1 {
		return data, nil
	}
	bigendian = make([]uint8, len(data))
	switch bytesPerVoxel {
	case 2:
		for beg := 0; beg < len(data)-1; beg += 2 {
			bigendian[beg], bigendian[beg+1] = data[beg+1], data[beg]
		}
	case 4:
		for beg := 0; beg < len(data)-3; beg += 4 {
			value := binary.LittleEndian.Uint32(data[beg : beg+4])
			binary.BigEndian.PutUint32(bigendian[beg:beg+4], value)
		}
	case 8:
		for beg := 0; beg < len(data)-7; beg += 8 {
			value := binary.LittleEndian.Uint64(data[beg : beg+8])
			binary.BigEndian.PutUint64(bigendian[beg:beg+8], value)
		}
	}
	return
}
