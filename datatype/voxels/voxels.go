/*
	Package voxels implements DVID support for data using voxels as elements.
*/
package voxels

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.8"
	RepoURL = "github.com/janelia-flyem/dvid/datatype/voxels"

	// Don't allow requests that will return more than this amount of data.
	MaxDataRequest = dvid.Giga
)

const HelpMessage = `
API for 'voxels' datatype (github.com/janelia-flyem/dvid/datatype/voxels)
=========================================================================

Command-line:

$ dvid repo <UUID> new <type name> <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example (note anisotropic resolution specified instead of default 8 nm isotropic):

	$ dvid repo 3f8c new grayscale8 mygrayscale BlockSize=32 Res=3.2,3.2,40.0

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    type name      Data type name, e.g., "grayscale8"
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")

$ dvid node <UUID> <data name> load <offset> <image glob>

    Initializes version node to a set of XY images described by glob of filenames.  The
    DVID server must have access to the named files.  Currently, XY images are required.

    Example: 

    $ dvid node 3f8c mygrayscale load 0,0,100 data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png

$ dvid node <UUID> <data name> put local  <plane> <offset> <image glob>
$ dvid node <UUID> <data name> put remote <plane> <offset> <image glob>

    Adds image data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").  If possible, use the
    "load" command instead because it is much more efficient.

    Example: 

    $ dvid node 3f8c mygrayscale put local xy 0,0,100 data/*.png

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    image glob    Filenames of images, e.g., foo-xy-*.png
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/grayscale/info

    Returns JSON with configuration settings that include location in DVID space and
    min/max block indices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of voxels data.


GET  <api URL>/node/<UUID>/<data name>/metadata

	Retrieves metadata in JSON format (application/vnd.dvid-nd-data+json) that describes the layout
	of bytes returned for n-d images.


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?throttle=on][?queryopts]
POST <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts voxel data.

    Example: 

    GET <api URL>/node/3f8c/grayscale/raw/0_1/512_256/0_0_100/jpg:80

    Returns a raw XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    By "raw", we mean that no additional processing is applied based on voxel
    resolutions to make sure the retrieved image has isotropic pixels.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream". 

    Throttling can be enabled by passing a "throttle=on" query string.  Throttling makes sure
    only one compute-intense operation (all API calls that can be throttled) is handled.
    If the server can't initiate the API call right away, a 503 (Service Unavailable) status
    code is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    roi       	  Name of roi data instance used to mask the requested data.
    attenuation   For attenuation n, this reduces the intensity of voxels outside ROI by 2^n.
    			  Valid range is n = 1 to n = 7.  Currently only implemented for 8-bit voxels.
    			  Default is to zero out voxels outside ROI.

GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>][?throttle=on]

    Retrieves or puts voxel data.

    Example: 

    GET <api URL>/node/3f8c/grayscale/isotropic/0_1/512_256/0_0_100/jpg:80

    Returns an isotropic XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    Additional processing is applied based on voxel resolutions to make sure the retrieved image 
    has isotropic pixels.  For example, if an XZ image is requested and the image volume has 
    X resolution 3 nm and Z resolution 40 nm, the returned image's height will be magnified 40/3
    relative to the raw data.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream".

    Throttling can be enabled by passing a "throttle=on" query string.  Throttling makes sure
    only one compute-intense operation (all API calls that can be throttled) is handled.
    If the server can't initiate the API call right away, a 503 (Service Unavailable) status
    code is returned.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>]

    Retrieves non-orthogonal (arbitrarily oriented planar) image data of named 3d data 
    within a version node.  Returns an image where the top left pixel corresponds to the
    real world coordinate (not in voxel space but in space defined by resolution, e.g.,
    nanometer space).  The real world coordinates are specified in  "x_y_z" format, e.g., "20.3_11.8_109.4".
    The resolution is used to determine the # pixels in the returned image.

    Example: 

    GET <api URL>/node/3f8c/grayscale/arb/100.2_90_80.7/200.2_90_80.7/100.2_190.0_80.7/10.0/jpg:80

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    top left      Real world coordinate (in nanometers) of top left pixel in returned image.
    top right     Real world coordinate of top right pixel.
    bottom left   Real world coordinate of bottom left pixel.
    res           The resolution/pixel that is used to calculate the returned image size in pixels.
    format        "png", "jpg" (default: "png")  
                    jpg allows lossy quality setting, e.g., "jpg:80"
`

var (
	// DefaultBlockSize specifies the default size for each block of this data type.
	DefaultBlockSize int32 = 32

	DefaultRes float32 = 8

	DefaultUnits = "nanometers"
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(binary.LittleEndian)
	gob.Register(binary.BigEndian)
}

// Type embeds the datastore's Type to create a unique type with voxel functions.
// Refinements of general voxel types can be implemented by embedding this type,
// choosing appropriate # of values and bytes/value, overriding functions as needed,
// and calling datastore.Register().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Type struct {
	datastore.Type

	// values describes the data type/label for each value within a voxel.
	values dvid.DataValues

	// can these values be interpolated?
	interpolable bool
}

// NewType returns a pointer to a new voxels Type with default values set.
func NewType(values dvid.DataValues, interpolable bool) *Type {
	dtype := &Type{
		values:       values,
		interpolable: interpolable,
	}
	dtype.Type = datastore.Type{
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}
	return dtype
}

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Type) NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (*Data, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	props := new(Properties)
	props.SetDefault(dtype.values, dtype.interpolable)
	if err := props.SetByConfig(c); err != nil {
		return nil, err
	}
	data := &Data{
		Data:       *basedata,
		Properties: *props,
	}
	return data, nil
}

// --- TypeService interface ---

// NewDataService returns a pointer to a new Voxels with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	return dtype.NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

type bulkLoadInfo struct {
	filenames     []string
	versionID     dvid.VersionID
	offset        dvid.Point
	extentChanged dvid.Bool
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
					for i := 0; i < numBlocks; i++ {
						blocks[layer][i].V = make([]byte, blockBytes, blockBytes)
					}
				}
				var bufSize uint64 = uint64(blockBytes) * uint64(numBlocks) * uint64(numLayers) / 1000000
				dvid.Debugf("Allocated %d MB for buffers.\n", bufSize)
			} else {
				blocks[curBlocks] = make(Blocks, numBlocks, numBlocks)
				for i := 0; i < numBlocks; i++ {
					blocks[curBlocks][i].V = make([]byte, blockBytes, blockBytes)
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
			dvid.Debugf("Wrote voxel blocks.  Before %s: %d bytes.  After: %d bytes\n",
				compress, preCompress, postCompress)
			server.HandlerToken <- 1
			wg1.Done()
			wg2.Done()
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
	img, _, err := dvid.ImageFromFile(filename)
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
			curIndex := e.Index(c)
			curIndexBytes := NewVoxelBlockIndex(curIndex)
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
				curIndex := e.Index(c)
				curIndexBytes := NewVoxelBlockIndex(curIndex)
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
	if err = ptIndex.IndexFromBytes(indexBytes); err != nil {
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
	bytesPerVoxel := v.Values().BytesPerElement()
	if bytesPerVoxel != 1 {
		return fmt.Errorf("Can only scale non-ROI blocks with 1 byte voxels")
	}

	// Compute the strides (in bytes)
	bX := blockSize.Value(0) * bytesPerVoxel
	bY := blockSize.Value(1) * bX
	dX := v.Stride()

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(1)*dX + dataBeg.Value(0)*bytesPerVoxel
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bX
			dataI += dX
		}

	case v.DataShape().Equals(dvid.XZ):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(2)*v.Stride() + dataBeg.Value(0)*bytesPerVoxel
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
				data[dataI+x] = (block.V[blockI+x] >> attenuation)
			}
			blockI += bY
			dataI += dX
		}

	case v.DataShape().Equals(dvid.YZ):
		bz := blockBeg.Value(2)
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			blockI := bz*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
			dataI := y*dX + dataBeg.Value(1)*bytesPerVoxel
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
		blockOffset := blockBeg.Value(0) * bytesPerVoxel
		dX = v.Size().Value(0) * bytesPerVoxel
		dY := v.Size().Value(1) * dX
		dataOffset := dataBeg.Value(0) * bytesPerVoxel
		blockZ := blockBeg.Value(2)

		for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
			blockY := blockBeg.Value(1)
			for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
				blockI := blockZ*bY + blockY*bX + blockOffset
				dataI := dataZ*dY + dataY*dX + dataOffset
				for x := dataBeg.Value(0); x <= dataEnd.Value(0); x++ {
					data[dataI] = (block.V[blockI] >> attenuation)
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
	bytesPerVoxel := v.Values().BytesPerElement()

	// Compute the strides (in bytes)
	bX := blockSize.Value(0) * bytesPerVoxel
	bY := blockSize.Value(1) * bX
	dX := v.Stride()

	// Do the transfers depending on shape of the external voxels.
	switch {
	case v.DataShape().Equals(dvid.XY):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(1)*dX + dataBeg.Value(0)*bytesPerVoxel
		bytes := (dataEnd.Value(0) - dataBeg.Value(0) + 1) * bytesPerVoxel
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
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(2)*v.Stride() + dataBeg.Value(0)*bytesPerVoxel
		bytes := (dataEnd.Value(0) - dataBeg.Value(0) + 1) * bytesPerVoxel
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
		bz := blockBeg.Value(2)
		switch op {
		case GetOp:
			for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
				blockI := bz*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
				dataI := y*dX + dataBeg.Value(1)*bytesPerVoxel
				for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
					copy(data[dataI:dataI+bytesPerVoxel], block.V[blockI:blockI+bytesPerVoxel])
					blockI += bX
					dataI += bytesPerVoxel
				}
				bz++
			}
		case PutOp:
			for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
				blockI := bz*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
				dataI := y*dX + dataBeg.Value(1)*bytesPerVoxel
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
		blockOffset := blockBeg.Value(0) * bytesPerVoxel
		dX = v.Size().Value(0) * bytesPerVoxel
		dY := v.Size().Value(1) * dX
		dataOffset := dataBeg.Value(0) * bytesPerVoxel
		bytes := (dataEnd.Value(0) - dataBeg.Value(0) + 1) * bytesPerVoxel
		blockZ := blockBeg.Value(2)

		switch op {
		case GetOp:
			for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
				blockY := blockBeg.Value(1)
				for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
					blockI := blockZ*bY + blockY*bX + blockOffset
					dataI := dataZ*dY + dataY*dX + dataOffset
					copy(data[dataI:dataI+bytes], block.V[blockI:blockI+bytes])
					blockY++
				}
				blockZ++
			}
		case PutOp:
			for dataZ := dataBeg.Value(2); dataZ <= dataEnd.Value(2); dataZ++ {
				blockY := blockBeg.Value(1)
				for dataY := dataBeg.Value(1); dataY <= dataEnd.Value(1); dataY++ {
					blockI := blockZ*bY + blockY*bX + blockOffset
					dataI := dataZ*dY + dataY*dX + dataOffset
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

// Voxels represents subvolumes or slices and implements the ExtData interface.
type Voxels struct {
	dvid.Geometry

	values dvid.DataValues

	// The data itself
	data []byte

	// The stride for 2d iteration in bytes between vertically adjacent pixels.
	// For 3d subvolumes, we don't reuse standard Go images but maintain fully
	// packed data slices, so stride isn't necessary.
	stride int32

	byteOrder binary.ByteOrder
}

func NewVoxels(geom dvid.Geometry, values dvid.DataValues, data []byte, stride int32,
	byteOrder binary.ByteOrder) *Voxels {

	return &Voxels{geom, values, data, stride, byteOrder}
}

func (v *Voxels) String() string {
	size := v.Size()
	return fmt.Sprintf("%s of size %s @ %s", v.DataShape(), size, v.StartPoint())
}

// -------  VoxelGetter interface implementation -------------

func (v *Voxels) Values() dvid.DataValues {
	return v.values
}

func (v *Voxels) Data() []byte {
	return v.data
}

func (v *Voxels) Stride() int32 {
	return v.stride
}

func (v *Voxels) BytesPerVoxel() int32 {
	return v.values.BytesPerElement()
}

func (v *Voxels) ByteOrder() binary.ByteOrder {
	return v.byteOrder
}

// -------  VoxelSetter interface implementation -------------

func (v *Voxels) SetGeometry(geom dvid.Geometry) {
	v.Geometry = geom
}

func (v *Voxels) SetValues(values dvid.DataValues) {
	v.values = values
}

func (v *Voxels) SetStride(stride int32) {
	v.stride = stride
}

func (v *Voxels) SetByteOrder(order binary.ByteOrder) {
	v.byteOrder = order
}

func (v *Voxels) SetData(data []byte) {
	v.data = data
}

// -------  ExtData interface implementation -------------

func (v *Voxels) NewChunkIndex() dvid.ChunkIndexer {
	return &dvid.IndexZYX{}
}

func (v *Voxels) Interpolable() bool {
	return true
}

// DownRes downsamples 2d Voxels data by averaging where the down-magnification are
// integers.  If the source image size in Voxels is not an integral multiple of the
// reduction factor, the edge voxels on the right and bottom side are truncated.
// This function modifies the Voxels data.  An error is returned if a non-2d Voxels
// receiver is used.
func (v *Voxels) DownRes(magnification dvid.Point) error {
	if v.DataShape().ShapeDimensions() != 2 {
		return fmt.Errorf("ImageDownres() only supports 2d images at this time.")
	}
	// Calculate new dimensions and allocate.
	srcW := v.Size().Value(0)
	srcH := v.Size().Value(1)
	reduceW, reduceH, err := v.DataShape().GetSize2D(magnification)
	if err != nil {
		return err
	}
	dstW := srcW / reduceW
	dstH := srcH / reduceH

	// Reduce the image.
	img, err := v.GetImage2d()
	if err != nil {
		return err
	}
	img, err = img.ScaleImage(int(dstW), int(dstH))
	if err != nil {
		return err
	}

	// Set data and dimensions to downres data.
	v.data = []byte(img.Data())
	geom, err := dvid.NewOrthogSlice(v.DataShape(), v.StartPoint(), dvid.Point2d{dstW, dstH})
	if err != nil {
		return err
	}
	v.Geometry = geom
	v.stride = dstW * v.values.BytesPerElement()
	return nil
}

func (v *Voxels) Index(c dvid.ChunkPoint) dvid.Index {
	chunkPt, ok := c.(dvid.ChunkPoint3d)
	if !ok {
		return nil
	}
	index := dvid.IndexZYX(chunkPt)
	return &index
}

// IndexIterator returns an iterator that can move across the voxel geometry,
// generating indices or index spans.
func (v *Voxels) IndexIterator(chunkSize dvid.Point) (dvid.IndexIterator, error) {
	// Setup traversal
	begVoxel, ok := v.StartPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData StartPoint() cannot handle Chunkable points.")
	}
	endVoxel, ok := v.EndPoint().(dvid.Chunkable)
	if !ok {
		return nil, fmt.Errorf("ExtData EndPoint() cannot handle Chunkable points.")
	}
	begBlock := begVoxel.Chunk(chunkSize).(dvid.ChunkPoint3d)
	endBlock := endVoxel.Chunk(chunkSize).(dvid.ChunkPoint3d)

	return dvid.NewIndexZYXIterator(begBlock, endBlock), nil
}

// GetImage2d returns a 2d image suitable for use external to DVID.
// TODO -- Create more comprehensive handling of endianness and encoding of
// multibytes/voxel data into appropriate images.
func (v *Voxels) GetImage2d() (*dvid.Image, error) {
	// Make sure each value has same # of bytes or else we can't generate a go image.
	// If so, we need to make another ExtData that knows how to convert the varying
	// values into an appropriate go image.
	valuesPerVoxel := int32(len(v.values))
	if valuesPerVoxel < 1 || valuesPerVoxel > 4 {
		return nil, fmt.Errorf("Standard voxels type can't convert %d values/voxel into image.",
			valuesPerVoxel)
	}
	bytesPerValue := v.values.ValueBytes(0)
	for _, dataValue := range v.values {
		if dvid.DataTypeBytes(dataValue.T) != bytesPerValue {
			return nil, fmt.Errorf("Standard voxels type can't handle varying sized values per voxel.")
		}
	}

	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			valuesPerVoxel, bytesPerValue)
	}

	var img image.Image
	width := v.Size().Value(0)
	height := v.Size().Value(1)
	sliceBytes := width * height * valuesPerVoxel * bytesPerValue
	beg := int32(0)
	end := beg + sliceBytes
	data := v.Data()
	if int(end) > len(data) {
		return nil, fmt.Errorf("Voxels %s has insufficient amount of data to return an image.", v)
	}
	r := image.Rect(0, 0, int(width), int(height))
	switch valuesPerVoxel {
	case 1:
		switch bytesPerValue {
		case 1:
			img = &image.Gray{data[beg:end], 1 * r.Dx(), r}
		case 2:
			bigendian, err := littleToBigEndian(v, data[beg:end])
			if err != nil {
				return nil, err
			}
			img = &image.Gray16{bigendian, 2 * r.Dx(), r}
		case 4:
			img = &image.NRGBA{data[beg:end], 4 * r.Dx(), r}
		case 8:
			img = &image.NRGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			return nil, unsupported()
		}
	case 4:
		switch bytesPerValue {
		case 1:
			img = &image.NRGBA{data[beg:end], 4 * r.Dx(), r}
		case 2:
			img = &image.NRGBA64{data[beg:end], 8 * r.Dx(), r}
		default:
			return nil, unsupported()
		}
	default:
		return nil, unsupported()
	}

	ret := new(dvid.Image)
	if err := ret.SetFromGoImage(img, v.Values(), v.Interpolable()); err != nil {
		return nil, err
	}
	return ret, nil
}

// Extents holds the extents of a volume in both absolute voxel coordinates
// and lexicographically sorted chunk indices.
type Extents struct {
	MinPoint dvid.Point
	MaxPoint dvid.Point

	MinIndex dvid.ChunkIndexer
	MaxIndex dvid.ChunkIndexer

	pointMu sync.Mutex
	indexMu sync.Mutex
}

// --- dvid.Bounder interface ----

// StartPoint returns the offset to first point of data.
func (ext *Extents) StartPoint() dvid.Point {
	return ext.MinPoint
}

// EndPoint returns the last point.
func (ext *Extents) EndPoint() dvid.Point {
	return ext.MaxPoint
}

// --------

// AdjustPoints modifies extents based on new voxel coordinates in concurrency-safe manner.
func (ext *Extents) AdjustPoints(pointBeg, pointEnd dvid.Point) bool {
	ext.pointMu.Lock()
	defer ext.pointMu.Unlock()

	var changed bool
	if ext.MinPoint == nil {
		ext.MinPoint = pointBeg
		changed = true
	} else {
		ext.MinPoint, changed = ext.MinPoint.Min(pointBeg)
	}
	if ext.MaxPoint == nil {
		ext.MaxPoint = pointEnd
		changed = true
	} else {
		ext.MaxPoint, changed = ext.MaxPoint.Max(pointEnd)
	}
	return changed
}

// AdjustIndices modifies extents based on new block indices in concurrency-safe manner.
func (ext *Extents) AdjustIndices(indexBeg, indexEnd dvid.ChunkIndexer) bool {
	ext.indexMu.Lock()
	defer ext.indexMu.Unlock()

	var changed bool
	if ext.MinIndex == nil {
		ext.MinIndex = indexBeg
		changed = true
	} else {
		ext.MinIndex, changed = ext.MinIndex.Min(indexBeg)
	}
	if ext.MaxIndex == nil {
		ext.MaxIndex = indexEnd
		changed = true
	} else {
		ext.MaxIndex, changed = ext.MaxIndex.Max(indexEnd)
	}
	return changed
}

type Resolution struct {
	// Resolution of voxels in volume
	VoxelSize dvid.NdFloat32

	// Units of resolution, e.g., "nanometers"
	VoxelUnits dvid.NdString
}

func (r Resolution) IsIsotropic() bool {
	if len(r.VoxelSize) <= 1 {
		return true
	}
	curRes := r.VoxelSize[0]
	for _, res := range r.VoxelSize[1:] {
		if res != curRes {
			return false
		}
	}
	return true
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Values describes the data type/label for each value within a voxel.
	Values dvid.DataValues

	// Interpolable is true if voxels can be interpolated when resizing.
	Interpolable bool

	// Block size for this repo
	BlockSize dvid.Point

	// The endianness of this loaded data.
	ByteOrder binary.ByteOrder

	Resolution
	Extents
}

// SetDefault sets Voxels properties to default values.
func (props *Properties) SetDefault(values dvid.DataValues, interpolable bool) error {
	props.Values = make([]dvid.DataValue, len(values))
	copy(props.Values, values)
	props.Interpolable = interpolable

	props.ByteOrder = binary.LittleEndian

	dimensions := 3
	size := make([]int32, dimensions)
	for d := 0; d < dimensions; d++ {
		size[d] = DefaultBlockSize
	}
	var err error
	props.BlockSize, err = dvid.NewPoint(size)
	if err != nil {
		return err
	}
	props.Resolution.VoxelSize = make(dvid.NdFloat32, dimensions)
	for d := 0; d < dimensions; d++ {
		props.Resolution.VoxelSize[d] = DefaultRes
	}
	props.Resolution.VoxelUnits = make(dvid.NdString, dimensions)
	for d := 0; d < dimensions; d++ {
		props.Resolution.VoxelUnits[d] = DefaultUnits
	}
	return nil
}

// SetByConfig sets Voxels properties based on type-specific keywords in the configuration.
// Any property not described in the config is left as is.  See the Voxels help for listing
// of configurations.
func (props *Properties) SetByConfig(config dvid.Config) error {
	s, found, err := config.GetString("BlockSize")
	if err != nil {
		return err
	}
	if found {
		props.BlockSize, err = dvid.StringToPoint(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelSize")
	if err != nil {
		return err
	}
	if found {
		dvid.Infof("Changing resolution of voxels to %s\n", s)
		props.Resolution.VoxelSize, err = dvid.StringToNdFloat32(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelUnits")
	if err != nil {
		return err
	}
	if found {
		props.Resolution.VoxelUnits, err = dvid.StringToNdString(s, ",")
		if err != nil {
			return err
		}
	}
	return nil
}

type metadataT struct {
	Axes       []axisT
	Properties Properties
}

type axisT struct {
	Label      string
	Resolution float32
	Units      string
	Size       int32
	Offset     int32
}

// NdDataSchema returns the metadata in JSON for this Data
func (props *Properties) NdDataMetadata() (string, error) {
	var err error
	var size, offset dvid.Point

	dims := int(props.BlockSize.NumDims())
	if props.MinPoint == nil || props.MaxPoint == nil {
		zeroPt := make([]int32, dims)
		size, err = dvid.NewPoint(zeroPt)
		if err != nil {
			return "", err
		}
		offset = size
	} else {
		size = props.MaxPoint.Sub(props.MinPoint).AddScalar(1)
		offset = props.MinPoint
	}

	var axesName = []string{"X", "Y", "Z", "t", "c"}
	var metadata metadataT
	metadata.Axes = []axisT{}
	for dim := 0; dim < dims; dim++ {
		metadata.Axes = append(metadata.Axes, axisT{
			Label:      axesName[dim],
			Resolution: props.Resolution.VoxelSize[dim],
			Units:      props.Resolution.VoxelUnits[dim],
			Size:       size.Value(uint8(dim)),
			Offset:     offset.Value(uint8(dim)),
		})
	}
	metadata.Properties = *props

	m, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	datastore.Data
	Properties
}

// BlankImage initializes a blank image of appropriate size and depth for the
// current data values.  Returns an error if the geometry is not 2d.
func (d *Data) BlankImage(dstW, dstH int32) (*dvid.Image, error) {
	// Make sure values for this data can be converted into an image.
	valuesPerVoxel := int32(len(d.Properties.Values))
	if valuesPerVoxel < 1 || valuesPerVoxel > 4 {
		return nil, fmt.Errorf("Standard voxels type can't convert %d values/voxel into image.",
			valuesPerVoxel)
	}
	bytesPerValue, err := d.Properties.Values.BytesPerValue()
	if err != nil {
		return nil, err
	}

	unsupported := func() error {
		return fmt.Errorf("DVID doesn't support images for %d channels and %d bytes/channel",
			valuesPerVoxel, bytesPerValue)
	}

	var img image.Image
	stride := int(dstW * valuesPerVoxel * bytesPerValue)
	r := image.Rect(0, 0, int(dstW), int(dstH))
	imageBytes := int(dstH) * stride
	data := make([]uint8, imageBytes, imageBytes)
	switch valuesPerVoxel {
	case 1:
		switch bytesPerValue {
		case 1:
			img = &image.Gray{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 2:
			img = &image.Gray16{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 4:
			img = &image.NRGBA{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 8:
			img = &image.NRGBA64{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		default:
			return nil, unsupported()
		}
	case 4:
		switch bytesPerValue {
		case 1:
			img = &image.NRGBA{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		case 2:
			img = &image.NRGBA64{
				Stride: stride,
				Rect:   r,
				Pix:    data,
			}
		default:
			return nil, unsupported()
		}
	default:
		return nil, unsupported()
	}

	// Package Go image
	dst := new(dvid.Image)
	dst.SetFromGoImage(img, d.Properties.Values, d.Properties.Interpolable)
	return dst, nil
}

// Returns the image size necessary to compute an isotropic slice of the given dimensions.
// If isotropic is false, simply returns the original slice geometry.  If isotropic is true,
// uses the higher resolution dimension.
func (d *Data) HandleIsotropy2D(geom dvid.Geometry, isotropic bool) (dvid.Geometry, error) {
	if !isotropic {
		return geom, nil
	}
	// Get the voxel resolutions for this particular slice orientation
	resX, resY, err := geom.DataShape().GetFloat2D(d.Properties.VoxelSize)
	if err != nil {
		return nil, err
	}
	if resX == resY {
		return geom, nil
	}
	srcW := geom.Size().Value(0)
	srcH := geom.Size().Value(1)
	var dstW, dstH int32
	if resX < resY {
		// Use x resolution for all pixels.
		dstW = srcW
		dstH = int32(float32(srcH)*resX/resY + 0.5)
	} else {
		dstH = srcH
		dstW = int32(float32(srcW)*resY/resX + 0.5)
	}

	// Make altered geometry
	slice, ok := geom.(*dvid.OrthogSlice)
	if !ok {
		return nil, fmt.Errorf("can only handle isotropy for orthogonal 2d slices")
	}
	dstSlice := slice.Duplicate()
	dstSlice.SetSize(dvid.Point2d{dstW, dstH})
	return dstSlice, nil
}

// PutLocal adds image data to a version node, altering underlying blocks if the image
// intersects the block.
//
// The image filename glob MUST BE absolute file paths that are visible to the server.
// This function is meant for mass ingestion of large data files, and it is inappropriate
// to read gigabytes of data just to send it over the network to a local DVID.
func (d *Data) PutLocal(request datastore.Request, reply *datastore.Response) error {
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceStr, planeStr, offsetStr string
	filenames := request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceStr,
		&planeStr, &offsetStr)
	if len(filenames) == 0 {
		return fmt.Errorf("Need to include at least one file to add: %s", request)
	}

	// Get offset
	offset, err := dvid.StringToPoint(offsetStr, ",")
	if err != nil {
		return fmt.Errorf("Illegal offset specification: %s: %s", offsetStr, err.Error())
	}

	// Get list of files to add
	var addedFiles string
	if len(filenames) == 1 {
		addedFiles = filenames[0]
	} else {
		addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
	}
	dvid.Debugf(addedFiles + "\n")

	// Get plane
	plane, err := dvid.DataShapeString(planeStr).DataShape()
	if err != nil {
		return err
	}

	// Get Repo and IDs
	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	ctx := datastore.NewVersionedContext(d, versionID)

	// Load and PUT each image.
	numSuccessful := 0
	for _, filename := range filenames {
		sliceLog := dvid.NewTimeLog()
		img, _, err := dvid.ImageFromFile(filename)
		if err != nil {
			return fmt.Errorf("Error after %d images successfully added: %s",
				numSuccessful, err.Error())
		}
		slice, err := dvid.NewOrthogSlice(plane, offset, dvid.RectSize(img.Bounds()))
		if err != nil {
			return fmt.Errorf("Unable to determine slice: %s", err.Error())
		}

		e, err := d.NewExtHandler(slice, img)
		if err != nil {
			return err
		}
		storage.FileBytesRead <- len(e.Data())
		err = PutVoxels(ctx, d, e)
		if err != nil {
			return err
		}
		sliceLog.Debugf("%s put local %s", d.DataName(), slice)
		numSuccessful++
		offset = offset.Add(dvid.Point3d{0, 0, 1})
	}

	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return err
	}
	repo.AddToLog(request.Command.String())
	timedLog.Infof("RPC put local (%s) completed", addedFiles)
	return nil
}

// ----- IntData interface implementation ----------

// NewExtHandler returns an ExtData given some geometry and optional image data.
// If img is passed in, the function will initialize the ExtData with data from the image.
// Otherwise, it will allocate a zero buffer of appropriate size.
func (d *Data) NewExtHandler(geom dvid.Geometry, img interface{}) (ExtData, error) {
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	stride := geom.Size().Value(0) * bytesPerVoxel

	voxels := &Voxels{
		Geometry:  geom,
		values:    d.Properties.Values,
		stride:    stride,
		byteOrder: d.ByteOrder,
	}

	if img == nil {
		numVoxels := geom.NumVoxels()
		if numVoxels <= 0 {
			return nil, fmt.Errorf("Illegal geometry requested: %s", geom)
		}
		requestSize := int64(bytesPerVoxel) * numVoxels
		if requestSize > MaxDataRequest {
			return nil, fmt.Errorf("Requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
				requestSize, MaxDataRequest)
		}
		voxels.data = make([]uint8, requestSize)
	} else {
		switch t := img.(type) {
		case image.Image:
			var actualStride int32
			var err error
			voxels.data, _, actualStride, err = dvid.ImageData(t)
			if err != nil {
				return nil, err
			}
			if actualStride < stride {
				return nil, fmt.Errorf("Too little data in input image (expected stride %d)", stride)
			}
			voxels.stride = actualStride
		case []byte:
			voxels.data = t
			actualLen := int64(len(voxels.data))
			expectedLen := int64(bytesPerVoxel) * geom.NumVoxels()
			if actualLen != expectedLen {
				return nil, fmt.Errorf("PUT data was %d bytes, expected %d bytes for %s",
					actualLen, expectedLen, geom)
			}
		default:
			return nil, fmt.Errorf("Unexpected image type given to NewExtHandler(): %T", t)
		}
	}
	return voxels, nil
}

func (d *Data) BaseData() dvid.Data {
	return d
}

func (d *Data) Values() dvid.DataValues {
	return d.Properties.Values
}

func (d *Data) BlockSize() dvid.Point {
	return d.Properties.BlockSize
}

func (d *Data) Extents() *Extents {
	return &(d.Properties.Extents)
}

func (d *Data) String() string {
	return string(d.DataName())
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		&(d.Data),
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}
func (d *Data) ModifyConfig(config dvid.Config) error {
	props := &(d.Properties)
	if err := props.SetByConfig(config); err != nil {
		return err
	}
	return nil
}

type SendOp struct {
	socket message.Socket
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
// TODO -- handle versioning of the ROI coming.  For not, only allow root version of ROI.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	db, err := storage.BigDataStore()
	if err != nil {
		return err
	}
	//wg := new(sync.WaitGroup)
	server.SpawnGoroutineMutex.Lock()

	// Get the ROI
	var roiIterator *roi.Iterator
	if len(roiname) != 0 {
		versionID, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			return err
		}
		roiIterator, err = roi.NewIterator(dvid.DataString(roiname), versionID, d)
		if err != nil {
			return err
		}
	}

	// Get the entire range of keys for this instance's voxel blocks
	keyBeg := VoxelBlockMinKey(d.InstanceID())
	keyEnd := VoxelBlockMaxKey(d.InstanceID())

	// Send this instance's voxel blocks down the socket
	var blocksTotal, blocksSent int
	chunkOp := &storage.ChunkOp{&SendOp{s}, nil}
	err = db.ProcessRange(nil, keyBeg, keyEnd, chunkOp, func(chunk *storage.Chunk) {
		if chunk.KeyValue == nil {
			dvid.Errorf("Received nil keyvalue sending voxel chunks\n")
		}
		blocksTotal++
		if roiIterator != nil && !roiIterator.Inside(chunk.K) {
			return // don't send if this chunk is outside ROI
		}
		blocksSent++
		if err := s.SendKeyValue("voxels", storage.BigData, chunk.KeyValue); err != nil {
			dvid.Errorf("Error sending voxel chunks through nanomsg socket: %s\n", err.Error())
		}
	})
	if err != nil {
		server.SpawnGoroutineMutex.Unlock()
		return fmt.Errorf("Error in voxels %q range query: %s", d.DataName(), err.Error())
	}

	server.SpawnGoroutineMutex.Unlock()
	if err != nil {
		return err
	}
	//wg.Wait()
	if roiIterator == nil {
		dvid.Infof("Sent %d %s voxel blocks\n", blocksTotal, d.DataName())
	} else {
		dvid.Infof("Sent %d %s voxel blocks (out of %d total) within ROI %q\n",
			blocksSent, d.DataName(), blocksTotal, roiname)
	}
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "load":
		if len(request.Command) < 5 {
			return fmt.Errorf("Poorly formatted load command.  See command-line help.")
		}
		// Parse the request
		var uuidStr, dataName, cmdStr, offsetStr string
		filenames, err := request.FilenameArgs(1, &uuidStr, &dataName, &cmdStr, &offsetStr)
		if err != nil {
			return err
		}
		if len(filenames) == 0 {
			hostname, _ := os.Hostname()
			return fmt.Errorf("Couldn't find any files to add.  Are they visible to DVID server on %s?",
				hostname)
		}

		// Get offset
		offset, err := dvid.StringToPoint(offsetStr, ",")
		if err != nil {
			return fmt.Errorf("Illegal offset specification: %s: %s", offsetStr, err.Error())
		}

		// Get list of files to add
		var addedFiles string
		if len(filenames) == 1 {
			addedFiles = filenames[0]
		} else {
			addedFiles = fmt.Sprintf("filenames: %s [%d more]", filenames[0], len(filenames)-1)
		}
		dvid.Debugf(addedFiles + "\n")

		uuid, versionID, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}
		repo, err := datastore.RepoFromUUID(uuid)
		if err != nil {
			return err
		}
		if err = repo.AddToLog(request.Command.String()); err != nil {
			return err
		}
		return LoadImages(versionID, d, offset, filenames)

	case "put":
		if len(request.Command) < 7 {
			return fmt.Errorf("Poorly formatted put command.  See command-line help.")
		}
		source := request.Command[4]
		switch source {
		case "local":
			return d.PutLocal(request, reply)
		case "remote":
			return fmt.Errorf("put remote not yet implemented")
		default:
			return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
				d.DataName(), d.TypeName(), request.TypeCommand())
		}

	default:
		return fmt.Errorf("Unknown command.  Data instance '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	return nil
}

// Prints RGBA of first n x n pixels of image with header string.
func debugData(img image.Image, message string) {
	data, _, stride, _ := dvid.ImageData(img)
	var n = 3 // neighborhood to write
	fmt.Printf("%s>\n", message)
	for y := 0; y < n; y++ {
		for x := 0; x < n; x++ {
			i := y*int(stride) + x*4
			fmt.Printf("[%3d %3d %3d %3d]  ", data[i], data[i+1], data[i+2], data[i+3])
		}
		fmt.Printf("\n")
	}
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	repo, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n",
			d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	dvid.Infof("voxels.ServeHTTP(): versionID %d\n", versionID)

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	var op OpType
	switch action {
	case "get":
		op = GetOp
	case "post":
		op = PutOp
	default:
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	// Get query strings and possible roi
	var roiptr *ROI
	queryValues := r.URL.Query()
	roiname := dvid.DataString(queryValues.Get("roi"))
	if len(roiname) != 0 {
		roiptr = new(ROI)
		attenuationStr := queryValues.Get("attenuation")
		if len(attenuationStr) != 0 {
			attenuation, err := strconv.Atoi(attenuationStr)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if attenuation < 1 || attenuation > 7 {
				server.BadRequest(w, r, "Attenuation should be from 1 to 7 (divides by 2^n)")
				return
			}
			roiptr.attenuation = uint8(attenuation)
		}
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && op == PutOp {
		fmt.Printf("Setting configuration of data '%s'\n", d.DataName())
		config, err := server.DecodeJSON(r)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := repo.Save(); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		fmt.Fprintf(w, "Changed '%s' based on received configuration:\n%s\n", d.DataName(), config)
		return
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "Incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return
	case "metadata":
		jsonStr, err := d.NdDataMetadata()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/vnd.dvid-nd-data+json")
		fmt.Fprintln(w, jsonStr)
		return
	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return
	case "arb":
		// GET  <api URL>/node/<UUID>/<data name>/arb/<top left>/<top right>/<bottom left>/<res>[/<format>]
		if len(parts) < 8 {
			server.BadRequest(w, r, "'%s' must be followed by top-left/top-right/bottom-left/res", parts[3])
			return
		}
		queryStrings := r.URL.Query()
		if queryStrings.Get("throttle") == "on" {
			select {
			case <-server.Throttle:
				// Proceed with operation, returning throttle token to server at end.
				defer func() {
					server.Throttle <- 1
				}()
			default:
				throttleMsg := fmt.Sprintf("Server already running maximum of %d throttled operations",
					server.MaxThrottledOps)
				http.Error(w, throttleMsg, http.StatusServiceUnavailable)
				return
			}
		}
		img, err := d.GetArbitraryImage(storeCtx, parts[4], parts[5], parts[6], parts[7])
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		var formatStr string
		if len(parts) >= 9 {
			formatStr = parts[8]
		}
		err = dvid.WriteImageHttp(w, img.Get(), formatStr)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: Arbitrary image (%s)", r.Method, r.URL)

	case "raw", "isotropic":
		// GET  <api URL>/node/<UUID>/<data name>/isotropic/<dims>/<size>/<offset>[/<format>]
		if len(parts) < 7 {
			server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
			return
		}
		var isotropic bool = (parts[3] == "isotropic")
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		switch plane.ShapeDimensions() {
		case 2:
			slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if op == PutOp {
				if isotropic {
					err := fmt.Errorf("can only PUT 'raw' not 'isotropic' images")
					server.BadRequest(w, r, err.Error())
					return
				}
				// TODO -- Put in format checks for POSTed image.
				postedImg, _, err := dvid.ImageFromPOST(r)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				e, err := d.NewExtHandler(slice, postedImg)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				err = PutVoxels(storeCtx, d, e)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			} else {
				rawSlice, err := d.HandleIsotropy2D(slice, isotropic)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				e, err := d.NewExtHandler(rawSlice, nil)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				img, err := GetImage(storeCtx, d, e, roiptr)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if isotropic {
					dstW := int(slice.Size().Value(0))
					dstH := int(slice.Size().Value(1))
					img, err = img.ScaleImage(dstW, dstH)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				var formatStr string
				if len(parts) >= 8 {
					formatStr = parts[7]
				}
				err = dvid.WriteImageHttp(w, img.Get(), formatStr)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, plane, r.URL)
		case 3:
			queryStrings := r.URL.Query()
			if queryStrings.Get("throttle") == "on" {
				select {
				case <-server.Throttle:
					// Proceed with operation, returning throttle token to server at end.
					defer func() {
						server.Throttle <- 1
					}()
				default:
					throttleMsg := fmt.Sprintf("Server already running maximum of %d throttled operations",
						server.MaxThrottledOps)
					http.Error(w, throttleMsg, http.StatusServiceUnavailable)
					return
				}
			}
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			if op == GetOp {
				e, err := d.NewExtHandler(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				if roiptr != nil {
					roiptr.Iter, err = roi.NewIterator(roiname, versionID, e)
					if err != nil {
						server.BadRequest(w, r, err.Error())
						return
					}
				}
				data, err := GetVolume(storeCtx, d, e, roiptr)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				w.Header().Set("Content-type", "application/octet-stream")
				_, err = w.Write(data)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			} else {
				if isotropic {
					err := fmt.Errorf("can only PUT 'raw' not 'isotropic' images")
					server.BadRequest(w, r, err.Error())
					return
				}
				data, err := ioutil.ReadAll(r.Body)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				e, err := d.NewExtHandler(subvol, data)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
				err = PutVoxels(storeCtx, d, e)
				if err != nil {
					server.BadRequest(w, r, err.Error())
					return
				}
			}
			timedLog.Infof("HTTP %s: %s (%s)", r.Method, subvol, r.URL)
		default:
			server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
		}
	default:
		server.BadRequest(w, r, "Unrecognized API call for voxels %q.  See API help.", d.DataName())
	}
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

	// If there's an ROI, if outside ROI, use blank buffer or allow scaling via attenuation.
	var zeroOut bool
	var attenuation uint8
	if op.ROI != nil && op.ROI.Iter != nil && !op.ROI.Iter.Inside(chunk.K) {
		if op.ROI.attenuation == 0 {
			zeroOut = true
		}
		attenuation = op.ROI.attenuation
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var err error
	var blockData []byte
	if zeroOut || chunk == nil || chunk.V == nil {
		blockData = make([]byte, d.BlockSize().Prod()*int64(op.Values().BytesPerElement()))
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in '%s': %s\n",
				d.DataName(), err.Error())
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
		db, err := storage.BigDataStore()
		if err != nil {
			dvid.Errorf("Unable to obtain BigData store in %q: %s\n", d.DataName(), err.Error())
			return
		}
		serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
		if err != nil {
			dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
			return
		}
		db.Put(nil, chunk.K, serialization)
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
