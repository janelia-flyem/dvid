/*
	This file contains code for denormalized representations of labelmap data, e.g., indices
	for fast queries of all labels meeting given size restrictions, or sparse volume
	representations for a label.
*/

package labelmap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

func init() {
	sqrt3div3 := math.Sqrt(3.0) / 3.0
	sqrt2div2 := math.Sqrt(2.0) / 2.0

	// Initialize Zucker-Hummel 3x3x3 filter

	// Fill in z = 0 for Z gradient kernel
	zhZ[0][0][0] = -sqrt3div3
	zhZ[1][0][0] = -sqrt2div2
	zhZ[2][0][0] = -sqrt3div3

	zhZ[0][1][0] = -sqrt2div2
	zhZ[1][1][0] = -1.0
	zhZ[2][1][0] = -sqrt2div2

	zhZ[0][2][0] = -sqrt3div3
	zhZ[1][2][0] = -sqrt2div2
	zhZ[2][2][0] = -sqrt3div3

	// Fill in z=1,2 for Z gradient kernel
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			zhZ[x][y][1] = 0.0
			zhZ[x][y][2] = -zhZ[x][y][0]
		}
	}

	// Copy Z gradient kernel to X and Y gradient kernels
	for z := 0; z < 3; z++ {
		for y := 0; y < 3; y++ {
			for x := 0; x < 3; x++ {
				zhX[z][x][y] = zhZ[x][y][z]
				zhY[x][z][y] = zhZ[x][y][z]
			}
		}
	}
}

// Sparse Volume binary encoding payload descriptors.
const (
	PayloadBinary      byte = 0x00
	PayloadGrayscale8       = 0x01
	PayloadGrayscale16      = 0x02
	PayloadNormal16         = 0x04
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
)

var (
	emptyValue     = []byte{}
	zeroLabelBytes = make([]byte, 8, 8)

	zhX, zhY, zhZ [3][3][3]float64
)

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
	default:
		return "Unknown Key Type"
	}
}

// NewLabelSpatialMapKey returns a datastore.DataKey that encodes a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func (d *Data) NewLabelSpatialMapKey(vID dvid.VersionLocalID, label uint64, block dvid.IndexZYX) *datastore.DataKey {
	index := make([]byte, 1+8+dvid.IndexZYXSize)
	index[0] = byte(KeyLabelSpatialMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:9+dvid.IndexZYXSize], block.Bytes())
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSizesKey returns a datastore.DataKey that encodes a "size + mapped label".
func (d *Data) NewLabelSizesKey(vID dvid.VersionLocalID, size, label uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSurfaceKey returns a datastore.DataKey that provides a surface for a given label.
func (d *Data) NewLabelSurfaceKey(vID dvid.VersionLocalID, label uint64) *datastore.DataKey {
	index := make([]byte, 8)
	binary.BigEndian.PutUint64(index, label)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

type sparseOp struct {
	versionID dvid.VersionLocalID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// Adds retrieved RLE runs to an encoding.
func (d *Data) processLabelRuns(chunk *storage.Chunk) {
	op := chunk.Op.(*sparseOp)
	op.numBlocks++
	op.encoding = append(op.encoding, chunk.V...)
	op.numRuns += uint32(len(chunk.V) / 16)
	chunk.Wg.Done()
}

// Encodes RLE as bytes.
func encodeRuns(starts []dvid.Point3d, lengths []int32) ([]byte, error) {
	if starts == nil {
		return nil, fmt.Errorf("Cannot encode run with nil slice of start points")
	}
	if lengths == nil {
		return nil, fmt.Errorf("Cannot encode run with nil slice of lengths")
	}
	if len(starts) != len(lengths) {
		return nil, fmt.Errorf("#start points (%d) != #lengths (%d)", len(starts), len(lengths))
	}
	buf := new(bytes.Buffer)
	for i, start := range starts {
		binary.Write(buf, binary.LittleEndian, start[0])
		binary.Write(buf, binary.LittleEndian, start[1])
		binary.Write(buf, binary.LittleEndian, start[2])
		binary.Write(buf, binary.LittleEndian, lengths[i])
	}
	return buf.Bytes(), nil
}

// Get the total number of voxels and runs in an encoded RLE.
func statsRuns(encoding []byte) (numVoxels, numRuns int32, err error) {
	if len(encoding)%16 != 0 {
		err = fmt.Errorf("RLE encoding doesn't have correct # bytes: %d", len(encoding))
		return
	}
	coord := make([]byte, 12)
	buf := bytes.NewBuffer(encoding)
	var n int
	var length int32
	for {
		n, err = buf.Read(coord)
		if n == 0 || err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		numRuns++
		err = binary.Read(buf, binary.LittleEndian, &length)
		if err != nil {
			return
		}
		numVoxels += length
	}
	return
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func (d *Data) computeSizes(sizeCh chan *storage.Chunk, db storage.OrderedKeyValueSetter,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	dvid.Log(dvid.Debug, "Storing size in voxels for all labels in labelmap '%s'\n", d.DataName())

	const BATCH_SIZE = 10000
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Log(dvid.Normal, "Storage engine does not support Batch PUT.  Aborting\n")
		return
	}
	batch := batcher.NewBatch()

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
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			batch.Put(key, emptyValue)
			if err := batch.Commit(); err != nil {
				dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
					d.DataName(), err.Error())
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)

		// Compute the size
		numVoxels, _, err := statsRuns(chunk.V)
		if err != nil {
			dvid.Log(dvid.Normal, "Error on computing label sizes: %s\n", err.Error())
			return
		}

		// If we are a new label, store size
		if notFirst && label != curLabel {
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			curSize = 0
			batch.Put(key, emptyValue)
			putsInBatch++
			if putsInBatch%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
						d.DataName(), err.Error())
					return
				}
				batch = batcher.NewBatch()
			}
		}
		curLabel = label
		curSize += uint64(numVoxels)
		notFirst = true
	}
}

type sparseVol struct {
	alreadySet bool
	numVoxels  int32
	minPt      dvid.Point3d
	maxPt      dvid.Point3d
	//minChunk   dvid.ChunkPoint3d
	//maxChunk   dvid.ChunkPoint3d
	label uint64
	key   *datastore.DataKey
	rles  []rle
	pos   int // Current index into rle.
}

type rle struct {
	//block  dvid.ChunkPoint3d
	start  dvid.Point3d
	length int32
}

// Adds RLEs to sparseVol and increments the position accordingly.
// The RLE buffer is expanded as needed.  Min and max 3d positions are noted.
func (vol *sparseVol) AddRLEs(encoding []byte) error {
	if vol.rles == nil {
		vol.rles = make([]rle, 10)
	}
	lenEncoding := len(encoding)
	if lenEncoding%16 != 0 {
		return fmt.Errorf("RLE encoding doesn't have correct # bytes: %d", len(encoding))
	}
	if !vol.alreadySet {
		vol.pos = 0
		vol.numVoxels = 0
	}
	var x, y, z, length int32
	buf := bytes.NewBuffer(encoding)
	for {
		err := binary.Read(buf, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &y); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &z); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return err
		}
		if vol.pos >= len(vol.rles) {
			newsize := cap(vol.rles) * 2
			tmp := make([]rle, newsize, newsize)
			copy(tmp[0:len(vol.rles)], vol.rles)
			vol.rles = tmp
		}
		pt := dvid.Point3d{x, y, z}
		vol.rles[vol.pos] = rle{pt, length}
		vol.numVoxels += length
		vol.pos++
		if vol.alreadySet {
			vol.minPt.SetMinimum(pt)
			vol.maxPt.SetMaximum(dvid.Point3d{x + length, y, z})
		} else {
			vol.minPt = pt
			vol.maxPt = pt
			vol.alreadySet = true
		}
	}
	return nil
}

type BinaryVolume struct {
	offset      dvid.Point3d
	size        dvid.Point3d
	xanisotropy float64
	yanisotropy float64
	zanisotropy float64
	data        []byte
}

func (d *Data) NewBinaryVolume(offset, size dvid.Point3d) (*BinaryVolume, error) {
	labels, err := d.Labels.GetData()
	if err != nil {
		return nil, err
	}

	minRes := labels.Resolution.VoxelSize.GetMin()
	numBytes := size[0] * size[1] * size[2]
	return &BinaryVolume{
		offset:      offset,
		size:        size,
		xanisotropy: float64(labels.Resolution.VoxelSize[0] / minRes),
		yanisotropy: float64(labels.Resolution.VoxelSize[1] / minRes),
		zanisotropy: float64(labels.Resolution.VoxelSize[2] / minRes),
		data:        make([]byte, numBytes, numBytes),
	}, nil
}

// Shift the buffer up by dz voxels.
func (binvol *BinaryVolume) ShiftUp(dz int32) {
	binvol.offset[2] += dz
	sliceBytes := binvol.size[0] * binvol.size[1]
	var i0, j0, z int32
	j0 = dz * sliceBytes
	maxStartI := int32(len(binvol.data)) - sliceBytes
	for z = 0; z < binvol.size[2]; z++ {
		if j0 <= maxStartI {
			copy(binvol.data[i0:i0+sliceBytes], binvol.data[j0:j0+sliceBytes])
			j0 += sliceBytes
		} else if i0 <= maxStartI {
			for i := i0; i < i0+sliceBytes; i++ {
				binvol.data[i] = 0
			}
		}
		i0 += sliceBytes
	}
}

func (binvol *BinaryVolume) CheckSurface(x, y, z int32) (normx, normy, normz float32, isSurface bool) {
	nx := binvol.size[0]
	nxy := binvol.size[1] * nx
	if binvol.data[z*nxy+y*nx+x] == 0 {
		return
	}
	// If any neighbor is 0, this is a surface voxel.
	var ix, iy, iz, pz, py, p int32
	for iz = z - 1; iz <= z+1; iz++ {
		pz = iz * nxy
		for iy = y - 1; iy <= y+1; iy++ {
			p = pz + iy*nx + x - 1
			for ix = 0; ix < 3; ix++ {
				if binvol.data[p] == 0 {
					isSurface = true
					goto ComputeNormal
				}
				p++
			}
		}
	}

ComputeNormal:
	var xgrad, ygrad, zgrad float64
	pz = (z - 1) * nxy
	for iz = 0; iz < 3; iz++ {
		py = (y - 1) * nx
		for iy = 0; iy < 3; iy++ {
			p = pz + py + x - 1
			for ix = 0; ix < 3; ix++ {
				value := float64(binvol.data[p])
				xgrad += value * zhX[ix][iy][iz]
				ygrad += value * zhY[ix][iy][iz]
				zgrad += value * zhZ[ix][iy][iz]
				p++
			}
			py += nx
		}
		pz += nxy
	}

	// Cheap hack to try to compensate for anisotropy.
	// TODO -- Implement distance transform followed by gradient to better smooth
	// and handle anisotropy.
	xgrad /= binvol.xanisotropy
	ygrad /= binvol.yanisotropy
	zgrad /= binvol.zanisotropy

	mag := math.Sqrt(xgrad*xgrad + ygrad*ygrad + zgrad*zgrad)
	normx = float32(xgrad / mag)
	normy = float32(ygrad / mag)
	normz = float32(zgrad / mag)
	return
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func (d *Data) computeSurface(surfaceCh chan *storage.Chunk, db storage.OrderedKeyValueSetter,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	defer func() {
		wg.Done()
		server.HandlerToken <- 1
	}()

	// Sequentially process all the sparse volume data for each label
	var curVol sparseVol
	var curLabel uint64
	notFirst := false
	for {
		chunk := <-surfaceCh
		if chunk == nil {
			if notFirst {
				if err := d.computeAndSaveSurface(&curVol); err != nil {
					dvid.Log(dvid.Normal, "Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)
		if label != curLabel || label == 0 {
			if notFirst {
				if err := d.computeAndSaveSurface(&curVol); err != nil {
					dvid.Log(dvid.Normal, "Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			curVol.key = d.NewLabelSurfaceKey(versionID, label)
			curVol.label = label
			curVol.alreadySet = false
		}

		curVol.AddRLEs(chunk.V)
		curLabel = label
		notFirst = true
	}
}

// TODO -- can be more efficient in buffer space by only needing 8 blocks worth
// of data (4 for current XY processing and 4 for next Z), but for simplicity this
// function uses total XY extents + 2 * block size in Z.
func (d *Data) computeAndSaveSurface(vol *sparseVol) error {
	startTime := time.Now()

	labels, err := d.Labels.GetData()
	if err != nil {
		return err
	}

	// Allocate buffer for processing
	dx := vol.maxPt[0] - vol.minPt[0] + 3
	dy := vol.maxPt[1] - vol.minPt[1] + 3
	dz := vol.maxPt[2] - vol.minPt[2] + 3

	blockNz := labels.BlockSize().Value(2)
	if dz > 2*blockNz+1 {
		dz = 2*blockNz + 1
	}

	// Allocate buffer for processing
	offset := vol.minPt.AddScalar(-1).(dvid.Point3d)
	binvol, err := d.NewBinaryVolume(offset, dvid.Point3d{dx, dy, dz})
	if err != nil {
		return err
	}

	var vertexBuf, normalBuf bytes.Buffer
	var surfaceSize uint32
	rleI := 0
	dvid.Log(dvid.Debug, "Label %d, # voxels %d, size %s, minPt %s, maxPt %s: ",
		vol.label, vol.numVoxels, binvol.size, vol.minPt, vol.maxPt)
	defer func() {
		dvid.Log(dvid.Debug, "%s", time.Since(startTime))
	}()

	for {
		var minX int32 = dx
		var maxX int32 = 0
		var minY int32 = dy
		var maxY int32 = 0
		// Populate the buffer
		for {
			if rleI >= vol.pos {
				// We've added entire volume.
				break
			}
			r := vol.rles[rleI]
			bz := r.start[2] - binvol.offset[2]
			if bz >= dz {
				// rles have filled this buffer.
				break
			}
			by := r.start[1] - binvol.offset[1]
			bx := r.start[0] - binvol.offset[0]
			p := bz*dx*dy + by*dx + bx
			for i := int32(0); i < r.length; i++ {
				binvol.data[p+i] = 255
			}

			// For this buffer, set bounds.  For large sparse volumes that snake
			// through a lot of space, the XY footprint might be relatively small.
			if minX > bx {
				minX = bx
			}
			if maxX < bx+r.length {
				maxX = bx + r.length
			}
			if minY > by {
				minY = by
			}
			if maxY < by {
				maxY = by
			}
			rleI++
		}

		// Iterate through XY layers to compute surface and normal
		var x, y, z int32
		for z = 1; z <= blockNz; z++ {
			if binvol.offset[2]+z > vol.maxPt[2] {
				// We've passed through all of this sparse volume's voxels
				break
			}
			// TODO -- Keep track of bounding box per Z and limit checks to it.
			for y = minY; y <= maxY; y++ {
				for x = minX; x <= maxX; x++ {
					nx, ny, nz, isSurface := binvol.CheckSurface(x, y, z)
					if isSurface {
						surfaceSize++
						fx := float32(x + binvol.offset[0])
						fy := float32(y + binvol.offset[1])
						fz := float32(z + binvol.offset[2])
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fx); err != nil {
							return err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fy); err != nil {
							return err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fz); err != nil {
							return err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nx); err != nil {
							return err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, ny); err != nil {
							return err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nz); err != nil {
							return err
						}
					}
				}
			}
		}

		// Shift buffer
		if binvol.offset[2]+blockNz < vol.maxPt[2] {
			binvol.ShiftUp(blockNz)
		} else {
			break
		}
	}

	// Store computation
	// TODO -- Make this more efficient in terms of memory
	numBytes := 4 + vertexBuf.Len() + normalBuf.Len()
	data := make([]byte, numBytes, numBytes)
	i := 0
	j := 4
	binary.LittleEndian.PutUint32(data[i:j], surfaceSize)
	i = j
	j += vertexBuf.Len()
	copy(data[i:j], vertexBuf.Bytes())
	i = j
	j += normalBuf.Len()
	copy(data[i:j], normalBuf.Bytes())

	db, err := server.OrderedKeyValueSetter()
	if err != nil {
		return err
	}
	// Surface blobs are always stored using gzip with best compression, trading off time
	// during the store for speed during interactive GETs.
	compression, _ := dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
	serialization, err := dvid.SerializeData(data, compression, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("Unable to serialize data in surface computation: %s\n", err.Error())
	}
	return db.Put(vol.key, serialization)
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
// If maxSize is 0, all mapped labels are returned >= minSize.
func (d *Data) GetSizeRange(uuid dvid.UUID, minSize, maxSize uint64) (string, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return "{}", err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return "{}", err
	}

	// Get the start/end keys for the size range.
	firstKey := d.NewLabelSizesKey(versionID, minSize, 0)
	var upperBound uint64
	if maxSize != 0 {
		upperBound = maxSize
	} else {
		upperBound = math.MaxUint64
	}
	lastKey := d.NewLabelSizesKey(versionID, upperBound, math.MaxUint64)

	// Grab all keys for this range in one sequential read.
	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return "{}", err
	}
	fmt.Printf("# keys: %d\n", len(keys))

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		dataKey := key.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		labels[i] = binary.LittleEndian.Uint64(indexBytes[9:17])
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// GetLabelsInVolume returns a JSON list of mapped labels that intersect a volume bounded
// by the specified block coordinates.  Note that the blocks are specified using block
// coordinates, so if this data instance has 32 x 32 x 32 voxel blocks, and we specify
// min block (1,2,3) and max block (3,4,5), the subvolume in voxels will be from min voxel
// point (32, 64, 96) to max voxel point (96, 128, 160).
func (d *Data) GetLabelsInVolume(uuid dvid.UUID, minBlock, maxBlock dvid.ChunkPoint3d) (string, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return "{}", err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return "{}", err
	}

	// Get the mappings for this span of keys by using just spatial indices.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	maxLabelBytes := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

	offset := 1 + dvid.IndexZYXSize + 8 // index here = s + a + b, and we want only b
	labelset := make(map[uint64]bool, 10)
	for it := dvid.NewIndexZYXIterator(minBlock, maxBlock); it.Valid(); it.NextSpan() {
		// Get keys for this span of blocks
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return "{}", err
		}
		startKey := d.NewSpatialMapKey(versionID, indexBeg, nil, 0)
		endKey := d.NewSpatialMapKey(versionID, indexEnd, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		var keys []storage.Key
		keys, err = db.KeysInRange(startKey, endKey)
		if err != nil {
			return "{}", err
		}

		// Add mapped labels for these keys into the set
		for _, key := range keys {
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
			mappedLabel := binary.BigEndian.Uint64(indexBytes[offset : offset+8])
			labelset[mappedLabel] = true
		}
	}

	// Convert set to a JSON compatible list.
	numLabels := len(labelset)
	dvid.Log(dvid.Debug, "Found %d labels that intersect subvolume with block coords %s -> %s\n", numLabels,
		minBlock, maxBlock)
	labellist := make([]uint64, numLabels, numLabels)
	i := 0
	for label, _ := range labelset {
		labellist[i] = label
		i++
	}
	m, err := json.Marshal(labellist)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// GetLabelAtPoint returns a mapped label for a given point.
func (d *Data) GetLabelAtPoint(uuid dvid.UUID, pt dvid.Point) (uint64, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return 0, err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return 0, err
	}

	// Compute the block key that contains the given point.
	coord, ok := pt.(dvid.Chunkable)
	if !ok {
		return 0, fmt.Errorf("Can't determine block of point %s", pt)
	}
	labels, err := d.Labels.GetData()
	if err != nil {
		return 0, err
	}
	blockSize := labels.BlockSize()
	blockCoord := coord.Chunk(blockSize).(dvid.ChunkPoint3d) // TODO -- Get rid of this cast
	key := d.DataKey(versionID, dvid.IndexZYX(blockCoord))

	// Retrieve the block of labels
	serialization, err := db.Get(key)
	if err != nil {
		return 0, fmt.Errorf("Error getting '%s' block for index %s\n", d.DataName(), blockCoord)
	}
	labelData, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return 0, fmt.Errorf("Unable to deserialize block %s in '%s': %s\n",
			blockCoord, d.DataName(), err.Error())
	}

	// Retrieve the particular label within the block.
	ptInBlock := coord.PointInChunk(blockSize)
	nx := blockSize.Value(0)
	nxy := nx * blockSize.Value(1)
	i := (ptInBlock.Value(0) + ptInBlock.Value(1)*nx + ptInBlock.Value(2)*nxy) * 8

	// Apply mapping.
	return d.GetLabelMapping(versionID, labelData[i:i+8])
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
func (d *Data) GetSparseVol(uuid dvid.UUID, label uint64) ([]byte, error) {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return nil, err
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(PayloadBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))
	binary.Write(buf, binary.LittleEndian, byte(0))
	buf.WriteByte(byte(0))
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	// Get the start/end keys for this body's KeyLabelSpatialMap (b + s) keys.
	firstKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MinIndexZYX)
	lastKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MaxIndexZYX)

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	wg := new(sync.WaitGroup)
	op := &sparseOp{versionID: versionID, encoding: buf.Bytes()}
	err = db.ProcessRange(firstKey, lastKey, &storage.ChunkOp{op, wg}, d.processLabelRuns)
	if err != nil {
		return nil, err
	}
	wg.Wait()

	binary.LittleEndian.PutUint32(op.encoding[8:12], op.numRuns)

	dvid.Log(dvid.Debug, "For data '%s' label %d: found %d blocks, %d runs\n",
		d.DataName(), label, op.numBlocks, op.numRuns)
	return op.encoding, nil
}

// GetSurface returns a byte array with # voxels and float32 arrays for vertices and
// normals.
func (d *Data) GetSurface(uuid dvid.UUID, label uint64) (s []byte, found bool, err error) {
	service := server.DatastoreService()
	_, versionID, e := service.LocalIDFromUUID(uuid)
	if e != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, e.Error())
		return
	}

	// Retrieve the precomputed surface or that it's not available.
	key := d.NewLabelSurfaceKey(versionID, label)

	db, e := server.OrderedKeyValueGetter()
	if e != nil {
		err = e
		return
	}
	data, e := db.Get(key)
	if e != nil {
		err = fmt.Errorf("Error in retrieving surface for key '%s': %s", key, e.Error())
		return
	}
	if data == nil {
		return
	}
	uncompress := false
	s, _, e = dvid.DeserializeData(data, uncompress)
	if e != nil {
		err = fmt.Errorf("Unable to deserialize surface for key '%s': %s\n", key, e.Error())
		return
	}
	found = true
	return
}

type denormOp struct {
	source    *labels64.Data
	mapped    voxels.ExtHandler // Can store mapped labels into this if provided.
	destID    dvid.DataLocalID
	versionID dvid.VersionLocalID
	mapping   map[string]uint64
}

// GetMappedImage retrieves a 2d image from a version node given a geometry of voxels.
func (d *Data) GetMappedImage(uuid dvid.UUID, e voxels.ExtHandler) (*dvid.Image, error) {
	if err := d.GetMappedVoxels(uuid, e); err != nil {
		return nil, err
	}
	return e.GetImage2d()
}

// GetMappedVolume retrieves a n-d volume from a version node given a geometry of voxels.
func (d *Data) GetMappedVolume(uuid dvid.UUID, e voxels.ExtHandler) ([]byte, error) {
	if err := d.GetMappedVoxels(uuid, e); err != nil {
		return nil, err
	}
	return e.Data(), nil
}

// GetMappedVoxels copies mapped labels for each voxel for a version to an ExtHandler, e.g.,
// a requested subvolume or 2d image.
func (d *Data) GetMappedVoxels(uuid dvid.UUID, e voxels.ExtHandler) error {
	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		return fmt.Errorf("Could not determine versionID in %s.ProcessSpatially(): %s",
			d.DataID.DataName(), err.Error())
	}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return err
	}

	labels, err := d.Labels.GetData()
	if err != nil {
		dvid.Error("Could not get labels64 data for '%s'", d.Labels)
	}

	wg := new(sync.WaitGroup)
	dataID := d.DataID.ID
	datasetID := d.DataID.DsetID
	for it, err := e.IndexIterator(labels.BlockSize()); err == nil && it.Valid(); it.NextSpan() {
		indexBeg, indexEnd, err := it.IndexSpan()
		if err != nil {
			return err
		}
		indexBegZYX, ok := indexBeg.(dvid.IndexZYX)
		if !ok {
			return fmt.Errorf("First spatial index for mapped voxels request was not dvid.IndexZYX: %s", indexBeg)
		}
		indexEndZYX, ok := indexEnd.(dvid.IndexZYX)
		if !ok {
			return fmt.Errorf("Last spatial index for mapped voxels request was not dvid.IndexZYX: %s", indexEnd)
		}

		// Get the mappings for this span of key/value by using just spatial indices.
		// We work with the spatial index (s), original label (a), and mapped label (b).
		maxLabelBytes := make([]byte, 8, 8)
		binary.BigEndian.PutUint64(maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		sabKeyBeg := d.NewSpatialMapKey(versionID, indexBegZYX, nil, 0)
		sabKeyEnd := d.NewSpatialMapKey(versionID, indexEndZYX, maxLabelBytes, 0xFFFFFFFFFFFFFFFF)

		var keys []storage.Key
		keys, err = db.KeysInRange(sabKeyBeg, sabKeyEnd)
		if err != nil {
			return err
		}
		numKeys := len(keys)
		if numKeys == 0 {
			continue
		}

		// Cache this layer of blocks' mappings.
		labelOffset := 1 + dvid.IndexZYXSize // index here = s + a + b
		mapping := make(map[string]uint64, numKeys)
		for _, key := range keys {
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
			label := string(indexBytes[labelOffset : labelOffset+8])
			mappedLabel := binary.BigEndian.Uint64(indexBytes[labelOffset+8 : labelOffset+16])
			mapping[label] = mappedLabel
		}

		// Send the entire range of key/value pairs to chunk mapper
		chunkOp := &storage.ChunkOp{&denormOp{labels, e, 0, versionID, mapping}, wg}
		startKey := &datastore.DataKey{datasetID, dataID, versionID, indexBeg}
		endKey := &datastore.DataKey{datasetID, dataID, versionID, indexEnd}
		err = db.ProcessRange(startKey, endKey, chunkOp, d.MapChunk)
		if err != nil {
			return fmt.Errorf("Unable to GET data %s: %s", d.DataID.DataName(), err.Error())
		}
	}
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

// Iterate through all blocks in the associated label volume, computing the spatial indices
// for bodies and the mappings for each spatial index.
func (d *Data) ProcessSpatially(uuid dvid.UUID) {
	dvid.Log(dvid.Normal, "Adding spatial information from label volume %s ...\n", d.DataName())

	_, versionID, err := server.DatastoreService().LocalIDFromUUID(uuid)
	if err != nil {
		dvid.Error("Could not determine versionID in %s.ProcessSpatially(): %s", d.DataID.DataName(), err.Error())
		return
	}
	db, err := server.OrderedKeyValueDB()
	if err != nil {
		dvid.Error("Could not determine key value datastore in %s.ProcessSpatially(): %s\n", d.DataID.DataName(), err.Error())
		return
	}

	labels, err := d.Labels.GetData()
	if err != nil {
		dvid.Error("Could not get labels64 data for '%s'", d.Labels)
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	startTime := time.Now()
	wg := new(sync.WaitGroup)
	op := &denormOp{labels, nil, 0, versionID, nil}

	dataID := labels.DataID()
	extents := labels.Extents()
	minIndexZ := extents.MinIndex.(dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(dvid.IndexZYX)[2]
	for z := minIndexZ; z <= maxIndexZ; z++ {
		t := time.Now()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			dvid.Error("Error getting label mapping for block Z %d: %s\n", z, err.Error())
			return
		}

		// Process the labels chunks for this Z
		minIndex := dvid.IndexZYX(minChunkPt)
		maxIndex := dvid.IndexZYX(maxChunkPt)
		if op.mapping != nil {
			startKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, minIndex}
			endKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, maxIndex}
			chunkOp := &storage.ChunkOp{op, wg}
			err = db.ProcessRange(startKey, endKey, chunkOp, d.DenormalizeChunk)
			wg.Wait()
		} else {
			dvid.Log(dvid.Normal, "No mapping for block layer %d found!\n", z)
		}

		dvid.ElapsedTime(dvid.Debug, t, "Processed all '%s' blocks for layer %d/%d",
			d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	startTime = time.Now()
	startKey := d.NewLabelSpatialMapKey(versionID, 0, dvid.MinIndexZYX)
	endKey := d.NewLabelSpatialMapKey(versionID, math.MaxUint64, dvid.MaxIndexZYX)
	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go d.computeSizes(sizeCh, db, versionID, wg)

	// Create a number of label-specific surface calculation jobs
	// TODO: Spawn as many surface calculators as we have handler tokens for.
	// Each surface calculator can use memory ~ XY slice x block Z so memory
	// and # of cores is important.  Might have to pass this in if there's no
	// introspection.
	const numSurfCalculators = 3
	var surfaceCh [numSurfCalculators]chan *storage.Chunk
	for i := 0; i < numSurfCalculators; i++ {
		<-server.HandlerToken
		surfaceCh[i] = make(chan *storage.Chunk, 10000)
		wg.Add(1)
		go d.computeSurface(surfaceCh[i], db, versionID, wg)
	}

	// Wait for results then set Updating.
	go func() {
		wg.Wait()
		dvid.ElapsedTime(dvid.Debug, startTime, "Finished processing all RLEs for labels '%s'", d.DataName())
		d.Ready = true
		if err := server.DatastoreService().SaveDataset(uuid); err != nil {
			dvid.Error("Could not save READY state to data '%s', uuid %s: %s", d.DataName(), uuid, err.Error())
		}
	}()

	// Iterate through all mapped labels and send to size and surface processing goroutines.
	err = db.ProcessRange(startKey, endKey, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		// Get label associated with this sparse volume.
		dataKey := chunk.K.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%numSurfCalculators] <- chunk
	})
	if err != nil {
		dvid.Error("Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	for i := 0; i < numSurfCalculators; i++ {
		surfaceCh[i] <- nil
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Finished reading all RLEs for labels '%s'", d.DataName())
}

// MapChunk processes a chunk of label data, storing the mapped labels.  The data may be
// thinner, wider, and longer than the chunk, depending on the data shape (XY, XZ, etc).
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) MapChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.mapChunk(chunk)
}

func (d *Data) mapChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op, ok := chunk.Op.(*denormOp)
	if !ok {
		log.Fatalf("Illegal operation passed to ProcessChunk() for data %s\n", d.DataName())
	}

	// Initialize the block buffer using the chunk of data.  For voxels, this chunk of
	// data needs to be uncompressed and deserialized.
	var err error
	var blockData []byte
	if chunk == nil || chunk.V == nil {
		blockData = make([]byte, op.source.BlockSize().Prod()*int64(op.source.Values().BytesPerElement()))
	} else {
		blockData, _, err = dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Error("Unable to deserialize block in '%s': %s\n",
				d.DataID.DataName(), err.Error())
			return
		}
	}

	// Transfer the mapped data.
	block := &voxels.Block{K: chunk.K, V: blockData}
	blockSize := op.source.BlockSize()

	blockBeg, dataBeg, dataEnd, err := voxels.ComputeTransform(op.mapped, block, blockSize)
	if err != nil {
		dvid.Error("Error in mapChunk(): %s\n", err.Error())
		return
	}
	data := op.mapped.Data()
	bytesPerVoxel := op.mapped.Values().BytesPerElement()

	// Compute the strides (in bytes)
	bX := blockSize.Value(0) * bytesPerVoxel
	bY := blockSize.Value(1) * bX
	dX := op.mapped.Stride()

	// Do the transfers depending on shape of the external voxels.
	switch {
	case op.mapped.DataShape().Equals(dvid.XY):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(1)*dX + dataBeg.Value(0)*bytesPerVoxel
		span := (dataEnd.Value(0) - dataBeg.Value(0) + 1)
		for y := dataBeg.Value(1); y <= dataEnd.Value(1); y++ {
			b0 := blockI
			b1 := blockI + bytesPerVoxel
			d0 := dataI
			d1 := dataI + bytesPerVoxel
			for x := int32(0); x < span; x++ {
				origLabel := string(block.V[b0:b1])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Error("No mapping found for label %s ... aborting\n", origLabel)
					return
				}
				binary.BigEndian.PutUint64(data[d0:d1], mappedLabel)
				b0 += bytesPerVoxel
				b1 += bytesPerVoxel
				d0 += bytesPerVoxel
				d1 += bytesPerVoxel
			}
			blockI += bX
			dataI += dX
		}

	case op.mapped.DataShape().Equals(dvid.XZ):
		blockI := blockBeg.Value(2)*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
		dataI := dataBeg.Value(2)*op.mapped.Stride() + dataBeg.Value(0)*bytesPerVoxel
		span := (dataEnd.Value(0) - dataBeg.Value(0) + 1)
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			b0 := blockI
			b1 := blockI + bytesPerVoxel
			d0 := dataI
			d1 := dataI + bytesPerVoxel
			for x := int32(0); x < span; x++ {
				origLabel := string(block.V[b0:b1])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Error("No mapping found for label %s ... aborting\n", origLabel)
					return
				}
				binary.BigEndian.PutUint64(data[d0:d1], mappedLabel)
				b0 += bytesPerVoxel
				b1 += bytesPerVoxel
				d0 += bytesPerVoxel
				d1 += bytesPerVoxel
			}
			blockI += bY
			dataI += dX
		}

	case op.mapped.DataShape().Equals(dvid.YZ):
		bz := blockBeg.Value(2)
		for y := dataBeg.Value(2); y <= dataEnd.Value(2); y++ {
			blockI := bz*bY + blockBeg.Value(1)*bX + blockBeg.Value(0)*bytesPerVoxel
			dataI := y*dX + dataBeg.Value(1)*bytesPerVoxel
			for x := dataBeg.Value(1); x <= dataEnd.Value(1); x++ {
				origLabel := string(block.V[blockI : blockI+bytesPerVoxel])
				mappedLabel, found := op.mapping[origLabel]
				if !found {
					dvid.Log(dvid.Normal, "No mapping found for label %s ... aborting\n", origLabel)
					return
				}
				binary.BigEndian.PutUint64(data[dataI:dataI+bytesPerVoxel], mappedLabel)
				blockI += bX
				dataI += bytesPerVoxel
			}
			bz++
		}

	case op.mapped.DataShape().ShapeDimensions() == 2:
		// TODO: General code for handling 2d ExtHandler in n-d space.
		dvid.Error("DVID currently does not support 2d in n-d space.")

	case op.mapped.DataShape().Equals(dvid.Vol3d):
		blockOffset := blockBeg.Value(0) * bytesPerVoxel
		dX = op.mapped.Size().Value(0) * bytesPerVoxel
		dY := op.mapped.Size().Value(1) * dX
		dataOffset := dataBeg.Value(0) * bytesPerVoxel
		bytes := (dataEnd.Value(0) - dataBeg.Value(0) + 1) * bytesPerVoxel
		blockZ := blockBeg.Value(2)

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

	default:
		dvid.Error("Cannot ReadFromBlock() unsupported voxels data shape %s", op.mapped.DataShape())
	}
}

// DenormalizeChunk processes a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) DenormalizeChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.denormalizeChunk(chunk)
}

func (d *Data) denormalizeChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*denormOp)
	db, err := server.OrderedKeyValueDB()
	if err != nil {
		dvid.Log(dvid.Normal, "Error in %s.denormalizeChunk(): %s\n", d.DataName(), err.Error())
		return
	}
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Log(dvid.Normal, "Database doesn't support Batch ops in %s.denormalizeChunk()", d.DataName())
		return
	}
	batch := batcher.NewBatch()

	// Get the spatial index associated with this chunk.
	dataKey := chunk.K.(*datastore.DataKey)
	zyx := dataKey.Index.(*dvid.IndexZYX)
	zyxBytes := zyx.Bytes()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}

	// Construct block-level mapping keys that allow quick range queries pertinent to access patterns.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	offsetSAB := 1 + dvid.IndexZYXSize
	sabIndex := make([]byte, offsetSAB+8+8) // s + a + b
	sabIndex[0] = byte(KeySpatialMap)
	copy(sabIndex[1:offsetSAB], zyxBytes)

	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Log(dvid.Normal, "Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	written := make(map[string]bool, blockBytes/10)
	runStarts := make(map[uint64]([]dvid.Point3d), 10)
	runLengths := make(map[uint64]([]int32), 10)

	firstPt := zyx.MinPoint(op.source.BlockSize()).(dvid.Point3d)
	lastPt := zyx.MaxPoint(op.source.BlockSize()).(dvid.Point3d)
	var curPt dvid.Point3d
	var b, curLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				// Get the label to which the current label is mapped.
				a := blockData[start : start+8]
				start += 8

				if bytes.Compare(a, zeroLabelBytes) == 0 {
					b = 0
				} else {
					b, ok = op.mapping[string(a)]
					if !ok {
						zBeg := zyx.MinPoint(op.source.BlockSize()).Value(2)
						zEnd := zyx.MaxPoint(op.source.BlockSize()).Value(2)
						slice := binary.BigEndian.Uint32(a[0:4])
						dvid.Log(dvid.Normal, "No mapping found for %x (slice %d) in block with Z %d to %d\n",
							a, slice, zBeg, zEnd)
						dvid.Log(dvid.Normal, "Aborting processing of '%s' chunk using '%s' labelmap\n",
							op.source.DataName(), d.DataName())
						return
					}
				}

				// If we hit background or have switched label, save old run and start new one.
				if b == 0 || b != curLabel {
					// Save old run
					if curRun > 0 {
						runLengths[curLabel] = append(runLengths[curLabel], curRun)
					}
					// Start new one if not zero label.
					if b != 0 {
						curPt = dvid.Point3d{x, y, z}
						if runStarts[b] == nil {
							runStarts[b] = []dvid.Point3d{curPt}
							runLengths[b] = []int32{}
						} else {
							runStarts[b] = append(runStarts[b], curPt)
						}
						curRun = 1
					} else {
						curRun = 0
					}
					curLabel = b
				} else {
					curRun++
				}

				// Store a KeySpatialMap key (index = s + a + b)
				if b != 0 {
					copy(sabIndex[offsetSAB:offsetSAB+8], a)
					binary.BigEndian.PutUint64(sabIndex[offsetSAB+8:offsetSAB+16], b)
					_, found := written[string(sabIndex)]
					if !found {
						key := d.DataKey(op.versionID, dvid.IndexBytes(sabIndex))
						batch.Put(key, emptyValue)
						written[string(sabIndex)] = true
					}
				}
			}
			// Force break of any runs when we finish x scan.
			if curRun > 0 {
				runLengths[curLabel] = append(runLengths[curLabel], curRun)
				curLabel = 0
				curRun = 0
			}
		}
	}

	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeySpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
		return
	}
	batch = batcher.NewBatch()

	// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(KeyLabelSpatialMap)
	copy(bsIndex[9:9+dvid.IndexZYXSize], zyxBytes)
	for b, coords := range runStarts {
		binary.BigEndian.PutUint64(bsIndex[1:9], b)
		key := d.DataKey(op.versionID, dvid.IndexBytes(bsIndex))
		runsBytes, err := encodeRuns(coords, runLengths[b])
		if err != nil {
			dvid.Log(dvid.Normal, "Error encoding KeyLabelSpatialMap keys for mapped label %d: %s\n",
				b, err.Error())
			return
		}
		batch.Put(key, runsBytes)
	}
	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeyLabelSpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
	}
}
