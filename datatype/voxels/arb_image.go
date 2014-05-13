package voxels

import (
	"fmt"
	"math"
	"strconv"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// ArbSlice is a 2d rectangle that can be positioned arbitrarily in 3D.
type ArbSlice struct {
	topLeft    dvid.Vector3d
	topRight   dvid.Vector3d
	bottomLeft dvid.Vector3d
	res        float64
	// Calculated from above
	size  dvid.Point2d
	incrX dvid.Vector3d
	incrY dvid.Vector3d

	// The image buffer.  We don't worry about strides and byte order for now because
	// we only GET and don't PUT arbitrary images, where we have to worry about receiving
	// external data.
	bytesPerVoxel int32
	data          []byte
}

// NewArbSliceFromStrings returns an image with arbitrary 3D orientation given string parameters.
// The 3d points are in real world space definited by resolution, e.g., nanometer space.
func (d *Data) NewArbSliceFromStrings(tlStr, trStr, blStr, resStr, sep string) (*ArbSlice, error) {
	topLeft, err := dvid.StringToVector3d(tlStr, sep)
	if err != nil {
		return nil, err
	}
	topRight, err := dvid.StringToVector3d(trStr, sep)
	if err != nil {
		return nil, err
	}
	bottomLeft, err := dvid.StringToVector3d(blStr, sep)
	if err != nil {
		return nil, err
	}
	res, err := strconv.ParseFloat(resStr, 64)
	if err != nil {
		return nil, err
	}
	return d.NewArbSlice(topLeft, topRight, bottomLeft, res)
}

// NewArbSlice returns an image with arbitrary 3D orientation.
// The 3d points are in real world space definited by resolution, e.g., nanometer space.
func (d *Data) NewArbSlice(topLeft, topRight, bottomLeft dvid.Vector3d, res float64) (*ArbSlice, error) {
	// Compute the increments in x,y and number of pixes in each direction.
	dx := topRight.Distance(topLeft)
	dy := bottomLeft.Distance(topLeft)
	nxFloat := math.Floor(dx / res)
	nyFloat := math.Floor(dy / res)
	incrX := topRight.Subtract(topLeft).DivideScalar(nxFloat)
	incrY := bottomLeft.Subtract(topLeft).DivideScalar(nyFloat)
	size := dvid.Point2d{int32(nxFloat) + 1, int32(nyFloat) + 1}
	bytesPerVoxel := d.Properties.Values.BytesPerElement()
	arb := &ArbSlice{topLeft, topRight, bottomLeft, res, size, incrX, incrY, bytesPerVoxel, nil}

	// Allocate the image buffer
	numVoxels := size[0] * size[1]
	fmt.Printf("Allocating new arbitrary image %d x %d\n", size[0], size[1])
	if numVoxels <= 0 {
		return nil, fmt.Errorf("Bad arbitrary image size requested: %s", arb)
	}
	if numVoxels > MaxVoxelsRequest {
		return nil, fmt.Errorf("Requested # voxels (%d) exceeds this DVID server's set limit (%d): %s",
			numVoxels, MaxVoxelsRequest, arb)
	}
	arb.data = make([]byte, bytesPerVoxel*numVoxels)
	return arb, nil
}

func (s ArbSlice) String() string {
	return fmt.Sprintf("Arbitrary %d x %d image: top left %q, top right %q, bottom left %q, res %f",
		s.size[0], s.size[1], s.topLeft, s.topRight, s.bottomLeft, s.res)
}

func (d *Data) GetArbitraryImage(uuid dvid.UUID, tlStr, trStr, blStr, resStr string) (*dvid.Image, error) {
	// Setup the image buffer
	arb, err := d.NewArbSliceFromStrings(tlStr, trStr, blStr, resStr, "_")
	if err != nil {
		return nil, err
	}

	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		return nil, err
	}

	// Iterate across arbitrary image using res increments, retrieving trilinear interpolation
	// at each point.
	cache := NewValueCache(100)
	dataID := d.DataID()
	keyF := func(pt dvid.Point3d) storage.Key {
		chunkPt := pt.Chunk(d.BlockSize())
		index := dvid.IndexZYX(chunkPt.(dvid.ChunkPoint3d)) // TODO: Can we remove this ugliness?
		return &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, index}
	}

	// TODO: Add concurrency.
	leftPt := arb.topLeft
	var x, y, i int32
	for y = 0; y < arb.size[1]; y++ {
		curPt := leftPt
		for x = 0; x < arb.size[0]; x++ {
			value, err := d.computeValue(curPt, KeyFunc(keyF), cache)
			if err != nil {
				return nil, err
			}
			copy(arb.data[i:i+arb.bytesPerVoxel], value)

			curPt.Increment(arb.incrX)
			i += arb.bytesPerVoxel
		}
		leftPt.Increment(arb.incrY)
	}

	// Insert the image data into a dvid.Image struct
	img := new(dvid.Image)
	img.SetFromData(arb.size[0], arb.size[1], arb.data, d.Properties.Values, d.Properties.Interpolable)
	return img, nil
}

type neighbors struct {
	xd, yd, zd float64
	coords     [8]dvid.Point3d
	values     []byte
}

func (d *Data) neighborhood(pt dvid.Vector3d) neighbors {
	res32 := d.Properties.Resolution
	res := dvid.Vector3d{float64(res32.VoxelSize[0]), float64(res32.VoxelSize[1]), float64(res32.VoxelSize[2])}

	// Calculate voxel lattice points
	voxelCoord := dvid.Vector3d{pt[0] / res[0], pt[1] / res[1], pt[2] / res[2]}

	x0 := math.Floor(voxelCoord[0])
	x1 := math.Ceil(voxelCoord[0])
	y0 := math.Floor(voxelCoord[1])
	y1 := math.Ceil(voxelCoord[1])
	z0 := math.Floor(voxelCoord[2])
	z1 := math.Ceil(voxelCoord[2])

	ix0 := int32(x0)
	ix1 := int32(x1)
	iy0 := int32(y0)
	iy1 := int32(y1)
	iz0 := int32(z0)
	iz1 := int32(z1)

	// Calculate real-world lattice points in given resolution
	rx0 := x0 * res[0]
	rx1 := x1 * res[0]
	ry0 := y0 * res[1]
	ry1 := y1 * res[1]
	rz0 := z0 * res[2]
	rz1 := z1 * res[2]

	var n neighbors
	if ix0 != ix1 {
		n.xd = (pt[0] - rx0) / (rx1 - rx0)
	}
	if iy0 != iy1 {
		n.yd = (pt[1] - ry0) / (ry1 - ry0)
	}
	if iz0 != iz1 {
		n.zd = (pt[2] - rz0) / (rz1 - rz0)
	}

	n.coords[0] = dvid.Point3d{ix0, iy0, iz0}
	n.coords[1] = dvid.Point3d{ix1, iy0, iz0}
	n.coords[2] = dvid.Point3d{ix0, iy1, iz0}
	n.coords[3] = dvid.Point3d{ix1, iy1, iz0}
	n.coords[4] = dvid.Point3d{ix0, iy0, iz1}
	n.coords[5] = dvid.Point3d{ix1, iy0, iz1}
	n.coords[6] = dvid.Point3d{ix0, iy1, iz1}
	n.coords[7] = dvid.Point3d{ix1, iy1, iz1}

	// Allocate the values slice buffer based on bytes/voxel.
	bufSize := 8 * d.Properties.Values.BytesPerElement()
	n.values = make([]byte, bufSize, bufSize)

	return n
}

type KeyFunc func(dvid.Point3d) storage.Key

type ValueCache struct {
	deserializedBlocks map[string]([]byte)
	keyQueue           []string
	size               int
}

func NewValueCache(size int) *ValueCache {
	var vc ValueCache
	vc.deserializedBlocks = make(map[string]([]byte), size)
	vc.keyQueue = make([]string, size)
	vc.size = size
	return &vc
}

func (vc ValueCache) Get(key storage.Key) (data []byte, found bool) {
	data, found = vc.deserializedBlocks[key.BytesString()]
	return
}

func (vc *ValueCache) Add(key storage.Key, data []byte) {
	stringKey := key.BytesString()
	if len(vc.keyQueue) >= vc.size {
		delete(vc.deserializedBlocks, vc.keyQueue[0])
		vc.keyQueue = append(vc.keyQueue[1:], stringKey)
	} else {
		vc.keyQueue = append(vc.keyQueue, stringKey)
	}
	vc.deserializedBlocks[stringKey] = data
}

// Calculates value of a 3d real world point in space defined by underlying data resolution.
func (d *Data) computeValue(pt dvid.Vector3d, keyF KeyFunc, cache *ValueCache) ([]byte, error) {
	valuesPerElement := d.Properties.Values.ValuesPerElement()
	bytesPerValue, err := d.Properties.Values.BytesPerValue()
	if err != nil {
		return nil, err
	}
	bytesPerVoxel := valuesPerElement * bytesPerValue

	// Setup datastore access.
	db, err := server.KeyValueGetter()
	if err != nil {
		return nil, err
	}

	// Allocate an empty block.
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		return nil, fmt.Errorf("Data %q does not have a 3d block size", d.DataName())
	}
	nx := blockSize[0]
	nxy := nx * blockSize[1]
	nxyz := nxy * blockSize[2]
	blockBytes := nxyz * bytesPerVoxel
	emptyBlock := make([]byte, blockBytes, blockBytes)

	// For the given point, compute surrounding lattice points and retrieve values.
	neighbors := d.neighborhood(pt)
	var valuesI int32
	for _, voxelCoord := range neighbors.coords {
		key := keyF(voxelCoord)
		deserializedData, found := cache.Get(key)
		if !found {
			serializedData, err := db.Get(key)
			if err != nil {
				return nil, err
			}
			if serializedData == nil || len(serializedData) == 0 {
				deserializedData = emptyBlock
			} else {
				deserializedData, _, err = dvid.DeserializeData(serializedData, true)
				if err != nil {
					return nil, fmt.Errorf("Unable to deserialize block: %s", err.Error())
				}
			}
			cache.Add(key, deserializedData)
		}
		blockPt := voxelCoord.PointInChunk(blockSize).(dvid.Point3d)
		blockI := blockPt[2]*nxy + blockPt[1]*nx + blockPt[0]
		//fmt.Printf("Block %s (%d) len %d -> Neighbor %s (buffer %d, len %d)\n",
		//	blockPt, blockI, len(blockData), voxelCoord, valuesI, len(neighbors.values))
		copy(neighbors.values[valuesI:valuesI+bytesPerVoxel], deserializedData[blockI:blockI+bytesPerVoxel])
		valuesI += bytesPerVoxel
	}

	// Perform trilinear interpolation on the underlying data values.
	var value []byte
	switch valuesPerElement {
	case 1:
		switch bytesPerValue {
		case 1:
			interpValue := trilinearInterpUint8(neighbors.xd, neighbors.yd, neighbors.zd, []uint8(neighbors.values))
			value = []byte{byte(interpValue)}
		case 2:
		case 4:
		case 8:
		default:
		}
	case 4:
		switch bytesPerValue {
		case 1:
		case 2:
		default:
		}
	default:
	}

	return value, nil
}

// Returns the trilinear interpolation of a point 'pt' where 'pt0' is the lattice point below and
// 'pt1' is the lattice point above.  The values c000...c111 are at lattice points surrounding
// the interpolated point.  This can be used for interpolation of anisotropic space.  Formulation
// follows Wikipedia trilinear interpolation page although direction of y axes is flipped, which
// shouldn't matter for formulae.
func trilinearInterpUint8(xd, yd, zd float64, values []uint8) uint8 {
	c000 := float64(values[0])
	c100 := float64(values[1])
	c010 := float64(values[2])
	c110 := float64(values[3])
	c001 := float64(values[4])
	c101 := float64(values[5])
	c011 := float64(values[6])
	c111 := float64(values[7])
	c00 := c000*(1.0-xd) + c100*xd
	c10 := c010*(1.0-xd) + c110*xd
	c01 := c001*(1.0-xd) + c101*xd
	c11 := c011*(1.0-xd) + c111*xd

	c0 := c00*(1.0-yd) + c10*yd
	c1 := c01*(1.0-yd) + c11*yd

	c := math.Floor(c0*(1-zd) + c1*zd + 0.5)
	if c > 255 {
		return 255
	}
	return uint8(c)
}
