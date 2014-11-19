/*
	This file contains code that supports sparse and binary volumes.
*/

package dvid

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
	// EncodingBinary denotes no payload bytes since binary sparse volume is
	// defined by just start and run length.
	EncodingBinary byte = 0x00

	// EncodingGrayscale8 denotes an 8-bit grayscale payload.
	EncodingGrayscale8 = 0x01

	// EncodingGrayscale16 denotes a 16-bit grayscale payload.
	EncodingGrayscale16 = 0x02

	// EncodingNormal16 denotes 16-bit encoded normals.
	EncodingNormal16 = 0x04
)

var (
	zhX, zhY, zhZ [3][3][3]float64
)

// RLE is a single run-length encoded span with a start coordinate and length along
// a coordinate (typically X).
type RLE struct {
	start  Point3d
	length int32
}

func NewRLE(start Point3d, length int32) RLE {
	return RLE{start, length}
}

// RLEs are simply a slice of RLE.
type RLEs []RLE

// MarshalBinary fulfills the encoding.BinaryMarshaler interface.
func (rles RLEs) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if rles != nil {
		for _, rle := range rles {
			if err := binary.Write(buf, binary.LittleEndian, rle.start[0]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.start[1]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.start[2]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, rle.length); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary fulfills the encoding.BinaryUnmarshaler interface.
func (rles *RLEs) UnmarshalBinary(b []byte) error {
	lenEncoding := len(b)
	if lenEncoding%16 != 0 {
		return fmt.Errorf("RLE encoding # bytes is not divisible by 16: %d", len(b))
	}
	buf := bytes.NewBuffer(b)
	numRLEs := lenEncoding / 16
	*rles = make(RLEs, numRLEs, numRLEs)
	for i := 0; i < numRLEs; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[0])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[1])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].start[2])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*rles)[i].length)); err != nil {
			return err
		}
	}
	return nil
}

// Add adds the given RLEs to the receiver when there's a possibility of overlapping RLEs.
// If you are guaranteed the RLEs are disjoint, e.g., the passed and receiver RLEs are in
// different subvolumes, then just concatenate the RLEs instead of calling this function.
// TODO: If this is a bottleneck, employ better than this brute force insertion method.
func (rles *RLEs) Add(rles2 RLEs) {
	for _, rle2 := range rles2 {
		var found bool
		for i, rle := range *rles {
			// If this rle has same z and y, modify the RLE, else just add rle.
			if rle.start[1] == rle2.start[1] && rle.start[2] == rle2.start[2] {
				x0 := rle.start[0]
				x1 := x0 + rle.length - 1
				cur_x0 := rle2.start[0]
				cur_x1 := cur_x0 + rle2.length - 1
				if x1 < cur_x0 {
					continue
				}
				if x0 > cur_x1 {
					continue
				}
				if x0 > cur_x0 {
					x0 = cur_x0
				}
				if x1 < cur_x1 {
					x1 = cur_x1
				}
				rle.start[0] = x0
				rle.length = x1 - x0 + 1
				(*rles)[i] = rle
				found = true
				break
			}
		}
		if !found {
			*rles = append(*rles, rle2)
		}
	}
}

// Stats returns the total number of voxels and runs.
func (rles RLEs) Stats() (numVoxels, numRuns int32) {
	if rles == nil || len(rles) == 0 {
		return 0, 0
	}
	for _, rle := range rles {
		numVoxels += rle.length
	}
	return numVoxels, int32(len(rles))
}

// SparseVol represents a collection of voxels that may be in an arbitrary shape and have a label.
// It is particularly good for storing sparse voxels that may traverse large amounts of space.
type SparseVol struct {
	initialized bool
	numVoxels   uint64
	minPt       Point3d
	maxPt       Point3d
	label       uint64
	rles        RLEs
	pos         int // Current index into rle.
}

func (vol *SparseVol) MinimumPoint3d() Point3d {
	return vol.minPt
}

func (vol *SparseVol) MaximumPoint3d() Point3d {
	return vol.maxPt
}

func (vol *SparseVol) Size() Point3d {
	return Point3d{vol.maxPt[0] - vol.minPt[0] + 1, vol.maxPt[1] - vol.minPt[1] + 1, vol.maxPt[2] - vol.minPt[2] + 1}
}

func (vol *SparseVol) RLEs() RLEs {
	return vol.rles
}

func (vol *SparseVol) NumVoxels() uint64 {
	return vol.numVoxels
}

func (vol *SparseVol) Label() uint64 {
	return vol.label
}

func (vol *SparseVol) SetLabel(label uint64) {
	vol.label = label
}

func (vol *SparseVol) Clear() {
	vol.initialized = false
	vol.pos = 0
	vol.numVoxels = 0
}

// AddSerializedRLEs adds binary encoding of RLEs to SparseVol.
func (vol *SparseVol) AddSerializedRLEs(encoding []byte) error {
	lenEncoding := len(encoding)
	if lenEncoding%16 != 0 {
		return fmt.Errorf("RLE encoding # bytes is not divisible by 16: %d", len(encoding))
	}
	numRLEs := lenEncoding / 16
	if vol.pos+numRLEs >= cap(vol.rles) {
		newsize := vol.pos + numRLEs
		tmp := make(RLEs, newsize, newsize)
		copy(tmp[0:len(vol.rles)], vol.rles)
		vol.rles = tmp
	}

	buf := bytes.NewBuffer(encoding)
	var x, y, z, length int32
	for i := 0; i < numRLEs; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &x); err != nil {
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
		pt := Point3d{x, y, z}
		vol.rles[vol.pos] = RLE{pt, length}
		vol.numVoxels += uint64(length)
		vol.pos++
		if vol.initialized {
			vol.minPt.SetMinimum(pt)
			vol.maxPt.SetMaximum(Point3d{x + length - 1, y, z})
		} else {
			vol.minPt = pt
			vol.maxPt = Point3d{x + length - 1, y, z}
			vol.initialized = true
		}
	}
	return nil
}

// AddRLE adds an RLE to a SparseVol.
func (vol *SparseVol) AddRLE(rles RLEs) {
	if vol.pos+len(rles) >= cap(vol.rles) {
		newsize := vol.pos + len(rles)
		tmp := make(RLEs, newsize, newsize)
		copy(tmp[0:len(vol.rles)], vol.rles)
		vol.rles = tmp
	}
	for _, rle := range rles {
		vol.rles[vol.pos] = rle
		vol.numVoxels += uint64(rle.length)
		vol.pos++
		endPt := rle.start
		endPt[0] += rle.length - 1
		if vol.initialized {
			vol.minPt.SetMinimum(rle.start)
			vol.maxPt.SetMaximum(endPt)
		} else {
			vol.minPt = rle.start
			vol.maxPt = endPt
			vol.initialized = true
		}
	}
}

// SurfaceSerialization returns binary-encoded surface data with the following format:
//    First 4 bytes (little-endian) # voxels (N)
//    Array of N vertices, each with 3 little-endian float32 (x,y,z)
//    Array of N normals, each with 3 little-endian float32 (nx,ny,nz)
//
// The blockNz parameter is necessary since underlying RLEs in the SparseVol are ordered
// by blocks in Z but not within a block, so RLEs can have different Z within a block.
func (vol *SparseVol) SurfaceSerialization(blockNz int32, res NdFloat32) ([]byte, error) {
	// TODO -- can be more efficient in buffer space by only needing 8 blocks worth
	// of data (4 for current XY processing and 4 for next Z), but for simplicity this
	// function uses total XY extents + some # of slices (blockNz).
	dx := vol.maxPt[0] - vol.minPt[0] + 3
	dy := vol.maxPt[1] - vol.minPt[1] + 3
	dz := vol.maxPt[2] - vol.minPt[2] + 3

	// Note that SparseVol RLEs can jump in Z within a block of data, so buffer must be at least
	// as big as a block in Z.
	if dz > blockNz*2+1 {
		dz = blockNz*2 + 1
	}

	// Allocate buffer for processing
	offset := vol.minPt.AddScalar(-1).(Point3d)
	binvol := NewBinaryVolume(offset, Point3d{dx, dy, dz}, res)

	var vertexBuf, normalBuf bytes.Buffer
	var surfaceSize uint32
	rleI := 0
	Debugf("Label %d, # voxels %d, buffer size %s, minPt %s, maxPt %s",
		vol.label, vol.numVoxels, binvol.size, vol.minPt, vol.maxPt)

	for {
		var minX int32 = 1      //dx
		var maxX int32 = dx - 1 //0
		var minY int32 = 1      //dy
		var maxY int32 = dy - 1 //0
		// Populate the buffer.
		for {
			if rleI >= vol.pos {
				// We've added entire volume.
				break
			}
			r := vol.rles[rleI]
			bz := int64(r.start[2] - binvol.offset[2])
			if bz >= int64(dz) {
				// rles have filled this buffer.
				break
			}
			by := int64(r.start[1] - binvol.offset[1])
			bx := int64(r.start[0] - binvol.offset[0])
			p := bz*int64(dx*dy) + by*int64(dx) + bx
			for i := int64(0); i < int64(r.length); i++ {
				binvol.data[p+i] = 255
			}

			// For this buffer, set bounds.  For large sparse volumes that snake
			// through a lot of space, the XY footprint might be relatively small.
			// This is currently not working but is a TODO.
			/*
				if minX > bx {
					minX = bx
				}
				if maxX < bx+r.length-1 {
					maxX = bx + r.length - 1
				}
				if minY > by {
					minY = by
				}
				if maxY < by {
					maxY = by
				}
			*/
			rleI++
		}

		// Iterate through XY layers to compute surface and normal
		for z := int32(1); z <= blockNz; z++ {
			if binvol.offset[2]+z > vol.maxPt[2] {
				// We've passed through all of this sparse volume's voxels
				break
			}
			// TODO -- Keep track of bounding box per Z and limit checks to it.
			for y := minY; y <= maxY; y++ {
				for x := minX; x <= maxX; x++ {
					nx, ny, nz, isSurface := binvol.CheckSurface(x, y, z)
					if isSurface {
						surfaceSize++
						fx := float32(x + binvol.offset[0])
						fy := float32(y + binvol.offset[1])
						fz := float32(z + binvol.offset[2])
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fx); err != nil {
							return nil, err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fy); err != nil {
							return nil, err
						}
						if err := binary.Write(&vertexBuf, binary.LittleEndian, fz); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nx); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, ny); err != nil {
							return nil, err
						}
						if err := binary.Write(&normalBuf, binary.LittleEndian, nz); err != nil {
							return nil, err
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
	return data, nil
}

// BinaryVolume holds 3d binary data including a 3d offset.
type BinaryVolume struct {
	offset      Point3d
	size        Point3d
	xanisotropy float64
	yanisotropy float64
	zanisotropy float64
	data        []byte
}

// NewBinaryVolume returns a BinaryVolume with an allocated 3d volume for data.
func NewBinaryVolume(offset, size Point3d, res NdFloat32) *BinaryVolume {
	minRes := res.GetMin()
	numBytes := size[0] * size[1] * size[2]
	return &BinaryVolume{
		offset:      offset,
		size:        size,
		xanisotropy: float64(res[0] / minRes),
		yanisotropy: float64(res[1] / minRes),
		zanisotropy: float64(res[2] / minRes),
		data:        make([]byte, numBytes, numBytes),
	}
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

// CheckSurface checks to see if the given voxel within the BinaryVolume is set, and if so,
// calculates a normal based on a Zucker-Hummel 3x3x3 convolution.
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
	//fmt.Printf("CheckSurface (%d, %d, %d) with binvol size %d x %d x %d\n", x, y, z, binvol.size[0], binvol.size[1], binvol.size[2])
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
