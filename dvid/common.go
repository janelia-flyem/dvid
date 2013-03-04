/*
	Package dvid provides types, constants and functions that have no other dependencies 
	and can be used by all packages within DVID.
*/
package dvid

import (
	"fmt"
	"strconv"
)

// Notes:
//   Whenever the units of a type are different, e.g., voxel coordinate versus
//   a block coordinate, we should make a separate type to reinforce the distinct
//   natures of the values.  While this may cause more verbosity in code, it will
//   prevent accidental misuse and also allow segregation of functions.
//
//   While these are defined as 3d and not n-dimensional, in future versions of
//   DVID we may generalize these data structures.

// VoxelCoord is the (X,Y,Z) of a Voxel.
type VoxelCoord [3]int32

func (c VoxelCoord) Add(x VoxelCoord) (result VoxelCoord) {
	result[0] = c[0] + x[0]
	result[1] = c[1] + x[1]
	result[2] = c[2] + x[2]
	return
}

// AddSize returns a voxel coordinate that is moved by the x vector
// minus one.  If x is the size of a box, this has the effect of
// returning the maximum voxel coordinate still within the box.
func (c VoxelCoord) AddSize(x VoxelCoord) (result VoxelCoord) {
	result[0] = c[0] + x[0] - 1
	result[1] = c[1] + x[1] - 1
	result[2] = c[2] + x[2] - 1
	return
}

func (c VoxelCoord) Sub(x VoxelCoord) (result VoxelCoord) {
	result[0] = c[0] - x[0]
	result[1] = c[1] - x[1]
	result[2] = c[2] - x[2]
	return
}

func (c VoxelCoord) Mod(x VoxelCoord) (result VoxelCoord) {
	result[0] = c[0] % x[0]
	result[1] = c[1] % x[1]
	result[2] = c[2] % x[2]
	return
}

func (c VoxelCoord) Div(x VoxelCoord) (result VoxelCoord) {
	result[0] = c[0] / x[0]
	result[1] = c[1] / x[1]
	result[2] = c[2] / x[2]
	return
}

func (c VoxelCoord) Mag() int {
	return int(c[0] * c[1] * c[2])
}

func (c VoxelCoord) String() string {
	return fmt.Sprintf("(%d,%d,%d)", c[0], c[1], c[2])
}

// BoundMin returns a voxel coordinate where each of its elements
// are not smaller than the corresponding element in x.
func (c VoxelCoord) BoundMin(x VoxelCoord) (result VoxelCoord) {
	result = c
	if c[0] < x[0] {
		result[0] = x[0]
	}
	if c[1] < x[1] {
		result[1] = x[1]
	}
	if c[2] < x[2] {
		result[2] = x[2]
	}
	return
}

// BoundMax returns a voxel coordinate where each of its elements
// are not greater than the corresponding element in x.
func (c VoxelCoord) BoundMax(x VoxelCoord) (result VoxelCoord) {
	result = c
	if c[0] > x[0] {
		result[0] = x[0]
	}
	if c[1] > x[1] {
		result[1] = x[1]
	}
	if c[2] > x[2] {
		result[2] = x[2]
	}
	return
}

// Prompt asks the user to enter components of a voxel coordinate
// with empty entries returning the numerical equivalent of defaultValue. 
func (c *VoxelCoord) Prompt(message, defaultValue string) {
	axes := [3]string{"X", "Y", "Z"}
	var coord int64
	var err error
	for i, axis := range axes {
		for {
			input := Prompt(message+" along "+axis, defaultValue)
			coord, err = strconv.ParseInt(input, 0, 32)
			if err != nil {
				fmt.Printf("\n--> Error.  Can't convert '%s' into a 32-bit int!\n", input)
			} else {
				break
			}
		}

		c[i] = int32(coord)
	}
}

// BlockCoord is the (X,Y,Z) of a Block.
type BlockCoord [3]int32

func (c BlockCoord) Add(x BlockCoord) (result BlockCoord) {
	result[0] = c[0] + x[0]
	result[1] = c[1] + x[1]
	result[2] = c[2] + x[2]
	return
}

func (c BlockCoord) Sub(x BlockCoord) (result BlockCoord) {
	result[0] = c[0] - x[0]
	result[1] = c[1] - x[1]
	result[2] = c[2] - x[2]
	return
}

func (c BlockCoord) String() string {
	return fmt.Sprintf("(%d,%d,%d)", c[0], c[1], c[2])
}

// BoundMin returns a block coordinate where each of its elements
// are not smaller than the corresponding element in x.
func (c BlockCoord) BoundMin(x BlockCoord) (result BlockCoord) {
	result = c
	if c[0] < x[0] {
		result[0] = x[0]
	}
	if c[1] < x[1] {
		result[1] = x[1]
	}
	if c[2] < x[2] {
		result[2] = x[2]
	}
	return
}

// BoundMax returns a block coordinate where each of its elements
// are not greater than the corresponding element in x.
func (c BlockCoord) BoundMax(x BlockCoord) (result BlockCoord) {
	result = c
	if c[0] > x[0] {
		result[0] = x[0]
	}
	if c[1] > x[1] {
		result[1] = x[1]
	}
	if c[2] > x[2] {
		result[2] = x[2]
	}
	return
}

// VoxelResolution holds the relative resolutions along each dimension.  Since 
// voxel resolutions should be fixed for the lifetime of a datastore, we assume
// there is one base unit of resolution (e.g., nanometers) and all resolutions
// are based on that.
type VoxelResolution [3]float32

// The description of the units of voxel resolution, e.g., "nanometer".
type VoxelResolutionUnits string

// Prompt asks the user to enter components of a voxel's resolution
// with empty entries returning the numerical equivalent of defaultValue. 
func (res *VoxelResolution) Prompt(message, defaultValue string) {
	axes := [3]string{"X", "Y", "Z"}
	var f float64
	var err error
	for i, axis := range axes {
		for {
			input := Prompt(message+" along "+axis, defaultValue)
			f, err = strconv.ParseFloat(input, 32)
			if err != nil {
				fmt.Printf("\n--> Error!  Can't convert '%s' into a 32-bit float!\n", input)
			} else {
				break
			}
		}
		res[i] = float32(f)
	}
}

// SliceType describes the orientation of a rectangle of voxels in 3d space.
type SliceType byte

const (
	// XY describes a rectangle of voxels that share a z-coord.
	XY SliceType = iota

	// XZ describes a rectangle of voxels that share a y-coord.
	XZ

	// YZ describes a rectangle of voxels that share a x-coord.
	YZ

	// Arbitrary describes a rectangle of voxels angled in the 3d volume.
	Arbitrary
)

// Point2d is a 2d point, each coordinate a 32-bit int.
type Point2d [2]int32

// Vector3d is a floating point 3d vector.
type Vector3d [3]float32

// Slice holds a 2d slice of voxels that sits in 3d space.
// It can be oriented in XY, XZ, YZ, and Arbitrary.
type Slice interface {
	SliceType() SliceType
	Origin() *VoxelCoord
	Size() *Point2d
	Normal() *Vector3d
}

// SliceData specifies a rectangle in 3d space that is orthogonal to an axis.
type SliceData struct {
	origin *VoxelCoord
	size   *Point2d
}

func (s *SliceData) Origin() *VoxelCoord {
	return s.origin
}

func (s *SliceData) Size() *Point2d {
	return s.size
}

// SliceXY is a slice in the XY plane.
type SliceXY struct {
	SliceData
}

func (s *SliceXY) SliceType() SliceType {
	return XY
}

func (s *SliceXY) Normal() *Vector3d {
	return &Vector3d{0.0, 0.0, 1.0}
}

func NewSliceXY(origin *VoxelCoord, size *Point2d) Slice {
	return &SliceXY{SliceData{origin, size}}
}

// SliceXZ is a slice in the XZ plane.
type SliceXZ struct {
	SliceData
}

func (s *SliceXZ) SliceType() SliceType {
	return XZ
}

func (s *SliceXZ) Normal() *Vector3d {
	return &Vector3d{0.0, 1.0, 0.0}
}

func NewSliceXZ(origin *VoxelCoord, size *Point2d) Slice {
	return &SliceXZ{SliceData{origin, size}}
}

// SliceYZ is a slice in the YZ plane.
type SliceYZ struct {
	SliceData
}

func (s *SliceYZ) SliceType() SliceType {
	return YZ
}

func (s *SliceYZ) Normal() *Vector3d {
	return &Vector3d{1.0, 0.0, 0.0}
}

func NewSliceYZ(origin *VoxelCoord, size *Point2d) Slice {
	return &SliceYZ{SliceData{origin, size}}
}

// SliceArb is an arbitray slice in 3d space with orientation determined
// by a normal vector.
type SliceArb struct {
	SliceData
	normal *Vector3d
}

func (s *SliceArb) SliceType() SliceType {
	return Arbitrary
}

func (s *SliceArb) Normal() *Vector3d {
	return s.normal
}

func NewSliceArb(origin *VoxelCoord, size *Point2d, normal *Vector3d) Slice {
	return &SliceArb{SliceData{origin, size}, normal}
}

type SliceVoxels struct {
	Slice

	// Number of bytes per voxel.  Frequently, we don't need to know the underlying
	// data format but we do need to know what constitutes a voxel when iterating
	// through subvolume data slices.  If BytesPerVoxel is the empty value (0),
	// processing can assume that
	BytesPerVoxel int

	// The data itself.  Go image data is usually held in []uint8.
	Data []uint8
}

// Subvolume packages the location, extent, and data of a data type corresponding
// to a rectangular box of voxels.  The "Sub" prefix emphasizes that the data is 
// usually a smaller portion of the volume held by the DVID datastore.  Although
// this type usually holds voxel values, it's possible to transmit other types
// of data that is associated with this region of the volume, e.g., a region
// adjacency graph or a serialized label->label map.
type Subvolume struct {
	// Description of data
	Text string

	// 3d offset
	Offset VoxelCoord

	// 3d size of data
	Size VoxelCoord

	// Number of bytes per voxel.  Frequently, we don't need to know the underlying
	// data format but we do need to know what constitutes a voxel when iterating
	// through subvolume data slices.  If BytesPerVoxel is the empty value (0),
	// processing can assume that
	BytesPerVoxel int

	// The data itself.  Go image data is usually held in []uint8.
	Data []uint8
}

func (p *Subvolume) NonZeroBytes(message string) {
	nonZeros := 0
	for _, b := range p.Data {
		if b != 0 {
			nonZeros++
		}
	}
	fmt.Printf("%s> Number of non-zeros: %d\n", message, nonZeros)
}

func (p *Subvolume) String() string {
	return fmt.Sprintf("%s (%d x %d x %d) at offset (%d, %d, %d)",
		p.Text, p.Size[0], p.Size[1], p.Size[2], p.Offset[0], p.Offset[1], p.Offset[2])
}

// VoxelCoordToDataIndex returns an index that can be used to access the first byte
// corresponding to the given voxel coordinate in the subvolume's Data slice.  The
// data element will constitute p.BytesPerVoxel bytes.
func (p *Subvolume) VoxelCoordToDataIndex(c VoxelCoord) (index int) {
	pt := c.Sub(p.Offset)
	index = int(pt[2]*p.Size[0]*p.Size[1] + pt[1]*p.Size[0] + pt[0])
	index *= p.BytesPerVoxel
	return
}
