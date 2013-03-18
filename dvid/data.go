/*
	This file contains data types and functions that support a number of layers in DVID.
*/

package dvid

import (
	"fmt"
	"math"
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

func (c VoxelCoord) Distance(x VoxelCoord) int32 {
	dx := c[0] - x[0]
	dy := c[1] - x[1]
	dz := c[2] - x[2]
	return int32(math.Sqrt(float64(dx*dx + dy*dy + dz*dz)))
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

// BlockCoord returns the coordinate of the block containing the voxel coordinate.
func (c VoxelCoord) BlockCoord(blockSize VoxelCoord) (bc BlockCoord) {
	bc[0] = c[0] / blockSize[0]
	bc[1] = c[1] / blockSize[1]
	bc[2] = c[2] / blockSize[2]
	return
}

// BlockVoxel returns the voxel coordinate within a containing block for the given voxel 
// coordinate.
func (c VoxelCoord) BlockVoxel(blockSize VoxelCoord) (bc VoxelCoord) {
	bc[0] = c[0] % blockSize[0]
	bc[1] = c[1] % blockSize[1]
	bc[2] = c[2] % blockSize[2]
	return
}

// ToBlockIndex returns an index into a block's data that corresponds to
// a given voxel coordinate.  Note that the index is NOT multiplied by BytesPerVoxel
// but gives the element number, since we also want to set dirty flags.
func (c VoxelCoord) ToBlockIndex(blockSize VoxelCoord) (index int) {
	bv := c.BlockVoxel(blockSize)
	index = int(bv[2]*blockSize[0]*blockSize[1] + bv[1]*blockSize[0] + bv[0])
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

// DataShape describes the way data is positioned in 3d space
type DataShape byte

const (
	// XY describes a rectangle of voxels that share a z-coord.
	XY DataShape = iota

	// XZ describes a rectangle of voxels that share a y-coord.
	XZ

	// YZ describes a rectangle of voxels that share a x-coord.
	YZ

	// Arb describes a rectangle of voxels with arbitrary 3d orientation.
	Arb

	// Vol describes a Subvolume of voxels
	Vol
)

func (shape DataShape) String() string {
	switch shape {
	case XY:
		return "XY slice"
	case XZ:
		return "XZ slice"
	case YZ:
		return "YZ slice"
	case Arb:
		return "slice with arbitrary orientation"
	case Vol:
		return "subvolume"
	}
	return "Unknown shape"
}

// String for specifying a slice orientation or subvolume
type DataShapeString string

var dataShapeStrings = map[DataShapeString]DataShape{
	"xy":  XY,
	"xz":  XZ,
	"yz":  YZ,
	"arb": Arb,
	"vol": Vol,
}

// ListDataShapes returns a slice of shape names
func ListDataShapes() (shapes []string) {
	shapes = []string{}
	for key, _ := range dataShapeStrings {
		shapes = append(shapes, string(key))
	}
	return
}

// DataShape returns the data shape constant associated with the string.
func (s DataShapeString) DataShape() (shape DataShape, err error) {
	shape, found := dataShapeStrings[s]
	if !found {
		err = fmt.Errorf("Unknown data shape specification (%s)", s)
	}
	return
}

// Point2d is a 2d point, each coordinate a 32-bit int.
type Point2d [2]int32

// Vector3d is a floating point 3d vector.
type Vector3d [3]float32

// DataShaper is the interface that wraps a DataShape method
type DataShaper interface {
	DataShape() DataShape
	String() string
}

type Volume interface {
	Origin() VoxelCoord
	TopRight() VoxelCoord
	BottomLeft() VoxelCoord
	EndVoxel() VoxelCoord
}

// Slice holds a 2d slice of voxels that sits in 3d space.
// It can be oriented in XY, XZ, YZ, and Arbitrary.  Although slightly
// counter-intuitive, a slice is a volume of thickness 1 and additional
// information on its orientation.
//
// Note that the 3d coordinate system is a Right-Hand system
// and works with OpenGL.  The origin is considered the top left
// corner of an XY slice.  X increases as you move from left to right.
// Y increases as you move down from the origin.  Z increases as you
// add more slices deeper than the current slice.
type Slice interface {
	// A Slice has data with a particular orientation relative to volume. 
	DataShaper

	// A Slice is a Volume of single voxel thickness in one dimension. 
	Volume

	// Origin returns the coordinate of top left corner. 
	Origin() VoxelCoord

	// Size returns the width and height as a Point2d. 
	Size() Point2d

	// NumVoxels returns the number of voxels within this slice. 
	NumVoxels() int
}

// SliceData specifies a rectangle in 3d space that is orthogonal to an axis.
type SliceData struct {
	origin VoxelCoord
	size   Point2d
}

func (s *SliceData) Origin() VoxelCoord {
	return s.origin
}

func (s *SliceData) Size() Point2d {
	return s.size
}

func (s *SliceData) NumVoxels() int {
	if s == nil {
		return 0
	}
	return int(s.size[0] * s.size[1])
}

// SliceXY is a slice in the XY plane.
type SliceXY struct {
	SliceData
}

func (s *SliceXY) DataShape() DataShape {
	return XY
}

func (s *SliceXY) String() string {
	return fmt.Sprintf("XY Slice with offset %s and size %s", s.origin, s.size)
}

func (s *SliceXY) TopRight() VoxelCoord {
	return VoxelCoord{s.origin[0] + s.size[0], s.origin[1], s.origin[2]}
}

func (s *SliceXY) BottomLeft() VoxelCoord {
	return VoxelCoord{s.origin[0], s.origin[1] + s.size[1], s.origin[2]}
}

func (s *SliceXY) Normal() Vector3d {
	return Vector3d{0.0, 0.0, 1.0}
}

func (s *SliceXY) EndVoxel() VoxelCoord {
	return VoxelCoord{s.origin[0] + s.size[0], s.origin[1] + s.size[1], s.origin[2]}
}

func NewSliceXY(origin VoxelCoord, size Point2d) Slice {
	return &SliceXY{SliceData{origin, size}}
}

// SliceXZ is a slice in the XZ plane.
type SliceXZ struct {
	SliceData
}

func (s *SliceXZ) DataShape() DataShape {
	return XZ
}

func (s *SliceXZ) String() string {
	return fmt.Sprintf("XZ Slice with offset %s and size %s", s.origin, s.size)
}

func (s *SliceXZ) TopRight() VoxelCoord {
	return VoxelCoord{s.origin[0] + s.size[0], s.origin[1], s.origin[2]}
}

func (s *SliceXZ) BottomLeft() VoxelCoord {
	return VoxelCoord{s.origin[0], s.origin[1], s.origin[2] + s.size[1]}
}

func (s *SliceXZ) Normal() Vector3d {
	return Vector3d{0.0, 1.0, 0.0}
}

func (s *SliceXZ) EndVoxel() VoxelCoord {
	return VoxelCoord{s.origin[0] + s.size[0], s.origin[1], s.origin[2] + s.size[1]}
}

func NewSliceXZ(origin VoxelCoord, size Point2d) Slice {
	return &SliceXZ{SliceData{origin, size}}
}

// SliceYZ is a slice in the YZ plane.
type SliceYZ struct {
	SliceData
}

func (s *SliceYZ) DataShape() DataShape {
	return YZ
}

func (s *SliceYZ) String() string {
	return fmt.Sprintf("YZ Slice with offset %s and size %s", s.origin, s.size)
}

func (s *SliceYZ) TopRight() VoxelCoord {
	return VoxelCoord{s.origin[0], s.origin[1] + s.size[0], s.origin[2]}
}

func (s *SliceYZ) BottomLeft() VoxelCoord {
	return VoxelCoord{s.origin[0], s.origin[1], s.origin[2] + s.size[1]}
}

func (s *SliceYZ) Normal() Vector3d {
	return Vector3d{1.0, 0.0, 0.0}
}

func (s *SliceYZ) EndVoxel() VoxelCoord {
	return VoxelCoord{s.origin[0], s.origin[1] + s.size[0], s.origin[2] + s.size[1]}
}

func NewSliceYZ(origin VoxelCoord, size Point2d) Slice {
	return &SliceYZ{SliceData{origin, size}}
}

// SliceArb is an arbitray slice in 3d space.
type SliceArb struct {
	origin     VoxelCoord
	topRight   VoxelCoord
	bottomLeft VoxelCoord
}

func (s *SliceArb) DataShape() DataShape {
	return Arb
}

func (s *SliceArb) String() string {
	return fmt.Sprintf("Arbitrary Slice with offset %s, top left %s, bottom left %s",
		s.origin, s.topRight, s.bottomLeft)
}

func (s *SliceArb) TopRight() VoxelCoord {
	return s.topRight
}

func (s *SliceArb) BottomLeft() VoxelCoord {
	return s.bottomLeft
}

func (s *SliceArb) Size() Point2d {
	dx := s.topRight.Distance(s.origin)
	dy := s.bottomLeft.Distance(s.origin)
	return Point2d{dx, dy}
}

func (s *SliceArb) NumVoxels() int {
	if s == nil {
		return 0
	}
	size := s.Size()
	return int(size[0] * size[1])
}

func (s *SliceArb) Origin() VoxelCoord {
	return s.origin
}

func (s *SliceArb) EndVoxel() VoxelCoord {
	return VoxelCoord{s.topRight[0], s.bottomLeft[1], s.bottomLeft[2]}
}

func NewSliceArb(origin, topRight, bottomLeft VoxelCoord) Slice {
	return &SliceArb{origin, topRight, bottomLeft}
}
