package voxels

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	_ "log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
)

// Notes:
//   Whenever the units of a type are different, e.g., voxel coordinate versus
//   a block coordinate, we should make a separate type to reinforce the distinct
//   natures of the values.  While this may cause more verbosity in code, it will
//   prevent accidental misuse and also allow segregation of functions.
//
//   While these are defined as 3d and not n-dimensional, in future versions of
//   DVID we may generalize these data structures.

// Coord is the (X,Y,Z) of a Voxel.
type Coord [3]int32

func (c Coord) Add(x Coord) (result Coord) {
	result[0] = c[0] + x[0]
	result[1] = c[1] + x[1]
	result[2] = c[2] + x[2]
	return
}

// AddSize returns a voxel coordinate that is moved by the s vector
// minus one.  If s is the size of a box, this has the effect of
// returning the maximum voxel coordinate still within the box.
func (c Coord) AddSize(s Point3d) (result Coord) {
	result[0] = c[0] + s[0] - 1
	result[1] = c[1] + s[1] - 1
	result[2] = c[2] + s[2] - 1
	return
}

func (c Coord) Sub(x Coord) (result Coord) {
	result[0] = c[0] - x[0]
	result[1] = c[1] - x[1]
	result[2] = c[2] - x[2]
	return
}

func (c Coord) Mod(x Coord) (result Coord) {
	result[0] = c[0] % x[0]
	result[1] = c[1] % x[1]
	result[2] = c[2] % x[2]
	return
}

func (c Coord) Div(x Coord) (result Coord) {
	result[0] = c[0] / x[0]
	result[1] = c[1] / x[1]
	result[2] = c[2] / x[2]
	return
}

// Distance returns the integer distance (rounding down) between two voxel coordinates.
func (c Coord) Distance(x Coord) int32 {
	dx := c[0] - x[0]
	dy := c[1] - x[1]
	dz := c[2] - x[2]
	return int32(math.Sqrt(float64(dx*dx + dy*dy + dz*dz)))
}

func (c Coord) String() string {
	return fmt.Sprintf("(%d,%d,%d)", c[0], c[1], c[2])
}

// BoundMin returns a voxel coordinate where each of its elements
// are not smaller than the corresponding element in x.
func (c Coord) Max(x Coord) (result Coord) {
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
func (c Coord) Min(x Coord) (result Coord) {
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
func (c Coord) BlockCoord(blockSize Point3d) (bc BlockCoord) {
	bc[0] = c[0] / blockSize[0]
	bc[1] = c[1] / blockSize[1]
	bc[2] = c[2] / blockSize[2]
	return
}

// BlockVoxel returns the voxel coordinate within a containing block for the given voxel
// coordinate.
func (c Coord) BlockVoxel(blockSize Point3d) (bc Coord) {
	bc[0] = c[0] % blockSize[0]
	bc[1] = c[1] % blockSize[1]
	bc[2] = c[2] % blockSize[2]
	return
}

// ToBlockIndex returns an index into a block's data that corresponds to
// a given voxel coordinate.  Note that the index is NOT multiplied by BytesPerVoxel
// but gives the element number, since we also want to set dirty flags.
func (c Coord) ToBlockIndex(blockSize Point3d) (index int) {
	bv := c.BlockVoxel(blockSize)
	index = int(bv[2]*blockSize[0]*blockSize[1] + bv[1]*blockSize[0] + bv[0])
	return
}

// Prompt asks the user to enter components of a voxel coordinate
// with empty entries returning the numerical equivalent of defaultValue.
func (c *Coord) Prompt(message, defaultValue string) {
	axes := [3]string{"X", "Y", "Z"}
	var coord int64
	var err error
	for i, axis := range axes {
		for {
			input := dvid.Prompt(message+" along "+axis, defaultValue)
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
func (c BlockCoord) Max(x BlockCoord) (result BlockCoord) {
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
func (c BlockCoord) Min(x BlockCoord) (result BlockCoord) {
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

// PointStr is a n-dimensional coordinate in string format "x,y,z,..."
// where each coordinate is a 32-bit integer.
type PointStr string

func (s PointStr) Coord() (coord Coord, err error) {
	_, err = fmt.Sscanf(string(s), "%d,%d,%d", &coord[0], &coord[1], &coord[2])
	return
}

func (s PointStr) Point3d() (coord Point3d, err error) {
	_, err = fmt.Sscanf(string(s), "%d,%d,%d", &coord[0], &coord[1], &coord[2])
	return
}

func (s PointStr) Point2d() (point Point2d, err error) {
	_, err = fmt.Sscanf(string(s), "%d,%d", &point[0], &point[1])
	return
}

// VectorStr is a n-dimensional coordinate in string format "x,y,z,....""
// where each coordinate is a 32-bit float.
type VectorStr string

func (s VectorStr) Vector3d() (v Vector3d, err error) {
	_, err = fmt.Sscanf(string(s), "%f,%f,%f", &v[0], &v[1], &v[2])
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
			input := dvid.Prompt(message+" along "+axis, defaultValue)
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

// Volume defines the extent and resolution of a volume.
type Volume struct {
	// Volume extents
	VolumeMax Coord

	// Relative resolution of voxels in volume
	VoxelRes VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits VoxelResolutionUnits
}

// WriteJSON writes a Volume to a JSON file.
func (vol *Volume) WriteJSON(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("Failed to create JSON Volume file: %s [%s]", filename, err)
	}
	defer file.Close()
	m, err := json.Marshal(vol)
	if err != nil {
		return fmt.Errorf("Error in writing JSON config file: %s [%s]", filename, err)
	}
	var buf bytes.Buffer
	json.Indent(&buf, m, "", "    ")
	buf.WriteTo(file)
	return nil
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

var dataShapeStrings = map[string]DataShape{
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
	shape, found := dataShapeStrings[strings.ToLower(string(s))]
	if !found {
		err = fmt.Errorf("Unknown data shape specification (%s)", s)
	}
	return
}

// Point2d is a 2d point.
type Point2d [2]int32

func (pt Point2d) String() string {
	return fmt.Sprintf("(%d, %d)", pt[0], pt[1])
}

func SizeFromRect(rect image.Rectangle) (size Point2d) {
	size[0] = int32(rect.Dx())
	size[1] = int32(rect.Dy())
	return
}

// Point3d is a 3d point.
type Point3d [3]int32

func (pt Point3d) String() string {
	return fmt.Sprintf("(%d, %d, %d)", pt[0], pt[1], pt[2])
}

// Vector3d is a floating point 3d vector.
type Vector3d [3]float32

// Geometry describes the shape, size, and position of data in the DVID volume.
type Geometry interface {
	// DataShape describes the shape of the data.
	DataShape() DataShape

	// Origin returns the offset to the voxel at the top left corner of the data.
	Origin() Coord

	// Size returns the size as a Point3d that shows the extent in each dimension.
	Size() Point3d

	// Width is defined as size along different axis per shape:
	//   x for Arb, Vol, XY, and XZ shapes
	//   y for the YZ shape.
	Width() int32

	// Height is defined as size along different axis per shape:
	//   y for Arb, Vol, XY shapes
	//   z for XZ, YZ
	Height() int32

	// Depth is defined as 1 for XY, XZ, YZ shapes.  Depth returns
	// the size along z for Arb and Vol shapes.
	Depth() int32

	// NumVoxels returns the number of voxels within this space.
	NumVoxels() int64

	// EndVoxel returns the last voxel coordinate usually traversed so that
	// iteration from Origin->EndVoxel will visit all the voxels.
	EndVoxel() Coord

	String() string
}

// Subvolume describes the Geometry for a rectangular box of voxels.  The "Sub" prefix
// emphasizes that the data is usually a smaller portion of the volume held by the
// DVID datastore.  Note that the 3d coordinate system is a Right-Hand system
// and works with OpenGL.  The origin is considered the top left corner of an XY slice.
// X increases as you move from left to right.  Y increases as you move down from the origin.
// Z increases as you add more slices deeper than the current slice.
type Subvolume struct {
	// 3d offset
	origin Coord

	// 3d size of data
	size Point3d
}

func (s *Subvolume) DataShape() DataShape {
	return Vol
}

func (s *Subvolume) Origin() Coord {
	return s.origin
}

func (s *Subvolume) Size() Point3d {
	return s.size
}

func (s *Subvolume) Width() int32 {
	return s.size[0]
}

func (s *Subvolume) Height() int32 {
	return s.size[1]
}

func (s *Subvolume) Depth() int32 {
	return s.size[2]
}

func (s *Subvolume) NumVoxels() int64 {
	if s == nil {
		return 0
	}
	return int64(s.size[0]) * int64(s.size[1]) * int64(s.size[2])
}

func (s *Subvolume) EndVoxel() Coord {
	return s.origin.AddSize(s.size)
}

func (s *Subvolume) String() string {
	return fmt.Sprintf("Subvolume (%d x %d x %d) at offset (%d, %d, %d)",
		s.size[0], s.size[1], s.size[2], s.origin[0], s.origin[1], s.origin[2])
}

func (s *Subvolume) TopRight() Coord {
	tr := s.origin
	tr[0] += s.size[0] - 1
	return tr
}

func (s *Subvolume) BottomLeft() Coord {
	tr := s.origin
	tr[1] += s.size[1] - 1
	tr[2] += s.size[2] - 1
	return tr
}

// NewSubvolume returns a Subvolume given a subvolume's origin and size.
func NewSubvolume(origin Coord, size Point3d) *Subvolume {
	return &Subvolume{origin, size}
}

// NewSubvolumeFromStrings returns a Subvolume given string representations of
// origin ("0,10,20") and size ("250,250,250").
func NewSubvolumeFromStrings(originStr, sizeStr string) (subvol *Subvolume, err error) {
	origin, err := PointStr(originStr).Coord()
	if err != nil {
		return
	}
	size, err := PointStr(sizeStr).Point3d()
	if err != nil {
		return
	}
	subvol = NewSubvolume(origin, size)
	return
}

// NewSlice returns a Geometry object for a XY, XZ, or YZ slice given the data's
// orientation, offset, and size.
func NewSlice(shape DataShape, offset Coord, size Point2d) (slice Geometry, err error) {
	switch shape {
	case XY:
		slice = NewSliceXY(offset, size)
	case XZ:
		slice = NewSliceXZ(offset, size)
	case YZ:
		slice = NewSliceYZ(offset, size)
	case Arb:
		err = fmt.Errorf("Arbitrary slices not supported yet.")
	default:
		err = fmt.Errorf("Illegal slice plane specification (%s)", shape)
	}
	return
}

// NewSliceFromStrings returns a Geometry object for a XY, XZ, or YZ slice given
// string representations of shape ("xy"), offset ("0,10,20"), and size ("250,250").
func NewSliceFromStrings(shapeStr, offsetStr, sizeStr string) (slice Geometry, err error) {
	shape, err := DataShapeString(shapeStr).DataShape()
	if err != nil {
		return
	}
	offset, err := PointStr(offsetStr).Coord()
	if err != nil {
		return
	}
	size, err := PointStr(sizeStr).Point2d()
	if err != nil {
		return
	}
	return NewSlice(shape, offset, size)
}

// SliceData specifies a rectangle in 3d space that is orthogonal to an axis.
type SliceData struct {
	origin Coord
	size   Point3d
}

func (s *SliceData) Origin() Coord {
	return s.origin
}

func (s *SliceData) Size() Point3d {
	return s.size
}

func (s *SliceData) Depth() int32 {
	return 1
}

func (s *SliceData) EndVoxel() Coord {
	return s.origin.AddSize(s.size)
}

func (s *SliceData) NumVoxels() int64 {
	if s == nil {
		return 0
	}
	return int64(s.size[0]) * int64(s.size[1]) * int64(s.size[2])
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

func (s *SliceXY) TopRight() Coord {
	return Coord{s.origin[0] + s.size[0] - 1, s.origin[1], s.origin[2]}
}

func (s *SliceXY) BottomLeft() Coord {
	return Coord{s.origin[0], s.origin[1] + s.size[1] - 1, s.origin[2]}
}

func (s *SliceXY) Normal() Vector3d {
	return Vector3d{0.0, 0.0, 1.0}
}

func (s *SliceXY) Width() int32 {
	return s.size[0]
}

func (s *SliceXY) Height() int32 {
	return s.size[1]
}

func NewSliceXY(origin Coord, size Point2d) Geometry {
	return &SliceXY{SliceData{origin, Point3d{size[0], size[1], 1}}}
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

func (s *SliceXZ) TopRight() Coord {
	return Coord{s.origin[0] + s.size[0] - 1, s.origin[1], s.origin[2]}
}

func (s *SliceXZ) BottomLeft() Coord {
	return Coord{s.origin[0], s.origin[1], s.origin[2] + s.size[2] - 1}
}

func (s *SliceXZ) Normal() Vector3d {
	return Vector3d{0.0, 1.0, 0.0}
}

func (s *SliceXZ) Width() int32 {
	return s.size[0]
}

func (s *SliceXZ) Height() int32 {
	return s.size[2]
}

func NewSliceXZ(origin Coord, size Point2d) Geometry {
	return &SliceXZ{SliceData{origin, Point3d{size[0], 1, size[1]}}}
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

func (s *SliceYZ) TopRight() Coord {
	return Coord{s.origin[0], s.origin[1] + s.size[1] - 1, s.origin[2]}
}

func (s *SliceYZ) BottomLeft() Coord {
	return Coord{s.origin[0], s.origin[1], s.origin[2] + s.size[2] - 1}
}

func (s *SliceYZ) Normal() Vector3d {
	return Vector3d{1.0, 0.0, 0.0}
}

func (s *SliceYZ) Width() int32 {
	return s.size[1]
}

func (s *SliceYZ) Height() int32 {
	return s.size[2]
}

func NewSliceYZ(origin Coord, size Point2d) Geometry {
	return &SliceYZ{SliceData{origin, Point3d{1, size[0], size[1]}}}
}

// SliceArb is an arbitray slice in 3d space.
type SliceArb struct {
	origin     Coord
	topRight   Coord
	bottomLeft Coord
}

func (s *SliceArb) DataShape() DataShape {
	return Arb
}

func (s *SliceArb) Origin() Coord {
	return s.origin
}

func (s *SliceArb) Size() Point3d {
	dx := s.topRight.Distance(s.origin)
	dy := s.bottomLeft.Distance(s.origin)
	return Point3d{dx, dy, 1}
}

func (s *SliceArb) Width() int32 {
	return s.Size()[0]
}

func (s *SliceArb) Height() int32 {
	return s.Size()[1]
}

func (s *SliceArb) Depth() int32 {
	return 1
}

func (s *SliceArb) NumVoxels() int64 {
	if s == nil {
		return 0
	}
	size := s.Size()
	return int64(size[0]) * int64(size[1])
}

func (s *SliceArb) String() string {
	return fmt.Sprintf("Arbitrary Slice with offset %s, top left %s, bottom left %s",
		s.origin, s.topRight, s.bottomLeft)
}

func (s *SliceArb) TopRight() Coord {
	return s.topRight
}

func (s *SliceArb) BottomLeft() Coord {
	return s.bottomLeft
}

func (s *SliceArb) EndVoxel() Coord {
	return Coord{s.topRight[0], s.bottomLeft[1], s.bottomLeft[2]}
}

func NewSliceArb(origin, topRight, bottomLeft Coord) Geometry {
	return &SliceArb{origin, topRight, bottomLeft}
}
