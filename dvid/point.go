package dvid

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"math"
	"sort"
	"strconv"
	"strings"
)

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(Point3d{})
	gob.Register(Point2d{})
	gob.Register(PointNd{})
	gob.Register(ChunkPoint2d{})
	gob.Register(ChunkPoint3d{})
	gob.Register(ChunkPointNd{})
}

type SimplePoint interface {
	// NumDims returns the dimensionality of this point.
	NumDims() uint8

	// Value returns the point's value for the specified dimension without checking dim bounds.
	Value(dim uint8) int32
}

// Point is an interface for n-dimensional points.   Types that implement the
// interface can optimize for particular dimensionality.
type Point interface {
	SimplePoint

	// CheckedValue returns the point's value for the specified dimension and checks dim bounds.
	CheckedValue(dim uint8) (int32, error)

	// Duplicate returns a copy of the point.
	Duplicate() Point

	// Modify returns a copy of the point with the given (dim, value) components modified.
	Modify(map[uint8]int32) Point

	// AddScalar adds a scalar value to this point.
	AddScalar(int32) Point

	// DivScalar divides this point by a scalar value.
	DivScalar(int32) Point

	// Add returns the addition of two points.
	Add(Point) Point

	// Sub returns the subtraction of the passed point from the receiver.
	Sub(Point) Point

	// Mod returns a point where each component is the receiver modulo the passed point's components.
	Mod(Point) Point

	// Div returns the division of the receiver by the passed point.
	Div(Point) Point

	// Mult returns the multiplication of the receiver by the passed point.
	Mult(Point) Point

	// Max returns a Point where each of its elements are the maximum of two points' elements.
	Max(Point) (Point, bool)

	// Min returns a Point where each of its elements are the minimum of two points' elements.
	Min(Point) (Point, bool)

	// Distance returns the integer distance (rounding down).
	Distance(Point) int32

	// Prod returns the product of the point elements.
	Prod() int64

	String() string
}

// Chunkable is an interface for n-dimensional points that can be partitioned into chunks.
type Chunkable interface {
	// Chunk returns a point in chunk space, the partition in which the given point falls.
	// For example, a chunk could be a tile (or a subvolume/block) and this returns the
	// tile's coordinate in tile space.
	Chunk(size Point) ChunkPoint

	// PointInChunk returns a point in a particular chunk's space.  For example, if a chunk
	// is a block of voxels, then the returned point is in that block coordinate space with
	// the first voxel in the block as the origin or zero point.
	PointInChunk(size Point) Point
}

// ChunkPoint describes a particular chunk in chunk space.
type ChunkPoint interface {
	SimplePoint

	// MinPoint returns the minimum point within a chunk and first in an iteration.
	MinPoint(size Point) Point

	// MaxPoint returns the maximum point within a chunk and last in an iteration.
	MaxPoint(size Point) Point
}

// NewPoint returns an appropriate Point implementation for the number of dimensions
// passed in.
func NewPoint(values []int32) (Point, error) {
	switch len(values) {
	case 0, 1:
		return nil, fmt.Errorf("No Point implementation for 0 or 1-d slice")
	case 2:
		return Point2d{values[0], values[1]}, nil
	case 3:
		return Point3d{values[0], values[1], values[2]}, nil
	default:
		p := make(PointNd, len(values))
		for i, value := range values {
			p[i] = value
		}
		return p, nil
	}
}

// BlockAligned returns true if the bounds for a n-d volume are aligned to blocks
// of the given size.  Alignment requires that the start point is the first voxel
// in a block and the end point is the last voxel in a block.
func BlockAligned(geom Bounder, blockSize Point) bool {
	pt0 := geom.StartPoint()
	pt1 := geom.EndPoint()
	if pt0.NumDims() != pt1.NumDims() || pt1.NumDims() != blockSize.NumDims() {
		Criticalf("Can't check block alignment when bounder %v and block size %v have differing dimensions\n",
			geom, blockSize)
		return false
	}

	var dim uint8
	for dim = 0; dim < blockSize.NumDims(); dim++ {
		if pt0.Value(dim)%blockSize.Value(dim) != 0 {
			return false
		}
		if (pt1.Value(dim)+1)%blockSize.Value(dim) != 0 {
			return false
		}
	}
	return true
}

// --- Implementations of the above interfaces in 2d and 3d ---------

const CoordinateBits = 32

// The middle value of a 32 bit space.  This should match the # of bits
// used for coordinates.
var middleValue int64 = 1 << (CoordinateBits - 1)

// Point2d is a 2d point.
type Point2d [2]int32

// RectSize returns the size of a rectangle as a Point2d.
func RectSize(rect image.Rectangle) (size Point2d) {
	size[0] = int32(rect.Dx())
	size[1] = int32(rect.Dy())
	return
}

// --- Point interface support -----

// Duplicate returns a copy of the point without any pointer references.
func (p Point2d) Duplicate() Point {
	return p // Go arrays are passed by value not reference (like slices)
}

// NumDims returns the dimensionality of this point.
func (p Point2d) NumDims() uint8 {
	return 2
}

// Value returns the point's value for the specified dimension without checking dim bounds.
func (p Point2d) Value(dim uint8) int32 {
	return p[dim]
}

// CheckedValue returns the point's value for the specified dimension and checks dim bounds.
func (p Point2d) CheckedValue(dim uint8) (int32, error) {
	if dim >= 2 {
		return 0, fmt.Errorf("Cannot return dimension %d of 2d point!", dim)
	}
	return p[dim], nil
}

// Modify returns a copy of the point with the given (dim, value) components modified.
func (p Point2d) Modify(settings map[uint8]int32) Point {
	if settings == nil {
		return p
	}
	altered := p
	for dim, value := range settings {
		altered[dim] = value
	}
	return altered
}

// AddScalar adds a scalar value to this point.
func (p Point2d) AddScalar(value int32) Point {
	return Point2d{p[0] + value, p[1] + value}
}

// DivScalar divides this point by a scalar value.
func (p Point2d) DivScalar(value int32) Point {
	return Point2d{p[0] / value, p[1] / value}
}

// Add returns the addition of two points.
func (p Point2d) Add(x Point) Point {
	p2 := x.(Point2d)
	return Point2d{
		p[0] + p2[0],
		p[1] + p2[1],
	}
}

// Sub returns the subtraction of the passed point from the receiver.
func (p Point2d) Sub(x Point) Point {
	p2 := x.(Point2d)
	return Point2d{
		p[0] - p2[0],
		p[1] - p2[1],
	}
}

// Mod returns a point where each component is the receiver modulo the passed point's components.
func (p Point2d) Mod(x Point) (result Point) {
	p2 := x.(Point2d)
	return Point2d{
		p[0] % p2[0],
		p[1] % p2[1],
	}
}

// Div returns the division of the receiver by the passed point.
func (p Point2d) Div(x Point) (result Point) {
	p2 := x.(Point2d)
	return Point2d{
		p[0] / p2[0],
		p[1] / p2[1],
	}
}

// Mult returns the multiplication of the receiver by the passed point.
func (p Point2d) Mult(x Point) (result Point) {
	p2 := x.(Point2d)
	return Point2d{
		p[0] * p2[0],
		p[1] * p2[1],
	}
}

// Max returns a Point where each of its elements are the maximum of two points' elements.
func (p Point2d) Max(x Point) (Point, bool) {
	var changed bool
	p2 := x.(Point2d)
	result := p
	if p[0] < p2[0] {
		result[0] = p2[0]
		changed = true
	}
	if p[1] < p2[1] {
		result[1] = p2[1]
		changed = true
	}
	return result, changed
}

// Min returns a Point where each of its elements are the minimum of two points' elements.
func (p Point2d) Min(x Point) (Point, bool) {
	var changed bool
	p2 := x.(Point2d)
	result := p
	if p[0] > p2[0] {
		result[0] = p2[0]
		changed = true
	}
	if p[1] > p2[1] {
		result[1] = p2[1]
		changed = true
	}
	return result, changed
}

// Distance returns the integer distance (rounding down).
func (p Point2d) Distance(x Point) int32 {
	p2 := x.(Point2d)
	dx := p[0] - p2[0]
	dy := p[1] - p2[1]
	return int32(math.Sqrt(float64(dx*dx + dy*dy)))
}

func (p Point2d) Prod() int64 {
	return int64(p[0]) * int64(p[1])
}

func (pt Point2d) String() string {
	return fmt.Sprintf("(%d, %d)", pt[0], pt[1])
}

// --- Chunkable interface support -----

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p Point2d) Chunk(size Point) ChunkPoint {
	var c0, c1 int32
	s0 := size.Value(0)
	s1 := size.Value(1)
	if p[0] < 0 {
		c0 = (p[0] - s0 + 1) / s0
	} else {
		c0 = p[0] / s0
	}
	if p[1] < 0 {
		c1 = (p[1] - s1 + 1) / s1
	} else {
		c1 = p[1] / s1
	}
	return ChunkPoint2d{c0, c1}
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p Point2d) PointInChunk(size Point) Point {
	var p0, p1 int32
	s0 := size.Value(0)
	s1 := size.Value(1)
	if p[0] < 0 {
		p0 = s0 - ((p[0] + 1) % s0) - 1
	} else {
		p0 = p[0] % s0
	}
	if p[1] < 0 {
		p1 = s1 - ((p[1] + 1) % s1) - 1
	} else {
		p1 = p[1] % s1
	}
	return Point2d{p0, p1}
}

// Point3d is an ordered list of three 32-bit signed integers that implements the Point interface.
type Point3d [3]int32

var (
	MaxPoint3d = Point3d{math.MaxInt32, math.MaxInt32, math.MaxInt32}
	MinPoint3d = Point3d{math.MinInt32, math.MinInt32, math.MinInt32}
)

func (p Point3d) MapKey() string {
	return string(p.Bytes())
}

func (p Point3d) Equals(p2 Point3d) bool {
	return p[0] == p2[0] && p[1] == p2[1] && p[2] == p2[2]
}

func (p Point3d) Less(p2 Point3d) bool {
	if p[2] < p2[2] {
		return true
	}
	if p[2] > p2[2] {
		return false
	}
	if p[1] < p2[1] {
		return true
	}
	if p[1] > p2[2] {
		return false
	}
	if p[0] < p2[0] {
		return true
	}
	return false
}

// Bytes returns a byte representation of the Point3d in little endian format.
func (p Point3d) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, p[0])
	binary.Write(buf, binary.LittleEndian, p[1])
	binary.Write(buf, binary.LittleEndian, p[2])
	return buf.Bytes()
}

// PointFromBytes returns a Point3d from bytes.  The passed point is used just
// to choose the appropriate byte decoding scheme.
func (p Point3d) PointFromBytes(b []byte) (readPt Point3d, err error) {
	buf := bytes.NewReader(b)
	if err = binary.Read(buf, binary.LittleEndian, &(readPt[0])); err != nil {
		return
	}
	if err = binary.Read(buf, binary.LittleEndian, &(readPt[1])); err != nil {
		return
	}
	if err = binary.Read(buf, binary.LittleEndian, &(readPt[2])); err != nil {
		return
	}
	return
}

// SetMinimum sets the point to the minimum elements of current and passed points.
func (p *Point3d) SetMinimum(p2 Point3d) {
	if p[0] > p2[0] {
		p[0] = p2[0]
	}
	if p[1] > p2[1] {
		p[1] = p2[1]
	}
	if p[2] > p2[2] {
		p[2] = p2[2]
	}
}

// SetMaximum sets the point to the maximum elements of current and passed points.
func (p *Point3d) SetMaximum(p2 Point3d) {
	if p[0] < p2[0] {
		p[0] = p2[0]
	}
	if p[1] < p2[1] {
		p[1] = p2[1]
	}
	if p[2] < p2[2] {
		p[2] = p2[2]
	}
}

// --- Point interface support -----

// NumDims returns the dimensionality of this point.
func (p Point3d) NumDims() uint8 {
	return 3
}

// Value returns the point's value for the specified dimension without checking dim bounds.
func (p Point3d) Value(dim uint8) int32 {
	return p[dim]
}

// CheckedValue returns the point's value for the specified dimension and checks dim bounds.
func (p Point3d) CheckedValue(dim uint8) (int32, error) {
	if dim >= 3 {
		return 0, fmt.Errorf("Cannot return dimension %d of 3d point!", dim)
	}
	return p[dim], nil
}

// Duplicate returns a copy of the point without any pointer references.
func (p Point3d) Duplicate() Point {
	return p // Go arrays are passed by value not reference (like slices)
}

// Modify returns a copy of the point with the given (dim, value) components modified.
func (p Point3d) Modify(settings map[uint8]int32) Point {
	if settings == nil {
		return p
	}
	altered := p
	for dim, value := range settings {
		altered[dim] = value
	}
	return altered
}

// AddScalar adds a scalar value to this point.
func (p Point3d) AddScalar(value int32) Point {
	return Point3d{p[0] + value, p[1] + value, p[2] + value}
}

// DivScalar divides this point by a scalar value.
func (p Point3d) DivScalar(value int32) Point {
	return Point3d{p[0] / value, p[1] / value, p[2] / value}
}

// Add returns the addition of two points.
func (p Point3d) Add(x Point) Point {
	p2 := x.(Point3d)
	return Point3d{
		p[0] + p2[0],
		p[1] + p2[1],
		p[2] + p2[2],
	}
}

// Sub returns the subtraction of the passed point from the receiver.
func (p Point3d) Sub(x Point) Point {
	p2 := x.(Point3d)
	return Point3d{
		p[0] - p2[0],
		p[1] - p2[1],
		p[2] - p2[2],
	}
}

// Mod returns a point where each component is the receiver modulo the passed point's components.
func (p Point3d) Mod(x Point) (result Point) {
	p2 := x.(Point3d)
	return Point3d{
		p[0] % p2[0],
		p[1] % p2[1],
		p[2] % p2[2],
	}
}

// Div returns the division of the receiver by the passed point.
func (p Point3d) Div(x Point) (result Point) {
	p2 := x.(Point3d)
	return Point3d{
		p[0] / p2[0],
		p[1] / p2[1],
		p[2] / p2[2],
	}
}

// Div returns the multiplication of the receiver by the passed point.
func (p Point3d) Mult(x Point) (result Point) {
	p2 := x.(Point3d)
	return Point3d{
		p[0] * p2[0],
		p[1] * p2[1],
		p[2] * p2[2],
	}
}

// Max returns a Point where each of its elements are the maximum of two points' elements.
func (p Point3d) Max(x Point) (Point, bool) {
	var changed bool
	p2 := x.(Point3d)
	result := p
	if p[0] < p2[0] {
		result[0] = p2[0]
		changed = true
	}
	if p[1] < p2[1] {
		result[1] = p2[1]
		changed = true
	}
	if p[2] < p2[2] {
		result[2] = p2[2]
		changed = true
	}
	return result, changed
}

// Min returns a Point where each of its elements are the minimum of two points' elements.
func (p Point3d) Min(x Point) (Point, bool) {
	var changed bool
	p2 := x.(Point3d)
	result := p
	if p[0] > p2[0] {
		result[0] = p2[0]
		changed = true
	}
	if p[1] > p2[1] {
		result[1] = p2[1]
		changed = true
	}
	if p[2] > p2[2] {
		result[2] = p2[2]
		changed = true
	}
	return result, changed
}

// Distance returns the integer distance (rounding down).
func (p Point3d) Distance(x Point) int32 {
	p2 := x.(Point3d)
	dx := p[0] - p2[0]
	dy := p[1] - p2[1]
	dz := p[2] - p2[2]
	return int32(math.Sqrt(float64(dx*dx + dy*dy + dz*dz)))
}

func (p Point3d) Prod() int64 {
	return int64(p[0]) * int64(p[1]) * int64(p[2])
}

func (p Point3d) String() string {
	return fmt.Sprintf("dvid.Point3d{%d,%d,%d}", p[0], p[1], p[2])
}

// --- Chunkable interface support -----

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p Point3d) Chunk(size Point) ChunkPoint {
	var c0, c1, c2 int32
	s0 := size.Value(0)
	s1 := size.Value(1)
	s2 := size.Value(2)
	if p[0] < 0 {
		c0 = (p[0] - s0 + 1) / s0
	} else {
		c0 = p[0] / s0
	}
	if p[1] < 0 {
		c1 = (p[1] - s1 + 1) / s1
	} else {
		c1 = p[1] / s1
	}
	if p[2] < 0 {
		c2 = (p[2] - s2 + 1) / s2
	} else {
		c2 = p[2] / s2
	}
	return ChunkPoint3d{c0, c1, c2}
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p Point3d) PointInChunk(size Point) Point {
	var p0, p1, p2 int32
	s0 := size.Value(0)
	s1 := size.Value(1)
	s2 := size.Value(2)
	if p[0] < 0 {
		p0 = s0 - ((p[0] + 1) % s0) - 1
	} else {
		p0 = p[0] % s0
	}
	if p[1] < 0 {
		p1 = s1 - ((p[1] + 1) % s1) - 1
	} else {
		p1 = p[1] % s1
	}
	if p[2] < 0 {
		p2 = s2 - ((p[2] + 1) % s2) - 1
	} else {
		p2 = p[2] % s2
	}
	return Point3d{p0, p1, p2}
}

// -------

// GetPoint3dFrom2d returns a 3d point from a 2d point in a plane.  The fill
// is used for the dimension not on the plane.
func GetPoint3dFrom2d(plane DataShape, p2d Point2d, fill int32) (Point3d, error) {
	var p Point3d
	switch {
	case plane.Equals(XY):
		p[0] = p2d[0]
		p[1] = p2d[1]
		p[2] = fill
	case plane.Equals(XZ):
		p[0] = p2d[0]
		p[1] = fill
		p[2] = p2d[1]
	case plane.Equals(YZ):
		p[0] = fill
		p[1] = p2d[0]
		p[2] = p2d[1]
	default:
		return Point3d{}, fmt.Errorf("Invalid 2d plane: %s", plane)
	}
	return p, nil
}

// Expand2d returns a 3d point increased by given size in the given plane
func (p Point3d) Expand2d(plane DataShape, size Point2d) (Point3d, error) {
	pt := p
	switch {
	case plane.Equals(XY):
		p[0] += size[0]
		p[1] += size[1]
	case plane.Equals(XZ):
		p[0] += size[0]
		p[2] += size[1]
	case plane.Equals(YZ):
		p[1] += size[0]
		p[2] += size[1]
	default:
		return Point3d{}, fmt.Errorf("Can't expand 3d point by %s", plane)
	}
	return pt, nil
}

type ListChunkPoint3d struct {
	Points  []ChunkPoint3d
	Indices []int
}

// ListChunkPoint3dFromVoxels creates a ListChunkPoint3d from JSON of voxel coordinates:
//   [ [x0, y0, z0], [x1, y1, z1], ... ]
func ListChunkPoint3dFromVoxels(jsonBytes []byte, blockSize Point) (*ListChunkPoint3d, error) {
	var pts []Point3d
	if err := json.Unmarshal(jsonBytes, &pts); err != nil {
		return nil, err
	}
	var list ListChunkPoint3d
	list.Points = make([]ChunkPoint3d, len(pts))
	list.Indices = make([]int, len(pts))
	for i, pt := range pts {
		list.Points[i], _ = pt.Chunk(blockSize).(ChunkPoint3d)
		list.Indices[i] = i
	}
	return &list, nil
}

type ByZYX ListChunkPoint3d

func (list *ByZYX) Len() int {
	return len(list.Points)
}

func (list *ByZYX) Swap(i, j int) {
	list.Points[i], list.Points[j] = list.Points[j], list.Points[i]
	list.Indices[i], list.Indices[j] = list.Indices[j], list.Indices[i]
}

// Points are ordered by Z, then Y, then X.
func (list *ByZYX) Less(i, j int) bool {
	if list.Points[i][2] < list.Points[j][2] {
		return true
	}
	if list.Points[i][2] > list.Points[j][2] {
		return false
	}
	if list.Points[i][1] < list.Points[j][1] {
		return true
	}
	if list.Points[i][1] > list.Points[j][1] {
		return false
	}
	return list.Points[i][0] < list.Points[j][0]
}

// PointNd is a slice of N 32-bit signed integers that implements the Point interface.
type PointNd []int32

// StringToPointNd parses a string of format "%d,%d,%d,..." into a slice of int32.
func StringToPointNd(str, separator string) (nd PointNd, err error) {
	elems := strings.Split(str, separator)
	nd = make(PointNd, len(elems))
	var n int64
	for i, elem := range elems {
		n, err = strconv.ParseInt(strings.TrimSpace(elem), 10, 32)
		if err != nil {
			return
		}
		nd[i] = int32(n)
	}
	return
}

// --- Point interface support -----

// NumDims returns the dimensionality of this point.
func (p PointNd) NumDims() uint8 {
	return uint8(len(p))
}

// Value returns the point's value for the specified dimension without checking dim bounds.
func (p PointNd) Value(dim uint8) int32 {
	return p[dim]
}

// CheckedValue returns the point's value for the specified dimension and checks dim bounds.
func (p PointNd) CheckedValue(dim uint8) (int32, error) {
	if int(dim) >= len(p) {
		return 0, fmt.Errorf("Cannot return dimension %d of %d-d point!", dim, len(p))
	}
	return p[dim], nil
}

// Duplicate returns a copy of the point without any pointer references.
func (p PointNd) Duplicate() Point {
	nd := make(PointNd, len(p))
	copy(nd, p)
	return nd
}

// Modify returns a copy of the point with the given (dim, value) components modified.
func (p PointNd) Modify(settings map[uint8]int32) Point {
	if settings == nil {
		return p
	}
	for dim, value := range settings {
		p[dim] = value
	}
	return p
}

// AddScalar adds a scalar value to this point.
func (p PointNd) AddScalar(value int32) Point {
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] + value
	}
	return result
}

// DivScalar divides this point by a scalar value.
func (p PointNd) DivScalar(value int32) Point {
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] / value
	}
	return result
}

// Add returns the addition of two points.
func (p PointNd) Add(x Point) Point {
	p2 := x.(PointNd)
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] + p2[i]
	}
	return result
}

// Sub returns the subtraction of the passed point from the receiver.
func (p PointNd) Sub(x Point) Point {
	p2 := x.(PointNd)
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] - p2[i]
	}
	return result
}

// Mod returns a point where each component is the receiver modulo the passed point's components.
func (p PointNd) Mod(x Point) Point {
	p2 := x.(PointNd)
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] % p2[i]
	}
	return result
}

// Div returns the division of the receiver by the passed point.
func (p PointNd) Div(x Point) Point {
	p2 := x.(PointNd)
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] / p2[i]
	}
	return result
}

// Div returns the multiplication of the receiver by the passed point.
func (p PointNd) Mult(x Point) Point {
	p2 := x.(PointNd)
	result := make(PointNd, len(p))
	for i, _ := range p {
		result[i] = p[i] * p2[i]
	}
	return result
}

// Max returns a Point where each of its elements are the maximum of two points' elements.
func (p PointNd) Max(x Point) (Point, bool) {
	p2 := x.(PointNd)
	var changed bool
	result := make(PointNd, len(p))
	for i, _ := range p {
		if p[i] < p2[i] {
			result[i] = p2[i]
			changed = true
		} else {
			result[i] = p[i]
		}
	}
	return result, changed
}

// Min returns a Point where each of its elements are the minimum of two points' elements.
func (p PointNd) Min(x Point) (Point, bool) {
	p2 := x.(PointNd)
	var changed bool
	result := make(PointNd, len(p))
	for i, _ := range p {
		if p[i] > p2[i] {
			result[i] = p2[i]
			changed = true
		} else {
			result[i] = p[i]
		}
	}
	return result, changed
}

// Distance returns the integer distance (rounding down).
func (p PointNd) Distance(x Point) int32 {
	p2 := x.(PointNd)
	var sqrDist float64
	for i, _ := range p {
		delta := p[i] - p2[i]
		sqrDist += float64(delta * delta)
	}
	return int32(math.Sqrt(sqrDist))
}

func (p PointNd) Prod() int64 {
	prod := int64(1)
	for _, val := range p {
		prod *= int64(val)
	}
	return prod
}

func (p PointNd) String() string {
	output := "("
	for _, val := range p {
		if len(output) > 1 {
			output += ","
		}
		output += strconv.Itoa(int(val))
	}
	output += ")"
	return output
}

// --- Chunkable interface support -----

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p PointNd) Chunk(size Point) ChunkPoint {
	cp := make(ChunkPointNd, len(p))
	for i, _ := range p {
		s := size.Value(uint8(i))
		if p[i] < 0 {
			cp[i] = (p[i] - s + 1) / s
		} else {
			cp[i] = p[i] / s
		}
	}
	return cp
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p PointNd) PointInChunk(size Point) Point {
	cp := make(PointNd, len(p))
	for i, _ := range p {
		s := size.Value(uint8(i))
		if p[i] < 0 {
			cp[i] = s - ((p[i] + 1) % s) - 1
		} else {
			cp[i] = p[i] % s
		}
	}
	return cp
}

// ChunkPoint2d handles 2d signed chunk coordinates.
type ChunkPoint2d [2]int32

var (
	MaxChunkPoint2d = ChunkPoint2d{math.MaxInt32, math.MaxInt32}
	MinChunkPoint2d = ChunkPoint2d{math.MinInt32, math.MinInt32}
)

const ChunkPoint2dSize = 8

func (c ChunkPoint2d) String() string {
	return fmt.Sprintf("(%d,%d)", c[0], c[1])
}

// --------- ChunkPoint interface -------------
// also fulfills SimplePoint interface

func (c ChunkPoint2d) NumDims() uint8 {
	return 2
}

// Value returns the value at the specified dimension.
func (c ChunkPoint2d) Value(dim uint8) int32 {
	return c[dim]
}

// MinPoint returns the smallest voxel coordinate of the given 2d chunk.
func (c ChunkPoint2d) MinPoint(size Point) Point {
	return Point2d{
		c[0] * size.Value(0),
		c[1] * size.Value(1),
	}
}

// MaxPoint returns the maximum voxel coordinate of the given 2d chunk.
func (c ChunkPoint2d) MaxPoint(size Point) Point {
	return Point2d{
		(c[0]+1)*size.Value(0) - 1,
		(c[1]+1)*size.Value(1) - 1,
	}
}

// ChunkPoint3d handles 3d signed chunk coordinates.
type ChunkPoint3d [3]int32

var (
	MaxChunkPoint3d = ChunkPoint3d{math.MaxInt32, math.MaxInt32, math.MaxInt32}
	MinChunkPoint3d = ChunkPoint3d{math.MinInt32, math.MinInt32, math.MinInt32}
)

const ChunkPoint3dSize = 12

func (c ChunkPoint3d) String() string {
	return fmt.Sprintf("(%d,%d,%d)", c[0], c[1], c[2])
}

// SetMinimum sets the point to the minimum elements of current and passed points.
func (p *ChunkPoint3d) SetMinimum(p2 ChunkPoint3d) {
	if p[0] > p2[0] {
		p[0] = p2[0]
	}
	if p[1] > p2[1] {
		p[1] = p2[1]
	}
	if p[2] > p2[2] {
		p[2] = p2[2]
	}
}

// SetMaximum sets the point to the maximum elements of current and passed points.
func (p *ChunkPoint3d) SetMaximum(p2 ChunkPoint3d) {
	if p[0] < p2[0] {
		p[0] = p2[0]
	}
	if p[1] < p2[1] {
		p[1] = p2[1]
	}
	if p[2] < p2[2] {
		p[2] = p2[2]
	}
}

func (p ChunkPoint3d) ToIZYXString() IZYXString {
	i := IndexZYX(p)
	return i.ToIZYXString()
}

// --------- ChunkPoint interface -------------

func (c ChunkPoint3d) NumDims() uint8 {
	return 3
}

// Value returns the value at the specified dimension.
func (c ChunkPoint3d) Value(dim uint8) int32 {
	return c[dim]
}

// MinPoint returns the smallest voxel coordinate of the given 3d chunk.
func (c ChunkPoint3d) MinPoint(size Point) Point {
	return Point3d{
		c[0] * size.Value(0),
		c[1] * size.Value(1),
		c[2] * size.Value(2),
	}
}

// MaxPoint returns the maximum voxel coordinate of the given 3d chunk.
func (c ChunkPoint3d) MaxPoint(size Point) Point {
	return Point3d{
		(c[0]+1)*size.Value(0) - 1,
		(c[1]+1)*size.Value(1) - 1,
		(c[2]+1)*size.Value(2) - 1,
	}
}

// Parse a string of format "%d<sep>%d<sep>%d,..." into a ChunkPoint3d
func StringToChunkPoint3d(str, separator string) (pt ChunkPoint3d, err error) {
	elems := strings.Split(str, separator)
	if len(elems) != 3 {
		err = fmt.Errorf("Cannot convert %q into a ChunkPoint3d", str)
		return
	}
	return NdString(elems).ChunkPoint3d()
}

// ChunkPointNd handles N-dimensional signed chunk coordinates.
type ChunkPointNd []int32

func (c ChunkPointNd) String() string {
	output := "("
	for _, val := range c {
		if len(output) > 1 {
			output += ","
		}
		output += strconv.Itoa(int(val))
	}
	output += ")"
	return output
}

// --------- ChunkPoint interface -------------

func (c ChunkPointNd) NumDims() uint8 {
	return uint8(len(c))
}

// Value returns the value at the specified dimension.
func (c ChunkPointNd) Value(dim uint8) int32 {
	return c[dim]
}

// MinPoint returns the smallest voxel coordinate of the given 3d chunk.
func (c ChunkPointNd) MinPoint(size Point) Point {
	min := make(PointNd, len(c))
	for i, _ := range c {
		min[i] = c[i] * size.Value(uint8(i))
	}
	return min
}

// MaxPoint returns the maximum voxel coordinate of the given 3d chunk.
func (c ChunkPointNd) MaxPoint(size Point) Point {
	max := make(PointNd, len(c))
	for i, _ := range c {
		max[i] = c[i] * size.Value(uint8(i))
	}
	return max
}

// Convert a slice of int32 into an appropriate Point implementation.
func SliceToPoint(coord []int32) (p Point, err error) {
	switch len(coord) {
	case 0, 1:
		return nil, fmt.Errorf("Cannot convert 0 or 1 integers into a Point")
	case 2:
		return Point2d{coord[0], coord[1]}, nil
	case 3:
		return Point3d{coord[0], coord[1], coord[2]}, nil
	default:
		return PointNd(coord), nil
	}
}

// StringToPoint2d parses a string of format "%d<sep>%d,..." into a Point2d
func StringToPoint2d(str, separator string) (Point2d, error) {
	elems := strings.Split(str, separator)
	if len(elems) != 2 {
		return Point2d{}, fmt.Errorf("String %q cannot be converted to a 2d point", str)
	}
	return NdString(elems).Point2d()
}

// StringToPoint3d parses a string of format "%d<sep>%d<sep>%d,..." into a Point3d
func StringToPoint3d(str, separator string) (Point3d, error) {
	elems := strings.Split(str, separator)
	if len(elems) != 3 {
		return Point3d{}, fmt.Errorf("String %q cannot be converted to a 3d point", str)
	}
	return NdString(elems).Point3d()
}

// StringToPoint parses a string of format "%d<sep>%d<sep>%d..." into a Point
func StringToPoint(str, separator string) (p Point, err error) {
	elems := strings.Split(str, separator)
	switch len(elems) {
	case 0, 1:
		return nil, fmt.Errorf("Cannot convert '%s' into a Point.", str)
	case 2:
		p, err = NdString(elems).Point2d()
	case 3:
		p, err = NdString(elems).Point3d()
	default:
		p, err = NdString(elems).PointNd()
	}
	return
}

// -- Handle N-dimensional floating points and strings --------

// Vector3d is a 3D vector of 64-bit floats, a recommended type for math operations.
type Vector3d [3]float64

func StringToVector3d(str, separator string) (Vector3d, error) {
	elems := strings.Split(str, separator)
	if len(elems) != 3 {
		return Vector3d{}, fmt.Errorf("Can't convert string '%s' (length %d) to Vector3d", str, len(elems))
	}
	var v Vector3d
	var err error
	for i, elem := range elems {
		v[i], err = strconv.ParseFloat(strings.TrimSpace(elem), 64)
		if err != nil {
			return Vector3d{}, err
		}
	}
	return v, nil
}

// Distance returns the distance between two points a and b.
func (v Vector3d) Distance(x Vector3d) float64 {
	dx := x[0] - v[0]
	dy := x[1] - v[1]
	dz := x[2] - v[2]
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

func (v Vector3d) Subtract(x Vector3d) Vector3d {
	return Vector3d{v[0] - x[0], v[1] - x[1], v[2] - x[2]}
}

func (v Vector3d) Add(x Vector3d) Vector3d {
	return Vector3d{v[0] + x[0], v[1] + x[1], v[2] + x[2]}
}

func (v Vector3d) DivideScalar(x float64) Vector3d {
	return Vector3d{v[0] / x, v[1] / x, v[2] / x}
}

func (v *Vector3d) Increment(x Vector3d) {
	(*v)[0] += x[0]
	(*v)[1] += x[1]
	(*v)[2] += x[2]
}

func (v Vector3d) String() string {
	return fmt.Sprintf("(%f,%f,%f)", v[0], v[1], v[2])
}

// NdFloat32 is an N-dimensional slice of float32
type NdFloat32 []float32

func (n NdFloat32) String() string {
	s := make([]string, len(n))
	for i, f := range n {
		s[i] = fmt.Sprintf("%f", f)
	}
	return strings.Join(s, ",")
}

// Equals returns true if two NdFloat32 are equal for each component.
func (n NdFloat32) Equals(n2 NdFloat32) bool {
	for i := range n {
		if n[i] != n2[i] {
			return false
		}
	}
	return true
}

// GetMin returns the minimum element of the N-dimensional float.
func (n NdFloat32) GetMin() float32 {
	if n == nil || len(n) == 0 {
		Criticalf("GetMin() called on bad ndfloat32!")
		return 0.0
	}
	min := n[0]
	for i := 1; i < len(n); i++ {
		if n[i] < min {
			min = n[i]
		}
	}
	return min
}

// GetMax returns the maximum element of the N-dimensional float.
func (n NdFloat32) GetMax() float32 {
	if n == nil || len(n) == 0 {
		Criticalf("GetMax() called on bad ndfloat32!")
		return 0.0
	}
	max := n[0]
	for i := 1; i < len(n); i++ {
		if n[i] > max {
			max = n[i]
		}
	}
	return max
}

// MultScalar multiples a N-dimensional float by a float32
func (n NdFloat32) MultScalar(x float32) NdFloat32 {
	result := make(NdFloat32, len(n))
	for i := 0; i < len(n); i++ {
		result[i] = n[i] * x
	}
	return result
}

// Parse a string of format "%f,%f,%f,..." into a slice of float32.
func StringToNdFloat32(str, separator string) (nd NdFloat32, err error) {
	elems := strings.Split(str, separator)
	nd = make(NdFloat32, len(elems))
	var f float64
	for i, elem := range elems {
		f, err = strconv.ParseFloat(strings.TrimSpace(elem), 32)
		if err != nil {
			return
		}
		nd[i] = float32(f)
	}
	return
}

// NdString is an N-dimensional slice of strings
type NdString []string

// Parse a string of format "%f,%f,%f,..." into a slice of float32.
func StringToNdString(str, separator string) (nd NdString, err error) {
	return NdString(strings.Split(str, separator)), nil
}

func (n NdString) Point2d() (p Point2d, err error) {
	if len(n) != 2 {
		err = fmt.Errorf("Cannot parse into a 2d point")
		return
	}
	var i, j int64
	i, err = strconv.ParseInt(strings.TrimSpace(n[0]), 10, 32)
	if err != nil {
		return
	}
	j, err = strconv.ParseInt(strings.TrimSpace(n[1]), 10, 32)
	if err != nil {
		return
	}
	return Point2d{int32(i), int32(j)}, nil
}

func (n NdString) Point3d() (p Point3d, err error) {
	if len(n) != 3 {
		err = fmt.Errorf("Cannot parse into a 3d point")
		return
	}
	var i, j, k int64
	i, err = strconv.ParseInt(strings.TrimSpace(n[0]), 10, 32)
	if err != nil {
		return
	}
	j, err = strconv.ParseInt(strings.TrimSpace(n[1]), 10, 32)
	if err != nil {
		return
	}
	k, err = strconv.ParseInt(strings.TrimSpace(n[2]), 10, 32)
	if err != nil {
		return
	}
	return Point3d{int32(i), int32(j), int32(k)}, nil
}

func (n NdString) ChunkPoint3d() (p ChunkPoint3d, err error) {
	if len(n) != 3 {
		err = fmt.Errorf("Cannot parse into a 3d chunk point")
		return
	}
	var i, j, k int64
	i, err = strconv.ParseInt(strings.TrimSpace(n[0]), 10, 32)
	if err != nil {
		return
	}
	j, err = strconv.ParseInt(strings.TrimSpace(n[1]), 10, 32)
	if err != nil {
		return
	}
	k, err = strconv.ParseInt(strings.TrimSpace(n[2]), 10, 32)
	if err != nil {
		return
	}
	return ChunkPoint3d{int32(i), int32(j), int32(k)}, nil
}

func (n NdString) PointNd() (PointNd, error) {
	result := make(PointNd, len(n))
	for i, _ := range n {
		val, err := strconv.ParseInt(strings.TrimSpace(n[i]), 10, 32)
		if err != nil {
			return nil, err
		}
		result[i] = int32(val)
	}
	return result, nil
}

// Extents defines a 3d volume
type Extents3d struct {
	MinPoint Point3d
	MaxPoint Point3d
}

// NewExtents3dFromStrings returns an Extents3d given string representations of
// offset and size in voxels.
func NewExtents3dFromStrings(offsetStr, sizeStr, sep string) (*Extents3d, error) {
	offset, err := StringToPoint3d(offsetStr, sep)
	if err != nil {
		return nil, err
	}
	size, err := StringToPoint3d(sizeStr, sep)
	if err != nil {
		return nil, err
	}
	ext := Extents3d{
		MinPoint: offset,
		MaxPoint: Point3d{offset[0] + size[0] - 1, offset[1] + size[1] - 1, offset[2] + size[2] - 1},
	}
	return &ext, nil
}

func (ext *Extents3d) ExtendDim(dim int, val int32) bool {
	var changed bool
	if ext == nil {
		return false
	}
	if val < ext.MinPoint[dim] {
		ext.MinPoint[dim] = val
		changed = true
	}
	if val > ext.MaxPoint[dim] {
		ext.MaxPoint[dim] = val
		changed = true
	}
	return changed
}

func (ext *Extents3d) Extend(pt Point3d) bool {
	var changed bool
	if ext == nil {
		return false
	}
	if pt[0] < ext.MinPoint[0] {
		ext.MinPoint[0] = pt[0]
		changed = true
	}
	if pt[1] < ext.MinPoint[1] {
		ext.MinPoint[1] = pt[1]
		changed = true
	}
	if pt[2] < ext.MinPoint[2] {
		ext.MinPoint[2] = pt[2]
		changed = true
	}
	if pt[0] > ext.MaxPoint[0] {
		ext.MaxPoint[0] = pt[0]
		changed = true
	}
	if pt[1] > ext.MaxPoint[1] {
		ext.MaxPoint[1] = pt[1]
		changed = true
	}
	if pt[2] > ext.MaxPoint[2] {
		ext.MaxPoint[2] = pt[2]
		changed = true
	}
	return changed
}

// VoxelWithin returns true if given point is within the extents.
func (ext *Extents3d) VoxelWithin(pt Point3d) bool {
	if pt[2] < ext.MinPoint[2] {
		return false
	}
	if pt[2] > ext.MaxPoint[2] {
		return false
	}
	if pt[1] < ext.MinPoint[1] {
		return false
	}
	if pt[1] > ext.MaxPoint[1] {
		return false
	}
	if pt[0] < ext.MinPoint[0] {
		return false
	}
	if pt[0] > ext.MaxPoint[0] {
		return false
	}
	return true
}

// BlockWithin returns true if given block coord is within the extents partitioned
// using the give block size, or false if it is not given a 3d point size or block coord.
func (ext *Extents3d) BlockWithin(size Point3d, coord ChunkPoint3d) bool {
	min, max := ext.BlockRange(size)
	if coord[2] < min[2] {
		return false
	}
	if coord[2] > max[2] {
		return false
	}
	if coord[1] < min[1] {
		return false
	}
	if coord[1] > max[1] {
		return false
	}
	if coord[0] < min[0] {
		return false
	}
	if coord[0] > max[0] {
		return false
	}
	return true
}

// BlockRange returns the starting and ending block coordinate for given extents
// and block size.
func (ext *Extents3d) BlockRange(size Point3d) (min, max ChunkPoint3d) {
	min = ext.MinPoint.Chunk(size).(ChunkPoint3d)
	max = ext.MaxPoint.Chunk(size).(ChunkPoint3d)
	return
}

// ChunkExtents3d defines a 3d volume of chunks
type ChunkExtents3d struct {
	MinChunk ChunkPoint3d
	MaxChunk ChunkPoint3d
}

func (ext *ChunkExtents3d) ExtendDim(dim int, val int32) bool {
	var changed bool
	if ext == nil {
		return false
	}
	if val < ext.MinChunk[dim] {
		ext.MinChunk[dim] = val
		changed = true
	}
	if val > ext.MaxChunk[dim] {
		ext.MaxChunk[dim] = val
		changed = true
	}
	return changed
}

func (ext *ChunkExtents3d) Extend(pt ChunkPoint3d) bool {
	var changed bool
	if ext == nil {
		return false
	}
	if pt[0] < ext.MinChunk[0] {
		ext.MinChunk[0] = pt[0]
		changed = true
	}
	if pt[1] < ext.MinChunk[1] {
		ext.MinChunk[1] = pt[1]
		changed = true
	}
	if pt[2] < ext.MinChunk[2] {
		ext.MinChunk[2] = pt[2]
		changed = true
	}
	if pt[0] > ext.MaxChunk[0] {
		ext.MaxChunk[0] = pt[0]
		changed = true
	}
	if pt[1] > ext.MaxChunk[1] {
		ext.MaxChunk[1] = pt[1]
		changed = true
	}
	if pt[2] > ext.MaxChunk[2] {
		ext.MaxChunk[2] = pt[2]
		changed = true
	}
	return changed
}

// Span is (Z, Y, X0, X1).
// TODO -- Consolidate with dvid.RLE since both handle run-length encodings in X, although
// dvid.RLE handles voxel coordinates not block (chunk) coordinates.
type Span [4]int32

func (s Span) String() string {
	return fmt.Sprintf("[%d, %d, %d, %d]", s[0], s[1], s[2], s[3])
}

// Extends returns true and modifies the span if the given point
// is one more in x direction than this span.  Else it returns false.
func (s *Span) Extends(x, y, z int32) bool {
	if s == nil || (*s)[0] != z || (*s)[1] != y || (*s)[3] != x-1 {
		return false
	}
	(*s)[3] = x
	return true
}

func (s Span) Unpack() (z, y, x0, x1 int32) {
	return s[0], s[1], s[2], s[3]
}

func (s Span) Less(s2 Span) bool {
	if s[0] < s2[0] {
		return true
	}
	if s[0] > s2[0] {
		return false
	}
	if s[1] < s2[1] {
		return true
	}
	if s[1] > s2[1] {
		return false
	}
	if s[2] < s2[2] {
		return true
	}
	if s[2] > s2[2] {
		return false
	}
	return s[3] < s2[3]
}

func (s Span) LessChunkPoint3d(block ChunkPoint3d) bool {
	if s[0] < block[2] {
		return true
	}
	if s[0] > block[2] {
		return false
	}
	if s[1] < block[1] {
		return true
	}
	if s[1] > block[1] {
		return false
	}
	if s[3] < block[0] {
		return true
	}
	return false
}

func (s Span) Includes(block ChunkPoint3d) bool {
	if s[0] != block[2] {
		return false
	}
	if s[1] != block[1] {
		return false
	}
	if s[2] > block[0] || s[3] < block[0] {
		return false
	}
	return true
}

type Spans []Span

// MarshalBinary returns a binary serialization of the RLEs for the
// spans with the first 4 bytes corresponding to the number of spans.
func (s Spans) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
		return nil, err
	}
	if s != nil {
		for _, span := range s {
			if err := binary.Write(buf, binary.LittleEndian, span[2]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, span[1]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, span[0]); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, span[3]-span[2]+1); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func (s *Spans) UnmarshalBinary(b []byte) error {
	buf := bytes.NewBuffer(b)
	var numSpans uint32
	if err := binary.Read(buf, binary.LittleEndian, &numSpans); err != nil {
		return err
	}
	if numSpans == 0 {
		*s = Spans{}
		return nil
	}
	*s = make(Spans, int(numSpans), int(numSpans))
	for i := uint32(0); i < numSpans; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &((*s)[i][2])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*s)[i][1])); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &((*s)[i][0])); err != nil {
			return err
		}
		var length int32
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return err
		}
		(*s)[i][3] = (*s)[i][2] + length - 1
	}
	return nil
}

// --- Sort interface -----

func (s Spans) Len() int {
	return len(s)
}

func (s Spans) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Spans) Less(i, j int) bool {
	return s[i].Less(s[j])
}

// Normalize returns a sorted and merged list of Span.  Spans are in z, y, then x order.
// Any adjacent spans are merged into a larger span.
func (s Spans) Normalize() Spans {
	if s == nil || len(s) == 0 {
		return Spans{}
	}

	// Sort all spans in (z,y) keys
	nOrig := len(s)
	norm := make(Spans, nOrig)
	copy(norm, s)
	sort.Sort(norm)

	// Iterate through each (y,z) and combine adjacent spans.
	n := 0 // current position of normalized slice end
	for o := 1; o < nOrig; o++ {
		// If non-contiguous span
		if norm[n][0] != norm[o][0] || norm[n][1] != norm[o][1] || norm[n][3]+1 < norm[o][2] {
			n++
			if n < nOrig {
				norm[n] = norm[o]
			}
		} else if norm[o][3] > norm[n][3] {
			norm[n][3] = norm[o][3]
		}
	}
	if n+1 <= nOrig {
		n++
	}
	return norm[:n]
}

type Resolution struct {
	// Resolution of voxels in volume
	VoxelSize NdFloat32

	// Units of resolution, e.g., "nanometers"
	VoxelUnits NdString
}

// Returns true if resolution in all dimensions is equal.
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
