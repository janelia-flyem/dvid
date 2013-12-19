package dvid

import (
	"encoding/gob"
	"fmt"
	"image"
	"math"
	"strconv"
	"strings"

	"github.com/janelia-flyem/go/go-uuid/uuid"
)

// DataString is a string that is the name of DVID data.
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data name.
type DataString string

// DatasetLocalID is a DVID server-specific ID that is more compact than a UUID.
type DatasetLocalID LocalID32

// DataLocalID is a DVID server-specific ID that is more compact than a (UUID, Data URL).
type DataLocalID LocalID

// VersionalLocalID is a DVID server-specific ID that is more compact than a UUID.
// We assume we do not need more than 16-bits to represent the number of nodes in a
// version DAG.
type VersionLocalID LocalID

// UUID is a 32 character hexidecimal string ("" if invalid) that uniquely identifies
// nodes in a datastore's DAG.  We need universally unique identifiers to prevent collisions
// during creation of child nodes by distributed DVIDs:
// http://en.wikipedia.org/wiki/Universally_unique_identifier
type UUID string

// NewUUID returns a UUID
func NewUUID() UUID {
	u := uuid.NewUUID()
	if u == nil || len(u) != 16 {
		return UUID("")
	}
	return UUID(fmt.Sprintf("%032x", []byte(u)))
}

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(Point3d{})
	gob.Register(Point2d{})
}

// Point is an interface for n-dimensional points.   Types that implement the
// interface can optimize for particular dimensionality.
type Point interface {
	// NumDims returns the dimensionality of this point.
	NumDims() uint8

	// Value returns the point's value for the specified dimension without checking dim bounds.
	Value(dim uint8) int32

	// CheckedValue returns the point's value for the specified dimension and checks dim bounds.
	CheckedValue(dim uint8) (int32, error)

	// Duplicate returns a copy of the point without any pointer references.
	Duplicate() Point

	// Modify returns a copy of the point with the given (dim, value) components modified.
	Modify(map[uint8]int32) Point

	// AddScalar adds a scalar value to this point.
	AddScalar(int32) Point

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
	// Chunk returns a point in chunk space, which partitions the underlying point space.
	// For example, a chunk could be a tile (or a subvolume/block) and this returns the
	// tile's coordinate in tile space.
	Chunk(size Point) ChunkPoint

	// PointInChunk returns a point in a particular chunk's space.  For example, if a chunk
	// is a block of voxels, then the returned point is in that block coordinate space with
	// the first voxel in the block as the origin or zero point.
	PointInChunk(size Point) Point
}

// ChunkPoint describes a particular chunk in chunk space.
// The binary representation of a chunk point must behave reasonably for both negative and
// positive coordinates, e.g., when moving from -1 to 0 the binary representation isn't
// discontinous so the lexicographical ordering switches.  The simplest way to achieve
// this is to convert to an unsigned (positive) integer space where all coordinates are
// greater or equal to (0,0,...).
type ChunkPoint interface {
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

// ChunkPoint2d handles unsigned chunk coordinates.
type ChunkPoint2d [2]uint32

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p Point2d) Chunk(size Point) ChunkPoint {
	return ChunkPoint2d{
		uint32((int64(p[0]) + middleValue) / int64(size.Value(0))),
		uint32((int64(p[1]) + middleValue) / int64(size.Value(1))),
	}
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p Point2d) PointInChunk(size Point) Point {
	return Point2d{
		int32((int64(p[0]) + middleValue) % int64(size.Value(0))),
		int32((int64(p[1]) + middleValue) % int64(size.Value(1))),
	}
}

// Point3d is an ordered list of three 32-bit signed integers that implements the Point interface.
type Point3d [3]int32

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
	return fmt.Sprintf("(%d,%d,%d)", p[0], p[1], p[2])
}

// --- Chunkable interface support -----

// ChunkPoint3d handles unsigned chunk coordinates.
type ChunkPoint3d [3]uint32

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p Point3d) Chunk(size Point) ChunkPoint {
	return ChunkPoint3d{
		uint32((int64(p[0]) + middleValue) / int64(size.Value(0))),
		uint32((int64(p[1]) + middleValue) / int64(size.Value(1))),
		uint32((int64(p[2]) + middleValue) / int64(size.Value(2))),
	}
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p Point3d) PointInChunk(size Point) Point {
	return Point3d{
		int32((int64(p[0]) + middleValue) % int64(size.Value(0))),
		int32((int64(p[1]) + middleValue) % int64(size.Value(1))),
		int32((int64(p[2]) + middleValue) % int64(size.Value(2))),
	}
}

// PointNd is a slice of N 32-bit signed integers that implements the Point interface.
type PointNd []int32

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

// ChunkPointNd handles unsigned chunk coordinates.
type ChunkPointNd []uint32

// Chunk returns the chunk space coordinate of the chunk containing the point.
func (p PointNd) Chunk(size Point) ChunkPoint {
	chunkPoint := make(ChunkPointNd, len(p))
	for i, _ := range p {
		chunkPoint[i] = uint32((int64(p[i]) + middleValue) / int64(size.Value(uint8(i))))
	}
	return chunkPoint
}

// PointInChunk returns a point in containing block (chunk) space for the given point.
func (p PointNd) PointInChunk(size Point) Point {
	point := make(PointNd, len(p))
	for i, _ := range p {
		point[i] = int32((int64(p[i]) + middleValue) % int64(size.Value(uint8(i))))
	}
	return point
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

// Parse a string of format "%d<sep>%d<sep>%d,..." into a Point
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

// NdFloat32 is an N-dimensional slice of float32
type NdFloat32 []float32

// Parse a string of format "%f,%f,%f,..." into a slice of float32.
func StringToNdFloat32(str, separator string) (nd NdFloat32, err error) {
	elems := strings.Split(str, separator)
	nd = make(NdFloat32, len(elems))
	var f float64
	for i, elem := range elems {
		f, err = strconv.ParseFloat(elem, 32)
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
	i, err = strconv.ParseInt(n[0], 10, 32)
	if err != nil {
		return
	}
	j, err = strconv.ParseInt(n[1], 10, 32)
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
	i, err = strconv.ParseInt(n[0], 10, 32)
	if err != nil {
		return
	}
	j, err = strconv.ParseInt(n[1], 10, 32)
	if err != nil {
		return
	}
	k, err = strconv.ParseInt(n[2], 10, 32)
	if err != nil {
		return
	}
	return Point3d{int32(i), int32(j), int32(k)}, nil
}

func (n NdString) PointNd() (PointNd, error) {
	result := make(PointNd, len(n))
	for i, _ := range n {
		val, err := strconv.ParseInt(n[i], 10, 32)
		if err != nil {
			return nil, err
		}
		result[i] = int32(val)
	}
	return result, nil
}
