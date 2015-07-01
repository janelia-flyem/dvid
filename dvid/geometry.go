package dvid

import (
	"fmt"
	"strings"
	"sync"
)

type Bounder interface {
	// StartPoint returns the offset to first point of data.
	StartPoint() Point

	// EndPoint returns the last point.
	EndPoint() Point
}

// Geometry describes the shape, size, and position of data in the DVID volume.
type Geometry interface {
	Bounder

	// DataShape describes the shape of the data.
	DataShape() DataShape

	// Size returns the extent in each dimension.
	Size() Point

	// NumVoxels returns the number of voxels within this space.
	NumVoxels() int64

	String() string
}

// Extents holds the extents of a volume in both absolute voxel coordinates
// and lexicographically sorted chunk indices.
type Extents struct {
	MinPoint Point
	MaxPoint Point

	MinIndex ChunkIndexer
	MaxIndex ChunkIndexer

	pointMu sync.Mutex
	indexMu sync.Mutex
}

// --- dvid.Bounder interface ----

// StartPoint returns the offset to first point of data.
func (ext *Extents) StartPoint() Point {
	return ext.MinPoint
}

// EndPoint returns the last point.
func (ext *Extents) EndPoint() Point {
	return ext.MaxPoint
}

// --------

// AdjustPoints modifies extents based on new voxel coordinates in concurrency-safe manner.
func (ext *Extents) AdjustPoints(pointBeg, pointEnd Point) bool {
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
func (ext *Extents) AdjustIndices(indexBeg, indexEnd ChunkIndexer) bool {
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

// GetNumBlocks returns the number of n-d blocks necessary to cover the given geometry.
func GetNumBlocks(geom Geometry, blockSize Point) int {
	startPt := geom.StartPoint()
	size := geom.Size()
	numBlocks := 1
	for dim := uint8(0); dim < geom.Size().NumDims(); dim++ {
		blockLength := blockSize.Value(dim)
		startMod := startPt.Value(dim) % blockLength
		length := size.Value(dim) + startMod
		blocks := length / blockLength
		if length%blockLength != 0 {
			blocks++
		}
		numBlocks *= int(blocks)
	}
	return numBlocks
}

type Dimension struct {
	Name     string
	Units    string
	beg, end int32
}

var (
	// XY describes a 2d rectangle of voxels that share a z-coord.
	XY = DataShape{3, []uint8{0, 1}}

	// XZ describes a 2d rectangle of voxels that share a y-coord.
	XZ = DataShape{3, []uint8{0, 2}}

	// YZ describes a 2d rectangle of voxels that share a x-coord.
	YZ = DataShape{3, []uint8{1, 2}}

	// Arb describes a 2d rectangle of voxels with arbitrary 3d orientation.
	Arb = DataShape{3, nil}

	// Vol3d describes a 3d volume of voxels
	Vol3d = DataShape{3, []uint8{0, 1, 2}}
)

// DataShape describes the number of dimensions and the ordering of the dimensions.
type DataShape struct {
	dims  uint8
	shape []uint8
}

const DataShapeBytes = 7

// BytesToDataShape recovers a DataShape from a series of bytes.
func BytesToDataShape(b []byte) (s DataShape, err error) {
	if b == nil {
		err = fmt.Errorf("Cannot convert nil to DataShape!")
		return
	}
	if len(b) != DataShapeBytes {
		err = fmt.Errorf("Cannot convert %d bytes to DataShape", len(b))
		return
	}
	s = DataShape{dims: uint8(b[0])}
	shapeLen := int(b[1])
	s.shape = make([]uint8, shapeLen)
	for i := 0; i < shapeLen; i++ {
		s.shape[i] = b[i+2]
	}
	return
}

// AxisName returns common axis descriptions like X, Y, and Z for a shapes dimensions.
func (s DataShape) AxisName(axis uint8) string {
	if int(axis) >= len(s.shape) {
		return "Unknown"
	}
	switch s.shape[axis] {
	case 0:
		return "X"
	case 1:
		return "Y"
	case 2:
		return "Z"
	default:
		return fmt.Sprintf("Dim %d", axis)
	}
}

// Bytes returns a fixed length byte representation that can be used for keys.
// Up to 5-d shapes can be used.
func (s DataShape) Bytes() []byte {
	b := make([]byte, DataShapeBytes)
	b[0] = byte(s.dims)
	b[1] = byte(len(s.shape))
	for i := 0; i < len(s.shape); i++ {
		b[i+2] = s.shape[i]
	}
	return b
}

// TotalDimensions returns the full dimensionality of space within which there is this DataShape.
func (s DataShape) TotalDimensions() int8 {
	return int8(s.dims)
}

// ShapeDimensions returns the number of dimensions for this shape.
func (s DataShape) ShapeDimensions() int8 {
	if s.shape == nil {
		return 0
	}
	return int8(len(s.shape))
}

// ShapeDimension returns the axis number for a shape dimension.
func (s DataShape) ShapeDimension(axis uint8) (uint8, error) {
	if s.shape == nil {
		return 0, fmt.Errorf("Cannot request ShapeDimension from nil DataShape")
	}
	if len(s.shape) <= int(axis) {
		return 0, fmt.Errorf("Illegal dimension requested from DataShape: %d", axis)
	}
	return s.shape[axis], nil
}

// GetSize2D returns the width and height of a 2D shape given a n-D size.
func (s DataShape) GetSize2D(size SimplePoint) (width, height int32, err error) {
	if len(s.shape) != 2 {
		err = fmt.Errorf("Can't get 2D size from a non-2D shape: %s", s)
		return
	}
	width = size.Value(s.shape[0])
	height = size.Value(s.shape[1])
	return
}

// GetFloat2D returns elements of a N-d float array given a 2d shape.
func (s DataShape) GetFloat2D(fslice NdFloat32) (x, y float32, err error) {
	if len(s.shape) != 2 {
		err = fmt.Errorf("Can't get 2D data from a non-2D shape: %s", s)
		return
	}
	x = fslice[s.shape[0]]
	y = fslice[s.shape[1]]
	return
}

// ChunkPoint3d returns a chunk point where the XY is determined by the
// type of slice orientation of the DataShape, and the Z is the non-chunked
// coordinate.  This is useful for tile generation where you have 2d tiles
// in a 3d space.
func (s DataShape) ChunkPoint3d(p, size Point) (ChunkPoint3d, error) {
	if len(s.shape) != 2 {
		return ChunkPoint3d{}, fmt.Errorf("Can't get process slice from a non-2D shape: %s", s)
	}
	if s.dims != 3 {
		return ChunkPoint3d{}, fmt.Errorf("ChunkPoint3d() can only be called on 3d points!")
	}
	chunkable, ok := p.(Chunkable)
	if !ok {
		return ChunkPoint3d{}, fmt.Errorf("ChunkPoint3d() requires Chunkable point.")
	}
	chunk := chunkable.Chunk(size)
	switch {
	case s.Equals(XY):
		return ChunkPoint3d{chunk.Value(0), chunk.Value(1), p.Value(2)}, nil
	case s.Equals(XZ):
		return ChunkPoint3d{chunk.Value(0), chunk.Value(2), p.Value(1)}, nil
	case s.Equals(YZ):
		return ChunkPoint3d{chunk.Value(1), chunk.Value(2), p.Value(0)}, nil
	default:
		return ChunkPoint3d{}, fmt.Errorf("ChunkPoint3d() can only be run on slices: given %s", s)
	}
}

// PlaneToChunkPoint3d returns a chunk point corresponding to the given point on the DataShape's
// plane.  If DataShape is not a plane, returns an error.
func (s DataShape) PlaneToChunkPoint3d(x, y int32, offset, size Point) (ChunkPoint3d, error) {
	if len(s.shape) != 2 {
		return ChunkPoint3d{}, fmt.Errorf("Can't get plane point from a non-2D shape: %s", s)
	}
	if s.dims != 3 {
		return ChunkPoint3d{}, fmt.Errorf("PlaneToChunkPoint3d() requires 3D shape: %s", s)
	}
	var p Point3d
	switch {
	case s.Equals(XY):
		p = Point3d{x + offset.Value(0), y + offset.Value(1), offset.Value(2)}
		chunkPt := p.Chunk(size)
		return ChunkPoint3d{chunkPt.Value(0), chunkPt.Value(1), p[2]}, nil
	case s.Equals(XZ):
		p = Point3d{x + offset.Value(0), offset.Value(1), y + offset.Value(2)}
		chunkPt := p.Chunk(size)
		return ChunkPoint3d{chunkPt.Value(0), p[1], chunkPt.Value(2)}, nil
	case s.Equals(YZ):
		p = Point3d{offset.Value(0), x + offset.Value(1), y + offset.Value(2)}
		chunkPt := p.Chunk(size)
		return ChunkPoint3d{p[0], chunkPt.Value(1), chunkPt.Value(2)}, nil
	default:
		return ChunkPoint3d{}, fmt.Errorf("ChunkPoint3d() can only be run on slices: given %s", s)
	}
}

// Duplicate returns a duplicate of the DataShape.
func (s DataShape) Duplicate() DataShape {
	return DataShape{dims: s.dims, shape: append([]uint8{}, s.shape...)}
}

// Equals returns true if the passed DataShape is identical.
func (s DataShape) Equals(s2 DataShape) bool {
	if s.dims == s2.dims && len(s.shape) == len(s2.shape) {
		for i, dim := range s.shape {
			if s2.shape[i] != dim {
				return false
			}
		}
		return true
	}
	return false
}

func (s DataShape) String() string {
	switch {
	case s.Equals(XY):
		return "XY slice"
	case s.Equals(XZ):
		return "XZ slice"
	case s.Equals(YZ):
		return "YZ slice"
	case s.Equals(Arb):
		return "slice with arbitrary orientation"
	case s.Equals(Vol3d):
		return "3d volume"
	case s.dims > 3:
		return "n-D volume"
	default:
		return "Unknown shape"
	}
}

// DataShapes are a slice of DataShape.
type DataShapes []DataShape

// GetShapes returns DataShapes from a string where each shape specification
// is delimited by a separator.  If the key is not found, nil is returned.
func (c Config) GetShapes(key, separator string) ([]DataShape, error) {
	s, found, err := c.GetString(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	parts := strings.Split(s, separator)
	shapes := []DataShape{}
	for _, part := range parts {
		shape, err := DataShapeString(part).DataShape()
		if err != nil {
			return nil, err
		}
		shapes = append(shapes, shape)
	}
	return shapes, nil
}

// String for specifying a slice orientation or subvolume
type DataShapeString string

// List of strings associated with shapes up to 3d
var dataShapeStrings = map[string]DataShape{
	"xy":    XY,
	"xz":    XZ,
	"yz":    YZ,
	"vol":   Vol3d,
	"arb":   Arb,
	"0_1":   XY,
	"0_2":   XZ,
	"1_2":   YZ,
	"0_1_2": Vol3d,
	"0,1":   XY,
	"0,2":   XZ,
	"1,2":   YZ,
	"0,1,2": Vol3d,
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

// Returns the image size necessary to compute an isotropic slice of the given dimensions.
// If isotropic is false, simply returns the original slice geometry.  If isotropic is true,
// uses the higher resolution dimension.
func Isotropy2D(voxelSize NdFloat32, geom Geometry, isotropic bool) (Geometry, error) {
	if !isotropic {
		return geom, nil
	}
	// Get the voxel resolutions for this particular slice orientation
	resX, resY, err := geom.DataShape().GetFloat2D(voxelSize)
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
	slice, ok := geom.(*OrthogSlice)
	if !ok {
		return nil, fmt.Errorf("can only handle isotropy for orthogonal 2d slices")
	}
	dstSlice := slice.Duplicate()
	dstSlice.SetSize(Point2d{dstW, dstH})
	return dstSlice, nil
}

// ---- Geometry implementations ------

// Subvolume describes a 3d box Geometry.  The "Sub" prefix emphasizes that the
// data is usually a smaller portion of the volume held by the DVID datastore.
// Note that the 3d coordinate system is assumed to be a Right-Hand system like OpenGL.
type Subvolume struct {
	shape  DataShape
	offset Point
	size   Point
}

// NewSubvolumeFromStrings returns a Subvolume given string representations of
// offset ("0,10,20") and size ("250,250,250").
func NewSubvolumeFromStrings(offsetStr, sizeStr, sep string) (*Subvolume, error) {
	offset, err := StringToPoint(offsetStr, sep)
	if err != nil {
		return nil, err
	}
	if offset.NumDims() != 3 {
		return nil, fmt.Errorf("Offset must be 3d coordinate, not %d-d coordinate", offset.NumDims())
	}
	size, err := StringToPoint(sizeStr, sep)
	if err != nil {
		return nil, err
	}
	if size.NumDims() != 3 {
		return nil, fmt.Errorf("Size must be 3 (not %d) dimensions", size.NumDims())
	}
	return NewSubvolume(offset, size), nil
}

// NewSubvolume returns a Subvolume given a subvolume's origin and size.
func NewSubvolume(offset, size Point) *Subvolume {
	return &Subvolume{Vol3d, offset, size}
}

func (s *Subvolume) DataShape() DataShape {
	return Vol3d
}

func (s *Subvolume) Size() Point {
	return s.size
}

func (s *Subvolume) NumVoxels() int64 {
	if s == nil || s.size.NumDims() == 0 {
		return 0
	}
	voxels := int64(s.size.Value(0))
	for dim := uint8(1); dim < s.size.NumDims(); dim++ {
		voxels *= int64(s.size.Value(dim))
	}
	return voxels
}

func (s *Subvolume) StartPoint() Point {
	return s.offset
}

func (s *Subvolume) EndPoint() Point {
	return s.offset.Add(s.size.Sub(Point3d{1, 1, 1}))
}

func (s *Subvolume) String() string {
	return fmt.Sprintf("%s %s at offset %s", s.shape, s.size, s.offset)
}

// OrthogSlice is a 2d rectangle orthogonal to two axis of the space that is slices.
// It fulfills a Geometry interface.
type OrthogSlice struct {
	shape    DataShape
	offset   Point
	size     Point2d
	endPoint Point
}

// NewSliceFromStrings returns a Geometry object for a XY, XZ, or YZ slice given
// a data shape string, offset ("0,10,20"), and size ("250,250").
func NewSliceFromStrings(str DataShapeString, offsetStr, sizeStr, sep string) (Geometry, error) {
	shape, err := str.DataShape()
	if err != nil {
		return nil, err
	}
	offset, err := StringToPoint(offsetStr, sep)
	if err != nil {
		return nil, err
	}
	if offset.NumDims() != 3 {
		return nil, fmt.Errorf("Offset must be 3d coordinate, not %d-d coordinate", offset.NumDims())
	}
	// Enforce that size string is 2d since this is supposed to be a slice.
	ndstring, err := StringToNdString(sizeStr, sep)
	if err != nil {
		return nil, err
	}
	size, err := ndstring.Point2d()
	if err != nil {
		return nil, err
	}
	return NewOrthogSlice(shape, offset, size)
}

// NewOrthogSlice returns an OrthogSlice of chosen orientation, offset, and size.
func NewOrthogSlice(s DataShape, offset Point, size Point2d) (Geometry, error) {
	if offset.NumDims() != s.dims {
		return nil, fmt.Errorf("NewOrthogSlice: offset dimensionality %d != shape %d",
			offset.NumDims(), s.dims)
	}
	if s.shape == nil || len(s.shape) != 2 {
		return nil, fmt.Errorf("NewOrthogSlice: shape not properly specified")
	}
	xDim := s.shape[0]
	if xDim >= s.dims {
		return nil, fmt.Errorf("NewOrthogSlice: X dimension of slice (%d) > # avail dims (%d)",
			xDim, s.dims)
	}
	yDim := s.shape[1]
	if yDim >= s.dims {
		return nil, fmt.Errorf("NewOrthogSlice: Y dimension of slice (%d) > # avail dims (%d)",
			yDim, s.dims)
	}
	settings := map[uint8]int32{
		xDim: offset.Value(xDim) + size[0] - 1,
		yDim: offset.Value(yDim) + size[1] - 1,
	}
	geom := &OrthogSlice{
		shape:    s,
		offset:   offset.Duplicate(),
		size:     size,
		endPoint: offset.Modify(settings),
	}
	return geom, nil
}

func (s *OrthogSlice) SetSize(size Point2d) {
	s.size = size
	xDim := s.shape.shape[0]
	yDim := s.shape.shape[1]
	settings := map[uint8]int32{
		xDim: s.offset.Value(xDim) + size[0] - 1,
		yDim: s.offset.Value(yDim) + size[1] - 1,
	}
	s.endPoint = s.offset.Modify(settings)
}

func (s OrthogSlice) Duplicate() OrthogSlice {
	return OrthogSlice{
		shape:    s.shape.Duplicate(),
		offset:   s.offset.Duplicate(),
		size:     s.size.Duplicate().(Point2d),
		endPoint: s.endPoint.Duplicate(),
	}
}

// --- Geometry interface -----------

func (s OrthogSlice) DataShape() DataShape {
	return s.shape
}

func (s OrthogSlice) Size() Point {
	return s.size
}

func (s OrthogSlice) NumVoxels() int64 {
	return int64(s.size[0] * s.size[1])
}

func (s OrthogSlice) StartPoint() Point {
	return s.offset
}

func (s OrthogSlice) EndPoint() Point {
	return s.endPoint
}

func (s OrthogSlice) String() string {
	return fmt.Sprintf("%s @ offset %s, size %s", s.shape, s.offset, s.size)
}
