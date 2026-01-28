// Package nanovdb provides a pure Go implementation for writing NanoVDB IndexGrid files.
//
// NanoVDB is a lightweight, GPU-friendly sparse volume data structure. This package
// implements the binary format for IndexGrid (OnIndex) grids, which store only the
// topology (active voxel positions) without values.
//
// The implementation is based on NanoVDB version 32.8.0 from the OpenVDB project:
// https://github.com/AcademySoftwareFoundation/openvdb/blob/master/nanovdb/nanovdb/NanoVDB.h
//
// Key format details:
//   - All data is little-endian
//   - All major structures are 32-byte aligned
//   - Tree structure: Root -> Upper (32³) -> Lower (16³) -> Leaf (8³)
//   - For IndexGrid, leaf nodes store only active state bitmasks, not values
package nanovdb

import (
	"encoding/binary"
	"math"
)

// Format constants from NanoVDB.h
const (
	// Magic numbers for NanoVDB format identification
	MagicNumber uint64 = 0x304244566f6e614e // "NanoVDB0" in little-endian
	MagicGrid   uint64 = 0x314244566f6e614e // "NanoVDB1"
	MagicFile   uint64 = 0x324244566f6e614e // "NanoVDB2"

	// Version: 32.8.0 packed as (major << 21) | (minor << 10) | patch
	// major=32, minor=8, patch=0
	Version uint32 = (32 << 21) | (8 << 10) | 0

	// Memory alignment requirement
	Alignment = 32

	// Node dimensions (log2)
	LeafLog2Dim  = 3 // 8³ = 512 voxels per leaf
	LowerLog2Dim = 4 // 16³ = 4096 children per lower internal node
	UpperLog2Dim = 5 // 32³ = 32768 children per upper internal node

	// Node dimensions
	LeafDim  = 1 << LeafLog2Dim  // 8
	LowerDim = 1 << LowerLog2Dim // 16
	UpperDim = 1 << UpperLog2Dim // 32

	// Total voxels spanned by each node type
	LeafTotalDim  = LeafDim                          // 8 voxels
	LowerTotalDim = LowerDim * LeafTotalDim          // 128 voxels
	UpperTotalDim = UpperDim * LowerTotalDim         // 4096 voxels

	// Number of children/values per node
	LeafValues   = LeafDim * LeafDim * LeafDim    // 512
	LowerChildren = LowerDim * LowerDim * LowerDim // 4096
	UpperChildren = UpperDim * UpperDim * UpperDim // 32768

	// Structure sizes (must match C++ struct layouts exactly)
	GridDataSize          = 672
	TreeDataSize          = 64
	MapSize               = 264
	GridBlindMetaDataSize = 288

	// Maximum grid name length
	MaxNameSize = 256
)

// GridType enum values from NanoVDB.h
type GridType uint32

const (
	GridTypeUnknown    GridType = 0
	GridTypeFloat      GridType = 1
	GridTypeDouble     GridType = 2
	GridTypeInt16      GridType = 3
	GridTypeInt32      GridType = 4
	GridTypeInt64      GridType = 5
	GridTypeVec3f      GridType = 6
	GridTypeVec3d      GridType = 7
	GridTypeMask       GridType = 8  // No value, just active state
	GridTypeHalf       GridType = 9  // IEEE 754 half-precision float
	GridTypeUInt32     GridType = 10
	GridTypeBoolean    GridType = 11 // Boolean value, encoded in bit array
	GridTypeIndex      GridType = 19 // Index into external array (active + inactive)
	GridTypeOnIndex    GridType = 20 // Index into external array (active only)
	GridTypeIndexMask  GridType = 21 // Index with mutable mask
	GridTypeOnIndexMask GridType = 22 // OnIndex with mutable mask
)

// GridClass enum values from NanoVDB.h
type GridClass uint32

const (
	GridClassUnknown    GridClass = 0
	GridClassLevelSet   GridClass = 1
	GridClassFogVolume  GridClass = 2
	GridClassStaggered  GridClass = 3
	GridClassPointIndex GridClass = 4
	GridClassPointData  GridClass = 5
	GridClassTopology   GridClass = 6
	GridClassVoxelVolume GridClass = 7
	GridClassIndexGrid  GridClass = 8 // Grid whose values are offsets into external array
	GridClassTensorGrid GridClass = 9 // Index grid for learnable tensor features (fVDB)
)

// Coord represents a 3D integer coordinate (matches nanovdb::Coord)
type Coord struct {
	X, Y, Z int32
}

// CoordBBox represents a bounding box of coordinates
type CoordBBox struct {
	Min, Max Coord
}

// Vec3d represents a 3D double-precision vector
type Vec3d struct {
	X, Y, Z float64
}

// Vec3f represents a 3D single-precision vector
type Vec3f struct {
	X, Y, Z float32
}

// BBox3d represents a bounding box with double-precision coordinates
type BBox3d struct {
	Min, Max Vec3d
}

// Map represents the affine transformation from index to world space.
// Size: 264 bytes (must match C++ nanovdb::Map exactly)
type Map struct {
	MatF    [9]float32  // 36 bytes: forward 3x3 matrix (single precision)
	InvMatF [9]float32  // 36 bytes: inverse 3x3 matrix (single precision)
	VecF    [3]float32  // 12 bytes: translation (single precision)
	TaperF  float32     // 4 bytes: taper value (single precision)
	MatD    [9]float64  // 72 bytes: forward 3x3 matrix (double precision)
	InvMatD [9]float64  // 72 bytes: inverse 3x3 matrix (double precision)
	VecD    [3]float64  // 24 bytes: translation (double precision)
	TaperD  float64     // 8 bytes: taper value (double precision)
}

// NewIdentityMap creates a Map with identity transform and given voxel size and origin.
func NewIdentityMap(voxelSize Vec3d, origin Vec3d) Map {
	m := Map{
		TaperF: 1.0,
		TaperD: 1.0,
	}

	// Forward matrix: scale by voxel size
	m.MatF[0] = float32(voxelSize.X)
	m.MatF[4] = float32(voxelSize.Y)
	m.MatF[8] = float32(voxelSize.Z)

	m.MatD[0] = voxelSize.X
	m.MatD[4] = voxelSize.Y
	m.MatD[8] = voxelSize.Z

	// Inverse matrix: scale by 1/voxelSize
	if voxelSize.X != 0 {
		m.InvMatF[0] = float32(1.0 / voxelSize.X)
		m.InvMatD[0] = 1.0 / voxelSize.X
	}
	if voxelSize.Y != 0 {
		m.InvMatF[4] = float32(1.0 / voxelSize.Y)
		m.InvMatD[4] = 1.0 / voxelSize.Y
	}
	if voxelSize.Z != 0 {
		m.InvMatF[8] = float32(1.0 / voxelSize.Z)
		m.InvMatD[8] = 1.0 / voxelSize.Z
	}

	// Translation (origin)
	m.VecF[0] = float32(origin.X)
	m.VecF[1] = float32(origin.Y)
	m.VecF[2] = float32(origin.Z)

	m.VecD[0] = origin.X
	m.VecD[1] = origin.Y
	m.VecD[2] = origin.Z

	return m
}

// GridData is the main header structure for a NanoVDB grid.
// Size: 672 bytes, 32-byte aligned
// This must match the C++ nanovdb::GridData struct exactly.
type GridData struct {
	Magic               uint64      // 8 bytes: magic number
	Checksum            [2]uint32   // 8 bytes: CRC32 checksums [head, tail]
	Version             uint32      // 4 bytes: version (packed major.minor.patch)
	Flags               uint32      // 4 bytes: bit flags
	GridIndex           uint32      // 4 bytes: index of this grid in file
	GridCount           uint32      // 4 bytes: total number of grids in file
	GridSize            uint64      // 8 bytes: size of this grid in bytes
	GridName            [256]byte   // 256 bytes: null-terminated grid name
	Map                 Map         // 264 bytes: transform
	WorldBBox           BBox3d      // 48 bytes: world-space bounding box
	VoxelSize           Vec3d       // 24 bytes: voxel dimensions
	GridClass           uint32      // 4 bytes: GridClass enum
	GridType            uint32      // 4 bytes: GridType enum
	BlindMetadataOffset int64       // 8 bytes: offset to blind metadata from grid start
	BlindMetadataCount  uint32      // 4 bytes: number of blind metadata entries
	Data0               uint32      // 4 bytes: padding/reserved
	Data1               uint64      // 8 bytes: voxel count for index grids
	Data2               uint64      // 8 bytes: padding/reserved
}

// TreeData contains metadata about the tree structure.
// Size: 64 bytes, immediately follows GridData
type TreeData struct {
	NodeOffset [4]int64  // 32 bytes: byte offsets from TreeData to [leaf, lower, upper, root]
	NodeCount  [3]uint32 // 12 bytes: number of nodes [leaf, lower, upper]
	TileCount  [3]uint32 // 12 bytes: active tile counts [lower, upper, root]
	VoxelCount uint64    // 8 bytes: total active voxels
}

// GridBlindMetaData describes blind (opaque) data attached to a grid.
// Size: 288 bytes, 32-byte aligned
type GridBlindMetaData struct {
	DataOffset int64     // 8 bytes: offset from this struct to blind data
	ValueCount uint64    // 8 bytes: number of values (or bytes for raw data)
	ValueSize  uint32    // 4 bytes: size of each value in bytes
	Semantic   uint32    // 4 bytes: semantic hint
	DataClass  uint32    // 4 bytes: data class
	DataType   uint32    // 4 bytes: data type
	Name       [256]byte // 256 bytes: null-terminated name
}

// Mask64 represents a 64-bit bitmask for 64 elements
type Mask64 uint64

// Mask512 represents a bitmask for 512 elements (8 x uint64 = 512 bits)
// Used for leaf nodes (8³ = 512 voxels)
type Mask512 [8]uint64

// Mask4096 represents a bitmask for 4096 elements (64 x uint64 = 4096 bits)
// Used for lower internal nodes (16³ = 4096 children)
type Mask4096 [64]uint64

// Mask32768 represents a bitmask for 32768 elements (512 x uint64 = 32768 bits)
// Used for upper internal nodes (32³ = 32768 children)
type Mask32768 [512]uint64

// SetBit sets the bit at position i in a Mask512
func (m *Mask512) SetBit(i int) {
	if i >= 0 && i < 512 {
		m[i>>6] |= 1 << (i & 63)
	}
}

// GetBit returns true if bit at position i is set
func (m *Mask512) GetBit(i int) bool {
	if i >= 0 && i < 512 {
		return (m[i>>6] & (1 << (i & 63))) != 0
	}
	return false
}

// CountOn returns the number of set bits (popcount)
func (m *Mask512) CountOn() int {
	count := 0
	for _, v := range m {
		count += popcount64(v)
	}
	return count
}

// SetBit sets the bit at position i in a Mask4096
func (m *Mask4096) SetBit(i int) {
	if i >= 0 && i < 4096 {
		m[i>>6] |= 1 << (i & 63)
	}
}

// GetBit returns true if bit at position i is set
func (m *Mask4096) GetBit(i int) bool {
	if i >= 0 && i < 4096 {
		return (m[i>>6] & (1 << (i & 63))) != 0
	}
	return false
}

// CountOn returns the number of set bits
func (m *Mask4096) CountOn() int {
	count := 0
	for _, v := range m {
		count += popcount64(v)
	}
	return count
}

// SetBit sets the bit at position i in a Mask32768
func (m *Mask32768) SetBit(i int) {
	if i >= 0 && i < 32768 {
		m[i>>6] |= 1 << (i & 63)
	}
}

// GetBit returns true if bit at position i is set
func (m *Mask32768) GetBit(i int) bool {
	if i >= 0 && i < 32768 {
		return (m[i>>6] & (1 << (i & 63))) != 0
	}
	return false
}

// CountOn returns the number of set bits
func (m *Mask32768) CountOn() int {
	count := 0
	for _, v := range m {
		count += popcount64(v)
	}
	return count
}

// popcount64 counts the number of set bits in a uint64
func popcount64(x uint64) int {
	// Using the standard bit counting algorithm
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	x = x + (x >> 8)
	x = x + (x >> 16)
	x = x + (x >> 32)
	return int(x & 0x7f)
}

// alignUp rounds size up to the nearest multiple of alignment
func alignUp(size, alignment int) int {
	return (size + alignment - 1) &^ (alignment - 1)
}

// AlignUp32 rounds size up to 32-byte alignment
func AlignUp32(size int) int {
	return alignUp(size, Alignment)
}

// ByteOrder is the byte order used by NanoVDB (little-endian)
var ByteOrder = binary.LittleEndian

// coordToLeafOffset converts local coordinates within a leaf (0-7 each) to linear offset
func coordToLeafOffset(x, y, z int) int {
	return (z << (LeafLog2Dim + LeafLog2Dim)) | (y << LeafLog2Dim) | x
}

// coordToLowerOffset converts local coordinates within a lower node (0-15 each) to linear offset
func coordToLowerOffset(x, y, z int) int {
	return (z << (LowerLog2Dim + LowerLog2Dim)) | (y << LowerLog2Dim) | x
}

// coordToUpperOffset converts local coordinates within an upper node (0-31 each) to linear offset
func coordToUpperOffset(x, y, z int) int {
	return (z << (UpperLog2Dim + UpperLog2Dim)) | (y << UpperLog2Dim) | x
}

// leafOrigin returns the origin coordinate of the leaf containing the given voxel
func leafOrigin(x, y, z int32) Coord {
	mask := int32(^(LeafDim - 1))
	return Coord{x & mask, y & mask, z & mask}
}

// lowerOrigin returns the origin coordinate of the lower node containing the given voxel
func lowerOrigin(x, y, z int32) Coord {
	mask := int32(^(LowerTotalDim - 1))
	return Coord{x & mask, y & mask, z & mask}
}

// upperOrigin returns the origin coordinate of the upper node containing the given voxel
func upperOrigin(x, y, z int32) Coord {
	mask := int32(^(UpperTotalDim - 1))
	return Coord{x & mask, y & mask, z & mask}
}

// MinInt32 returns the minimum of two int32 values
func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// MaxInt32 returns the maximum of two int32 values
func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// ExpandBBox expands the bounding box to include the given coordinate
func (bbox *CoordBBox) ExpandBBox(c Coord) {
	if c.X < bbox.Min.X {
		bbox.Min.X = c.X
	}
	if c.Y < bbox.Min.Y {
		bbox.Min.Y = c.Y
	}
	if c.Z < bbox.Min.Z {
		bbox.Min.Z = c.Z
	}
	if c.X > bbox.Max.X {
		bbox.Max.X = c.X
	}
	if c.Y > bbox.Max.Y {
		bbox.Max.Y = c.Y
	}
	if c.Z > bbox.Max.Z {
		bbox.Max.Z = c.Z
	}
}

// NewEmptyBBox creates a bounding box initialized to "empty" state
func NewEmptyBBox() CoordBBox {
	return CoordBBox{
		Min: Coord{math.MaxInt32, math.MaxInt32, math.MaxInt32},
		Max: Coord{math.MinInt32, math.MinInt32, math.MinInt32},
	}
}

// IsEmpty returns true if the bounding box is empty
func (bbox *CoordBBox) IsEmpty() bool {
	return bbox.Min.X > bbox.Max.X || bbox.Min.Y > bbox.Max.Y || bbox.Min.Z > bbox.Max.Z
}
