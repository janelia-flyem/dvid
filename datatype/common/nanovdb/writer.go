package nanovdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Writer writes NanoVDB grids to binary format.
type Writer struct {
	buf *bytes.Buffer
}

// NewWriter creates a new NanoVDB writer.
func NewWriter() *Writer {
	return &Writer{
		buf: new(bytes.Buffer),
	}
}

// writeContext holds precomputed layout information needed during serialization.
type writeContext struct {
	// Byte offsets from the start of the file to each section
	treeDataStart int
	rootStart     int
	upperStart    int
	lowerStart    int
	leafStart     int

	// Node sizes
	upperNodeSize int
	lowerNodeSize int
	leafNodeSize  int

	// Origin -> sorted index mappings
	leafIndex  map[Coord]int
	lowerIndex map[Coord]int
	upperIndex map[Coord]int
}

// WriteIndexGrid writes an IndexGrid to the internal buffer.
// Returns the number of bytes written.
func (w *Writer) WriteIndexGrid(grid *IndexGrid) (int, error) {
	if grid == nil {
		return 0, fmt.Errorf("grid is nil")
	}

	startLen := w.buf.Len()

	// Calculate node sizes (matching C++ struct layouts exactly)
	leafNodeSize := calcLeafNodeSize()
	lowerNodeSize := calcLowerNodeSize()
	upperNodeSize := calcUpperNodeSize()

	numLeaves := len(grid.LeafNodes)
	numLower := len(grid.LowerNodes)
	numUpper := len(grid.UpperNodes)
	numRootTiles := len(grid.Root.Children)

	rootHeaderSize := calcRootHeaderSize()
	rootTileSize := calcRootTileSize()
	rootNodeSize := rootHeaderSize + numRootTiles*rootTileSize

	// Calculate byte offsets from start of file
	// Layout: [GridData][TreeData][RootData+Tiles][UpperNodes][LowerNodes][LeafNodes]
	treeDataStart := GridDataSize
	rootStart := treeDataStart + TreeDataSize
	upperStart := rootStart + AlignUp32(rootNodeSize)
	lowerStart := upperStart + numUpper*upperNodeSize
	leafStart := lowerStart + numLower*lowerNodeSize
	totalSize := leafStart + numLeaves*leafNodeSize

	// Build origin -> index mappings for child offset computation
	ctx := &writeContext{
		treeDataStart: treeDataStart,
		rootStart:     rootStart,
		upperStart:    upperStart,
		lowerStart:    lowerStart,
		leafStart:     leafStart,
		upperNodeSize: upperNodeSize,
		lowerNodeSize: lowerNodeSize,
		leafNodeSize:  leafNodeSize,
		leafIndex:     make(map[Coord]int, numLeaves),
		lowerIndex:    make(map[Coord]int, numLower),
		upperIndex:    make(map[Coord]int, numUpper),
	}
	for i, leaf := range grid.LeafNodes {
		ctx.leafIndex[leaf.Origin] = i
	}
	for i, lower := range grid.LowerNodes {
		ctx.lowerIndex[lower.Origin] = i
	}
	for i, upper := range grid.UpperNodes {
		ctx.upperIndex[upper.Origin] = i
	}

	// Write GridData header (672 bytes)
	if err := w.writeGridData(grid, uint64(totalSize)); err != nil {
		return 0, fmt.Errorf("writing GridData: %w", err)
	}

	// Write TreeData (64 bytes)
	if err := w.writeTreeData(grid, ctx); err != nil {
		return 0, fmt.Errorf("writing TreeData: %w", err)
	}

	// Write Root node (header + tiles)
	if err := w.writeRootNode(grid.Root, ctx); err != nil {
		return 0, fmt.Errorf("writing RootNode: %w", err)
	}
	w.writePadding(AlignUp32(rootNodeSize) - rootNodeSize)

	// Write Upper nodes
	for i, upper := range grid.UpperNodes {
		if err := w.writeUpperNode(upper, ctx, i); err != nil {
			return 0, fmt.Errorf("writing UpperNode %d: %w", i, err)
		}
	}

	// Write Lower nodes
	for i, lower := range grid.LowerNodes {
		if err := w.writeLowerNode(lower, ctx, i); err != nil {
			return 0, fmt.Errorf("writing LowerNode %d: %w", i, err)
		}
	}

	// Write Leaf nodes
	for _, leaf := range grid.LeafNodes {
		if err := w.writeLeafNode(leaf); err != nil {
			return 0, fmt.Errorf("writing LeafNode: %w", err)
		}
	}

	return w.buf.Len() - startLen, nil
}

// writeGridData writes the 672-byte GridData header
func (w *Writer) writeGridData(grid *IndexGrid, gridSize uint64) error {
	// Prepare grid name
	var nameBytes [MaxNameSize]byte
	copy(nameBytes[:], grid.Name)

	// Create transform map
	mapData := NewIdentityMap(grid.VoxelSize, grid.Origin)

	// World bounding box
	worldBBox := BBox3d{
		Min: Vec3d{
			float64(grid.BBox.Min.X)*grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Min.Y)*grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Min.Z)*grid.VoxelSize.Z + grid.Origin.Z,
		},
		Max: Vec3d{
			float64(grid.BBox.Max.X+1)*grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Max.Y+1)*grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Max.Z+1)*grid.VoxelSize.Z + grid.Origin.Z,
		},
	}

	// Write each field in order (total 672 bytes)
	binary.Write(w.buf, ByteOrder, MagicNumber)              // 8 bytes (0)
	binary.Write(w.buf, ByteOrder, uint64(0))                // 8 bytes (8): checksum
	binary.Write(w.buf, ByteOrder, Version)                  // 4 bytes (16)
	binary.Write(w.buf, ByteOrder, uint32(0))                // 4 bytes (20): flags
	binary.Write(w.buf, ByteOrder, uint32(0))                // 4 bytes (24): grid index
	binary.Write(w.buf, ByteOrder, uint32(1))                // 4 bytes (28): grid count
	binary.Write(w.buf, ByteOrder, gridSize)                 // 8 bytes (32)
	w.buf.Write(nameBytes[:])                                // 256 bytes (40)
	w.writeMap(&mapData)                                     // 264 bytes (296)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.X)          // 48 bytes (560)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Z)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.X)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Z)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.X)         // 24 bytes (608)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Y)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Z)
	binary.Write(w.buf, ByteOrder, uint32(grid.GridClass))   // 4 bytes (632)
	binary.Write(w.buf, ByteOrder, uint32(GridTypeOnIndex))  // 4 bytes (636)
	binary.Write(w.buf, ByteOrder, int64(0))                 // 8 bytes (640): blind metadata offset
	binary.Write(w.buf, ByteOrder, uint32(0))                // 4 bytes (648): blind metadata count
	binary.Write(w.buf, ByteOrder, uint32(0))                // 4 bytes (652): Data0
	binary.Write(w.buf, ByteOrder, grid.ActiveVoxelCount)    // 8 bytes (656): Data1 = voxel count
	binary.Write(w.buf, ByteOrder, uint64(0))                // 8 bytes (664): Data2

	return nil
}

// writeMap writes the 264-byte Map structure
func (w *Writer) writeMap(m *Map) {
	for i := 0; i < 9; i++ {
		binary.Write(w.buf, ByteOrder, m.MatF[i])
	}
	for i := 0; i < 9; i++ {
		binary.Write(w.buf, ByteOrder, m.InvMatF[i])
	}
	for i := 0; i < 3; i++ {
		binary.Write(w.buf, ByteOrder, m.VecF[i])
	}
	binary.Write(w.buf, ByteOrder, m.TaperF)
	for i := 0; i < 9; i++ {
		binary.Write(w.buf, ByteOrder, m.MatD[i])
	}
	for i := 0; i < 9; i++ {
		binary.Write(w.buf, ByteOrder, m.InvMatD[i])
	}
	for i := 0; i < 3; i++ {
		binary.Write(w.buf, ByteOrder, m.VecD[i])
	}
	binary.Write(w.buf, ByteOrder, m.TaperD)
}

// writeTreeData writes the 64-byte TreeData structure
func (w *Writer) writeTreeData(grid *IndexGrid, ctx *writeContext) error {
	// Node offsets: byte offsets from TreeData to each node level
	// mNodeOffset[0] = leaf, [1] = lower, [2] = upper, [3] = root
	binary.Write(w.buf, ByteOrder, int64(ctx.leafStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.lowerStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.upperStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.rootStart-ctx.treeDataStart))

	// Node counts [leaf, lower, upper] (3 × uint32 = 12 bytes)
	binary.Write(w.buf, ByteOrder, uint32(len(grid.LeafNodes)))
	binary.Write(w.buf, ByteOrder, uint32(len(grid.LowerNodes)))
	binary.Write(w.buf, ByteOrder, uint32(len(grid.UpperNodes)))

	// Tile counts [lower, upper, root] (3 × uint32 = 12 bytes)
	// For our IndexGrid, all internal entries are child nodes, not tiles
	binary.Write(w.buf, ByteOrder, uint32(0))
	binary.Write(w.buf, ByteOrder, uint32(0))
	binary.Write(w.buf, ByteOrder, uint32(0))

	// Voxel count (uint64 = 8 bytes)
	binary.Write(w.buf, ByteOrder, grid.ActiveVoxelCount)

	return nil
}

// writeRootNode writes the root node header and tile table.
//
// RootData layout for ValueOnIndex (C++ NanoVDB):
//   CoordBBox mBBox         (24 bytes)
//   uint32_t  mTableSize    (4 bytes)
//   ValueT    mBackground   (8 bytes, uint64 for OnIndex)
//   ValueT    mMinimum      (8 bytes)
//   ValueT    mMaximum      (8 bytes)
//   StatsT    mAverage      (8 bytes, uint64 for OnIndex)
//   StatsT    mStdDevi      (8 bytes)
//   [padding to 32-byte alignment = 28 bytes to reach 96]
//   Tile[mTableSize]        (32 bytes each)
func (w *Writer) writeRootNode(root *IndexRootNode, ctx *writeContext) error {
	// Collect and sort root tile origins (for deterministic output)
	origins := make([]Coord, 0, len(root.Children))
	for origin := range root.Children {
		origins = append(origins, origin)
	}
	sortCoords(origins)

	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Z)

	// mTableSize (4 bytes)
	binary.Write(w.buf, ByteOrder, uint32(len(origins)))

	// mBackground (8 bytes) - 0 for OnIndex
	binary.Write(w.buf, ByteOrder, uint64(0))
	// mMinimum (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))
	// mMaximum (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))
	// mAverage (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))
	// mStdDevi (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))

	// Padding to 32-byte alignment
	// Header so far: 24 + 4 + 5*8 = 68 bytes. Next 32-byte boundary = 96. Pad = 28 bytes.
	w.writePadding(calcRootHeaderSize() - 68)

	// Write tiles (32 bytes each)
	for _, origin := range origins {
		// Key: CoordT (12 bytes)
		binary.Write(w.buf, ByteOrder, origin.X)
		binary.Write(w.buf, ByteOrder, origin.Y)
		binary.Write(w.buf, ByteOrder, origin.Z)

		// child: int64 byte offset from RootData to the upper node
		upperIdx, ok := ctx.upperIndex[origin]
		if !ok {
			return fmt.Errorf("root tile origin %v not found in upper node index", origin)
		}
		childOffset := int64(ctx.upperStart+upperIdx*ctx.upperNodeSize) - int64(ctx.rootStart)
		binary.Write(w.buf, ByteOrder, childOffset)

		// state: uint32 (4 bytes) - 0 for child tiles
		binary.Write(w.buf, ByteOrder, uint32(0))

		// value: ValueT (8 bytes, uint64) - 0 for child tiles
		binary.Write(w.buf, ByteOrder, uint64(0))
	}

	return nil
}

// writeUpperNode writes an upper internal node (32³ = 32768 children).
//
// InternalData layout (C++ NanoVDB):
//   CoordBBox    mBBox         (24 bytes)
//   uint64_t     mFlags        (8 bytes)
//   Mask<5>      mValueMask    (4096 bytes)
//   Mask<5>      mChildMask    (4096 bytes)
//   ValueT       mMinimum      (8 bytes)
//   ValueT       mMaximum      (8 bytes)
//   StatsT       mAverage      (8 bytes)
//   StatsT       mStdDevi      (8 bytes)
//   Tile[32768]  mTable        (32768 × 8 = 262144 bytes)
//   Total: 270400 bytes
func (w *Writer) writeUpperNode(upper *IndexUpperNode, ctx *writeContext, nodeIndex int) error {
	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Z)

	// Flags (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))

	// Value mask (4096 bytes)
	for _, v := range upper.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Child mask (4096 bytes)
	for _, v := range upper.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Min/Max/Avg/StdDev (4 × 8 = 32 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0)) // mMinimum
	binary.Write(w.buf, ByteOrder, uint64(0)) // mMaximum
	binary.Write(w.buf, ByteOrder, uint64(0)) // mAverage
	binary.Write(w.buf, ByteOrder, uint64(0)) // mStdDevi

	// Tile table (32768 × 8 = 262144 bytes)
	// Each entry is either a child byte offset (int64) or a tile value (uint64).
	thisNodeStart := ctx.upperStart + nodeIndex*ctx.upperNodeSize
	for n := 0; n < UpperChildren; n++ {
		if upper.ChildMask.GetBit(n) {
			// This is a child pointer - compute byte offset to the lower node
			childOrigin := upperChildOrigin(upper.Origin, n)
			lowerIdx, ok := ctx.lowerIndex[childOrigin]
			if !ok {
				return fmt.Errorf("upper node child origin %v not found in lower index", childOrigin)
			}
			childOffset := int64(ctx.lowerStart+lowerIdx*ctx.lowerNodeSize) - int64(thisNodeStart)
			binary.Write(w.buf, ByteOrder, childOffset)
		} else {
			// Inactive tile value (0 for OnIndex)
			binary.Write(w.buf, ByteOrder, uint64(0))
		}
	}

	return nil
}

// writeLowerNode writes a lower internal node (16³ = 4096 children).
//
// Same layout pattern as upper node but with Mask<4> (512 bytes each) and 4096 tiles.
// Total: 24 + 8 + 512 + 512 + 4*8 + 4096*8 = 33856 bytes
func (w *Writer) writeLowerNode(lower *IndexLowerNode, ctx *writeContext, nodeIndex int) error {
	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Z)

	// Flags (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))

	// Value mask (512 bytes)
	for _, v := range lower.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Child mask (512 bytes)
	for _, v := range lower.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Min/Max/Avg/StdDev (4 × 8 = 32 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0)) // mMinimum
	binary.Write(w.buf, ByteOrder, uint64(0)) // mMaximum
	binary.Write(w.buf, ByteOrder, uint64(0)) // mAverage
	binary.Write(w.buf, ByteOrder, uint64(0)) // mStdDevi

	// Tile table (4096 × 8 = 32768 bytes)
	thisNodeStart := ctx.lowerStart + nodeIndex*ctx.lowerNodeSize
	for n := 0; n < LowerChildren; n++ {
		if lower.ChildMask.GetBit(n) {
			// Child pointer - byte offset to leaf node
			childOrigin := lowerChildOrigin(lower.Origin, n)
			leafIdx, ok := ctx.leafIndex[childOrigin]
			if !ok {
				return fmt.Errorf("lower node child origin %v not found in leaf index", childOrigin)
			}
			childOffset := int64(ctx.leafStart+leafIdx*ctx.leafNodeSize) - int64(thisNodeStart)
			binary.Write(w.buf, ByteOrder, childOffset)
		} else {
			// Inactive tile value
			binary.Write(w.buf, ByteOrder, uint64(0))
		}
	}

	return nil
}

// writeLeafNode writes a leaf node for OnIndex grid.
//
// LeafIndexBase layout (C++ NanoVDB):
//   CoordT     mBBoxMin     (12 bytes: 3 × int32)
//   uint8_t    mBBoxDif[3]  (3 bytes: max - min per axis, clamped to 255)
//   uint8_t    mFlags       (1 byte)
//   Mask<3>    mValueMask   (64 bytes: 8 × uint64)
//   uint64_t   mOffset      (8 bytes: offset to first value)
//   uint64_t   mPrefixSum   (8 bytes: packed 9-bit prefix sums)
//   Total: 96 bytes
func (w *Writer) writeLeafNode(leaf *IndexLeafNode) error {
	// mBBoxMin (12 bytes)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Z)

	// mBBoxDif (3 bytes) - delta from min to max per axis, clamped to 255
	bboxDifX := clampUint8(leaf.BBox.Max.X - leaf.BBox.Min.X)
	bboxDifY := clampUint8(leaf.BBox.Max.Y - leaf.BBox.Min.Y)
	bboxDifZ := clampUint8(leaf.BBox.Max.Z - leaf.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, bboxDifX)
	binary.Write(w.buf, ByteOrder, bboxDifY)
	binary.Write(w.buf, ByteOrder, bboxDifZ)

	// mFlags (1 byte) - bit4 = has stats
	binary.Write(w.buf, ByteOrder, uint8(0))

	// mValueMask (64 bytes)
	for _, v := range leaf.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// mOffset (8 bytes)
	binary.Write(w.buf, ByteOrder, leaf.Offset)

	// mPrefixSum (8 bytes) - packed running prefix sums for index computation
	prefixSum := computePrefixSum(&leaf.ValueMask)
	binary.Write(w.buf, ByteOrder, prefixSum)

	return nil
}

// computePrefixSum computes the packed 9-bit prefix sum for a Mask512.
//
// The prefix sum encodes running popcounts of the mask words:
//   bits [0:8]   = popcount(word[0])
//   bits [9:17]  = popcount(word[0]) + popcount(word[1])
//   bits [18:26] = sum of popcounts of words[0..2]
//   ...
//   bits [54:62] = sum of popcounts of words[0..6]
//
// This enables O(1) index computation within a leaf node.
func computePrefixSum(mask *Mask512) uint64 {
	var prefixSum uint64
	var runningCount uint64
	for i := 0; i < 7; i++ { // only first 7 words (word 7 is handled separately)
		runningCount += uint64(popcount64(mask[i]))
		prefixSum |= (runningCount & 0x1FF) << (9 * uint(i))
	}
	return prefixSum
}

// clampUint8 clamps an int32 difference to [0, 255]
func clampUint8(v int32) uint8 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}

// upperChildOrigin computes the origin of the child at table position n within an upper node.
func upperChildOrigin(upperOrigin Coord, n int) Coord {
	// Reverse the offset computation: n = z*32*32 + y*32 + x
	x := n & (UpperDim - 1)
	y := (n >> UpperLog2Dim) & (UpperDim - 1)
	z := n >> (UpperLog2Dim + UpperLog2Dim)
	return Coord{
		X: upperOrigin.X + int32(x)*LowerTotalDim,
		Y: upperOrigin.Y + int32(y)*LowerTotalDim,
		Z: upperOrigin.Z + int32(z)*LowerTotalDim,
	}
}

// lowerChildOrigin computes the origin of the child at table position n within a lower node.
func lowerChildOrigin(lowerOrigin Coord, n int) Coord {
	// Reverse the offset computation: n = z*16*16 + y*16 + x
	x := n & (LowerDim - 1)
	y := (n >> LowerLog2Dim) & (LowerDim - 1)
	z := n >> (LowerLog2Dim + LowerLog2Dim)
	return Coord{
		X: lowerOrigin.X + int32(x)*LeafTotalDim,
		Y: lowerOrigin.Y + int32(y)*LeafTotalDim,
		Z: lowerOrigin.Z + int32(z)*LeafTotalDim,
	}
}

// Size calculation helpers - must match C++ sizeof() exactly

// calcLeafNodeSize returns 96 bytes (LeafIndexBase for ValueOnIndex)
func calcLeafNodeSize() int {
	// mBBoxMin(12) + mBBoxDif(3) + mFlags(1) + mValueMask(64) + mOffset(8) + mPrefixSum(8) = 96
	return 96
}

// calcLowerNodeSize returns the size of a lower internal node (LOG2DIM=4)
func calcLowerNodeSize() int {
	// BBox(24) + Flags(8) + ValueMask(512) + ChildMask(512) +
	// Min(8) + Max(8) + Avg(8) + StdDev(8) + Table(4096×8)
	return 24 + 8 + 512 + 512 + 4*8 + LowerChildren*8
	// = 24 + 8 + 512 + 512 + 32 + 32768 = 33856
}

// calcUpperNodeSize returns the size of an upper internal node (LOG2DIM=5)
func calcUpperNodeSize() int {
	// BBox(24) + Flags(8) + ValueMask(4096) + ChildMask(4096) +
	// Min(8) + Max(8) + Avg(8) + StdDev(8) + Table(32768×8)
	return 24 + 8 + 4096 + 4096 + 4*8 + UpperChildren*8
	// = 24 + 8 + 4096 + 4096 + 32 + 262144 = 270400
}

// calcRootHeaderSize returns the root node header size (padded to 32-byte alignment)
func calcRootHeaderSize() int {
	// BBox(24) + TableSize(4) + Background(8) + Min(8) + Max(8) + Avg(8) + StdDev(8) = 68
	// Padded to next 32-byte boundary = 96
	return AlignUp32(24 + 4 + 5*8)
}

// calcRootTileSize returns the size of a single root tile
func calcRootTileSize() int {
	// Key(12) + child(8) + state(4) + value(8) = 32
	return 32
}

// writePadding writes padding bytes
func (w *Writer) writePadding(n int) {
	if n > 0 {
		w.buf.Write(make([]byte, n))
	}
}

// Bytes returns the serialized data as a byte slice.
func (w *Writer) Bytes() []byte {
	return w.buf.Bytes()
}

// WriteTo writes the serialized data to an io.Writer.
func (w *Writer) WriteTo(writer io.Writer) (int64, error) {
	n, err := writer.Write(w.buf.Bytes())
	return int64(n), err
}

// WriteToFile writes the serialized data to a file.
func (w *Writer) WriteToFile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()

	_, err = w.WriteTo(f)
	if err != nil {
		return fmt.Errorf("writing to file: %w", err)
	}

	return nil
}

// Reset clears the internal buffer for reuse.
func (w *Writer) Reset() {
	w.buf.Reset()
}

// sortCoords sorts coordinates in Z, Y, X order
func sortCoords(coords []Coord) {
	for i := 0; i < len(coords)-1; i++ {
		for j := i + 1; j < len(coords); j++ {
			if coordLess(coords[j], coords[i]) {
				coords[i], coords[j] = coords[j], coords[i]
			}
		}
	}
}

// WriteIndexGridToFile is a convenience function that builds and writes an IndexGrid.
func WriteIndexGridToFile(filename string, name string, voxels []Coord, voxelSize Vec3d) error {
	builder := NewIndexGridBuilder(name)
	builder.SetVoxelSize(voxelSize)
	builder.AddVoxels(voxels)

	grid := builder.Build()
	if grid == nil {
		return fmt.Errorf("failed to build grid (no voxels?)")
	}

	writer := NewWriter()
	if _, err := writer.WriteIndexGrid(grid); err != nil {
		return fmt.Errorf("serializing grid: %w", err)
	}

	return writer.WriteToFile(filename)
}

// ============================================================================
// Int64Grid Writer Methods
// ============================================================================

// int64WriteContext holds precomputed layout information for Int64Grid serialization.
type int64WriteContext struct {
	treeDataStart int
	rootStart     int
	upperStart    int
	lowerStart    int
	leafStart     int

	upperNodeSize int
	lowerNodeSize int
	leafNodeSize  int

	leafIndex  map[Coord]int
	lowerIndex map[Coord]int
	upperIndex map[Coord]int
}

// WriteInt64Grid writes an Int64Grid to the internal buffer.
// Returns the number of bytes written.
func (w *Writer) WriteInt64Grid(grid *Int64Grid) (int, error) {
	if grid == nil {
		return 0, fmt.Errorf("grid is nil")
	}

	startLen := w.buf.Len()

	// Calculate node sizes
	leafNodeSize := calcInt64LeafNodeSize()
	lowerNodeSize := calcLowerNodeSize() // Same as IndexGrid
	upperNodeSize := calcUpperNodeSize() // Same as IndexGrid

	numLeaves := len(grid.LeafNodes)
	numLower := len(grid.LowerNodes)
	numUpper := len(grid.UpperNodes)
	numRootTiles := len(grid.Root.Children)

	rootHeaderSize := calcRootHeaderSize()
	rootTileSize := calcRootTileSize()
	rootNodeSize := rootHeaderSize + numRootTiles*rootTileSize

	// Calculate byte offsets from start of file
	treeDataStart := GridDataSize
	rootStart := treeDataStart + TreeDataSize
	upperStart := rootStart + AlignUp32(rootNodeSize)
	lowerStart := upperStart + numUpper*upperNodeSize
	leafStart := lowerStart + numLower*lowerNodeSize
	totalSize := leafStart + numLeaves*leafNodeSize

	// Build origin -> index mappings
	ctx := &int64WriteContext{
		treeDataStart: treeDataStart,
		rootStart:     rootStart,
		upperStart:    upperStart,
		lowerStart:    lowerStart,
		leafStart:     leafStart,
		upperNodeSize: upperNodeSize,
		lowerNodeSize: lowerNodeSize,
		leafNodeSize:  leafNodeSize,
		leafIndex:     make(map[Coord]int, numLeaves),
		lowerIndex:    make(map[Coord]int, numLower),
		upperIndex:    make(map[Coord]int, numUpper),
	}
	for i, leaf := range grid.LeafNodes {
		ctx.leafIndex[leaf.Origin] = i
	}
	for i, lower := range grid.LowerNodes {
		ctx.lowerIndex[lower.Origin] = i
	}
	for i, upper := range grid.UpperNodes {
		ctx.upperIndex[upper.Origin] = i
	}

	// Write GridData header
	if err := w.writeInt64GridData(grid, uint64(totalSize)); err != nil {
		return 0, fmt.Errorf("writing GridData: %w", err)
	}

	// Write TreeData
	if err := w.writeInt64TreeData(grid, ctx); err != nil {
		return 0, fmt.Errorf("writing TreeData: %w", err)
	}

	// Write Root node
	if err := w.writeInt64RootNode(grid.Root, ctx); err != nil {
		return 0, fmt.Errorf("writing RootNode: %w", err)
	}
	w.writePadding(AlignUp32(rootNodeSize) - rootNodeSize)

	// Write Upper nodes
	for i, upper := range grid.UpperNodes {
		if err := w.writeInt64UpperNode(upper, ctx, i); err != nil {
			return 0, fmt.Errorf("writing UpperNode %d: %w", i, err)
		}
	}

	// Write Lower nodes
	for i, lower := range grid.LowerNodes {
		if err := w.writeInt64LowerNode(lower, ctx, i); err != nil {
			return 0, fmt.Errorf("writing LowerNode %d: %w", i, err)
		}
	}

	// Write Leaf nodes
	for _, leaf := range grid.LeafNodes {
		if err := w.writeInt64LeafNode(leaf); err != nil {
			return 0, fmt.Errorf("writing LeafNode: %w", err)
		}
	}

	return w.buf.Len() - startLen, nil
}

// writeInt64GridData writes the 672-byte GridData header for Int64 grid
func (w *Writer) writeInt64GridData(grid *Int64Grid, gridSize uint64) error {
	var nameBytes [MaxNameSize]byte
	copy(nameBytes[:], grid.Name)

	mapData := NewIdentityMap(grid.VoxelSize, grid.Origin)

	worldBBox := BBox3d{
		Min: Vec3d{
			float64(grid.BBox.Min.X)*grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Min.Y)*grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Min.Z)*grid.VoxelSize.Z + grid.Origin.Z,
		},
		Max: Vec3d{
			float64(grid.BBox.Max.X+1)*grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Max.Y+1)*grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Max.Z+1)*grid.VoxelSize.Z + grid.Origin.Z,
		},
	}

	binary.Write(w.buf, ByteOrder, MagicNumber)
	binary.Write(w.buf, ByteOrder, uint64(0)) // checksum
	binary.Write(w.buf, ByteOrder, Version)
	binary.Write(w.buf, ByteOrder, uint32(0)) // flags
	binary.Write(w.buf, ByteOrder, uint32(0)) // grid index
	binary.Write(w.buf, ByteOrder, uint32(1)) // grid count
	binary.Write(w.buf, ByteOrder, gridSize)
	w.buf.Write(nameBytes[:])
	w.writeMap(&mapData)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.X)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Z)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.X)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Z)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.X)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Y)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Z)
	binary.Write(w.buf, ByteOrder, uint32(grid.GridClass))
	binary.Write(w.buf, ByteOrder, uint32(GridTypeInt64)) // GridType = Int64
	binary.Write(w.buf, ByteOrder, int64(0))              // blind metadata offset
	binary.Write(w.buf, ByteOrder, uint32(0))             // blind metadata count
	binary.Write(w.buf, ByteOrder, uint32(0))             // Data0
	binary.Write(w.buf, ByteOrder, grid.ActiveVoxelCount) // Data1
	binary.Write(w.buf, ByteOrder, uint64(0))             // Data2

	return nil
}

// writeInt64TreeData writes the 64-byte TreeData structure for Int64 grid
func (w *Writer) writeInt64TreeData(grid *Int64Grid, ctx *int64WriteContext) error {
	binary.Write(w.buf, ByteOrder, int64(ctx.leafStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.lowerStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.upperStart-ctx.treeDataStart))
	binary.Write(w.buf, ByteOrder, int64(ctx.rootStart-ctx.treeDataStart))

	binary.Write(w.buf, ByteOrder, uint32(len(grid.LeafNodes)))
	binary.Write(w.buf, ByteOrder, uint32(len(grid.LowerNodes)))
	binary.Write(w.buf, ByteOrder, uint32(len(grid.UpperNodes)))

	binary.Write(w.buf, ByteOrder, uint32(0)) // tile counts
	binary.Write(w.buf, ByteOrder, uint32(0))
	binary.Write(w.buf, ByteOrder, uint32(0))

	binary.Write(w.buf, ByteOrder, grid.ActiveVoxelCount)

	return nil
}

// writeInt64RootNode writes the root node for Int64 grid
func (w *Writer) writeInt64RootNode(root *Int64RootNode, ctx *int64WriteContext) error {
	origins := make([]Coord, 0, len(root.Children))
	for origin := range root.Children {
		origins = append(origins, origin)
	}
	sortCoords(origins)

	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Z)

	// mTableSize (4 bytes)
	binary.Write(w.buf, ByteOrder, uint32(len(origins)))

	// mBackground (8 bytes) - int64 for Int64 grids
	binary.Write(w.buf, ByteOrder, root.Background)
	// mMinimum (8 bytes)
	binary.Write(w.buf, ByteOrder, root.Minimum)
	// mMaximum (8 bytes)
	binary.Write(w.buf, ByteOrder, root.Maximum)
	// mAverage (8 bytes)
	binary.Write(w.buf, ByteOrder, root.Average)
	// mStdDevi (8 bytes)
	binary.Write(w.buf, ByteOrder, root.StdDevi)

	// Padding to 32-byte alignment
	w.writePadding(calcRootHeaderSize() - 68)

	// Write tiles (32 bytes each)
	for _, origin := range origins {
		binary.Write(w.buf, ByteOrder, origin.X)
		binary.Write(w.buf, ByteOrder, origin.Y)
		binary.Write(w.buf, ByteOrder, origin.Z)

		upperIdx, ok := ctx.upperIndex[origin]
		if !ok {
			return fmt.Errorf("root tile origin %v not found in upper node index", origin)
		}
		childOffset := int64(ctx.upperStart+upperIdx*ctx.upperNodeSize) - int64(ctx.rootStart)
		binary.Write(w.buf, ByteOrder, childOffset)

		binary.Write(w.buf, ByteOrder, uint32(0)) // state
		binary.Write(w.buf, ByteOrder, int64(0))  // tile value (not used for child)
	}

	return nil
}

// writeInt64UpperNode writes an upper internal node for Int64 grid
func (w *Writer) writeInt64UpperNode(upper *Int64UpperNode, ctx *int64WriteContext, nodeIndex int) error {
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Z)

	binary.Write(w.buf, ByteOrder, uint64(0)) // flags

	for _, v := range upper.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}
	for _, v := range upper.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	binary.Write(w.buf, ByteOrder, upper.Minimum)
	binary.Write(w.buf, ByteOrder, upper.Maximum)
	binary.Write(w.buf, ByteOrder, upper.Average)
	binary.Write(w.buf, ByteOrder, upper.StdDevi)

	thisNodeStart := ctx.upperStart + nodeIndex*ctx.upperNodeSize
	for n := 0; n < UpperChildren; n++ {
		if upper.ChildMask.GetBit(n) {
			childOrigin := upperChildOrigin(upper.Origin, n)
			lowerIdx, ok := ctx.lowerIndex[childOrigin]
			if !ok {
				return fmt.Errorf("upper node child origin %v not found in lower index", childOrigin)
			}
			childOffset := int64(ctx.lowerStart+lowerIdx*ctx.lowerNodeSize) - int64(thisNodeStart)
			binary.Write(w.buf, ByteOrder, childOffset)
		} else {
			binary.Write(w.buf, ByteOrder, int64(0)) // tile value
		}
	}

	return nil
}

// writeInt64LowerNode writes a lower internal node for Int64 grid
func (w *Writer) writeInt64LowerNode(lower *Int64LowerNode, ctx *int64WriteContext, nodeIndex int) error {
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Z)

	binary.Write(w.buf, ByteOrder, uint64(0)) // flags

	for _, v := range lower.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}
	for _, v := range lower.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	binary.Write(w.buf, ByteOrder, lower.Minimum)
	binary.Write(w.buf, ByteOrder, lower.Maximum)
	binary.Write(w.buf, ByteOrder, lower.Average)
	binary.Write(w.buf, ByteOrder, lower.StdDevi)

	thisNodeStart := ctx.lowerStart + nodeIndex*ctx.lowerNodeSize
	for n := 0; n < LowerChildren; n++ {
		if lower.ChildMask.GetBit(n) {
			childOrigin := lowerChildOrigin(lower.Origin, n)
			leafIdx, ok := ctx.leafIndex[childOrigin]
			if !ok {
				return fmt.Errorf("lower node child origin %v not found in leaf index", childOrigin)
			}
			childOffset := int64(ctx.leafStart+leafIdx*ctx.leafNodeSize) - int64(thisNodeStart)
			binary.Write(w.buf, ByteOrder, childOffset)
		} else {
			binary.Write(w.buf, ByteOrder, int64(0)) // tile value
		}
	}

	return nil
}

// writeInt64LeafNode writes a leaf node for Int64 grid.
//
// Binary layout (4224 bytes) - must match C++ LeafData<int64_t>:
//
//	Offset 0:    mBBoxMin    (12 bytes) - 3 × int32
//	Offset 12:   mBBoxDif    (3 bytes)  - delta from min to max per axis
//	Offset 15:   mFlags      (1 byte)   - bit flags
//	Offset 16:   mValueMask  (64 bytes) - 8 × uint64
//	Offset 80:   mMinimum    (8 bytes)  - int64
//	Offset 88:   mMaximum    (8 bytes)  - int64
//	Offset 96:   mAverage    (8 bytes)  - float64
//	Offset 104:  mStdDevi    (8 bytes)  - float64
//	Offset 112:  padding     (16 bytes) - alignas(32) before mValues
//	Offset 128:  mValues     (4096 bytes) - 512 × int64
//	Total: 4224 bytes
func (w *Writer) writeInt64LeafNode(leaf *Int64LeafNode) error {
	// mBBoxMin (12 bytes) - offset 0
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Z)

	// mBBoxDif (3 bytes) - offset 12
	bboxDifX := clampUint8(leaf.BBox.Max.X - leaf.BBox.Min.X)
	bboxDifY := clampUint8(leaf.BBox.Max.Y - leaf.BBox.Min.Y)
	bboxDifZ := clampUint8(leaf.BBox.Max.Z - leaf.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, bboxDifX)
	binary.Write(w.buf, ByteOrder, bboxDifY)
	binary.Write(w.buf, ByteOrder, bboxDifZ)

	// mFlags (1 byte) - offset 15, bit4 = has stats
	binary.Write(w.buf, ByteOrder, uint8(0x10))

	// mValueMask (64 bytes) - offset 16
	for _, v := range leaf.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Statistics (32 bytes) - offset 80
	binary.Write(w.buf, ByteOrder, leaf.Minimum)
	binary.Write(w.buf, ByteOrder, leaf.Maximum)
	binary.Write(w.buf, ByteOrder, leaf.Average)
	binary.Write(w.buf, ByteOrder, leaf.StdDevi)

	// Padding (16 bytes) - offset 112, to align mValues to 32-byte boundary
	w.writePadding(16)

	// mValues (4096 bytes) - offset 128
	for _, v := range leaf.Values {
		binary.Write(w.buf, ByteOrder, v)
	}

	return nil
}

// calcInt64LeafNodeSize returns 4224 bytes (LeafData for int64_t values)
func calcInt64LeafNodeSize() int {
	// mBBoxMin(12) + mBBoxDif(3) + mFlags(1) + mValueMask(64) +
	// mMinimum(8) + mMaximum(8) + mAverage(8) + mStdDevi(8) +
	// mValues(512*8=4096) + padding(16) = 4224
	return 4224
}

// WriteInt64GridToFile is a convenience function that builds and writes an Int64Grid.
func WriteInt64GridToFile(filename string, name string, voxels []Coord, values []int64, voxelSize Vec3d) error {
	if len(voxels) != len(values) {
		return fmt.Errorf("voxels and values must have same length: %d vs %d", len(voxels), len(values))
	}

	builder := NewInt64GridBuilder(name)
	builder.SetVoxelSize(voxelSize)

	for i, coord := range voxels {
		builder.AddVoxel(coord.X, coord.Y, coord.Z, values[i])
	}

	grid := builder.Build()
	if grid == nil {
		return fmt.Errorf("failed to build grid (no voxels?)")
	}

	writer := NewWriter()
	if _, err := writer.WriteInt64Grid(grid); err != nil {
		return fmt.Errorf("serializing grid: %w", err)
	}

	return writer.WriteToFile(filename)
}
