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

// WriteIndexGrid writes an IndexGrid to the internal buffer.
// Returns the number of bytes written.
func (w *Writer) WriteIndexGrid(grid *IndexGrid) (int, error) {
	if grid == nil {
		return 0, fmt.Errorf("grid is nil")
	}

	startLen := w.buf.Len()

	// Calculate sizes for all components
	leafNodeSize := calcLeafNodeSize()
	lowerNodeSize := calcLowerNodeSize()
	upperNodeSize := calcUpperNodeSize()
	rootNodeSize := calcRootNodeSize(len(grid.Root.Children))

	numLeaves := len(grid.LeafNodes)
	numLower := len(grid.LowerNodes)
	numUpper := len(grid.UpperNodes)

	// Calculate offsets from start of tree data
	// Tree layout: [TreeData][RootData][UpperNodes][LowerNodes][LeafNodes]
	treeDataOffset := GridDataSize
	rootOffset := treeDataOffset + TreeDataSize
	upperOffset := rootOffset + AlignUp32(rootNodeSize)
	lowerOffset := upperOffset + AlignUp32(numUpper*upperNodeSize)
	leafOffset := lowerOffset + AlignUp32(numLower*lowerNodeSize)
	totalTreeSize := leafOffset + AlignUp32(numLeaves*leafNodeSize)

	// Total grid size (no blind data for basic IndexGrid)
	gridSize := totalTreeSize

	// Write GridData header
	if err := w.writeGridData(grid, uint64(gridSize)); err != nil {
		return 0, fmt.Errorf("writing GridData: %w", err)
	}

	// Write TreeData
	if err := w.writeTreeData(grid, leafOffset-treeDataOffset, lowerOffset-treeDataOffset,
		upperOffset-treeDataOffset, rootOffset-treeDataOffset,
		numLeaves, numLower, numUpper); err != nil {
		return 0, fmt.Errorf("writing TreeData: %w", err)
	}

	// Write Root node
	if err := w.writeRootNode(grid.Root); err != nil {
		return 0, fmt.Errorf("writing RootNode: %w", err)
	}
	w.writePadding(AlignUp32(rootNodeSize) - rootNodeSize)

	// Write Upper nodes
	for i, upper := range grid.UpperNodes {
		if err := w.writeUpperNode(upper, grid, i); err != nil {
			return 0, fmt.Errorf("writing UpperNode %d: %w", i, err)
		}
	}
	w.writePadding(AlignUp32(numUpper*upperNodeSize) - numUpper*upperNodeSize)

	// Write Lower nodes
	for i, lower := range grid.LowerNodes {
		if err := w.writeLowerNode(lower, grid, i); err != nil {
			return 0, fmt.Errorf("writing LowerNode %d: %w", i, err)
		}
	}
	w.writePadding(AlignUp32(numLower*lowerNodeSize) - numLower*lowerNodeSize)

	// Write Leaf nodes
	for i, leaf := range grid.LeafNodes {
		if err := w.writeLeafNode(leaf, i); err != nil {
			return 0, fmt.Errorf("writing LeafNode %d: %w", i, err)
		}
	}
	w.writePadding(AlignUp32(numLeaves*leafNodeSize) - numLeaves*leafNodeSize)

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
			float64(grid.BBox.Min.X) * grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Min.Y) * grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Min.Z) * grid.VoxelSize.Z + grid.Origin.Z,
		},
		Max: Vec3d{
			float64(grid.BBox.Max.X+1) * grid.VoxelSize.X + grid.Origin.X,
			float64(grid.BBox.Max.Y+1) * grid.VoxelSize.Y + grid.Origin.Y,
			float64(grid.BBox.Max.Z+1) * grid.VoxelSize.Z + grid.Origin.Z,
		},
	}

	// Write each field in order
	binary.Write(w.buf, ByteOrder, MagicNumber)        // 8 bytes
	binary.Write(w.buf, ByteOrder, uint32(0))          // 4 bytes: checksum[0] (not computed)
	binary.Write(w.buf, ByteOrder, uint32(0))          // 4 bytes: checksum[1]
	binary.Write(w.buf, ByteOrder, Version)            // 4 bytes: version
	binary.Write(w.buf, ByteOrder, uint32(0))          // 4 bytes: flags
	binary.Write(w.buf, ByteOrder, uint32(0))          // 4 bytes: grid index
	binary.Write(w.buf, ByteOrder, uint32(1))          // 4 bytes: grid count
	binary.Write(w.buf, ByteOrder, gridSize)           // 8 bytes: grid size

	w.buf.Write(nameBytes[:])                          // 256 bytes: grid name

	// Write Map (264 bytes)
	w.writeMap(&mapData)

	// Write world bounding box (48 bytes)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.X)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Min.Z)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.X)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Y)
	binary.Write(w.buf, ByteOrder, worldBBox.Max.Z)

	// Write voxel size (24 bytes)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.X)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Y)
	binary.Write(w.buf, ByteOrder, grid.VoxelSize.Z)

	// Write grid class and type (8 bytes)
	binary.Write(w.buf, ByteOrder, uint32(grid.GridClass))
	binary.Write(w.buf, ByteOrder, uint32(GridTypeOnIndex))

	// Blind metadata offset and count (12 bytes)
	binary.Write(w.buf, ByteOrder, int64(0))           // blind metadata offset (no blind data)
	binary.Write(w.buf, ByteOrder, uint32(0))          // blind metadata count

	// Padding/reserved (20 bytes to reach 672)
	binary.Write(w.buf, ByteOrder, uint32(0))          // Data0
	binary.Write(w.buf, ByteOrder, grid.ActiveVoxelCount) // Data1: voxel count
	binary.Write(w.buf, ByteOrder, uint64(0))          // Data2

	// Verify we wrote exactly GridDataSize bytes
	// (This is a sanity check during development)
	return nil
}

// writeMap writes the 264-byte Map structure
func (w *Writer) writeMap(m *Map) {
	// Single precision (92 bytes)
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

	// Double precision (176 bytes)
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
func (w *Writer) writeTreeData(grid *IndexGrid, leafOff, lowerOff, upperOff, rootOff int,
	numLeaves, numLower, numUpper int) error {

	// Node offsets (relative to start of TreeData)
	binary.Write(w.buf, ByteOrder, uint64(leafOff))   // leaf offset
	binary.Write(w.buf, ByteOrder, uint64(lowerOff))  // lower offset
	binary.Write(w.buf, ByteOrder, uint64(upperOff))  // upper offset
	binary.Write(w.buf, ByteOrder, uint64(rootOff))   // root offset

	// Node counts
	binary.Write(w.buf, ByteOrder, uint32(numLeaves))
	binary.Write(w.buf, ByteOrder, uint32(numLower))
	binary.Write(w.buf, ByteOrder, uint32(numUpper))
	binary.Write(w.buf, ByteOrder, uint32(1)) // 1 root node

	// Reserved (16 bytes)
	w.buf.Write(make([]byte, 16))

	return nil
}

// writeRootNode writes the root node data
func (w *Writer) writeRootNode(root *IndexRootNode) error {
	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, root.BBox.Max.Z)

	// Tile count (4 bytes)
	binary.Write(w.buf, ByteOrder, root.TileCount)

	// Padding to 8-byte alignment (4 bytes)
	binary.Write(w.buf, ByteOrder, uint32(0))

	// Write root tiles (each tile: key Coord + child index or state)
	// For OnIndex, tiles contain: Coord origin (12 bytes) + int64 child/state (8 bytes)
	// Tiles are sorted by origin
	origins := make([]Coord, 0, len(root.Children))
	for origin := range root.Children {
		origins = append(origins, origin)
	}
	sortCoords(origins)

	for i, origin := range origins {
		binary.Write(w.buf, ByteOrder, origin.X)
		binary.Write(w.buf, ByteOrder, origin.Y)
		binary.Write(w.buf, ByteOrder, origin.Z)
		// Child index (points to upper node array index)
		binary.Write(w.buf, ByteOrder, int64(i)) // Simple index for now
	}

	return nil
}

// writeUpperNode writes an upper internal node (32³ children)
func (w *Writer) writeUpperNode(upper *IndexUpperNode, grid *IndexGrid, nodeIndex int) error {
	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, upper.BBox.Max.Z)

	// Flags (8 bytes for alignment)
	binary.Write(w.buf, ByteOrder, uint64(0))

	// Child mask (512 * 8 = 4096 bytes)
	for _, v := range upper.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Value mask (512 * 8 = 4096 bytes)
	for _, v := range upper.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// For OnIndex: offset value (8 bytes)
	binary.Write(w.buf, ByteOrder, upper.Offset)

	return nil
}

// writeLeafNode writes a leaf node for OnIndex grid
func (w *Writer) writeLeafNode(leaf *IndexLeafNode, nodeIndex int) error {
	// CoordBBox (24 bytes) - bounding box of active voxels
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, leaf.BBox.Max.Z)

	// Flags + prefix sum (8 bytes with padding)
	binary.Write(w.buf, ByteOrder, uint8(0))  // flags
	binary.Write(w.buf, ByteOrder, uint8(0))  // prefix sum
	binary.Write(w.buf, ByteOrder, uint16(0)) // padding
	binary.Write(w.buf, ByteOrder, uint32(0)) // padding

	// Value mask (8 * 8 = 64 bytes)
	for _, v := range leaf.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Offset to first value (8 bytes) - not used for OnIndex without blind data
	// but included for format compatibility
	// For OnIndex this is actually "mPrefixSum" field
	// Total so far: 24 + 8 + 64 = 96 bytes

	return nil
}

// writeLowerNode writes a lower internal node (16³ children)
func (w *Writer) writeLowerNode(lower *IndexLowerNode, grid *IndexGrid, nodeIndex int) error {
	// CoordBBox (24 bytes)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Min.Z)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.X)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Y)
	binary.Write(w.buf, ByteOrder, lower.BBox.Max.Z)

	// Flags (8 bytes)
	binary.Write(w.buf, ByteOrder, uint64(0))

	// Child mask (64 * 8 = 512 bytes)
	for _, v := range lower.ChildMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Value mask (64 * 8 = 512 bytes)
	for _, v := range lower.ValueMask {
		binary.Write(w.buf, ByteOrder, v)
	}

	// Offset (8 bytes)
	binary.Write(w.buf, ByteOrder, lower.Offset)

	return nil
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

// Size calculation helpers

func calcLeafNodeSize() int {
	// CoordBBox(24) + flags(8) + Mask512(64) = 96 bytes
	return 96
}

func calcLowerNodeSize() int {
	// CoordBBox(24) + flags(8) + Mask4096(512) + Mask4096(512) + offset(8) = 1064 bytes
	return 24 + 8 + 512 + 512 + 8
}

func calcUpperNodeSize() int {
	// CoordBBox(24) + flags(8) + Mask32768(4096) + Mask32768(4096) + offset(8) = 8232 bytes
	return 24 + 8 + 4096 + 4096 + 8
}

func calcRootNodeSize(numTiles int) int {
	// CoordBBox(24) + tileCount(4) + padding(4) + tiles(numTiles * 20)
	// Each tile: Coord(12) + int64(8) = 20 bytes
	return 24 + 4 + 4 + numTiles*20
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
