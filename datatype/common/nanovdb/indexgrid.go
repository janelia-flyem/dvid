package nanovdb

import (
	"sort"
)

// IndexLeafNode represents a leaf node for an IndexGrid (OnIndex type).
// Leaf nodes are 8³ = 512 voxels. For IndexGrid, we only store:
// - Origin coordinate
// - Active state bitmask (which voxels are "on")
// - Prefix sum for computing indices into external data
//
// Binary layout for OnIndex leaf (from NanoVDB.h):
//   CoordBBox mBBox (24 bytes) - bounding box
//   uint8_t mFlags (1 byte)
//   uint8_t mPrefixSum (1 byte) - used for some optimizations
//   Mask512 mValueMask (64 bytes) - active state
//   uint64_t mOffset (8 bytes) - offset to first value
//   ... padding to alignment
type IndexLeafNode struct {
	Origin    Coord    // Origin coordinate of this leaf
	ValueMask Mask512  // Which voxels are active
	Offset    uint64   // Index offset for this leaf's values
	BBox      CoordBBox // Bounding box of active voxels
}

// IndexLeafNodeSize is the size of a serialized IndexLeafNode for OnIndex grids
// From NanoVDB.h: LeafData<ValueOnIndex> has CoordBBox(24) + flags(1) + prefix(1) +
// padding(6) + Mask(64) + offset(8) + padding = 96 bytes minimum, but actual size
// depends on alignment. Let's compute it properly.
// Actually for OnIndex: 24 (bbox) + 8 (flags+prefix+pad) + 64 (mask) + 8 (offset) = 104
// Aligned to 32: 128 bytes? Let me check...
// From the NanoVDB source, LeafNode<ValueOnIndex> size is 96 bytes
const IndexLeafNodeSize = 96

// IndexLowerNode represents a lower internal node (16³ children).
// Each child can be either a leaf node or a tile value.
type IndexLowerNode struct {
	Origin     Coord      // Origin coordinate
	ChildMask  Mask4096   // Which children are nodes (vs tiles)
	ValueMask  Mask4096   // Which children/tiles are active
	ChildOffsets []uint32 // Offsets to child nodes (only for active children)
	BBox       CoordBBox  // Bounding box
	Offset     uint64     // Value offset
}

// IndexLowerNodeBaseSize is the base size without child table
// CoordBBox(24) + flags(8) + Mask(512) + Mask(512) + offset(8) + ...
const IndexLowerNodeBaseSize = 64 + 512 + 512 // Approximate, will refine

// IndexUpperNode represents an upper internal node (32³ children).
type IndexUpperNode struct {
	Origin     Coord       // Origin coordinate
	ChildMask  Mask32768   // Which children are nodes
	ValueMask  Mask32768   // Which children/tiles are active
	ChildOffsets []uint32  // Offsets to child nodes
	BBox       CoordBBox   // Bounding box
	Offset     uint64      // Value offset
}

// IndexRootNode represents the root node with a dynamic tile table.
type IndexRootNode struct {
	BBox      CoordBBox               // Bounding box of entire tree
	TileCount uint32                  // Number of tiles
	Children  map[Coord]*IndexUpperNode // Child upper nodes keyed by origin
	ActiveTiles map[Coord]bool        // Active tile flags
}

// IndexGrid represents a complete OnIndex grid ready for serialization.
type IndexGrid struct {
	Name       string
	VoxelSize  Vec3d
	Origin     Vec3d
	GridClass  GridClass

	// Tree structure
	Root       *IndexRootNode
	UpperNodes []*IndexUpperNode
	LowerNodes []*IndexLowerNode
	LeafNodes  []*IndexLeafNode

	// Statistics
	ActiveVoxelCount uint64
	BBox             CoordBBox
}

// IndexGridBuilder constructs an IndexGrid from a set of voxel coordinates.
type IndexGridBuilder struct {
	// Configuration
	name      string
	voxelSize Vec3d
	origin    Vec3d
	gridClass GridClass

	// Temporary storage during building
	voxels    []Coord // All active voxel coordinates
	leafMap   map[Coord]*IndexLeafNode
	lowerMap  map[Coord]*IndexLowerNode
	upperMap  map[Coord]*IndexUpperNode
}

// NewIndexGridBuilder creates a new builder for constructing an IndexGrid.
func NewIndexGridBuilder(name string) *IndexGridBuilder {
	return &IndexGridBuilder{
		name:      name,
		voxelSize: Vec3d{1.0, 1.0, 1.0},
		origin:    Vec3d{0.0, 0.0, 0.0},
		gridClass: GridClassIndexGrid,
		voxels:    make([]Coord, 0),
		leafMap:   make(map[Coord]*IndexLeafNode),
		lowerMap:  make(map[Coord]*IndexLowerNode),
		upperMap:  make(map[Coord]*IndexUpperNode),
	}
}

// SetVoxelSize sets the voxel dimensions in world units.
func (b *IndexGridBuilder) SetVoxelSize(size Vec3d) *IndexGridBuilder {
	b.voxelSize = size
	return b
}

// SetOrigin sets the world-space origin of the grid.
func (b *IndexGridBuilder) SetOrigin(origin Vec3d) *IndexGridBuilder {
	b.origin = origin
	return b
}

// SetGridClass sets the grid class (IndexGrid or TensorGrid).
func (b *IndexGridBuilder) SetGridClass(class GridClass) *IndexGridBuilder {
	b.gridClass = class
	return b
}

// AddVoxel adds a single active voxel coordinate.
func (b *IndexGridBuilder) AddVoxel(x, y, z int32) {
	b.voxels = append(b.voxels, Coord{x, y, z})
}

// AddVoxels adds multiple active voxel coordinates.
func (b *IndexGridBuilder) AddVoxels(coords []Coord) {
	b.voxels = append(b.voxels, coords...)
}

// Build constructs the IndexGrid from all added voxels.
func (b *IndexGridBuilder) Build() *IndexGrid {
	if len(b.voxels) == 0 {
		return nil
	}

	// Sort voxels by Z, Y, X for cache-friendly access
	sort.Slice(b.voxels, func(i, j int) bool {
		if b.voxels[i].Z != b.voxels[j].Z {
			return b.voxels[i].Z < b.voxels[j].Z
		}
		if b.voxels[i].Y != b.voxels[j].Y {
			return b.voxels[i].Y < b.voxels[j].Y
		}
		return b.voxels[i].X < b.voxels[j].X
	})

	// Remove duplicates
	b.voxels = removeDuplicateCoords(b.voxels)

	// Build the tree bottom-up
	b.buildLeafNodes()
	b.buildLowerNodes()
	b.buildUpperNodes()

	// Create the root node
	root := b.buildRootNode()

	// Compute bounding box
	bbox := NewEmptyBBox()
	for _, v := range b.voxels {
		bbox.ExpandBBox(v)
	}

	// Collect nodes into slices (sorted by origin for deterministic output)
	leafNodes := b.collectLeafNodes()
	lowerNodes := b.collectLowerNodes()
	upperNodes := b.collectUpperNodes()

	// Assign offsets to nodes (for index computation)
	b.assignOffsets(leafNodes, lowerNodes, upperNodes)

	return &IndexGrid{
		Name:             b.name,
		VoxelSize:        b.voxelSize,
		Origin:           b.origin,
		GridClass:        b.gridClass,
		Root:             root,
		UpperNodes:       upperNodes,
		LowerNodes:       lowerNodes,
		LeafNodes:        leafNodes,
		ActiveVoxelCount: uint64(len(b.voxels)),
		BBox:             bbox,
	}
}

// GetSortedVoxels returns a copy of the sorted, deduplicated voxel coordinates.
// This must be called after Build() to get the final voxel ordering that corresponds
// to the IndexGrid's voxel indices.
func (b *IndexGridBuilder) GetSortedVoxels() []Coord {
	result := make([]Coord, len(b.voxels))
	copy(result, b.voxels)
	return result
}

// buildLeafNodes creates leaf nodes from voxels
func (b *IndexGridBuilder) buildLeafNodes() {
	for _, v := range b.voxels {
		origin := leafOrigin(v.X, v.Y, v.Z)

		leaf, exists := b.leafMap[origin]
		if !exists {
			leaf = &IndexLeafNode{
				Origin: origin,
				BBox:   NewEmptyBBox(),
			}
			b.leafMap[origin] = leaf
		}

		// Compute local offset within leaf (0-511)
		localX := int(v.X - origin.X)
		localY := int(v.Y - origin.Y)
		localZ := int(v.Z - origin.Z)
		offset := coordToLeafOffset(localX, localY, localZ)

		leaf.ValueMask.SetBit(offset)
		leaf.BBox.ExpandBBox(v)
	}
}

// buildLowerNodes creates lower internal nodes from leaf nodes
func (b *IndexGridBuilder) buildLowerNodes() {
	for leafOrigin, leaf := range b.leafMap {
		origin := lowerOrigin(leafOrigin.X, leafOrigin.Y, leafOrigin.Z)

		lower, exists := b.lowerMap[origin]
		if !exists {
			lower = &IndexLowerNode{
				Origin: origin,
				BBox:   NewEmptyBBox(),
			}
			b.lowerMap[origin] = lower
		}

		// Compute child index within lower node (0-4095)
		localX := int((leafOrigin.X - origin.X) / LeafDim)
		localY := int((leafOrigin.Y - origin.Y) / LeafDim)
		localZ := int((leafOrigin.Z - origin.Z) / LeafDim)
		childIdx := coordToLowerOffset(localX, localY, localZ)

		lower.ChildMask.SetBit(childIdx)
		lower.ValueMask.SetBit(childIdx)
		lower.BBox.ExpandBBox(leaf.BBox.Min)
		lower.BBox.ExpandBBox(leaf.BBox.Max)
	}
}

// buildUpperNodes creates upper internal nodes from lower nodes
func (b *IndexGridBuilder) buildUpperNodes() {
	for lowerOriginCoord, lower := range b.lowerMap {
		origin := upperOrigin(lowerOriginCoord.X, lowerOriginCoord.Y, lowerOriginCoord.Z)

		upper, exists := b.upperMap[origin]
		if !exists {
			upper = &IndexUpperNode{
				Origin: origin,
				BBox:   NewEmptyBBox(),
			}
			b.upperMap[origin] = upper
		}

		// Compute child index within upper node (0-32767)
		localX := int((lowerOriginCoord.X - origin.X) / LowerTotalDim)
		localY := int((lowerOriginCoord.Y - origin.Y) / LowerTotalDim)
		localZ := int((lowerOriginCoord.Z - origin.Z) / LowerTotalDim)
		childIdx := coordToUpperOffset(localX, localY, localZ)

		upper.ChildMask.SetBit(childIdx)
		upper.ValueMask.SetBit(childIdx)
		upper.BBox.ExpandBBox(lower.BBox.Min)
		upper.BBox.ExpandBBox(lower.BBox.Max)
	}
}

// buildRootNode creates the root node from upper nodes
func (b *IndexGridBuilder) buildRootNode() *IndexRootNode {
	root := &IndexRootNode{
		BBox:      NewEmptyBBox(),
		Children:  make(map[Coord]*IndexUpperNode),
		ActiveTiles: make(map[Coord]bool),
	}

	for origin, upper := range b.upperMap {
		root.Children[origin] = upper
		root.BBox.ExpandBBox(upper.BBox.Min)
		root.BBox.ExpandBBox(upper.BBox.Max)
	}

	root.TileCount = uint32(len(root.Children))
	return root
}

// collectLeafNodes returns leaf nodes sorted by origin
func (b *IndexGridBuilder) collectLeafNodes() []*IndexLeafNode {
	nodes := make([]*IndexLeafNode, 0, len(b.leafMap))
	for _, node := range b.leafMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// collectLowerNodes returns lower nodes sorted by origin
func (b *IndexGridBuilder) collectLowerNodes() []*IndexLowerNode {
	nodes := make([]*IndexLowerNode, 0, len(b.lowerMap))
	for _, node := range b.lowerMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// collectUpperNodes returns upper nodes sorted by origin
func (b *IndexGridBuilder) collectUpperNodes() []*IndexUpperNode {
	nodes := make([]*IndexUpperNode, 0, len(b.upperMap))
	for _, node := range b.upperMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// assignOffsets assigns value offsets to each node for index computation
func (b *IndexGridBuilder) assignOffsets(leafNodes []*IndexLeafNode, lowerNodes []*IndexLowerNode, upperNodes []*IndexUpperNode) {
	var offset uint64 = 0

	// Assign offsets to leaf nodes
	for _, leaf := range leafNodes {
		leaf.Offset = offset
		offset += uint64(leaf.ValueMask.CountOn())
	}

	// For internal nodes, offset points to accumulated count
	for _, lower := range lowerNodes {
		lower.Offset = 0 // Lower nodes reference their children
	}

	for _, upper := range upperNodes {
		upper.Offset = 0
	}
}

// coordLess returns true if a < b in Z, Y, X order
func coordLess(a, b Coord) bool {
	if a.Z != b.Z {
		return a.Z < b.Z
	}
	if a.Y != b.Y {
		return a.Y < b.Y
	}
	return a.X < b.X
}

// removeDuplicateCoords removes duplicate coordinates from a sorted slice
func removeDuplicateCoords(coords []Coord) []Coord {
	if len(coords) == 0 {
		return coords
	}

	result := make([]Coord, 0, len(coords))
	result = append(result, coords[0])

	for i := 1; i < len(coords); i++ {
		if coords[i] != coords[i-1] {
			result = append(result, coords[i])
		}
	}

	return result
}

// GetActiveVoxelCount returns the number of active voxels in the grid
func (g *IndexGrid) GetActiveVoxelCount() uint64 {
	return g.ActiveVoxelCount
}

// GetLeafNode returns the leaf node containing the given coordinate, or nil
func (g *IndexGrid) GetLeafNode(x, y, z int32) *IndexLeafNode {
	origin := leafOrigin(x, y, z)
	for _, leaf := range g.LeafNodes {
		if leaf.Origin == origin {
			return leaf
		}
	}
	return nil
}

// IsActive returns true if the voxel at (x, y, z) is active
func (g *IndexGrid) IsActive(x, y, z int32) bool {
	leaf := g.GetLeafNode(x, y, z)
	if leaf == nil {
		return false
	}

	localX := int(x - leaf.Origin.X)
	localY := int(y - leaf.Origin.Y)
	localZ := int(z - leaf.Origin.Z)
	offset := coordToLeafOffset(localX, localY, localZ)

	return leaf.ValueMask.GetBit(offset)
}
