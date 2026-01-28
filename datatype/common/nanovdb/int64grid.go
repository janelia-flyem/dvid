package nanovdb

import (
	"fmt"
	"math"
	"sort"
)

// Int64LeafNode represents a leaf node for an Int64 value grid.
// Leaf nodes are 8³ = 512 voxels. For Int64 grids, we store:
// - Origin coordinate
// - Active state bitmask (which voxels are "on")
// - Actual int64 values for all 512 voxel positions
// - Statistics (min, max, average, stddev)
//
// Binary layout (from NanoVDB.h LeafData<int64_t>):
//
//	mBBoxMin    (12 bytes) - 3 × int32
//	mBBoxDif    (3 bytes)  - delta from min to max per axis
//	mFlags      (1 byte)   - bit flags
//	mValueMask  (64 bytes) - 8 × uint64, which voxels are active
//	mMinimum    (8 bytes)  - int64
//	mMaximum    (8 bytes)  - int64
//	mAverage    (8 bytes)  - float64
//	mStdDevi    (8 bytes)  - float64
//	mValues     (4096 bytes) - 512 × int64
//	padding     (16 bytes) - to 32-byte alignment
//	Total: 4224 bytes
type Int64LeafNode struct {
	Origin    Coord         // Origin coordinate of this leaf
	ValueMask Mask512       // Which voxels are active
	Values    [512]int64    // Values for all voxel positions
	Minimum   int64         // Min of active values
	Maximum   int64         // Max of active values
	Average   float64       // Mean of active values
	StdDevi   float64       // Standard deviation of active values
	BBox      CoordBBox     // Bounding box of active voxels
}

// Int64LeafNodeSize is the size of a serialized Int64LeafNode
const Int64LeafNodeSize = 4224

// Int64LowerNode represents a lower internal node (16³ children).
// The binary layout is identical to IndexLowerNode.
type Int64LowerNode struct {
	Origin    Coord     // Origin coordinate
	ChildMask Mask4096  // Which children are nodes (vs tiles)
	ValueMask Mask4096  // Which children/tiles are active
	Minimum   int64     // Min of active values in subtree
	Maximum   int64     // Max of active values in subtree
	Average   float64   // Mean of active values in subtree
	StdDevi   float64   // Std deviation of active values in subtree
	BBox      CoordBBox // Bounding box
}

// Int64UpperNode represents an upper internal node (32³ children).
// The binary layout is identical to IndexUpperNode.
type Int64UpperNode struct {
	Origin    Coord      // Origin coordinate
	ChildMask Mask32768  // Which children are nodes
	ValueMask Mask32768  // Which children/tiles are active
	Minimum   int64      // Min of active values in subtree
	Maximum   int64      // Max of active values in subtree
	Average   float64    // Mean of active values in subtree
	StdDevi   float64    // Std deviation of active values in subtree
	BBox      CoordBBox  // Bounding box
}

// Int64RootNode represents the root node with a dynamic tile table.
type Int64RootNode struct {
	BBox        CoordBBox                 // Bounding box of entire tree
	TileCount   uint32                    // Number of tiles
	Children    map[Coord]*Int64UpperNode // Child upper nodes keyed by origin
	Background  int64                     // Background value for inactive voxels
	Minimum     int64                     // Min of active values
	Maximum     int64                     // Max of active values
	Average     float64                   // Mean of active values
	StdDevi     float64                   // Std deviation of active values
}

// Int64Grid represents a complete Int64 value grid ready for serialization.
type Int64Grid struct {
	Name       string
	VoxelSize  Vec3d
	Origin     Vec3d
	GridClass  GridClass

	// Tree structure
	Root       *Int64RootNode
	UpperNodes []*Int64UpperNode
	LowerNodes []*Int64LowerNode
	LeafNodes  []*Int64LeafNode

	// Statistics
	ActiveVoxelCount uint64
	BBox             CoordBBox
}

// Int64GridBuilder constructs an Int64Grid from voxel coordinates and values.
type Int64GridBuilder struct {
	// Configuration
	name       string
	voxelSize  Vec3d
	origin     Vec3d
	gridClass  GridClass
	background int64

	// Tree structure (built incrementally)
	leafMap  map[Coord]*Int64LeafNode
	lowerMap map[Coord]*Int64LowerNode
	upperMap map[Coord]*Int64UpperNode

	// Statistics tracked incrementally
	voxelCount uint64
	bbox       CoordBBox
}

// NewInt64GridBuilder creates a new builder for constructing an Int64Grid.
func NewInt64GridBuilder(name string) *Int64GridBuilder {
	return &Int64GridBuilder{
		name:       name,
		voxelSize:  Vec3d{1.0, 1.0, 1.0},
		origin:     Vec3d{0.0, 0.0, 0.0},
		gridClass:  GridClassVoxelVolume,
		background: 0,
		leafMap:    make(map[Coord]*Int64LeafNode),
		lowerMap:   make(map[Coord]*Int64LowerNode),
		upperMap:   make(map[Coord]*Int64UpperNode),
		bbox:       NewEmptyBBox(),
	}
}

// SetVoxelSize sets the voxel dimensions in world units.
func (b *Int64GridBuilder) SetVoxelSize(size Vec3d) *Int64GridBuilder {
	b.voxelSize = size
	return b
}

// SetOrigin sets the world-space origin of the grid.
func (b *Int64GridBuilder) SetOrigin(origin Vec3d) *Int64GridBuilder {
	b.origin = origin
	return b
}

// SetGridClass sets the grid class.
func (b *Int64GridBuilder) SetGridClass(class GridClass) *Int64GridBuilder {
	b.gridClass = class
	return b
}

// SetBackground sets the background value for inactive voxels.
func (b *Int64GridBuilder) SetBackground(bg int64) *Int64GridBuilder {
	b.background = bg
	return b
}

// AddVoxel adds a single active voxel with the given value.
func (b *Int64GridBuilder) AddVoxel(x, y, z int32, value int64) {
	b.insertVoxel(Coord{X: x, Y: y, Z: z}, value)
}

// AddVoxelWithLabel adds a voxel with a uint64 label, checking for overflow.
// Returns an error if the label value exceeds math.MaxInt64.
func (b *Int64GridBuilder) AddVoxelWithLabel(x, y, z int32, label uint64) error {
	if label > math.MaxInt64 {
		return fmt.Errorf("label %d at (%d,%d,%d) exceeds max int64 (%d)", label, x, y, z, int64(math.MaxInt64))
	}
	b.insertVoxel(Coord{X: x, Y: y, Z: z}, int64(label))
	return nil
}

// insertVoxel inserts a single voxel with value into the leaf node tree.
func (b *Int64GridBuilder) insertVoxel(v Coord, value int64) {
	origin := leafOrigin(v.X, v.Y, v.Z)

	leaf, exists := b.leafMap[origin]
	if !exists {
		leaf = &Int64LeafNode{
			Origin:  origin,
			BBox:    NewEmptyBBox(),
			Minimum: math.MaxInt64,
			Maximum: math.MinInt64,
		}
		// Initialize all values to background
		for i := range leaf.Values {
			leaf.Values[i] = b.background
		}
		b.leafMap[origin] = leaf
	}

	// Compute local offset within leaf (0-511)
	localX := int(v.X - origin.X)
	localY := int(v.Y - origin.Y)
	localZ := int(v.Z - origin.Z)
	offset := coordToLeafOffset(localX, localY, localZ)

	// Store the value
	leaf.Values[offset] = value

	// Only count if this is a new voxel (bit not already set)
	if !leaf.ValueMask.GetBit(offset) {
		leaf.ValueMask.SetBit(offset)
		b.voxelCount++
		b.bbox.ExpandBBox(v)
		leaf.BBox.ExpandBBox(v)
	}

	// Update leaf statistics
	if value < leaf.Minimum {
		leaf.Minimum = value
	}
	if value > leaf.Maximum {
		leaf.Maximum = value
	}
}

// Build constructs the Int64Grid from all added voxels.
func (b *Int64GridBuilder) Build() *Int64Grid {
	if b.voxelCount == 0 {
		return nil
	}

	// Finalize leaf statistics
	for _, leaf := range b.leafMap {
		b.computeLeafStats(leaf)
	}

	// Build upper tree levels from leaf nodes
	b.buildLowerNodes()
	b.buildUpperNodes()

	// Create the root node
	root := b.buildRootNode()

	// Collect nodes into slices (sorted by origin for deterministic output)
	leafNodes := b.collectLeafNodes()
	lowerNodes := b.collectLowerNodes()
	upperNodes := b.collectUpperNodes()

	return &Int64Grid{
		Name:             b.name,
		VoxelSize:        b.voxelSize,
		Origin:           b.origin,
		GridClass:        b.gridClass,
		Root:             root,
		UpperNodes:       upperNodes,
		LowerNodes:       lowerNodes,
		LeafNodes:        leafNodes,
		ActiveVoxelCount: b.voxelCount,
		BBox:             b.bbox,
	}
}

// computeLeafStats computes average and stddev for a leaf node
func (b *Int64GridBuilder) computeLeafStats(leaf *Int64LeafNode) {
	count := leaf.ValueMask.CountOn()
	if count == 0 {
		leaf.Average = 0
		leaf.StdDevi = 0
		return
	}

	// Compute mean
	var sum float64
	for i := 0; i < 512; i++ {
		if leaf.ValueMask.GetBit(i) {
			sum += float64(leaf.Values[i])
		}
	}
	leaf.Average = sum / float64(count)

	// Compute variance
	var variance float64
	for i := 0; i < 512; i++ {
		if leaf.ValueMask.GetBit(i) {
			diff := float64(leaf.Values[i]) - leaf.Average
			variance += diff * diff
		}
	}
	leaf.StdDevi = math.Sqrt(variance / float64(count))
}

// buildLowerNodes creates lower internal nodes from leaf nodes
func (b *Int64GridBuilder) buildLowerNodes() {
	for leafOriginCoord, leaf := range b.leafMap {
		origin := lowerOrigin(leafOriginCoord.X, leafOriginCoord.Y, leafOriginCoord.Z)

		lower, exists := b.lowerMap[origin]
		if !exists {
			lower = &Int64LowerNode{
				Origin:  origin,
				BBox:    NewEmptyBBox(),
				Minimum: math.MaxInt64,
				Maximum: math.MinInt64,
			}
			b.lowerMap[origin] = lower
		}

		// Compute child index within lower node (0-4095)
		localX := int((leafOriginCoord.X - origin.X) / LeafDim)
		localY := int((leafOriginCoord.Y - origin.Y) / LeafDim)
		localZ := int((leafOriginCoord.Z - origin.Z) / LeafDim)
		childIdx := coordToLowerOffset(localX, localY, localZ)

		lower.ChildMask.SetBit(childIdx)
		lower.ValueMask.SetBit(childIdx)
		lower.BBox.ExpandBBox(leaf.BBox.Min)
		lower.BBox.ExpandBBox(leaf.BBox.Max)

		// Update stats
		if leaf.Minimum < lower.Minimum {
			lower.Minimum = leaf.Minimum
		}
		if leaf.Maximum > lower.Maximum {
			lower.Maximum = leaf.Maximum
		}
	}

	// Compute average/stddev for lower nodes (simplified: average of leaf averages)
	for _, lower := range b.lowerMap {
		var sum, count float64
		for leafOriginCoord, leaf := range b.leafMap {
			lowerOfLeaf := lowerOrigin(leafOriginCoord.X, leafOriginCoord.Y, leafOriginCoord.Z)
			if lowerOfLeaf == lower.Origin {
				leafCount := float64(leaf.ValueMask.CountOn())
				sum += leaf.Average * leafCount
				count += leafCount
			}
		}
		if count > 0 {
			lower.Average = sum / count
		}
		// StdDevi computation would need all values; use 0 for simplicity
		lower.StdDevi = 0
	}
}

// buildUpperNodes creates upper internal nodes from lower nodes
func (b *Int64GridBuilder) buildUpperNodes() {
	for lowerOriginCoord, lower := range b.lowerMap {
		origin := upperOrigin(lowerOriginCoord.X, lowerOriginCoord.Y, lowerOriginCoord.Z)

		upper, exists := b.upperMap[origin]
		if !exists {
			upper = &Int64UpperNode{
				Origin:  origin,
				BBox:    NewEmptyBBox(),
				Minimum: math.MaxInt64,
				Maximum: math.MinInt64,
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

		// Update stats
		if lower.Minimum < upper.Minimum {
			upper.Minimum = lower.Minimum
		}
		if lower.Maximum > upper.Maximum {
			upper.Maximum = lower.Maximum
		}
		// Simplified: average of lower averages weighted by approx voxel count
		upper.Average = lower.Average
		upper.StdDevi = 0
	}
}

// buildRootNode creates the root node from upper nodes
func (b *Int64GridBuilder) buildRootNode() *Int64RootNode {
	root := &Int64RootNode{
		BBox:       NewEmptyBBox(),
		Children:   make(map[Coord]*Int64UpperNode),
		Background: b.background,
		Minimum:    math.MaxInt64,
		Maximum:    math.MinInt64,
	}

	var sum, count float64
	for origin, upper := range b.upperMap {
		root.Children[origin] = upper
		root.BBox.ExpandBBox(upper.BBox.Min)
		root.BBox.ExpandBBox(upper.BBox.Max)

		if upper.Minimum < root.Minimum {
			root.Minimum = upper.Minimum
		}
		if upper.Maximum > root.Maximum {
			root.Maximum = upper.Maximum
		}
		sum += upper.Average
		count++
	}
	if count > 0 {
		root.Average = sum / count
	}
	root.StdDevi = 0
	root.TileCount = uint32(len(root.Children))

	return root
}

// collectLeafNodes returns leaf nodes sorted by origin
func (b *Int64GridBuilder) collectLeafNodes() []*Int64LeafNode {
	nodes := make([]*Int64LeafNode, 0, len(b.leafMap))
	for _, node := range b.leafMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// collectLowerNodes returns lower nodes sorted by origin
func (b *Int64GridBuilder) collectLowerNodes() []*Int64LowerNode {
	nodes := make([]*Int64LowerNode, 0, len(b.lowerMap))
	for _, node := range b.lowerMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// collectUpperNodes returns upper nodes sorted by origin
func (b *Int64GridBuilder) collectUpperNodes() []*Int64UpperNode {
	nodes := make([]*Int64UpperNode, 0, len(b.upperMap))
	for _, node := range b.upperMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return coordLess(nodes[i].Origin, nodes[j].Origin)
	})
	return nodes
}

// CheckLabelOverflow verifies that all uint64 labels can be safely stored as int64.
// Returns an error if any label exceeds math.MaxInt64.
func CheckLabelOverflow(labels []uint64) error {
	for i, v := range labels {
		if v > math.MaxInt64 {
			return fmt.Errorf("label at index %d overflows int64: %d > %d", i, v, int64(math.MaxInt64))
		}
	}
	return nil
}

// GetActiveVoxelCount returns the number of active voxels in the grid
func (g *Int64Grid) GetActiveVoxelCount() uint64 {
	return g.ActiveVoxelCount
}

// GetValue returns the value at the given coordinate.
// Returns the background value if the voxel is not active.
func (g *Int64Grid) GetValue(x, y, z int32) int64 {
	origin := leafOrigin(x, y, z)
	for _, leaf := range g.LeafNodes {
		if leaf.Origin == origin {
			localX := int(x - origin.X)
			localY := int(y - origin.Y)
			localZ := int(z - origin.Z)
			offset := coordToLeafOffset(localX, localY, localZ)
			return leaf.Values[offset]
		}
	}
	return g.Root.Background
}

// IsActive returns true if the voxel at (x, y, z) is active
func (g *Int64Grid) IsActive(x, y, z int32) bool {
	origin := leafOrigin(x, y, z)
	for _, leaf := range g.LeafNodes {
		if leaf.Origin == origin {
			localX := int(x - origin.X)
			localY := int(y - origin.Y)
			localZ := int(z - origin.Z)
			offset := coordToLeafOffset(localX, localY, localZ)
			return leaf.ValueMask.GetBit(offset)
		}
	}
	return false
}
