package nanovdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestMask512(t *testing.T) {
	var m Mask512

	// Test setting and getting bits
	m.SetBit(0)
	if !m.GetBit(0) {
		t.Error("Bit 0 should be set")
	}

	m.SetBit(63)
	if !m.GetBit(63) {
		t.Error("Bit 63 should be set")
	}

	m.SetBit(64)
	if !m.GetBit(64) {
		t.Error("Bit 64 should be set")
	}

	m.SetBit(511)
	if !m.GetBit(511) {
		t.Error("Bit 511 should be set")
	}

	// Test count
	if m.CountOn() != 4 {
		t.Errorf("Expected count 4, got %d", m.CountOn())
	}

	// Test out of range
	m.SetBit(-1)  // Should be no-op
	m.SetBit(512) // Should be no-op
	if m.CountOn() != 4 {
		t.Errorf("Out of range SetBit should be no-op, count is %d", m.CountOn())
	}
}

func TestMask4096(t *testing.T) {
	var m Mask4096

	m.SetBit(0)
	m.SetBit(4095)
	m.SetBit(1000)

	if !m.GetBit(0) || !m.GetBit(4095) || !m.GetBit(1000) {
		t.Error("Set bits should be retrievable")
	}

	if m.GetBit(1) {
		t.Error("Unset bit should return false")
	}

	if m.CountOn() != 3 {
		t.Errorf("Expected count 3, got %d", m.CountOn())
	}
}

func TestMask32768(t *testing.T) {
	var m Mask32768

	m.SetBit(0)
	m.SetBit(32767)
	m.SetBit(16384)

	if !m.GetBit(0) || !m.GetBit(32767) || !m.GetBit(16384) {
		t.Error("Set bits should be retrievable")
	}

	if m.CountOn() != 3 {
		t.Errorf("Expected count 3, got %d", m.CountOn())
	}
}

func TestCoordFunctions(t *testing.T) {
	// Test leaf origin calculation
	origin := leafOrigin(10, 20, 30)
	expected := Coord{8, 16, 24} // 10 & ~7 = 8, 20 & ~7 = 16, 30 & ~7 = 24
	if origin != expected {
		t.Errorf("leafOrigin(10,20,30) = %v, expected %v", origin, expected)
	}

	// Test with negative coordinates
	origin = leafOrigin(-1, -1, -1)
	expected = Coord{-8, -8, -8}
	if origin != expected {
		t.Errorf("leafOrigin(-1,-1,-1) = %v, expected %v", origin, expected)
	}

	// Test lower origin
	origin = lowerOrigin(100, 200, 300)
	// LowerTotalDim = 128, so origin should be multiples of 128
	expected = Coord{0, 128, 256}
	if origin != expected {
		t.Errorf("lowerOrigin(100,200,300) = %v, expected %v", origin, expected)
	}

	// Test upper origin
	origin = upperOrigin(5000, 5000, 5000)
	// UpperTotalDim = 4096, so origin should be multiples of 4096
	expected = Coord{4096, 4096, 4096}
	if origin != expected {
		t.Errorf("upperOrigin(5000,5000,5000) = %v, expected %v", origin, expected)
	}
}

func TestLeafOffset(t *testing.T) {
	// coordToLeafOffset should convert (x,y,z) in range [0,7] to linear offset [0,511]
	// Formula: z*64 + y*8 + x

	offset := coordToLeafOffset(0, 0, 0)
	if offset != 0 {
		t.Errorf("coordToLeafOffset(0,0,0) = %d, expected 0", offset)
	}

	offset = coordToLeafOffset(7, 7, 7)
	expected := 7*64 + 7*8 + 7 // = 448 + 56 + 7 = 511
	if offset != expected {
		t.Errorf("coordToLeafOffset(7,7,7) = %d, expected %d", offset, expected)
	}

	offset = coordToLeafOffset(1, 0, 0)
	if offset != 1 {
		t.Errorf("coordToLeafOffset(1,0,0) = %d, expected 1", offset)
	}

	offset = coordToLeafOffset(0, 1, 0)
	if offset != 8 {
		t.Errorf("coordToLeafOffset(0,1,0) = %d, expected 8", offset)
	}

	offset = coordToLeafOffset(0, 0, 1)
	if offset != 64 {
		t.Errorf("coordToLeafOffset(0,0,1) = %d, expected 64", offset)
	}
}

func TestBoundingBox(t *testing.T) {
	bbox := NewEmptyBBox()

	if !bbox.IsEmpty() {
		t.Error("New bounding box should be empty")
	}

	bbox.ExpandBBox(Coord{10, 20, 30})
	if bbox.IsEmpty() {
		t.Error("Bounding box should not be empty after expansion")
	}
	if bbox.Min.X != 10 || bbox.Min.Y != 20 || bbox.Min.Z != 30 {
		t.Errorf("Min should be (10,20,30), got %v", bbox.Min)
	}
	if bbox.Max.X != 10 || bbox.Max.Y != 20 || bbox.Max.Z != 30 {
		t.Errorf("Max should be (10,20,30), got %v", bbox.Max)
	}

	bbox.ExpandBBox(Coord{5, 25, 35})
	if bbox.Min.X != 5 || bbox.Max.Y != 25 || bbox.Max.Z != 35 {
		t.Errorf("Bounding box expansion failed: %v to %v", bbox.Min, bbox.Max)
	}
}

func TestIndexGridBuilderSimple(t *testing.T) {
	builder := NewIndexGridBuilder("test_grid")
	builder.SetVoxelSize(Vec3d{1.0, 1.0, 1.0})

	// Add a single voxel
	builder.AddVoxel(0, 0, 0)

	grid := builder.Build()

	if grid == nil {
		t.Fatal("Build() returned nil")
	}

	if grid.ActiveVoxelCount != 1 {
		t.Errorf("Expected 1 active voxel, got %d", grid.ActiveVoxelCount)
	}

	if len(grid.LeafNodes) != 1 {
		t.Errorf("Expected 1 leaf node, got %d", len(grid.LeafNodes))
	}

	if !grid.IsActive(0, 0, 0) {
		t.Error("Voxel (0,0,0) should be active")
	}

	if grid.IsActive(1, 0, 0) {
		t.Error("Voxel (1,0,0) should not be active")
	}
}

func TestIndexGridBuilderMultipleVoxels(t *testing.T) {
	builder := NewIndexGridBuilder("test_grid")

	// Add voxels that span multiple leaves
	voxels := []Coord{
		{0, 0, 0},
		{1, 1, 1},
		{7, 7, 7},   // Same leaf as above
		{8, 0, 0},   // Different leaf (X crosses 8 boundary)
		{100, 100, 100},
	}
	builder.AddVoxels(voxels)

	grid := builder.Build()

	if grid.ActiveVoxelCount != 5 {
		t.Errorf("Expected 5 active voxels, got %d", grid.ActiveVoxelCount)
	}

	// Should have 3 leaf nodes: [0,0,0], [8,0,0], [96,96,96]
	if len(grid.LeafNodes) != 3 {
		t.Errorf("Expected 3 leaf nodes, got %d", len(grid.LeafNodes))
	}

	// Check all voxels are active
	for _, v := range voxels {
		if !grid.IsActive(v.X, v.Y, v.Z) {
			t.Errorf("Voxel %v should be active", v)
		}
	}
}

func TestIndexGridBuilderDuplicates(t *testing.T) {
	builder := NewIndexGridBuilder("test_grid")

	// Add duplicate voxels
	builder.AddVoxel(5, 5, 5)
	builder.AddVoxel(5, 5, 5)
	builder.AddVoxel(5, 5, 5)

	grid := builder.Build()

	if grid.ActiveVoxelCount != 1 {
		t.Errorf("Duplicates should be removed, got %d voxels", grid.ActiveVoxelCount)
	}
}

func TestIdentityMap(t *testing.T) {
	m := NewIdentityMap(Vec3d{1.0, 1.0, 1.0}, Vec3d{0.0, 0.0, 0.0})

	// Forward matrix should be identity
	if m.MatD[0] != 1.0 || m.MatD[4] != 1.0 || m.MatD[8] != 1.0 {
		t.Error("Forward matrix diagonal should be 1.0")
	}

	// Check off-diagonals are zero
	if m.MatD[1] != 0 || m.MatD[2] != 0 || m.MatD[3] != 0 {
		t.Error("Off-diagonal elements should be 0")
	}

	// Inverse should also be identity
	if m.InvMatD[0] != 1.0 || m.InvMatD[4] != 1.0 || m.InvMatD[8] != 1.0 {
		t.Error("Inverse matrix diagonal should be 1.0")
	}

	// Test with non-unit voxel size
	m = NewIdentityMap(Vec3d{2.0, 3.0, 4.0}, Vec3d{10.0, 20.0, 30.0})

	if m.MatD[0] != 2.0 || m.MatD[4] != 3.0 || m.MatD[8] != 4.0 {
		t.Error("Forward matrix should have voxel sizes on diagonal")
	}

	if m.InvMatD[0] != 0.5 || m.InvMatD[4] != 1.0/3.0 || m.InvMatD[8] != 0.25 {
		t.Error("Inverse matrix should have 1/voxelSize on diagonal")
	}

	if m.VecD[0] != 10.0 || m.VecD[1] != 20.0 || m.VecD[2] != 30.0 {
		t.Error("Translation should match origin")
	}
}

func TestWriterGridDataSize(t *testing.T) {
	builder := NewIndexGridBuilder("test")
	builder.AddVoxel(0, 0, 0)
	grid := builder.Build()

	writer := NewWriter()
	_, err := writer.WriteIndexGrid(grid)
	if err != nil {
		t.Fatalf("WriteIndexGrid failed: %v", err)
	}

	data := writer.Bytes()

	// Check that we can read back the magic number
	magic := binary.LittleEndian.Uint64(data[0:8])
	if magic != MagicNumber {
		t.Errorf("Magic number mismatch: got 0x%x, expected 0x%x", magic, MagicNumber)
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[16:20])
	if version != Version {
		t.Errorf("Version mismatch: got %d, expected %d", version, Version)
	}

	// Check grid size matches actual size
	gridSize := binary.LittleEndian.Uint64(data[32:40])
	if gridSize != uint64(len(data)) {
		t.Errorf("Grid size in header (%d) doesn't match actual size (%d)", gridSize, len(data))
	}
}

func TestWriterMapSize(t *testing.T) {
	// Verify Map struct serializes to exactly 264 bytes
	m := NewIdentityMap(Vec3d{1, 1, 1}, Vec3d{0, 0, 0})
	var buf bytes.Buffer

	// Single precision: 9*4 + 9*4 + 3*4 + 4 = 36+36+12+4 = 88 bytes
	// Double precision: 9*8 + 9*8 + 3*8 + 8 = 72+72+24+8 = 176 bytes
	// Total: 88 + 176 = 264 bytes

	for i := 0; i < 9; i++ {
		binary.Write(&buf, ByteOrder, m.MatF[i])
	}
	for i := 0; i < 9; i++ {
		binary.Write(&buf, ByteOrder, m.InvMatF[i])
	}
	for i := 0; i < 3; i++ {
		binary.Write(&buf, ByteOrder, m.VecF[i])
	}
	binary.Write(&buf, ByteOrder, m.TaperF)

	for i := 0; i < 9; i++ {
		binary.Write(&buf, ByteOrder, m.MatD[i])
	}
	for i := 0; i < 9; i++ {
		binary.Write(&buf, ByteOrder, m.InvMatD[i])
	}
	for i := 0; i < 3; i++ {
		binary.Write(&buf, ByteOrder, m.VecD[i])
	}
	binary.Write(&buf, ByteOrder, m.TaperD)

	if buf.Len() != MapSize {
		t.Errorf("Map serializes to %d bytes, expected %d", buf.Len(), MapSize)
	}
}

func TestWriteToFile(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_grid.nvdb")

	// Create a simple grid
	voxels := []Coord{
		{0, 0, 0},
		{1, 1, 1},
		{10, 10, 10},
	}

	err := WriteIndexGridToFile(filename, "test_grid", voxels, Vec3d{1.0, 1.0, 1.0})
	if err != nil {
		t.Fatalf("WriteIndexGridToFile failed: %v", err)
	}

	// Verify file exists and has reasonable size
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}

	// Minimum size: GridData(672) + TreeData(64) + RootNode + some nodes
	if info.Size() < 672+64 {
		t.Errorf("Output file too small: %d bytes", info.Size())
	}

	// Read back and verify magic number
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	magic := binary.LittleEndian.Uint64(data[0:8])
	if magic != MagicNumber {
		t.Errorf("Magic number mismatch in file: got 0x%x, expected 0x%x", magic, MagicNumber)
	}

	t.Logf("Successfully wrote %d bytes to %s", info.Size(), filename)
}

func TestAlignUp32(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{1, 32},
		{31, 32},
		{32, 32},
		{33, 64},
		{64, 64},
		{100, 128},
	}

	for _, tt := range tests {
		result := AlignUp32(tt.input)
		if result != tt.expected {
			t.Errorf("AlignUp32(%d) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}

func TestPopcount64(t *testing.T) {
	tests := []struct {
		input    uint64
		expected int
	}{
		{0, 0},
		{1, 1},
		{0xFF, 8},
		{0xFFFFFFFFFFFFFFFF, 64},
		{0x5555555555555555, 32}, // alternating bits
	}

	for _, tt := range tests {
		result := popcount64(tt.input)
		if result != tt.expected {
			t.Errorf("popcount64(0x%x) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}

func TestLargeGrid(t *testing.T) {
	// Test with a larger grid to verify tree construction
	builder := NewIndexGridBuilder("large_grid")

	// Add a 10x10x10 block of voxels
	for z := int32(0); z < 10; z++ {
		for y := int32(0); y < 10; y++ {
			for x := int32(0); x < 10; x++ {
				builder.AddVoxel(x, y, z)
			}
		}
	}

	grid := builder.Build()

	expectedVoxels := 10 * 10 * 10
	if int(grid.ActiveVoxelCount) != expectedVoxels {
		t.Errorf("Expected %d voxels, got %d", expectedVoxels, grid.ActiveVoxelCount)
	}

	// All 1000 voxels fit in 2 leaf nodes (8^3 each, so need 2 leaves for 10^3)
	// Leaf 1: [0,0,0] to [7,7,7] = 8*8*8 = 512 voxels (but we only have 8*8*8 of them in this range)
	// Leaf 2: [8,0,0] etc.
	// Actually, 10x10x10 cube spans [0,9] in each dimension
	// Leaf origins: [0,0,0], [8,0,0], [0,8,0], [8,8,0], [0,0,8], [8,0,8], [0,8,8], [8,8,8]
	// That's up to 8 leaves for a cube that spans from 0-9 in each dim
	expectedLeaves := 8 // 2^3 leaves needed for 10x10x10 cube
	if len(grid.LeafNodes) != expectedLeaves {
		t.Errorf("Expected %d leaf nodes, got %d", expectedLeaves, len(grid.LeafNodes))
	}

	// Verify serialization works
	writer := NewWriter()
	_, err := writer.WriteIndexGrid(grid)
	if err != nil {
		t.Fatalf("Failed to write large grid: %v", err)
	}

	t.Logf("Large grid: %d voxels, %d leaves, %d bytes",
		grid.ActiveVoxelCount, len(grid.LeafNodes), len(writer.Bytes()))
}

// BenchmarkBuildGrid benchmarks grid construction
func BenchmarkBuildGrid(b *testing.B) {
	// Prepare voxels
	voxels := make([]Coord, 0, 64*64*64)
	for z := int32(0); z < 64; z++ {
		for y := int32(0); y < 64; y++ {
			for x := int32(0); x < 64; x++ {
				voxels = append(voxels, Coord{x, y, z})
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder := NewIndexGridBuilder("bench")
		builder.AddVoxels(voxels)
		builder.Build()
	}
}

// BenchmarkWriteGrid benchmarks grid serialization
func BenchmarkWriteGrid(b *testing.B) {
	// Build a grid once
	builder := NewIndexGridBuilder("bench")
	for z := int32(0); z < 64; z++ {
		for y := int32(0); y < 64; y++ {
			for x := int32(0); x < 64; x++ {
				builder.AddVoxel(x, y, z)
			}
		}
	}
	grid := builder.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := NewWriter()
		writer.WriteIndexGrid(grid)
	}
}
