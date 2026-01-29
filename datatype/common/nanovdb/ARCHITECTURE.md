# Go Native NanoVDB Exporter Architecture

This document describes the design and implementation of DVID's pure Go exporter for NanoVDB files, enabling direct export of label topology and values to the fVDB-compatible format without CGO or external dependencies. Both OnIndex (topology-only) and Int64 (value) grids are supported.

## Motivation

The goal was to export DVID labelmap segmentation data in a format compatible with [fVDB](https://github.com/openvdb/fvdb-core), a GPU-accelerated sparse voxel library. fVDB uses the NanoVDB binary format. Two grid types are supported:

- **OnIndex (IndexGrid)** — stores only topology (which voxels are active) without per-voxel values. Ideal for recording which voxels belong to a label.
- **Int64** — stores signed 64-bit values per voxel. Used for exporting label IDs directly. Since NanoVDB has no UInt64 GridType, DVID's uint64 labels are stored as int64 with overflow checking (values must be ≤ math.MaxInt64).

Key requirements:
1. **Pure Go implementation** - No CGO dependencies, enabling easy cross-compilation and deployment
2. **Binary compatibility** - Output files must be loadable by fVDB and other NanoVDB readers
3. **Memory efficient** - Handle large labels with millions of voxels
4. **Integration with DVID** - Work with DVID's existing labelmap block storage

## Understanding NanoVDB

### Format Overview

NanoVDB is a linearized, GPU-friendly representation of OpenVDB's hierarchical sparse volume structure. The key insight is that NanoVDB uses a static tree structure that can be memory-mapped directly to GPU memory.

The format is based on:
- [NanoVDB.h](https://github.com/AcademySoftwareFoundation/openvdb/blob/master/nanovdb/nanovdb/NanoVDB.h) - The authoritative C++ header (10,000+ lines)
- fVDB source code in `llm-research/fvdb-core/` for understanding IndexGrid specifics

**Important**: The Go GridType constants must match the C++ `nanovdb::GridType` enum exactly. NanoVDB has no UInt64 type; the enum includes Half (9) and UInt32 (10) where earlier versions of this package incorrectly mapped UInt32 and UInt64.

### Tree Structure

NanoVDB uses a B+ tree-like hierarchical structure:

```
Root Node (sparse hash map of children)
    └── Upper Internal Nodes (32³ = 32,768 children each)
            └── Lower Internal Nodes (16³ = 4,096 children each)
                    └── Leaf Nodes (8³ = 512 voxels each)
```

Each level covers progressively finer spatial regions:
- **Upper nodes**: 4096³ voxels (32 × 128 × 128)
- **Lower nodes**: 128³ voxels (16 × 8 × 8)
- **Leaf nodes**: 8³ = 512 voxels

### Grid Type Specifics

#### OnIndex (IndexGrid)

For IndexGrid, we only store topology—no per-voxel values. Each leaf node contains:
- A 512-bit mask indicating which of its 8³ voxels are "active"
- An offset for indexing into external data arrays (used by fVDB for features)

Leaf node size: **96 bytes**.

#### Int64 (Value Grid)

For Int64 grids, each leaf node stores both topology and per-voxel int64 values:
- A 512-bit mask indicating active voxels
- 512 × int64 values (one per voxel slot)
- Min/max/average/stddev statistics

Leaf node size: **4224 bytes**. The values array is `alignas(32)` in C++, requiring 16 bytes of padding before it.

Internal nodes (Upper and Lower) and the Root node have identical binary layouts for both grid types.

### Binary Layout

The file format is:
```
[GridData - 672 bytes]     # Header with metadata, transforms, statistics
[TreeData - 64 bytes]      # Tree structure pointers/counts
[Root tile table]          # Variable size hash map
[Upper nodes]              # 32-byte aligned, contiguous
[Lower nodes]              # 32-byte aligned, contiguous
[Leaf nodes]               # 32-byte aligned, contiguous
```

Critical details:
- **All structures are 32-byte aligned** for GPU memory access
- **Little-endian** byte order throughout
- **Version 32.8.0** was targeted (current as of implementation)

## Architecture

### Package Structure

```
datatype/common/nanovdb/
├── nanovdb.go      # Core types, constants, and data structures
├── indexgrid.go    # Tree builder for OnIndex grids from coordinates
├── int64grid.go    # Tree builder for Int64 value grids from coordinates + values
├── writer.go       # Binary serializer - writes NanoVDB format (both grid types)
└── nanovdb_test.go # Unit tests
```

### Design Decisions

#### 1. Bottom-Up Tree Construction

Rather than implementing a full mutable VDB tree, we build the tree bottom-up from a list of voxel coordinates:

```go
builder := NewIndexGridBuilder("label_123")
builder.AddVoxels(coords)  // Add all active voxel coordinates
grid := builder.Build()    // Construct the tree
```

This approach:
- Avoids complex tree mutation logic
- Is efficient for our use case (we have all coordinates upfront)
- Naturally deduplicates voxels

The build process:
1. **Sort coordinates** by Z, Y, X for cache-friendly access
2. **Remove duplicates** from sorted list
3. **Create leaf nodes** - group voxels by their containing 8³ leaf
4. **Create lower nodes** - group leaves by their containing 16³ region
5. **Create upper nodes** - group lower nodes by their containing 32³ region
6. **Create root node** - collect all upper nodes with their origins

#### 2. Coordinate Hashing for Node Lookup

Each node level uses a map keyed by the node's origin coordinate:

```go
type IndexGridBuilder struct {
    leafMap   map[Coord]*IndexLeafNode
    lowerMap  map[Coord]*IndexLowerNode
    upperMap  map[Coord]*IndexUpperNode
}
```

Origin calculation uses bit masking to find the containing node:
```go
func leafOrigin(x, y, z int32) Coord {
    mask := int32(^(LeafDim - 1))  // 0xFFFFFFF8 for 8³ leaves
    return Coord{x & mask, y & mask, z & mask}
}
```

#### 3. Bitmask Implementation

NanoVDB uses bitmasks to track active voxels/children:
- **Mask512** (64 bytes) - For leaf nodes (8³ = 512 voxels)
- **Mask4096** (512 bytes) - For lower internal nodes (16³ = 4096 children)
- **Mask32768** (4096 bytes) - For upper internal nodes (32³ = 32768 children)

Each mask type implements:
```go
type Mask512 struct {
    Words [8]uint64
}

func (m *Mask512) SetBit(n int)      // Set bit n to 1
func (m *Mask512) GetBit(n int) bool // Check if bit n is set
func (m *Mask512) CountOn() int      // Population count
```

#### 4. Deterministic Output

For reproducibility and testing, the output is deterministic:
- Nodes are collected into slices sorted by origin coordinate
- The sort order is Z, Y, X (matching NanoVDB conventions)

### Serialization

The `Writer` handles binary serialization with careful attention to:

#### Alignment
Every major structure must start at a 32-byte boundary:
```go
func AlignUp32(n int) int {
    return (n + 31) & ^31
}
```

#### Field Layout
Each structure's fields must match the C++ layout exactly. For example, `GridData`:

```go
func (w *Writer) writeGridData(grid *IndexGrid, ...) error {
    // Magic number (8 bytes)
    binary.Write(w.buf, binary.LittleEndian, MagicNumber)

    // Checksum (8 bytes)
    binary.Write(w.buf, binary.LittleEndian, uint64(0))

    // Version (4 bytes)
    binary.Write(w.buf, binary.LittleEndian, Version)

    // ... 672 bytes total, carefully ordered
}
```

#### Node Serialization

OnIndex leaf nodes (96 bytes each):
```
[BBoxMin - 12B] [BBoxDif - 3B] [Flags - 1B]
[ValueMask - 64B] [Padding - 16B]
```

Int64 leaf nodes (4224 bytes each):
```
Offset  Size   Field
0       12     mBBoxMin (3 × int32)
12      3      mBBoxDif (3 × uint8)
15      1      mFlags (uint8)
16      64     mValueMask (8 × uint64)
80      8      mMinimum (int64)
88      8      mMaximum (int64)
96      8      mAverage (float64)
104     8      mStdDevi (float64)
112     16     padding (alignas(32) before mValues)
128     4096   mValues (512 × int64)
Total:  4224 bytes
```

Internal nodes include child offset tables that point to their children's positions in the file.

## Integration with DVID

### Data Flow

```
DVID Labelmap Block Storage
         │
         ▼
┌─────────────────────┐
│ constrainLabelIndex │  Get block coordinates for a label
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│ For each block:     │
│  - Decompress       │
│  - Extract voxels   │  extractLabelVoxelsWithStats()
│  - Add to builder   │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│ IndexGridBuilder    │  Build NanoVDB tree structure
│   .Build()          │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│ Writer              │  Serialize to binary format
│   .WriteIndexGrid() │
└─────────────────────┘
         │
         ▼
      .nvdb file
```

### Block Processing

DVID stores segmentation in compressed 64³ blocks. For each block containing the target label:

1. **Fetch** compressed block from storage
2. **Decompress** using DVID's `DeserializeData`
3. **Unmarshal** into `labels.Block` structure
4. **Expand** to full 64³ label volume via `MakeLabelVolume()`
5. **Scan** all voxels, collecting those matching target supervoxels
6. **Convert** block-relative coordinates to absolute coordinates

```go
blockOriginX := int32(blockCoord[0]) * blockSize[0]  // e.g., block (1,2,3) → voxel (64,128,192)
```

## Testing

### Unit Tests

The `nanovdb_test.go` file includes:
- **Bitmask tests** - SetBit, GetBit, CountOn for all mask sizes
- **Coordinate functions** - Origin calculations, offset mappings
- **Tree building** - Single voxel, multiple voxels, duplicates
- **Serialization** - File writing, size verification, alignment checks

### Integration Testing

The `testing/fvdb/` directory contains an end-to-end verification suite:
1. A Go test generator (`generate_test_grids.go`) produces .nvdb files using this package
2. A C++ verifier (`nvdb_verify.cpp`) reads them back using the official NanoVDB header
3. A shell script (`run_tests.sh`) orchestrates the pipeline

This verifies binary compatibility across 8 test grids (4 OnIndex + 4 Int64), including edge cases like math.MaxInt64.

## Limitations and Future Work

### Current Limitations

1. **Two grid types only** - Only OnIndex and Int64 are supported (no Float, Double, Vec3, etc.)
2. **No blind data** - NanoVDB supports "blind data" attachments; not implemented
3. **No compression** - Output is uncompressed (Blosc compression not implemented)
4. **Single grid per file** - Multi-grid files not supported
5. **No UInt64** - NanoVDB has no UInt64 GridType; uint64 labels > math.MaxInt64 cannot be represented

### Potential Improvements

1. **Streaming construction** - Process voxels without holding all in memory
2. **Parallel building** - Parallelize leaf/node creation
3. **Compression** - Add Blosc compression for smaller files

## References

- [NanoVDB Repository](https://github.com/AcademySoftwareFoundation/openvdb/tree/master/nanovdb)
- [NanoVDB.h Header](https://github.com/AcademySoftwareFoundation/openvdb/blob/master/nanovdb/nanovdb/NanoVDB.h)
- [fVDB Core](https://github.com/openvdb/fvdb-core)
- [OpenVDB Documentation](https://www.openvdb.org/documentation/)
