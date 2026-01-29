# DVID System Overview for Claude Code

## What is DVID?

DVID (**D**istributed, **V**ersioned, **I**mage-oriented **D**ataservice) is a specialized database system designed for large-scale scientific data, particularly neural reconstruction and connectomics research. Think of it as "git for scientific data" - it provides branched versioning for teravoxel-scale datasets including 3D image volumes, segmentation data, and point annotations.

## Key Concepts

- **Versioning**: Like git, DVID tracks changes across versions using UUIDs and supports branching
- **Data Types**: Pluggable data types with specialized APIs (keyvalue, labelmap, annotation, etc.)
- **REST API**: HTTP endpoints following `/api/node/{UUID}/{data_name}/...` pattern
- **Storage Backends**: Pluggable storage engines (Badger, Google Cloud, etc.)
- **Scale**: Handles billions of data units and terabytes of data efficiently

## Code Ownership and External Dependencies

All code in this repository is authored by us and is ours to modify. When our code disagrees with an external specification, library, or file format, our code is what needs to change — do not treat discrepancies as upstream bugs. Fix our code to conform to the external authority.

More generally: when working with any well-established external library or specification (e.g., NanoVDB, PyTorch, CUDA, protobuf, standard file formats), assume the external project's core implementation is the source of correctness. If our code produces results that conflict with the external spec, the bug is in our code. This is especially true when we are relatively new users of that library or format.

## Data Types

Each data type is a separate package in `datatype/`. Each package's main Go file contains a `helpMessage` const with detailed API documentation.

### Segmentation and Label Data

- **labelmap** (`datatype/labelmap/`) — The primary 64-bit segmentation type. Stores label volumes with multi-scale support, supervoxel management, merge/split operations, label indexing, and sparse volume queries. Syncs with annotation and labelsz data. This is the most complex and actively used label type.
- **labelblk** (`datatype/labelblk/`) — Older block-aligned 64-bit label volumes. Supports raw/isotropic retrieval and pseudo-coloring. Can sync with labelvol for sparse representations.
- **labelarray** (`datatype/labelarray/`) — Array-based label storage with multi-scale downsampling and label indexing. Similar capabilities to labelmap.
- **labelvol** (`datatype/labelvol/`) — Sparse label volumes using run-length encoding (RLE). Provides memory-efficient coarse (block-level) sparse volume representations with merge/split and area deletion.
- **labelsz** (`datatype/labelsz/`) — Ranks labels by annotation counts. Maintains per-label statistics on annotation types (PreSyn, PostSyn, Gap, Note). Syncs with annotation data. Supports threshold-based queries and top-N ranking.
- **tarsupervoxels** (`datatype/tarsupervoxels/`) — Binary data blobs keyed by supervoxel ID, typically meshes. Stores data in tar format with bulk loading and missing-supervoxel queries.

### Image Data

- **imageblk** (`datatype/imageblk/`) — Multi-channel image volumes supporting various bit depths (uint8, uint16, uint32, uint64, float32). Provides raw and isotropic image retrieval with multi-scale support.
- **imagetile** (`datatype/imagetile/`) — Pre-computed multi-resolution image tiles in XY, XZ, and YZ orientations. Stores tiles as PNG, JPG, or LZ4 format for fast retrieval.
- **multichan16** (`datatype/multichan16/`) — Multi-channel 16-bit fluorescence image data with channels stored separately (not interleaved). Supports up to 3 channels composited into RGBA. Can load from V3D Raw format.
- **googlevoxels** (`datatype/googlevoxels/`) — Proxy to Google BrainMaps API for multi-scale image tiles and volumes. Handles OAuth2 authentication via JSON Web Token.

### Annotations and Metadata

- **annotation** (`datatype/annotation/`) — 3D point annotations (synapses, markers) with spatial indexing. Supports querying by location or label, relationships between annotations, and syncing with label data types.
- **neuronjson** (`datatype/neuronjson/`) — JSON data for neurons with optimized field-level queries. Manages structured neuron metadata with conditional queries and schema support.
- **keyvalue** (`datatype/keyvalue/`) — Generic key-value store for arbitrary binary data (JSON, configs, meshes). Supports individual key operations, batch operations, and range queries.
- **roi** (`datatype/roi/`) — Regions of interest defined as block-level RLE spans. Supports point-in-ROI queries, binary mask generation, and partitioning for distributed processing.

### Common Packages (`datatype/common/`)

- **labels** — Compressed label block handling, label index structures, merge/split operations shared across label types
- **downres** — Multi-scale downsampling shared across data types
- **nanovdb** — Pure Go NanoVDB binary format writer for exporting to fVDB (see `ARCHITECTURE.md` in that package)
- **proto** — Protocol buffer definitions for label indices

## API Structure

Most client data access follows the pattern:
```
/api/node/{UUID}/{data_instance_name}/{operation}
```

Where:
- `UUID`: Version identifier (can use ":master" for latest)
- `data_instance_name`: Name of the data instance
- `operation`: Specific operation (info, keys, raw data, etc.)

See the `helpMessage` const strings in each package's main Go file, e.g.
`datatype/labelmap/labelmap.go` or `datatype/keyvalue/keyvalue.go`.

There are also other data access patterns for server metadata, etc:
```
/api/server/...
```

See the embedded documentation string `webHelp` in `server/web.go`.

## When Working with DVID Code

1. **Go-based**: Written in Go with embedded C libraries for performance
2. **Modular**: Data types are separate packages in `datatype/` directory
3. **HTTP-centric**: Most functionality exposed via REST API
4. **Storage-agnostic**: Can use different backends for different data types
5. **Testing**: Run `make test` (requires `GOPATH` set) to validate changes. This runs `go test` with the required build tags (`badger filestore ngprecomputed`).

## Key Documentation Sources

1. **README.md** - High-level overview, installation, and general usage
2. **docs/llms-full.txt** - DVID overview, core concepts, all datatype summaries, and server/repo HTTP APIs
3. **docs/datatype-*.txt** - Per-datatype HTTP API reference (extracted from helpMessage consts). Covers keyvalue, neuronjson, imageblk, labelmap, annotation, roi, labelblk, labelvol, labelsz, and tarsupervoxels
4. **helpMessage consts** - Each datatype package's main Go file has a `helpMessage` const with the authoritative API docs for that type
5. **GitHub Repository**: https://github.com/janelia-flyem/dvid
   - Wiki has detailed documentation
6. **Live API Help**: `/api/help` endpoint on running DVID servers

## Quick Start for Understanding

1. Read the README.md for context and motivation
2. Review docs/llms-full.txt for selective API specifications for important datatype packages
3. Look at datatype packages (e.g., `datatype/keyvalue/`, `datatype/labelmap/`) for implementation details
4. Configuration examples in `scripts/distro-files/`

DVID is actively used for connectomics datasets like the Janelia FlyEM hemibrain and other large-scale neural reconstruction projects.

## Conda Environment Safety

The `dvid` conda environment includes `go-cgo`, which depends on conda's clang
compiler packages. These packages inject `-Wl,-rpath,${CONDA_PREFIX}/lib` into
`LDFLAGS` via activation scripts. DVID's Makefile accounts for this and avoids
adding a duplicate rpath.

NEVER install additional C/C++ compiler packages into the `dvid` conda env beyond
what go-cgo already pulls in. For C++ compilation needs (e.g.,
`testing/fvdb/nvdb_verify.cpp`), use the system compiler (Xcode Command Line
Tools on macOS, system g++ on Linux) or a separate conda environment.