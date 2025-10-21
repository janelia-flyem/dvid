# DVID System Overview for Claude Code

## What is DVID?

DVID (**D**istributed, **V**ersioned, **I**mage-oriented **D**ataservice) is a specialized database system designed for large-scale scientific data, particularly neural reconstruction and connectomics research. Think of it as "git for scientific data" - it provides branched versioning for teravoxel-scale datasets including 3D image volumes, segmentation data, and point annotations.

## Key Concepts

- **Versioning**: Like git, DVID tracks changes across versions using UUIDs and supports branching
- **Data Types**: Pluggable data types with specialized APIs (keyvalue, labelmap, annotation, etc.)
- **REST API**: HTTP endpoints following `/api/node/{UUID}/{data_name}/...` pattern
- **Storage Backends**: Pluggable storage engines (Badger, Google Cloud, etc.)
- **Scale**: Handles billions of data units and terabytes of data efficiently

## Important Data Types

- **keyvalue**: Simple key-value store for general data (JSON, configs, meshes)
- **labelmap**: 64-bit segmentation volumes with multi-scale support
- **uint8blk**: 3D grayscale image volumes  
- **annotation**: 3D point annotations (synapses, markers) with spatial indexing
- **neuronjson**: JSON data for neurons with optimized queries
- **roi**: Regions of interest using block-based subdivision

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

## Key Documentation Sources

1. **README.md** - High-level overview, installation, and general usage
2. **docs/llms-full.txt** - Comprehensive HTTP API reference for all data types
3. **GitHub Repository**: https://github.com/janelia-flyem/dvid
   - Wiki has detailed documentation
   - Source code with inline help constants
4. **Live API Help**: `/api/help` endpoint on running DVID servers

## Quick Start for Understanding

1. Read the README.md for context and motivation
2. Review docs/llms-full.txt for selective API specifications for important datatype packages
3. Look at datatype packages (e.g., `datatype/keyvalue/`, `datatype/labelmap/`) for implementation details
4. Configuration examples in `scripts/distro-files/`

DVID is actively used for connectomics datasets like the Janelia FlyEM hemibrain and other large-scale neural reconstruction projects.