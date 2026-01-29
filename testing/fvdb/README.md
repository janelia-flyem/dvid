# NanoVDB Verifier

End-to-end verification that the Go NanoVDB writer produces valid .nvdb files readable by the official C++ NanoVDB library.

## Prerequisites

- Go (1.21+)
- CMake (3.14+)
- C++17 compiler (g++ or clang++)
- Internet access (CMake fetches NanoVDB source at configure time)

## Usage

```bash
cd testing/fvdb
bash run_tests.sh
```

The script:
1. Builds a Go program that generates test .nvdb files using the dvid `nanovdb` package
2. Builds a C++ verifier that reads .nvdb files using the official NanoVDB header
3. Runs the verifier on each test file, checking active voxel counts match

## Manual Usage

```bash
# Build the verifier
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Verify a file
./build/nvdb_verify path/to/file.nvdb --expected-voxels 1000
./build/nvdb_verify path/to/file.nvdb --list-voxels
```
