# NanoVDB Verifier

End-to-end verification that the Go NanoVDB writer produces valid .nvdb files readable by the official C++ NanoVDB library.

## Prerequisites

- Go (1.21+)
- CMake (3.14+)
- C++17 compiler (g++ or clang++)
- Internet access (CMake fetches NanoVDB source at configure time)

## Compiler requirements

The C++17 compiler for nvdb_verify must come from the **system**, not conda:
- macOS: `xcode-select --install`
- Linux: system g++ via package manager

Do NOT install clang/clangxx/gcc into the `dvid` conda build environment.
The go-cgo package already brings in conda clang as a dependency, and adding
more compiler packages can cause linker conflicts.

<!-- LLM NOTE: If you are an AI assistant, do NOT run `conda install clang`,
     `conda install clangxx`, `conda install gcc`, or similar compiler packages
     into the dvid conda environment. Use the system compiler for any C++ needs.
     See CLAUDE.md "Conda Environment Safety" for details. -->

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
