// nvdb_verify.cpp — Standalone NanoVDB file verifier.
//
// Reads a .nvdb file using NanoVDB's readGrids() and reports
// grid metadata. Optionally asserts expected active voxel count.
// Supports both OnIndex (topology-only) and Int64 (value) grids.
//
// Usage:
//   nvdb_verify <file.nvdb> [--expected-voxels N] [--list-voxels] [--check-values V1,V2,...]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <sstream>

#include <nanovdb/NanoVDB.h>
#include <nanovdb/io/IO.h>

static const char* gridTypeName(nanovdb::GridType t) {
    switch (t) {
        case nanovdb::GridType::Float:      return "Float";
        case nanovdb::GridType::Double:     return "Double";
        case nanovdb::GridType::Int16:      return "Int16";
        case nanovdb::GridType::Int32:      return "Int32";
        case nanovdb::GridType::Int64:      return "Int64";
        case nanovdb::GridType::UInt32:     return "UInt32";
        case nanovdb::GridType::Index:      return "Index";
        case nanovdb::GridType::OnIndex:    return "OnIndex";
        case nanovdb::GridType::IndexMask:  return "IndexMask";
        case nanovdb::GridType::OnIndexMask:return "OnIndexMask";
        default:                            return "Unknown";
    }
}

static const char* gridClassName(nanovdb::GridClass c) {
    switch (c) {
        case nanovdb::GridClass::Unknown:     return "Unknown";
        case nanovdb::GridClass::LevelSet:    return "LevelSet";
        case nanovdb::GridClass::FogVolume:   return "FogVolume";
        case nanovdb::GridClass::Staggered:   return "Staggered";
        case nanovdb::GridClass::PointIndex:  return "PointIndex";
        case nanovdb::GridClass::PointData:   return "PointData";
        case nanovdb::GridClass::Topology:    return "Topology";
        case nanovdb::GridClass::VoxelVolume: return "VoxelVolume";
        case nanovdb::GridClass::IndexGrid:   return "IndexGrid";
        case nanovdb::GridClass::TensorGrid:  return "TensorGrid";
        default:                              return "Unknown";
    }
}

static std::vector<int64_t> parseValues(const char* str) {
    std::vector<int64_t> values;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        values.push_back(std::stoll(item));
    }
    return values;
}

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s <file.nvdb> [--expected-voxels N] [--list-voxels] [--check-values V1,V2,...]\n", prog);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  --expected-voxels N   Assert the active voxel count equals N\n");
    fprintf(stderr, "  --list-voxels         Print all active voxel coordinates (and values for Int64)\n");
    fprintf(stderr, "  --check-values V,...  For Int64 grids, verify these values exist (comma-separated)\n");
    exit(1);
}

int main(int argc, char** argv) {
    if (argc < 2) usage(argv[0]);

    std::string filepath;
    int64_t expectedVoxels = -1;
    bool listVoxels = false;
    std::vector<int64_t> checkValues;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--expected-voxels") == 0) {
            if (++i >= argc) usage(argv[0]);
            expectedVoxels = atoll(argv[i]);
        } else if (strcmp(argv[i], "--list-voxels") == 0) {
            listVoxels = true;
        } else if (strcmp(argv[i], "--check-values") == 0) {
            if (++i >= argc) usage(argv[0]);
            checkValues = parseValues(argv[i]);
        } else if (argv[i][0] == '-') {
            usage(argv[0]);
        } else {
            filepath = argv[i];
        }
    }
    if (filepath.empty()) usage(argv[0]);

    // Read the file
    std::vector<nanovdb::GridHandle<>> handles;
    try {
        handles = nanovdb::io::readGrids(filepath);
    } catch (const std::exception& e) {
        fprintf(stderr, "ERROR: Failed to read '%s': %s\n", filepath.c_str(), e.what());
        return 1;
    }

    if (handles.empty()) {
        fprintf(stderr, "ERROR: No grids found in '%s'\n", filepath.c_str());
        return 1;
    }

    printf("File: %s\n", filepath.c_str());
    printf("Grid count: %zu\n", handles.size());

    bool ok = true;

    for (size_t gi = 0; gi < handles.size(); gi++) {
        auto& handle = handles[gi];
        auto* meta = handle.gridMetaData();
        if (!meta) {
            fprintf(stderr, "ERROR: Grid %zu has null metadata\n", gi);
            ok = false;
            continue;
        }

        printf("\n--- Grid %zu ---\n", gi);
        printf("  Name:       %s\n", meta->shortGridName());
        printf("  Type:       %s\n", gridTypeName(meta->gridType()));
        printf("  Class:      %s\n", gridClassName(meta->gridClass()));
        printf("  Version:    %u.%u.%u\n",
               meta->version().getMajor(),
               meta->version().getMinor(),
               meta->version().getPatch());

        auto bbox = meta->indexBBox();
        printf("  BBox:       (%d,%d,%d) -> (%d,%d,%d)\n",
               bbox.min()[0], bbox.min()[1], bbox.min()[2],
               bbox.max()[0], bbox.max()[1], bbox.max()[2]);

        printf("  Node counts: leaf=%u lower=%u upper=%u\n",
               meta->nodeCount(0), meta->nodeCount(1), meta->nodeCount(2));

        uint64_t activeVoxels = meta->activeVoxelCount();
        printf("  Active voxels: %lu\n", (unsigned long)activeVoxels);

        // Handle OnIndex grids
        if (meta->gridType() == nanovdb::GridType::OnIndex) {
            auto* grid = handle.grid<nanovdb::ValueOnIndex>();
            if (!grid) {
                fprintf(stderr, "  ERROR: Could not access OnIndex grid data\n");
                ok = false;
                continue;
            }

            // Count active voxels by iterating leaf nodes
            auto& tree = grid->tree();
            uint64_t leafVoxelCount = 0;
            auto nodeCount0 = tree.nodeCount(0);

            for (uint32_t i = 0; i < nodeCount0; i++) {
                auto& leaf = tree.template getFirstNode<0>()[i];
                auto count = leaf.valueMask().countOn();
                leafVoxelCount += count;
            }

            printf("  Leaf voxel sum: %lu\n", (unsigned long)leafVoxelCount);
            if (leafVoxelCount != activeVoxels) {
                fprintf(stderr, "  ERROR: Leaf voxel sum (%lu) != header active count (%lu)\n",
                        (unsigned long)leafVoxelCount, (unsigned long)activeVoxels);
                ok = false;
            }

            if (listVoxels) {
                printf("  Active voxel coordinates:\n");
                for (uint32_t i = 0; i < nodeCount0; i++) {
                    auto& leaf = tree.template getFirstNode<0>()[i];
                    auto origin = leaf.origin();
                    for (int z = 0; z < 8; z++) {
                        for (int y = 0; y < 8; y++) {
                            for (int x = 0; x < 8; x++) {
                                uint32_t n = (z << 6) | (y << 3) | x;
                                if (leaf.valueMask().isOn(n)) {
                                    printf("    (%d, %d, %d)\n", origin[0]+x, origin[1]+y, origin[2]+z);
                                }
                            }
                        }
                    }
                }
            }
        }
        // Handle Int64 grids
        else if (meta->gridType() == nanovdb::GridType::Int64) {
            auto* grid = handle.grid<int64_t>();
            if (!grid) {
                fprintf(stderr, "  ERROR: Could not access Int64 grid data\n");
                ok = false;
                continue;
            }

            auto& tree = grid->tree();
            uint64_t leafVoxelCount = 0;
            auto nodeCount0 = tree.nodeCount(0);

            // Collect values for verification
            std::vector<int64_t> foundValues;

            for (uint32_t i = 0; i < nodeCount0; i++) {
                auto& leaf = tree.template getFirstNode<0>()[i];
                auto count = leaf.valueMask().countOn();
                leafVoxelCount += count;
            }

            printf("  Leaf voxel sum: %lu\n", (unsigned long)leafVoxelCount);
            if (leafVoxelCount != activeVoxels) {
                fprintf(stderr, "  ERROR: Leaf voxel sum (%lu) != header active count (%lu)\n",
                        (unsigned long)leafVoxelCount, (unsigned long)activeVoxels);
                ok = false;
            }

            // Print min/max from tree stats
            auto& root = tree.root();
            printf("  Value range: min=%ld max=%ld\n", (long)root.minimum(), (long)root.maximum());

            if (listVoxels) {
                printf("  Active voxels with values:\n");
                for (uint32_t i = 0; i < nodeCount0; i++) {
                    auto& leaf = tree.template getFirstNode<0>()[i];
                    auto origin = leaf.origin();
                    for (int z = 0; z < 8; z++) {
                        for (int y = 0; y < 8; y++) {
                            for (int x = 0; x < 8; x++) {
                                uint32_t n = (z << 6) | (y << 3) | x;
                                if (leaf.valueMask().isOn(n)) {
                                    auto coord = nanovdb::Coord(origin[0]+x, origin[1]+y, origin[2]+z);
                                    int64_t value = leaf.getValue(n);
                                    printf("    (%d, %d, %d) = %ld\n", coord[0], coord[1], coord[2], (long)value);
                                    foundValues.push_back(value);
                                }
                            }
                        }
                    }
                }
            } else if (!checkValues.empty()) {
                // Collect values without printing
                for (uint32_t i = 0; i < nodeCount0; i++) {
                    auto& leaf = tree.template getFirstNode<0>()[i];
                    for (int z = 0; z < 8; z++) {
                        for (int y = 0; y < 8; y++) {
                            for (int x = 0; x < 8; x++) {
                                uint32_t n = (z << 6) | (y << 3) | x;
                                if (leaf.valueMask().isOn(n)) {
                                    foundValues.push_back(leaf.getValue(n));
                                }
                            }
                        }
                    }
                }
            }

            // Verify expected values exist
            if (!checkValues.empty()) {
                for (int64_t expected : checkValues) {
                    bool found = false;
                    for (int64_t v : foundValues) {
                        if (v == expected) {
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        printf("  PASS: Found expected value %ld\n", (long)expected);
                    } else {
                        fprintf(stderr, "  FAIL: Expected value %ld not found\n", (long)expected);
                        ok = false;
                    }
                }
            }
        }

        // Check expected voxel count
        if (expectedVoxels >= 0) {
            if ((int64_t)activeVoxels != expectedVoxels) {
                fprintf(stderr, "  FAIL: Expected %ld active voxels, got %lu\n",
                        (long)expectedVoxels, (unsigned long)activeVoxels);
                ok = false;
            } else {
                printf("  PASS: Active voxel count matches expected (%ld)\n", (long)expectedVoxels);
            }
        }
    }

    return ok ? 0 : 1;
}
