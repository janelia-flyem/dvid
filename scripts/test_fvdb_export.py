#!/usr/bin/env python3
"""
Test script to verify DVID's fVDB/NanoVDB export functionality.

This script tests that IndexGrid files produced by DVID's /fvdb endpoint
can be correctly loaded by fVDB and contain the expected voxel topology.

Requirements:
    - fvdb (pip install fvdb or conda install from appropriate channel)
    - requests
    - numpy

Usage:
    # Test with a running DVID server
    python test_fvdb_export.py --server localhost:8000 --uuid abc123 --data segmentation --label 12345

    # Test with a local .nvdb file
    python test_fvdb_export.py --file /path/to/label.nvdb

    # Generate a test file from Go and verify
    python test_fvdb_export.py --test-go-output /path/to/test.nvdb
"""

import argparse
import sys
import tempfile
import os

def check_imports():
    """Check that required packages are available."""
    missing = []

    try:
        import fvdb
    except ImportError:
        missing.append("fvdb")

    try:
        import numpy as np
    except ImportError:
        missing.append("numpy")

    try:
        import requests
    except ImportError:
        missing.append("requests")

    if missing:
        print(f"Missing required packages: {', '.join(missing)}")
        print("\nTo install fVDB, follow instructions at: https://github.com/openvdb/fvdb-core")
        print("For other packages: pip install " + " ".join(missing))
        return False
    return True


def test_nvdb_file(filepath: str, expected_voxel_count: int = None, verbose: bool = True):
    """
    Test that a .nvdb file can be loaded and contains valid data.

    Args:
        filepath: Path to the .nvdb file
        expected_voxel_count: If provided, verify this many voxels are present
        verbose: Print detailed information

    Returns:
        True if test passes, False otherwise
    """
    import fvdb
    import numpy as np

    if verbose:
        print(f"\nTesting NanoVDB file: {filepath}")
        print("-" * 60)

    # Check file exists
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        return False

    file_size = os.path.getsize(filepath)
    if verbose:
        print(f"File size: {file_size} bytes")

    # Try to load the file
    try:
        grid = fvdb.load(filepath)
    except Exception as e:
        print(f"ERROR: Failed to load NanoVDB file: {e}")
        return False

    if verbose:
        print(f"Successfully loaded grid")
        print(f"Grid type: {type(grid)}")

    # Get grid statistics
    try:
        # Get active voxel coordinates
        ijk = grid.ijk  # Tensor of active voxel coordinates
        num_voxels = ijk.jdata.shape[0] if hasattr(ijk, 'jdata') else len(ijk)

        if verbose:
            print(f"Active voxel count: {num_voxels}")

            if num_voxels > 0:
                # Get bounding box
                coords = ijk.jdata.cpu().numpy() if hasattr(ijk, 'jdata') else np.array(ijk)
                if len(coords) > 0:
                    min_coord = coords.min(axis=0)
                    max_coord = coords.max(axis=0)
                    print(f"Bounding box: ({min_coord[0]}, {min_coord[1]}, {min_coord[2]}) to "
                          f"({max_coord[0]}, {max_coord[1]}, {max_coord[2]})")

                    # Show some sample coordinates
                    print(f"Sample coordinates (first 5):")
                    for i, coord in enumerate(coords[:5]):
                        print(f"  [{i}]: ({coord[0]}, {coord[1]}, {coord[2]})")

        # Verify expected count if provided
        if expected_voxel_count is not None:
            if num_voxels != expected_voxel_count:
                print(f"ERROR: Expected {expected_voxel_count} voxels, got {num_voxels}")
                return False
            if verbose:
                print(f"Voxel count matches expected: {expected_voxel_count}")

        return True

    except Exception as e:
        print(f"ERROR: Failed to read grid data: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_dvid_endpoint(server: str, uuid: str, data: str, label: int,
                       supervoxels: bool = False, verbose: bool = True):
    """
    Test the DVID /fvdb endpoint by fetching a label and verifying the output.

    Args:
        server: DVID server address (e.g., "localhost:8000")
        uuid: UUID or branch specification
        data: Name of the labelmap data instance
        label: Label ID to export
        supervoxels: If True, interpret label as supervoxel ID
        verbose: Print detailed information

    Returns:
        True if test passes, False otherwise
    """
    import requests

    # Build URL
    url = f"http://{server}/api/node/{uuid}/{data}/fvdb/{label}"
    params = {}
    if supervoxels:
        params["supervoxels"] = "true"

    if verbose:
        print(f"\nTesting DVID endpoint: {url}")
        print("-" * 60)

    # Make request
    try:
        response = requests.get(url, params=params, timeout=300)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to connect to DVID: {e}")
        return False

    if response.status_code == 404:
        print(f"Label {label} not found (404)")
        return False

    if response.status_code != 200:
        print(f"ERROR: DVID returned status {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return False

    if verbose:
        print(f"Received {len(response.content)} bytes")

    # Save to temp file and test
    with tempfile.NamedTemporaryFile(suffix=".nvdb", delete=False) as f:
        f.write(response.content)
        temp_path = f.name

    try:
        result = test_nvdb_file(temp_path, verbose=verbose)
    finally:
        os.unlink(temp_path)

    return result


def test_known_grid():
    """
    Test with a programmatically created grid to verify basic functionality.
    """
    import fvdb
    import torch

    print("\nCreating test grid with known voxels...")
    print("-" * 60)

    # Create a simple 10x10x10 cube of voxels at origin
    coords = []
    for z in range(10):
        for y in range(10):
            for x in range(10):
                coords.append([x, y, z])

    ijk = torch.tensor(coords, dtype=torch.int32)

    # Create grid from coordinates
    grid = fvdb.GridBatch(device="cpu")
    grid.set_from_ijk(ijk.unsqueeze(0))  # Add batch dimension

    print(f"Created grid with {len(coords)} voxels")
    print(f"Grid active voxel count: {grid.total_voxels}")

    # Save and reload to verify round-trip
    with tempfile.NamedTemporaryFile(suffix=".nvdb", delete=False) as f:
        temp_path = f.name

    try:
        grid.save(temp_path)
        print(f"Saved grid to {temp_path}")

        # Reload and verify
        loaded = fvdb.load(temp_path)
        loaded_count = loaded.total_voxels

        if loaded_count == len(coords):
            print(f"Round-trip successful: {loaded_count} voxels")
            return True
        else:
            print(f"ERROR: Round-trip failed: expected {len(coords)}, got {loaded_count}")
            return False
    finally:
        os.unlink(temp_path)


def main():
    parser = argparse.ArgumentParser(
        description="Test DVID's fVDB/NanoVDB export functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # DVID server options
    parser.add_argument("--server", help="DVID server address (e.g., localhost:8000)")
    parser.add_argument("--uuid", help="UUID or branch specification")
    parser.add_argument("--data", help="Name of labelmap data instance")
    parser.add_argument("--label", type=int, help="Label ID to export")
    parser.add_argument("--supervoxels", action="store_true",
                        help="Interpret label as supervoxel ID")

    # Local file options
    parser.add_argument("--file", help="Test a local .nvdb file")
    parser.add_argument("--expected-voxels", type=int,
                        help="Expected number of voxels in the file")

    # Test options
    parser.add_argument("--test-fvdb", action="store_true",
                        help="Run basic fVDB functionality test")
    parser.add_argument("--quiet", action="store_true",
                        help="Reduce output verbosity")

    args = parser.parse_args()

    # Check imports
    if not check_imports():
        return 1

    verbose = not args.quiet
    success = True

    # Run fVDB basic test if requested
    if args.test_fvdb:
        print("\n" + "=" * 60)
        print("Running fVDB basic functionality test")
        print("=" * 60)
        if not test_known_grid():
            success = False

    # Test local file if provided
    if args.file:
        print("\n" + "=" * 60)
        print("Testing local NanoVDB file")
        print("=" * 60)
        if not test_nvdb_file(args.file, args.expected_voxels, verbose):
            success = False

    # Test DVID endpoint if server info provided
    if args.server and args.uuid and args.data and args.label:
        print("\n" + "=" * 60)
        print("Testing DVID /fvdb endpoint")
        print("=" * 60)
        if not test_dvid_endpoint(args.server, args.uuid, args.data, args.label,
                                  args.supervoxels, verbose):
            success = False
    elif args.server or args.uuid or args.data or args.label:
        print("ERROR: Must provide all of --server, --uuid, --data, and --label together")
        return 1

    # If no specific test requested, show help
    if not (args.test_fvdb or args.file or args.server):
        print("No test specified. Use --help for options.")
        print("\nQuick tests:")
        print("  --test-fvdb           Run basic fVDB round-trip test")
        print("  --file PATH           Test a local .nvdb file")
        print("  --server/--uuid/...   Test DVID endpoint")
        return 0

    # Summary
    print("\n" + "=" * 60)
    if success:
        print("All tests PASSED")
    else:
        print("Some tests FAILED")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
