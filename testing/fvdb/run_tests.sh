#!/usr/bin/env bash
# run_tests.sh — Build and run the NanoVDB verifier against test grids.
#
# Prerequisites: cmake, g++ (C++17), Go
# Usage: cd testing/fvdb && bash run_tests.sh
#
# Supports both OnIndex (topology-only) and Int64 (value) grids.
# Manifest format: filename count type [check_values]

set -euo pipefail

# Warn if LDFLAGS contains duplicate -rpath entries (conda activation stacking).
# This doesn't block execution since cmake uses system compiler, but alerts users.
if [ -n "${LDFLAGS:-}" ]; then
    rpath_count=$(echo "$LDFLAGS" | grep -o '\-rpath' | wc -l)
    if [ "$rpath_count" -gt 1 ]; then
        echo "WARNING: LDFLAGS contains $rpath_count -rpath entries (conda activation stacking)." >&2
        echo "This won't affect this script (uses system compiler) but may affect DVID's make." >&2
    fi
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "=== Step 1: Build Go test grid generator ==="
go build -o "$TMPDIR/generate_test_grids" generate_test_grids.go

echo "=== Step 2: Generate test .nvdb files ==="
"$TMPDIR/generate_test_grids" "$TMPDIR/grids"

echo "=== Step 3: Build C++ verifier ==="
cmake -B "$TMPDIR/build" -S . -DCMAKE_BUILD_TYPE=Release
cmake --build "$TMPDIR/build" --parallel

VERIFIER="$TMPDIR/build/nvdb_verify"

echo "=== Step 4: Verify test grids ==="
PASS=0
FAIL=0

while read -r filename expected gridtype checkvalues; do
    filepath="$TMPDIR/grids/$filename"

    # Build verifier arguments
    ARGS="--expected-voxels $expected"
    if [ -n "$checkvalues" ]; then
        ARGS="$ARGS --check-values $checkvalues"
    fi

    echo -n "  $filename ($gridtype, $expected voxels)... "
    if "$VERIFIER" "$filepath" $ARGS > /dev/null 2>&1; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        # Re-run with output visible for diagnostics
        "$VERIFIER" "$filepath" $ARGS || true
        FAIL=$((FAIL + 1))
    fi
done < "$TMPDIR/grids/test_grids.txt"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
