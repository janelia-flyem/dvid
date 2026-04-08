# Badger Versioned Read Optimization

## Summary

Badger versioned reads in DVID store branch/version information in the user key, not in Badger's internal MVCC timestamp. That means a versioned read still has to scan the full key family for a logical key to determine which stored version is visible from the requested UUID.

The optimization implemented here changes the hot range-read path so it no longer reads values for losing versions.

## Current vs New `ProcessRange` Behavior

Previous behavior for versioned range reads:

1. Iterate all stored versions for a logical key.
2. Read every value with `ValueCopy`.
3. Use the DVID version DAG to choose the visible version.
4. Discard all non-winning values.

New behavior:

1. Iterate all stored versions for a logical key with `PrefetchValues=false`.
2. Resolve the visible winner from the grouped keys only.
3. Fetch exactly one value for the winning key.
4. Emit only that one result.

This preserves key layout compatibility with all existing DVID Badger datasets.

## Why This Might Help

This change is aimed at workloads like `labelmap` export where `ProcessRange` scans many large values, often compressed label blocks, and historical versions are frequently discarded after DAG filtering.

Expected benefit:

- lower value-log traffic
- less copying of large discarded values
- lower allocation pressure in versioned range scans

Expected non-benefit:

- it does not reduce version-key scans
- it does not change the primary on-disk format
- it may show smaller gains on fast NVMe systems when most logical keys only have one stored version

## Benchmarking

Synthetic benchmarks live in `storage/badger/badger_test.go` and create a temporary Badger database populated with versioned keys using the real DVID key layout.

Provided benchmarks:

- `BenchmarkVersionedRangeBaseline`
- `BenchmarkVersionedRangeOptimized`

The synthetic benchmark parameters can be adjusted with environment variables:

- `DVID_BADGER_BENCH_KEYS`
- `DVID_BADGER_BENCH_VERSIONS`
- `DVID_BADGER_BENCH_VALUE_SIZE`

Recommended synthetic value sizes based on `test_data/export.log` from the male CNS export:

- scale 0 representative: `4096`, `8192`, `16384`
- scale 1 representative: `16384`
- scale 2 representative: `32768`

The export log shows scale 0 compressed block sizes concentrated in the `2-16 KB`
range with a mean of `5.22 KB`, so `8 KB` is a reasonable default synthetic size
for export-shards-like testing.

Example:

```bash
go test -tags badger ./storage/badger -run '^$' -bench 'BenchmarkVersionedRange' -benchmem
```

Larger target-server run:

```bash
DVID_BADGER_BENCH_KEYS=200000 \
DVID_BADGER_BENCH_VERSIONS=8 \
DVID_BADGER_BENCH_VALUE_SIZE=8192 \
go test -tags badger ./storage/badger -run '^$' -bench 'BenchmarkVersionedRange' -benchmem
```

## Interpreting Results

Local workstation results are useful for:

- checking correctness
- detecting regressions
- seeing broad trend direction

They are not authoritative for deployment decisions.

The target Linux server with the intended NVMe layout is the authoritative benchmark platform because Badger's iterator and value-log costs can shift materially with:

- SSD topology
- filesystem and RAID behavior
- CPU/cache characteristics
- memory bandwidth

Success criteria should focus on target-server measurements, especially:

- elapsed time
- `value_bytes/op`
- `version_keys/op`
- `allocs/op`
