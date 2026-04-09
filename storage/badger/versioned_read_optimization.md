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
- `BenchmarkVersionedRangePipelined`

The pipelined strategy uses one goroutine to stream a key-only range scan and
resolve winning keys, and a second goroutine to fetch winner values
concurrently. This is now the default Badger versioned range-read path.
The `legacy` and single-stage `optimized` strategies remain available for
comparison and benchmarking.

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
go test -tags badger ./storage/badger -run '^$' -bench 'BenchmarkVersionedRange(Baseline|Optimized|Pipelined)$' -benchmem
```

Larger target-server run:

```bash
DVID_BADGER_BENCH_KEYS=200000 \
DVID_BADGER_BENCH_VERSIONS=8 \
DVID_BADGER_BENCH_VALUE_SIZE=8192 \
go test -tags badger ./storage/badger -run '^$' -bench 'BenchmarkVersionedRange' -benchmem
```

## Benchmarking a Real Labelmap Instance

The repository also includes an RPC command that benchmarks the Badger versioned
read path against an existing `labelmap` instance, such as `segmentation`.
This command runs asynchronously and returns immediately. Progress and final
status are written to the DVID server log.

Command form:

```bash
dvid node <UUID> <labelmap-name> benchmark-versioned-read <benchmark-spec.json> [report.json]
```

Example:

```bash
dvid node 28841 segmentation benchmark-versioned-read /tmp/versioned-read-bench.json /tmp/versioned-read-report.json
```

If `report.json` is provided, the final JSON report is written there when the
benchmark completes. If it is omitted, the final summary and report are written
to the server log instead.

Example benchmark spec:

```json
{
  "export_spec_path": "/data/export/info.json",
  "mode": "all",
  "iterations": 3,
  "num_scales": 1,
  "scale": 0,
  "max_strips": 2,
  "max_chunk_rows": 64
}
```

Supported `mode` values:

- `legacy`
- `optimized`
- `pipelined`
- `both`
- `all`

Useful optional filters:

- `scale`: benchmark only one scale from the export spec
- `shard_y`, `shard_z`: benchmark one specific export strip origin in voxels
- `max_strips`: limit how many export strips are scanned
- `max_chunk_rows`: limit how many `(chunkY, chunkZ)` X-row scans are run

Default behavior when omitted:

- if `scale` is omitted, the benchmark runs every scale from `0` through
  `num_scales - 1`
- if `shard_y` and `shard_z` are omitted, the benchmark scans all matching
  strips for the selected scales
- if `max_strips` is omitted or `0`, there is no strip limit
- if `max_chunk_rows` is omitted or `0`, there is no chunk-row limit

### What It Measures on a Real `labelmap`

When the data instance is something like `segmentation`, the benchmark does not
scan arbitrary keys. It reproduces the same kind of versioned block-range reads
used by `export-shards`:

1. It loads the Neuroglancer export volume spec referenced by `export_spec_path`.
2. It computes the shard dimensions for the requested scales exactly the way
   `export-shards` does.
3. For each selected strip and `(chunkY, chunkZ)` row, it constructs the same
   block-key range that `export-shards` would read across X for that scale.
4. It runs the selected Badger versioned read strategy on that range.

One `chunk row` in this benchmark means a single full-X range read for a fixed
`(chunkY, chunkZ)` row of blocks.

For a `labelmap` instance named `segmentation`, this means the benchmark is
measuring reads of `segmentation` block key-values, not metadata and not other
instances in the repo. The returned values are the compressed label blocks that
`export-shards` would later write into shard files.

The report records:

- `elapsed_ms`: wall-clock time for the selected strategy over the chosen scan
  set
- `visible_blocks`: how many visible `labelmap` blocks were returned
- `visible_value_bytes`: total bytes of visible block values returned by the
  strategy
- `scanned_versioned_kvs`: how many raw versioned key-values existed in the same
  range before version filtering
- `logical_keys`: how many distinct logical block keys were scanned
- `avg_versions_per_key`: average stored version fanout in the tested range
- `speedup_vs_legacy`: relative speedup versus the legacy read path for the same
  iteration and scale

This makes the RPC benchmark useful for answering questions that the synthetic
benchmark cannot answer, such as:

- how many rewritten versions exist per block in the actual `segmentation`
  instance
- whether the optimized or pipelined strategy helps on the real export ranges
- whether the benefit changes by scale, shard strip, or rewrite density

## Consistency Model of the Pipelined Default

The current pipelined default uses two separate Badger read-only transactions
for one logical versioned range read:

1. stage 1 scans keys only and resolves the winning versioned key
2. stage 2 fetches values for those winning keys

This is faster for some workloads because key scanning and value fetching can
overlap, but it does not preserve the same single-snapshot semantics as the
legacy and single-stage optimized implementations, which used one Badger read
transaction for both winner selection and value retrieval.

In DVID, this is treated as acceptable eventually consistent read behavior for
the Badger versioned range-read path. The important point is to understand the
tradeoff, not to treat it as an outright correctness bug.

### What This Means

If the database changes between stage 1 and stage 2, a logical range read can
mix information from two snapshots:

- stage 1 may choose a winner using snapshot A
- stage 2 may fetch values using snapshot B

That means the read is not guaranteed to reflect one coherent instant in time.
Instead, it should be understood as eventually consistent.

### DVID-Specific Write Pattern

In DVID mutable stores, versioned writes usually do not delete old historical
key-values from Badger:

- `Put()` for versioned data writes the key for the current version
- ancestor-version keys usually remain in the store
- `Delete()` usually writes a tombstone for the current version instead of
  physically removing all prior versions

So the common pattern is "add or overwrite the current version's key while older
versions remain available for DAG resolution."

### Important Consequence

Even if new data arrives between stage 1 and stage 2, it is not automatically
"the correct winner" for the pipelined read:

- if a write happens in the same mutable version after stage 1 has already
  resolved the winner from ancestor keys, stage 2 will still fetch the old
  winner and miss the newer same-version winner
- if a tombstone is written after stage 1, stage 2 can still return a value
  that should now be hidden
- if a write occurs on a different branch or descendant version that is not
  visible from the requested version context, it should not become the winner at
  all

There is one narrower case where stage 2 can still see the latest data:

- if stage 1 already chose the same physical key and stage 2 reads that same key
  after its value was overwritten in place for the same version

But even then, the read no longer reflects a single coherent snapshot of the
database. It reflects a later value fetch than the key-resolution step.

### Practical Interpretation

- For committed, quiescent export workloads such as `export-shards`, this is
  typically indistinguishable from a snapshot-consistent read because the
  relevant keys are not changing during the read.
- For general mutable versioned reads, DVID accepts this as eventually
  consistent behavior for the pipelined Badger range-read path.
- The tradeoff is deliberate: higher read overlap and throughput in exchange for
  no longer requiring single-snapshot semantics for a versioned range read.

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
