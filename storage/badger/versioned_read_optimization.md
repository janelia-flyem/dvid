# Badger Versioned Read Optimization

## Summary

This document records what we have learned so far about Badger-backed
versioned reads in DVID and the current plan for the next implementation pass.

The original optimization idea was:

1. scan versioned keys only
2. resolve the visible winner from the DAG
3. fetch only the winning value

That idea is still valid in principle, but the real-server benchmark on a large
`labelmap` instance showed that it is not the main win for `export-shards`
style block reads. The dominant win came from pipelining the read path, not
from avoiding losing-version value reads.

## What We Measured

We benchmarked a real `labelmap` instance (`segmentation`) on the target
deployment server using the `benchmark-versioned-read` RPC command.

Representative result for scale 0 block reads:

- sampled non-empty strips: `6`
- sampled non-empty chunk rows: `4375`
- visible blocks returned: `2,325,181`
- visible value bytes: `10,815,326,209`
- scanned versioned KVs: `2,327,789`
- logical keys: `2,325,181`
- average versions per key: `1.00112`

The key point is that version fanout in the sampled block ranges was extremely
low. Almost every logical block key had exactly one stored version in the tested
regions.

Measured warm-run behavior:

- `legacy`: about `54-57s`
- `optimized`: about `63-66s`
- `pipelined`: about `32-33s`

Interpretation:

- single-stage keys-first optimization was not a win for this workload
- pipelining was a large win
- the workload is not dominated by historical-version fanout
- the workload appears dominated by read-path overlap / streaming behavior

## Current State

Three Badger versioned range-read strategies now exist:

- `legacy`
  - read all values for all stored versions
  - resolve winner after values are loaded
- `optimized`
  - scan keys first
  - resolve winner
  - fetch only the winning value
- `pipelined`
  - stage 1 scans keys and resolves winner keys
  - stage 2 fetches winner values concurrently

The current default Badger versioned range-read path is `pipelined`.

The `GetVersion()` regression discovered by `labelmap` tests has already been
fixed by restoring its original `GetBestVersion(keys)` behavior.

## What We Learned

### 1. For block reads, pipeline overlap matters more than keys-first pruning

For `export-shards` style block reads on the tested dataset, the keys-first
optimization does not help much because there are almost no losing historical
versions to prune.

The pipelined path helps because it keeps reading and processing overlapped.

### 2. Keys-first optimization is workload-dependent

The keys-first approach should still help when:

- many logical keys have multiple stored versions
- values are large enough that loading losing versions is expensive
- winner selection eliminates a meaningful fraction of value reads

That likely describes some workloads better than `export-shards` block scans.

### 3. One optimization may not fit all versioned read types

The real-server block benchmark strongly suggests that block reads and label
index reads should be considered separately.

Potentially different read classes:

- block-range reads used by `export-shards`
- label index lookups and related range scans
- other metadata-like versioned reads

These may want different internal strategies.

## Consistency Model of the Current Pipeline

The current pipelined default uses two Badger read-only transactions for one
logical versioned range read:

1. stage 1 scans keys and resolves winner keys
2. stage 2 fetches values for those winners

This does not preserve strict single-snapshot semantics. DVID currently accepts
that as eventually consistent read behavior for this path.

Practical interpretation:

- for committed, quiescent export workloads, this is acceptable
- for mutable reads, the path may observe winner selection and value fetch from
  slightly different snapshots

DVID’s write pattern matters here:

- versioned `Put()` usually adds a new current-version key and leaves older
  version keys in place
- versioned `Delete()` usually writes a tombstone instead of physically removing
  all old versions

So the pipeline should be understood as an eventual-consistency throughput
tradeoff, not as a strict snapshot-preserving implementation.

## Benchmarking a Real Labelmap Instance

The RPC command is:

```bash
dvid node <UUID> <labelmap-name> benchmark-versioned-read <benchmark-spec.json> [report.json]
```

Example:

```bash
dvid node 28841 segmentation benchmark-versioned-read /tmp/versioned-read-bench.json /tmp/versioned-read-report.json
```

The command runs asynchronously:

- it returns immediately
- progress is written to the DVID server log
- the final JSON report is written to `report.json` if provided
- otherwise the final report is written to the server log

Example benchmark spec:

```json
{
  "workload": "all",
  "export_spec_path": "/data/export/info.json",
  "mode": "all",
  "iterations": 3,
  "num_scales": 1,
  "scale": 0,
  "max_strips": 10,
  "max_labels": 1024
}
```

Supported `workload` values:

- `blocks`
- `indices`
- `all`

Supported `mode` values:

- `legacy`
- `optimized`
- `pipelined`
- `both`
- `all`

Useful optional filters:

- `scale`
  - benchmark only one scale from the export spec
- `shard_y`, `shard_z`
  - benchmark one specific strip origin in voxels
- `max_strips`
  - limit how many non-empty strips are benchmarked
- `max_chunk_rows`
  - limit how many non-empty `(chunkY, chunkZ)` X-row scans are benchmarked
- `max_labels`
  - limit how many visible label indices are sampled for the `indices`
    workload

Default behavior when omitted:

- omitted `workload` means benchmark both `blocks` and `indices`
- omitted `scale` means benchmark scales `0` through `num_scales - 1`
- omitted `shard_y`/`shard_z` means all candidate strips for the selected scales
- omitted or `0` `max_strips` means no strip limit
- omitted or `0` `max_chunk_rows` means no row limit
- omitted or `0` `max_labels` means benchmark `1024` sampled visible label
  indices

Sampling behavior:

- when `max_strips` is set, strip candidates are sampled across the full volume
  extent instead of taking only the first strips near the volume origin
- empty strip candidates are skipped and do not count toward the strip limit
- empty X-rows are skipped before timing the strategies

## What the Labelmap Benchmark Measures

For a `labelmap` instance such as `segmentation`, the benchmark measures the
same kind of block-range reads used by `export-shards` when `workload=blocks`:

1. load the Neuroglancer export volume spec
2. compute the shard geometry used by `export-shards`
3. build full-X block-key ranges for selected `(chunkY, chunkZ)` rows
4. skip rows with no stored versioned keys
5. run each selected Badger strategy on the same non-empty sampled rows

One `chunk row` means one full-X range read for a fixed `(chunkY, chunkZ)` row
of blocks.

The benchmark report includes:

- `elapsed_ms`
- `visible_blocks`
- `visible_value_bytes`
- `scanned_versioned_kvs`
- `logical_keys`
- `avg_versions_per_key`
- `speedup_vs_legacy`

These numbers are enough to answer:

- how much real data was scanned
- how much version fanout existed in the sampled region
- whether block-read performance is driven by version pruning or by pipeline
  overlap

For `workload=indices`, the benchmark measures visible label index retrieval on
real label index keys from the chosen version:

1. scan visible label index keys in the instance once
2. keep a deterministic sample of up to `max_labels` labels using a stable
   hash-based sampler
3. run the selected Badger strategies on the same exact-key index reads for
   those sampled labels

The `indices` benchmark intentionally bypasses higher-level label index caching
and measures the underlying Badger versioned read path directly.  This is the
workload that is expected to show higher versions-per-key because label indices
are rewritten during body merges, cleaves, and other mutating operations.

## Revised Plan

### Goal

Keep the current pipelined default for block-range reads while exploring whether
different versioned read classes should use different internal strategies.

### Planned Next Work

1. Keep the current block-read benchmark and use it as the authority for
   `export-shards`-like workloads.

2. Add a separate benchmark path for label index retrieval.

   Motivation:

   - label index access is not part of `export-shards`
   - it may have much higher versions-per-key than block reads
   - the keys-first optimization may help there even if it does not help block
     reads

   Candidate operations to benchmark:

   - `getLabelIndex`
   - `GetVersion`
   - any range-style index retrieval paths used by `labelmap`

3. Explore a fourth internal strategy for block reads:

   - single-transaction streaming pipeline
   - keep the legacy-style one-pass range scan over full versioned KVs
   - send grouped key families downstream immediately

## Implemented So Far

The current implementation does the following:

1. `benchmark-versioned-read` now supports two workload classes in one report:

   - `blocks`
   - `indices`

2. Block benchmarking remains export-spec-driven.

   It still:

   - computes `export-shards` strip geometry from the Neuroglancer spec
   - samples strip candidates across the full volume
   - skips empty strips and empty X-rows before timing
   - runs `legacy`, `optimized`, and `pipelined` on the same non-empty rows

3. Index benchmarking is implemented as exact-key reads on sampled visible
   label index keys.

   The sampler:

   - scans visible label index keys using `SendKeysInRange`
   - does not require the caller to provide label IDs
   - keeps at most `max_labels` labels using a deterministic hash-based sample
   - sorts the selected labels so repeated runs use the same order

4. The report now records workload-specific rows.

   Each result row includes:

   - `workload`
   - `strategy`
   - `elapsed_ms`
   - `scanned_versioned_kvs`
   - `logical_keys`
   - `avg_versions_per_key`

   Block rows additionally populate:

   - `strips`
   - `chunk_rows`
   - `visible_blocks`

   Index rows additionally populate:

   - `sampled_labels`
   - `visible_indices`

5. Relative speedups are normalized within each workload, iteration, and scale.

   That means:

   - block results compare only against block `legacy`
   - index results compare only against index `legacy`

6. The current implementation for `indices` benchmarks index fetch behavior,
   not the standalone `GetVersion()` lookup path.

   It uses exact-key `ProcessRangeWithStrategy(...)` calls on real label index
   keys so that all three Badger strategies can be compared on the same index
   retrieval workload.

This is the current implementation baseline to measure before adding a fourth
single-transaction streaming strategy or adding a separate benchmark for pure
`index-version` lookups.
   - pipeline winner selection / downstream processing without splitting the read
     into two Badger transactions

   Motivation:

   - the current real-server evidence suggests pipeline overlap is the main win
   - the keys-first optimization does not help much when `avg_versions_per_key`
     is near `1`
   - a single-transaction pipeline could preserve one-snapshot semantics while
     still trying to keep the range scan unblocked

4. Compare strategy choice by workload class instead of assuming one universal
   winner.

   Working hypothesis:

   - block-range reads may prefer pipelined streaming behavior
   - label index reads may prefer keys-first pruning if version fanout is higher

### Non-Goals for the Next Pass

- no on-disk key format change
- no migration requirement
- no side index yet

## Decision Rule Going Forward

For any future Badger read-path change, prefer the result from:

- the real-server `labelmap` block benchmark for `export-shards`-like reads
- the planned label-index benchmark for index-heavy reads

Do not assume that a win on synthetic fanout-heavy data automatically transfers
to scale-0 block reads on production datasets.
