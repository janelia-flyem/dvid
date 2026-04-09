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
deployment server using the `benchmark-versioned-read` RPC command with
`workload=all`.

### Block Reads

Dataset and sample:

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

Measured behavior across the three iterations on male CNS `segmentation`:

| Approach | Speedup vs legacy | Iteration 1 | Iteration 2 | Iteration 3 | Notes |
| --- | --- | --- | --- | --- | --- |
| `legacy` | `1.00x` | `61.13s` | `55.53s` | `58.06s` | Reads all stored values for all versions. |
| `optimized` | `0.85x-0.89x` | `68.76s` | `65.20s` | `67.37s` | Slower than legacy for scale 0 block reads. |
| `pipelined` | `1.65x-1.72x` | `35.56s` | `33.57s` | `34.17s` | Best performer for block reads. |

Interpretation:

- single-stage keys-first optimization is not a win for this block workload
- pipelining is a large win
- the workload is not dominated by historical-version fanout
- the workload appears dominated by read-path overlap / streaming behavior

### Label Index Reads

Representative index result from the same run:

- sampled visible labels: `1024`
- visible indices returned: `1024`
- visible value bytes: `2,343,615,136`
- scanned versioned KVs: `12,325`
- logical keys: `1,024`
- average versions per key: `12.036`

The key point is that label indices are a high-rewrite workload. The sampled
label index keys averaged about twelve stored versions per logical key.

Measured behavior across the three iterations on male CNS `segmentation`:

| Approach | Speedup vs legacy | Iteration 1 | Iteration 2 | Iteration 3 | Notes |
| --- | --- | --- | --- | --- | --- |
| `legacy` | `1.00x` | `30.14s` | `17.43s` | `17.00s` | Reads all stored values for all historical index versions. |
| `optimized` | `16.69x-31.54x` | `0.956s` | `0.961s` | `1.018s` | Keys-first pruning is a huge win here. |
| `pipelined` | `24.08x-40.73x` | `0.740s` | `0.662s` | `0.706s` | Best performer for label index reads. |

Interpretation:

- keys-first pruning matters enormously for label index retrieval
- pipelining still helps on top of keys-first pruning
- this is the workload where historical-version fanout is genuinely dominant

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

### 2. For label indices, keys-first pruning matters a lot

For label index reads on the tested dataset:

- average versions per key is about `12`
- legacy pays for many losing historical versions
- optimized is dramatically better than legacy
- pipelined is better still

So label indices are exactly the kind of workload where keys-first winner
selection is valuable.

### 3. One optimization may not fit all versioned read types

The benchmark confirms that different versioned read classes have different
dominant costs:

- block-range reads used by `export-shards`
  - very low fanout
  - benefit mainly from pipeline overlap
- label index reads
  - high fanout
  - benefit heavily from keys-first pruning and also from pipelining

So one optimization should not be justified from only one workload class.

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
2. stop after collecting the first `max_labels` visible labels
3. skip labels whose visible index is nil or has `0` voxels
4. run the selected Badger strategies on the same exact-key index reads for
   those sampled labels

The `indices` benchmark intentionally bypasses higher-level label index caching
and measures the underlying Badger versioned read path directly.  This is the
workload that is expected to show higher versions-per-key because label indices
are rewritten during body merges, cleaves, and other mutating operations.

## Revised Plan

### Goal

Keep the current pipelined default for general Badger versioned range reads,
because it is the best performer so far on both measured workload classes.

### Planned Next Work

1. Keep the block benchmark as the authority for `export-shards`-like
   workloads.

2. Keep the index benchmark as the authority for label index retrieval
   workloads.

3. If another strategy is explored, the next candidate is still:

   - single-transaction streaming pipeline
   - keep the legacy-style one-pass range scan over full versioned KVs
   - send grouped key families downstream immediately
   - compare it against the current pipelined implementation on both workloads

4. Consider whether a separate benchmark for pure `GetVersion()` / `index-version`
   lookups is still worthwhile.

   The current `indices` benchmark measures index fetch behavior via exact-key
   `ProcessRangeWithStrategy(...)`, not the standalone `GetVersion()` path.

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
   - stops after collecting the first `max_labels` visible labels
   - filters out labels whose visible index is empty
   - sorts the selected labels before benchmarking

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

## Current Recommendation

The current recommendation is:

- keep `pipelined` as the default Badger versioned range-read strategy
- use the block benchmark to evaluate `export-shards`-like performance
- use the index benchmark to evaluate high-rewrite label index performance
- treat `optimized` as a comparison strategy, not the preferred production path

This recommendation is based on measured real-server results, not synthetic
benchmarks alone.

## Decision Rule Going Forward

For any future Badger read-path change, prefer the result from:

- the real-server `labelmap` block benchmark for `export-shards`-like reads
- the real-server label index benchmark for index-heavy reads

Do not assume that a win on synthetic fanout-heavy data automatically transfers
to scale-0 block reads on production datasets.
