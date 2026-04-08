package labelmap

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	badgerstore "github.com/janelia-flyem/dvid/storage/badger"
)

type versionedReadBenchmarkSpec struct {
	ExportSpecPath string `json:"export_spec_path"`
	Mode           string `json:"mode"`       // legacy, optimized, both
	Iterations     int    `json:"iterations"` // default 1
	NumScales      uint8  `json:"num_scales"` // default 1

	// Optional filters to limit work to a subset of the export-shards scan.
	Scale        *uint8 `json:"scale,omitempty"`
	ShardY       *int32 `json:"shard_y,omitempty"`        // voxel origin of selected shard strip in Y
	ShardZ       *int32 `json:"shard_z,omitempty"`        // voxel origin of selected shard strip in Z
	MaxStrips    int    `json:"max_strips,omitempty"`     // 0 = all matching strips
	MaxChunkRows int    `json:"max_chunk_rows,omitempty"` // 0 = all rows within selected strips
}

type versionedReadStrategyResult struct {
	Strategy            string  `json:"strategy"`
	Iteration           int     `json:"iteration"`
	Scale               uint8   `json:"scale"`
	Strips              int     `json:"strips"`
	ChunkRows           int     `json:"chunk_rows"`
	VisibleBlocks       int64   `json:"visible_blocks"`
	VisibleValueBytes   int64   `json:"visible_value_bytes"`
	ScannedVersionedKVs int64   `json:"scanned_versioned_kvs"`
	LogicalKeys         int64   `json:"logical_keys"`
	AvgVersionsPerKey   float64 `json:"avg_versions_per_key"`
	ElapsedMilliseconds float64 `json:"elapsed_ms"`
}

type versionedReadBenchmarkReport struct {
	UUID      string                        `json:"uuid"`
	DataName  string                        `json:"data_name"`
	Store     string                        `json:"store"`
	Spec      versionedReadBenchmarkSpec    `json:"spec"`
	Results   []versionedReadStrategyResult `json:"results"`
	StartedAt time.Time                     `json:"started_at"`
}

func loadVersionedReadBenchmarkSpec(path string) (versionedReadBenchmarkSpec, ngVolume, error) {
	specBytes, err := os.ReadFile(path)
	if err != nil {
		return versionedReadBenchmarkSpec{}, ngVolume{}, fmt.Errorf("error reading benchmark spec file %q: %v", path, err)
	}
	var bench versionedReadBenchmarkSpec
	if err := json.Unmarshal(specBytes, &bench); err != nil {
		return versionedReadBenchmarkSpec{}, ngVolume{}, fmt.Errorf("error unmarshalling benchmark spec %q: %v", path, err)
	}
	if bench.ExportSpecPath == "" {
		return versionedReadBenchmarkSpec{}, ngVolume{}, fmt.Errorf("benchmark spec must include export_spec_path")
	}
	exportBytes, err := os.ReadFile(bench.ExportSpecPath)
	if err != nil {
		return versionedReadBenchmarkSpec{}, ngVolume{}, fmt.Errorf("error reading export spec %q: %v", bench.ExportSpecPath, err)
	}
	var volSpec ngVolume
	if err := json.Unmarshal(exportBytes, &volSpec); err != nil {
		return versionedReadBenchmarkSpec{}, ngVolume{}, fmt.Errorf("error unmarshalling export spec %q: %v", bench.ExportSpecPath, err)
	}
	if bench.Mode == "" {
		bench.Mode = "both"
	}
	bench.Mode = strings.ToLower(bench.Mode)
	if bench.Iterations <= 0 {
		bench.Iterations = 1
	}
	if bench.NumScales == 0 {
		bench.NumScales = 1
	}
	return bench, volSpec, nil
}

func computeExportShardDims(spec ngVolume, numScales uint8) ([]ngScale, []int32, error) {
	if int(numScales) > len(spec.Scales) {
		return nil, nil, fmt.Errorf("num_scales %d exceeds export spec scales %d", numScales, len(spec.Scales))
	}
	scales := make([]ngScale, numScales)
	copy(scales, spec.Scales[:numScales])
	shardDims := make([]int32, len(scales))
	for level := range scales {
		scale := &scales[level]
		if err := scale.initialize(); err != nil {
			return nil, nil, err
		}
		nonShardBits := int(scale.Sharding.PreshiftBits + scale.Sharding.MinishardBits)
		totalZBits := int(scale.totChunkCoordBits)
		if nonShardBits > totalZBits {
			nonShardBits = totalZBits
		}
		curBit := [3]int{0, 0, 0}
		dimI := 0
		for i := 0; i < nonShardBits; i++ {
			for curBit[dimI] == int(scale.chunkCoordBits[dimI]) {
				dimI = (dimI + 1) % 3
			}
			curBit[dimI]++
			dimI = (dimI + 1) % 3
		}
		var maxShardDimVoxels int32
		for dim := 0; dim < 3; dim++ {
			shardChunks := int32(1) << curBit[dim]
			if shardChunks > int32(scale.gridSize[dim]) {
				shardChunks = int32(scale.gridSize[dim])
			}
			dimVoxels := shardChunks * dvidChunkVoxelsPerDim
			if dimVoxels > maxShardDimVoxels {
				maxShardDimVoxels = dimVoxels
			}
		}
		shardDims[level] = maxShardDimVoxels
	}
	return scales, shardDims, nil
}

func (d *Data) benchmarkVersionedReads(ctx *datastore.VersionedCtx, bench versionedReadBenchmarkSpec, volSpec ngVolume) (*versionedReadBenchmarkReport, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}
	badgerDB, ok := store.(*badgerstore.BadgerDB)
	if !ok {
		return nil, fmt.Errorf("benchmark-versioned-read currently supports only Badger backend, got %T", store)
	}
	scales, shardDims, err := computeExportShardDims(volSpec, bench.NumScales)
	if err != nil {
		return nil, err
	}
	report := &versionedReadBenchmarkReport{
		UUID:      string(ctx.VersionUUID()),
		DataName:  string(d.DataName()),
		Store:     store.String(),
		Spec:      bench,
		StartedAt: time.Now(),
	}

	runLegacy := bench.Mode == "legacy" || bench.Mode == "both"
	runOptimized := bench.Mode == "optimized" || bench.Mode == "both"
	if !runLegacy && !runOptimized {
		return nil, fmt.Errorf("unsupported benchmark mode %q", bench.Mode)
	}

	for iter := 1; iter <= bench.Iterations; iter++ {
		for scale := uint8(0); scale < bench.NumScales; scale++ {
			if bench.Scale != nil && *bench.Scale != scale {
				continue
			}
			shardDimVoxels := shardDims[scale]
			shardDimChunks := shardDimVoxels / dvidChunkVoxelsPerDim
			volumeExtents := scales[scale].Size
			volChunksX := volumeExtents[0] / dvidChunkVoxelsPerDim

			type agg struct {
				strips              int
				chunkRows           int
				visibleBlocks       int64
				visibleBytes        int64
				scannedVersionedKVs int64
				logicalKeys         int64
			}
			accum := map[string]*agg{}
			if runLegacy {
				accum["legacy"] = &agg{}
			}
			if runOptimized {
				accum["optimized"] = &agg{}
			}
			started := map[string]time.Time{}
			for name := range accum {
				started[name] = time.Now()
			}

			stripsSeen := 0
			chunkRowsSeen := 0
			for shardZ := int32(0); shardZ < volumeExtents[2]; shardZ += shardDimVoxels {
				for shardY := int32(0); shardY < volumeExtents[1]; shardY += shardDimVoxels {
					if bench.ShardZ != nil && *bench.ShardZ != shardZ {
						continue
					}
					if bench.ShardY != nil && *bench.ShardY != shardY {
						continue
					}
					if bench.MaxStrips > 0 && stripsSeen >= bench.MaxStrips {
						break
					}
					stripsSeen++
					shardChunkZ := shardZ / dvidChunkVoxelsPerDim
					shardChunkY := shardY / dvidChunkVoxelsPerDim

					for chunkZ := shardChunkZ; chunkZ < shardChunkZ+shardDimChunks; chunkZ++ {
						for chunkY := shardChunkY; chunkY < shardChunkY+shardDimChunks; chunkY++ {
							if bench.MaxChunkRows > 0 && chunkRowsSeen >= bench.MaxChunkRows {
								break
							}
							chunkRowsSeen++
							chunkBeg := dvid.ChunkPoint3d{0, chunkY, chunkZ}
							chunkEnd := dvid.ChunkPoint3d{volChunksX, chunkY, chunkZ}
							begTKey := NewBlockTKeyByCoord(scale, chunkBeg.ToIZYXString())
							endTKey := NewBlockTKeyByCoord(scale, chunkEnd.ToIZYXString())

							if runLegacy {
								var visibleBlocks, visibleBytes int64
								if err := badgerDB.ProcessRangeWithStrategy(ctx, begTKey, endTKey, badgerstore.VersionedReadLegacy, nil, func(c *storage.Chunk) error {
									if c == nil || c.V == nil {
										return nil
									}
									visibleBlocks++
									visibleBytes += int64(len(c.V))
									return nil
								}); err != nil {
									return nil, err
								}
								a := accum["legacy"]
								a.visibleBlocks += visibleBlocks
								a.visibleBytes += visibleBytes
							}
							if runOptimized {
								var visibleBlocks, visibleBytes int64
								if err := badgerDB.ProcessRangeWithStrategy(ctx, begTKey, endTKey, badgerstore.VersionedReadOptimized, nil, func(c *storage.Chunk) error {
									if c == nil || c.V == nil {
										return nil
									}
									visibleBlocks++
									visibleBytes += int64(len(c.V))
									return nil
								}); err != nil {
									return nil, err
								}
								a := accum["optimized"]
								a.visibleBlocks += visibleBlocks
								a.visibleBytes += visibleBytes
							}
							minKey, err := ctx.MinVersionKey(begTKey)
							if err != nil {
								return nil, err
							}
							maxKey, err := ctx.MaxVersionKey(endTKey)
							if err != nil {
								return nil, err
							}
							ch := make(chan *storage.KeyValue, 256)
							cancel := make(chan struct{})
							go func() {
								_ = store.RawRangeQuery(minKey, maxKey, true, ch, cancel)
							}()
							var scannedKVs, logicalKeys int64
							var lastTKey string
							for {
								kv := <-ch
								if kv == nil {
									close(cancel)
									break
								}
								scannedKVs++
								tk, err := storage.TKeyFromKey(kv.K)
								if err != nil {
									close(cancel)
									return nil, err
								}
								tkStr := string(tk)
								if tkStr != lastTKey {
									logicalKeys++
									lastTKey = tkStr
								}
							}
							for _, a := range accum {
								a.scannedVersionedKVs += scannedKVs
								a.logicalKeys += logicalKeys
							}
						}
						if bench.MaxChunkRows > 0 && chunkRowsSeen >= bench.MaxChunkRows {
							break
						}
					}
				}
				if bench.MaxStrips > 0 && stripsSeen >= bench.MaxStrips {
					break
				}
			}

			for strategy, a := range accum {
				result := versionedReadStrategyResult{
					Strategy:            strategy,
					Iteration:           iter,
					Scale:               scale,
					Strips:              stripsSeen,
					ChunkRows:           chunkRowsSeen,
					VisibleBlocks:       a.visibleBlocks,
					VisibleValueBytes:   a.visibleBytes,
					ScannedVersionedKVs: a.scannedVersionedKVs,
					LogicalKeys:         a.logicalKeys,
					ElapsedMilliseconds: float64(time.Since(started[strategy])) / float64(time.Millisecond),
				}
				if a.logicalKeys > 0 {
					result.AvgVersionsPerKey = float64(a.scannedVersionedKVs) / float64(a.logicalKeys)
				}
				report.Results = append(report.Results, result)
			}
		}
	}
	return report, nil
}
