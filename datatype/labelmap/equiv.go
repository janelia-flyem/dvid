// Equivalence maps for each version in DAG.

package labelmap

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dustin/go-humanize"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
)

type instanceMaps struct {
	maps map[dvid.UUID]*VCache
	sync.RWMutex
}

var (
	iMap instanceMaps
)

const (
	// NumSVMapShards is the number of shards for the supervoxel map.
	NumSVMapShards = 100
)

func init() {
	iMap.maps = make(map[dvid.UUID]*VCache)
}

// returns or creates an SVMap so nil is never returned unless there's an error
func getMapping(d dvid.Data, v dvid.VersionID) (*VCache, error) {
	m := initMapping(d, v)

	m.mappedVersionsMu.RLock()
	_, found := m.mappedVersions[v]
	m.mappedVersionsMu.RUnlock()
	if found {
		return m, nil // we have already loaded this version and its ancestors
	}
	if err := m.initToVersion(d, v, true); err != nil {
		return nil, err
	}
	return m, nil
}

// returns or creates an SVMap for data at a given version
func initMapping(d dvid.Data, v dvid.VersionID) *VCache {
	iMap.Lock()
	lmap, found := iMap.maps[d.DataUUID()]
	if !found {
		lmap = newVCache(NumSVMapShards)
		iMap.maps[d.DataUUID()] = lmap
	}
	iMap.Unlock()
	return lmap
}

// adds a merge into the equivalence map for a given instance version and also
// records the mappings into the log.
func addMergeToMapping(d dvid.Data, v dvid.VersionID, mutID, toLabel uint64, supervoxels labels.Set) error {
	if len(supervoxels) == 0 {
		return nil
	}
	lmap, err := getMapping(d, v)
	if err != nil {
		return err
	}
	for supervoxel := range supervoxels {
		lmap.setMapping(v, supervoxel, toLabel)
	}
	op := labels.MappingOp{
		MutID:    mutID,
		Mapped:   toLabel,
		Original: supervoxels,
	}
	return labels.LogMapping(d, v, op)
}

// // adds a renumber into the equivalence map for a given instance version and also
// // records the mappings into the log.
func addRenumberToMapping(d dvid.Data, v dvid.VersionID, mutID, origLabel, newLabel uint64, supervoxels labels.Set) error {
	if len(supervoxels) == 0 {
		return nil
	}
	lmap, err := getMapping(d, v)
	if err != nil {
		return err
	}
	for supervoxel := range supervoxels {
		lmap.setMapping(v, supervoxel, newLabel)
	}
	lmap.setMapping(v, newLabel, 0)
	op := labels.MappingOp{
		MutID:    mutID,
		Mapped:   newLabel,
		Original: supervoxels,
	}
	return labels.LogMapping(d, v, op)
}

// adds new arbitrary split into the equivalence map for a given instance version.
func addSplitToMapping(d dvid.Data, v dvid.VersionID, op labels.SplitOp) error {
	lmap, err := getMapping(d, v)
	if err != nil {
		return err
	}

	deleteSupervoxels := make(labels.Set)
	splitSupervoxels := make(labels.Set)
	remainSupervoxels := make(labels.Set)

	splits := lmap.splits[v]
	for supervoxel, svsplit := range op.SplitMap {
		deleteSupervoxels[supervoxel] = struct{}{}
		splitSupervoxels[svsplit.Split] = struct{}{}
		remainSupervoxels[svsplit.Remain] = struct{}{}

		lmap.setMapping(v, svsplit.Split, op.NewLabel)
		lmap.setMapping(v, svsplit.Remain, op.Target)
		lmap.setMapping(v, supervoxel, 0)

		rec := proto.SupervoxelSplitOp{
			Mutid:       op.MutID,
			Supervoxel:  supervoxel,
			Remainlabel: svsplit.Remain,
			Splitlabel:  svsplit.Split,
		}
		splits = append(splits, rec)

		// TODO -- for each split, we log each supervoxel split.
	}
	lmap.splitsMu.Lock()
	lmap.splits[v] = splits
	lmap.splitsMu.Unlock()

	mapOp := labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   0,
		Original: deleteSupervoxels,
	}
	if err := labels.LogMapping(d, v, mapOp); err != nil {
		dvid.Criticalf("unable to log the mapping of deleted supervoxels %s for split label %d: %v\n", deleteSupervoxels, op.Target, err)
		return err
	}
	mapOp = labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   op.NewLabel,
		Original: splitSupervoxels,
	}
	if err := labels.LogMapping(d, v, mapOp); err != nil {
		dvid.Criticalf("unable to log the mapping of split supervoxels %s to split body label %d: %v\n", splitSupervoxels, op.NewLabel, err)
		return err
	}
	mapOp = labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   op.Target,
		Original: remainSupervoxels,
	}
	return labels.LogMapping(d, v, mapOp)
}

// adds new cleave into the equivalence map for a given instance version and also
// records the mappings into the log.
func addCleaveToMapping(d dvid.Data, v dvid.VersionID, op labels.CleaveOp) error {
	lmap, err := getMapping(d, v)
	if err != nil {
		return err
	}
	if len(op.CleavedSupervoxels) == 0 {
		return nil
	}
	supervoxelSet := make(labels.Set, len(op.CleavedSupervoxels))
	for _, supervoxel := range op.CleavedSupervoxels {
		supervoxelSet[supervoxel] = struct{}{}
		lmap.setMapping(v, supervoxel, op.CleavedLabel)
	}
	lmap.setMapping(v, op.CleavedLabel, 0)
	mapOp := labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   op.CleavedLabel,
		Original: supervoxelSet,
	}
	return labels.LogMapping(d, v, mapOp)
}

// adds supervoxel split into the equivalence map for a given instance version and also
// records the mappings into the log.
func addSupervoxelSplitToMapping(d dvid.Data, v dvid.VersionID, op labels.SplitSupervoxelOp) error {
	lmap, err := getMapping(d, v)
	if err != nil {
		return err
	}
	label := op.Supervoxel
	mapped, found := lmap.MappedLabel(v, op.Supervoxel)
	if found {
		label = mapped
	}

	lmap.setMapping(v, op.SplitSupervoxel, label)
	lmap.setMapping(v, op.RemainSupervoxel, label)
	lmap.setMapping(v, op.Supervoxel, 0)

	rec := proto.SupervoxelSplitOp{
		Mutid:       op.MutID,
		Supervoxel:  op.Supervoxel,
		Remainlabel: op.RemainSupervoxel,
		Splitlabel:  op.SplitSupervoxel,
	}
	lmap.splitsMu.Lock()
	lmap.splits[v] = append(lmap.splits[v], rec)
	lmap.splitsMu.Unlock()

	if err := labels.LogSupervoxelSplit(d, v, op); err != nil {
		return err
	}

	mapOp := labels.MappingOp{
		MutID:  op.MutID,
		Mapped: 0,
		Original: labels.Set{
			op.Supervoxel: struct{}{},
		},
	}
	if err := labels.LogMapping(d, v, mapOp); err != nil {
		return fmt.Errorf("unable to log the mapping of deleted supervoxel %d: %v", op.Supervoxel, err)
	}
	newlabels := labels.Set{
		op.SplitSupervoxel:  struct{}{},
		op.RemainSupervoxel: struct{}{},
	}
	mapOp = labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   label,
		Original: newlabels,
	}
	return labels.LogMapping(d, v, mapOp)
}

// returns true if the given newLabel does not exist as forward mapping key
// TODO? Also check if it exists anywhere in mapping, which would probably
//
//	full set of ids.
func (d *Data) verifyIsNewLabel(v dvid.VersionID, newLabel uint64) (bool, error) {
	lmap, err := getMapping(d, v)
	if err != nil {
		return false, err
	}
	return !lmap.hasMapping(newLabel), nil
}

func (d *Data) ingestMappings(ctx *datastore.VersionedCtx, mappings *proto.MappingOps) error {
	lmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return err
	}
	vid := ctx.VersionID()
	for _, mapOp := range mappings.Mappings {
		for _, label := range mapOp.Original {
			lmap.setMapping(vid, label, mapOp.Mapped)
		}
	}
	return labels.LogMappings(d, ctx.VersionID(), mappings)
}

// GetMappedLabels returns an array of mapped labels, which could be the same as the passed slice,
// for the given version of the data instance.
func (d *Data) GetMappedLabels(v dvid.VersionID, supervoxels []uint64) (mapped []uint64, found []bool, err error) {
	var svmap *VCache
	if svmap, err = getMapping(d, v); err != nil {
		return
	}
	return svmap.MappedLabels(v, supervoxels)
}

type mapStats struct {
	MapEntries  uint64
	MapSize     string
	NumVersions int
	MaxVersion  int
}

// GetMapStats returns JSON describing in-memory mapping stats.
func (d *Data) GetMapStats(ctx *datastore.VersionedCtx) (jsonBytes []byte, err error) {
	stats := make(map[string]mapStats)
	for dataUUID, vc := range iMap.maps {
		var ds datastore.DataService
		if ds, err = datastore.GetDataByDataUUID(dataUUID); err != nil {
			return
		}
		vc.mappedVersionsMu.RLock()
		maxVersion := 0
		for v, _ := range vc.mappedVersions {
			if int(v) > maxVersion {
				maxVersion = int(v)
			}
		}
		name := string(ds.DataName())
		mapEntries, mapBytes := vc.mapStats()
		stats[name] = mapStats{
			MapEntries:  mapEntries,
			MapSize:     humanize.Bytes(mapBytes),
			NumVersions: len(vc.mappedVersions),
			MaxVersion:  maxVersion,
		}
		vc.mappedVersionsMu.RUnlock()
	}
	return json.Marshal(stats)
}
