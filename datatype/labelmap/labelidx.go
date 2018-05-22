package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/coocood/freecache"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	numIndexShards = 64
)

var (
	indexCache   *freecache.Cache
	indexMu      [numIndexShards]sync.RWMutex
	metaAttempts uint64
	metaHits     uint64
)

// Initialize makes sure index caching is initialized if cache size is specified
// in the server configuration.
func (d *Data) Initialize() {
	numBytes := server.CacheSize("labelmap")
	if indexCache == nil {
		if numBytes > 0 {
			indexCache = freecache.NewCache(numBytes)
			mbs := numBytes >> 20
			dvid.Infof("Created freecache of ~ %d MB for labelmap instances.\n", mbs)
		}
	} else if numBytes == 0 {
		indexCache = nil
	} else {
		indexCache.Clear()
	}
}

// indexKey is a three tuple (instance id, version, label)
type indexKey struct {
	data    dvid.Data
	version dvid.VersionID
	label   uint64
}

func (k indexKey) Bytes() []byte {
	b := make([]byte, 16)
	copy(b[0:4], k.data.InstanceID().Bytes())
	copy(b[4:8], k.version.Bytes())
	binary.LittleEndian.PutUint64(b[8:16], k.label)
	return b
}

func (k indexKey) VersionedCtx() *datastore.VersionedCtx {
	return datastore.NewVersionedCtx(k.data, k.version)
}

// returns nil if no Meta is found.
func getLabelIndex(ctx *datastore.VersionedCtx, label uint64) (*labels.Index, error) {
	store, err := datastore.GetKeyValueDB(ctx.Data())
	if err != nil {
		return nil, err
	}
	compressed, err := store.Get(ctx, NewLabelIndexTKey(label))
	if err != nil {
		return nil, err
	}
	if len(compressed) == 0 {
		return nil, nil
	}
	val, _, err := dvid.DeserializeData(compressed, true)
	if err != nil {
		return nil, err
	}

	idx := new(labels.Index)
	if err := idx.Unmarshal(val); err != nil {
		return nil, err
	}
	if idx.Label == 0 {
		idx.Label = label
	}
	return idx, nil
}

func putLabelIndex(ctx *datastore.VersionedCtx, idx *labels.Index) error {
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q PutLabelMeta had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(idx.Label)
	serialization, err := idx.Marshal()
	if err != nil {
		return fmt.Errorf("error trying to serialize index for label set %d, data %q: %v", idx.Label, ctx.Data().DataName(), err)
	}
	compressFormat, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	compressed, err := dvid.SerializeData(serialization, compressFormat, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("error trying to LZ4 compress label %d indexing in data %q", idx.Label, ctx.Data().DataName())
	}
	if err := store.Put(ctx, tk, compressed); err != nil {
		return fmt.Errorf("unable to store indices for label %d, data %s: %v", idx.Label, ctx.Data().DataName(), err)
	}
	return nil
}

func deleteLabelIndex(ctx *datastore.VersionedCtx, label uint64) error {
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q delete label set index had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(label)
	if err := store.Delete(ctx, tk); err != nil {
		return fmt.Errorf("unable to delete indices for label %d, data %s: %v", label, ctx.Data().DataName(), err)
	}
	return nil
}

func getCachedLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) (*labels.Index, error) {
	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	var err error
	var idxBytes []byte
	if indexCache != nil {
		idxBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return nil, err
		}
	}
	var idx *labels.Index
	if idxBytes != nil {
		idx = new(labels.Index)
		if err = idx.Unmarshal(idxBytes); err != nil {
			return nil, err
		}
		atomic.AddUint64(&metaHits, 1)
		return idx, nil
	}
	idx, err = getLabelIndex(k.VersionedCtx(), label)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	if indexCache != nil {
		idxBytes, err := idx.Marshal()
		if err != nil {
			dvid.Errorf("unable to marshal label %d index for %q: %v\n", label, d.DataName(), err)
		} else if err := indexCache.Set(k.Bytes(), idxBytes, 0); err != nil {
			dvid.Errorf("unable to set label %d index cache for %q: %v\n", label, d.DataName(), err)
		}
	}
	if idx.Label != label {
		dvid.Criticalf("label index for data %q, label %d has internal label value %d\n", d.DataName(), label, idx.Label)
		idx.Label = label
	}
	return idx, nil
}

func putCachedLabelIndex(d dvid.Data, v dvid.VersionID, idx *labels.Index) error {
	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelIndex(ctx, idx); err != nil {
		return err
	}
	if indexCache != nil {
		idxBytes, err := idx.Marshal()
		if err != nil {
			return err
		}
		k := indexKey{data: d, version: v, label: idx.Label}.Bytes()
		if err = indexCache.Set(k, idxBytes, 0); err != nil {
			return err
		}
	}
	return nil
}

func deleteCachedLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) error {
	ctx := datastore.NewVersionedCtx(d, v)
	if err := deleteLabelIndex(ctx, label); err != nil {
		return err
	}
	if indexCache != nil {
		k := indexKey{data: d, version: v, label: label}.Bytes()
		indexCache.Del(k)
	}
	return nil
}

////////////////////////
//

// should be launched as goroutine; locks the index of the split label for duration and
// splits both the index as well as relabels supervoxels that were split but not in
// split blocks.
func (d *Data) splitIndexAndBlocks(v dvid.VersionID, op labels.SplitOp, info dvid.ModInfo, downresMut *downres.Mutation, blockCh chan blockSplitCounts, doneCh chan struct{}, wg *sync.WaitGroup) {
	shard := op.Target % numIndexShards
	indexMu[shard].Lock()
	defer func() {
		indexMu[shard].Unlock()
		wg.Done()
	}()

	idx, err := getCachedLabelIndex(d, v, op.Target)
	if err != nil {
		dvid.Errorf("modify split index for data %q, label %d: %v\n", err)
		return
	}
	if idx == nil {
		dvid.Errorf("unable to modify split index for data %q: missing label %d\n", d.DataName(), op.Target)
		return
	}

	splitsPerBlock := make(map[uint64]map[uint64]labels.SVSplitCount)
	getFromChannel := true
	for getFromChannel {
		select {
		case blockSplit := <-blockCh:
			for supervoxel, counts := range blockSplit.counts {
				if supervoxel == 0 {
					continue
				}
				zyx, err := labels.IZYXStringToBlockIndex(blockSplit.bcoord)
				if err != nil {
					dvid.Errorf("unable to convert block %s to block index: %v\n", blockSplit.bcoord, err)
					continue
				}
				svc, found := splitsPerBlock[zyx]
				if !found {
					svc = make(map[uint64]labels.SVSplitCount)
				}
				svc[supervoxel] = counts
				splitsPerBlock[zyx] = svc
			}
		case <-doneCh:
			getFromChannel = false
		}
	}
	idx.LastMutId = op.MutID
	idx.LastModUser = info.User
	idx.LastModTime = info.Time
	idx.LastModApp = info.App

	sidx := new(labels.Index)
	sidx.LastMutId = op.MutID
	sidx.LastModUser = info.User
	sidx.LastModTime = info.Time
	sidx.LastModApp = info.App
	sidx.Label = op.NewLabel
	sidx.Blocks = make(map[uint64]*proto.SVCount, len(splitsPerBlock))

	relabeling := make(map[uint64]uint64)
	for zyx, counts := range splitsPerBlock {
		cursvc, found := idx.Blocks[zyx]
		if !found || cursvc == nil || len(cursvc.Counts) == 0 {
			dvid.Errorf("block %s was overwritten in split of label %d yet this block was not in the index!\n", labels.BlockIndexToIZYXString(zyx))
			continue
		}
		splitsvc := new(proto.SVCount)
		splitsvc.Counts = make(map[uint64]uint32, len(counts))
		var splitVoxels uint32
		for supervoxel, svsplit := range counts {
			relabeling[supervoxel] = svsplit.Remain
			splitVoxels += svsplit.Voxels
			numVoxels, found := cursvc.Counts[supervoxel]
			if !found {
				dvid.Errorf("block %s had %d voxels written over supervoxel %d in label %d split yet this supervoxel was not in the index!\n", labels.BlockIndexToIZYXString(zyx), svsplit, supervoxel, op.Target)
				continue
			}
			if numVoxels < svsplit.Voxels {
				dvid.Errorf("block %s had %d voxels written over supervoxel %d in label %d split yet this supervoxel only had %d voxels from index!\n", labels.BlockIndexToIZYXString(zyx), svsplit.Voxels, supervoxel, op.Target, numVoxels)
				continue
			}
			numVoxels -= svsplit.Voxels
			if numVoxels > 0 {
				cursvc.Counts[svsplit.Remain] = numVoxels
			}
			splitsvc.Counts[svsplit.Split] = svsplit.Voxels
			delete(cursvc.Counts, supervoxel)
		}
		sidx.Blocks[zyx] = splitsvc
	}

	// for all non-split blocks in this label, see if split supervoxels exist and relabel them.
	ctx := datastore.NewVersionedCtx(d, v)
	for zyx, svc := range idx.Blocks {
		if _, found := splitsPerBlock[zyx]; found {
			continue
		}
		var replacements map[uint64]uint64
		for supervoxel, size := range svc.Counts {
			remainLabel, found := relabeling[supervoxel]
			if found {
				if replacements == nil {
					replacements = make(map[uint64]uint64)
				}
				replacements[supervoxel] = remainLabel
				delete(svc.Counts, supervoxel)
				svc.Counts[remainLabel] = size
			}
		}
		if len(replacements) == 0 {
			continue
		}
		zyxStr := labels.BlockIndexToIZYXString(zyx)
		pb, err := d.getLabelBlock(ctx, 0, zyxStr)
		if err != nil {
			dvid.Errorf("error in getting block %s for relabeling during split: %v\n", zyxStr, err)
			continue
		}
		if pb == nil {
			dvid.Errorf("split on block %s attempted but block doesn't exist\n", zyxStr)
			continue
		}
		replacedBlock, replaced, err := pb.ReplaceLabels(replacements)
		if err != nil {
			dvid.Errorf("error replacing supervoxel labels in split on block %s: %v\n", zyxStr, err)
			continue
		}
		if replaced {
			splitpb := labels.PositionedBlock{Block: *replacedBlock, BCoord: zyxStr}
			if err := d.putLabelBlock(ctx, 0, &splitpb); err != nil {
				dvid.Errorf("unable to put block %s in split of label %d, data %q: %v\n", zyxStr, op.Target, d.DataName(), err)
				continue
			}

			if err := downresMut.BlockMutated(zyxStr, replacedBlock); err != nil {
				dvid.Errorf("data %q publishing downres: %v\n", d.DataName(), err)
			}
		}
	}

	// store the modified label index that remains after the split
	if err := putCachedLabelIndex(d, v, idx); err != nil {
		dvid.Errorf("modify split index for data %q, label %d: %v\n", d.DataName(), op.Target, err)
		return
	}

	// store the new label index corresponding to the split
	if err := putCachedLabelIndex(d, v, sidx); err != nil {
		dvid.Errorf("create new split index for data %q, label %d: %v\n", d.DataName(), op.NewLabel, err)
		return
	}
}

///////////////////////////////////////////////////////////////////////////
// The following public functions are concurrency-safe and support caching.

// GetLabelIndex gets label set index data from storage for a given data instance and version.
// If isSupervoxel is true, the label is interpreted as a supervoxel and the label set index
// containing the given supervoxel is returned.  Concurrency-safe access and supports caching.
// If a label has been mapped to another, a nil Index is returned.
func GetLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, isSupervoxel bool) (*labels.Index, error) {
	if isSupervoxel {
		mapping, err := getMapping(d, v)
		if err != nil {
			return nil, err
		}
		if mapping != nil {
			if mapped, found := mapping.MappedLabel(v, label); found {
				label = mapped
			}
		}
	}

	shard := label % numIndexShards
	indexMu[shard].RLock()
	idx, err := getCachedLabelIndex(d, v, label)
	indexMu[shard].RUnlock()
	return idx, err
}

// GetSupervoxelBlocks gets the blocks corresponding to a supervoxel id.
func GetSupervoxelBlocks(d dvid.Data, v dvid.VersionID, supervoxel uint64) (dvid.IZYXSlice, error) {
	idx, err := GetLabelIndex(d, v, supervoxel, true)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	var blocks dvid.IZYXSlice
	for zyx, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			sz, found := svc.Counts[supervoxel]
			if found && sz > 0 {
				blocks = append(blocks, labels.BlockIndexToIZYXString(zyx))
			}
		}
	}
	return blocks, nil
}

// GetLabelSize returns the # of voxels in the given label.  If isSupervoxel = true, the given
// label is interpreted as a supervoxel id and the size is of a supervoxel.  If a label doesn't
// exist, a zero (not error) is returned.
func GetLabelSize(d dvid.Data, v dvid.VersionID, label uint64, isSupervoxel bool) (uint64, error) {
	idx, err := GetLabelIndex(d, v, label, isSupervoxel)
	if err != nil {
		return 0, err
	}
	if idx == nil {
		return 0, nil
	}
	if isSupervoxel {
		return idx.GetSupervoxelCount(label), nil
	}
	return idx.NumVoxels(), nil
}

// GetLabelSizes returns the # of voxels in the given labels.  If isSupervoxel = true, the given
// labels are interpreted as supervoxel ids and the sizes are of a supervoxel.  If a label doesn't
// exist, a zero (not error) is returned.
func GetLabelSizes(d dvid.Data, v dvid.VersionID, labels []uint64, isSupervoxel bool) ([]uint64, error) {
	var supervoxels []uint64
	if isSupervoxel {
		svmap, err := getMapping(d, v)
		if err != nil {
			return nil, fmt.Errorf("couldn't get mapping for data %q, version %d: %v", d.DataName(), v, err)
		}
		supervoxels = make([]uint64, len(labels))
		copy(supervoxels, labels)

		labels, err = svmap.MappedLabels(v, labels)
		if err != nil {
			return nil, err
		}
	}
	// TODO -- could optimize by doing unique set of labels if supervoxels, since multiple supervoxels
	// may be in same label.  However, caching might simply remove this optimization issue since label
	// index will already be cached.
	sizes := make([]uint64, len(labels))
	for i, label := range labels {
		idx, err := GetLabelIndex(d, v, label, false)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			sizes[i] = 0
			continue
		}
		if isSupervoxel {
			sizes[i] = idx.GetSupervoxelCount(supervoxels[i])
		} else {
			sizes[i] = idx.NumVoxels()
		}
	}
	return sizes, nil
}

// GetBoundedIndex gets bounded label index data from storage for a given data instance.
func GetBoundedIndex(d dvid.Data, v dvid.VersionID, label uint64, bounds dvid.Bounds, isSupervoxel bool) (*labels.Index, error) {
	idx, err := GetLabelIndex(d, v, label, isSupervoxel)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	if bounds.Block != nil && bounds.Block.IsSet() {
		if err = idx.FitToBounds(bounds.Block); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

// GetMultiLabelIndex gets index data for all labels is a set with possible bounds.
func GetMultiLabelIndex(d dvid.Data, v dvid.VersionID, lbls labels.Set, bounds dvid.Bounds) (*labels.Index, error) {
	if len(lbls) == 0 {
		return nil, nil
	}
	idx := new(labels.Index)
	for label := range lbls {
		idx2, err := GetLabelIndex(d, v, label, false)
		if err != nil {
			return nil, err
		}
		if bounds.Block != nil && bounds.Block.IsSet() {
			if err := idx2.FitToBounds(bounds.Block); err != nil {
				return nil, err
			}
		}
		if err := idx.Add(idx2); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

// DeleteLabelIndex deletes the index for a given label set.
func DeleteLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) error {
	shard := label % numIndexShards
	indexMu[shard].Lock()
	err := deleteCachedLabelIndex(d, v, label)
	indexMu[shard].Unlock()
	return err
}

// PutLabelIndex persists a label index data for a given data instance and
// version. If the given index is nil, the index is deleted.  Concurrency-safe
// and supports caching.
func PutLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, idx *labels.Index) error {
	if idx == nil {
		return DeleteLabelIndex(d, v, label)
	}
	shard := label % numIndexShards
	indexMu[shard].Lock()

	idx.Label = label
	err := putCachedLabelIndex(d, v, idx)
	indexMu[shard].Unlock()
	return err
}

// SplitSupervoxelIndex modifies the label index for a given supervoxel, returning the slice of blocks
// that were modified.  NOTE: This assumes the split RLEs are accurate because they are not tested
// at the voxel level as being a subset of the supervoxel.
func SplitSupervoxelIndex(d dvid.Data, v dvid.VersionID, op labels.SplitSupervoxelOp, info dvid.ModInfo) (dvid.IZYXSlice, error) {
	mapping, err := getMapping(d, v)
	if err != nil {
		return nil, err
	}
	label := op.Supervoxel
	mapped, found := mapping.MappedLabel(v, op.Supervoxel)
	if found {
		label = mapped
	}

	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	idx, err := getCachedLabelIndex(d, v, label)
	if err != nil {
		return nil, fmt.Errorf("split supervoxel index for data %q, supervoxel %d: %v", d.DataName(), op.Supervoxel, err)
	}
	if idx == nil {
		return nil, fmt.Errorf("unable to split supervoxel %d for data %q: missing label index %d", op.Supervoxel, d.DataName(), label)
	}
	idx.LastMutId = op.MutID
	idx.LastModUser = info.User
	idx.LastModTime = info.Time
	idx.LastModApp = info.App

	// modify the index to reflect old supervoxel -> two new supervoxels.
	var svblocks dvid.IZYXSlice
	for zyx, svc := range idx.Blocks {
		origNumVoxels, found := svc.Counts[op.Supervoxel]
		if found { // split supervoxel is in this block
			delete(svc.Counts, op.Supervoxel)
			izyx := labels.BlockIndexToIZYXString(zyx)
			svblocks = append(svblocks, izyx)
			rles, found := op.Split[izyx]
			if found { // part of split
				splitNumVoxels, _ := rles.Stats()
				svc.Counts[op.SplitSupervoxel] = uint32(splitNumVoxels)
				svc.Counts[op.RemainSupervoxel] = origNumVoxels - uint32(splitNumVoxels)
			} else { // part of remainder
				svc.Counts[op.RemainSupervoxel] = origNumVoxels
			}
		}
	}

	// store the modified index
	if err := putCachedLabelIndex(d, v, idx); err != nil {
		return nil, fmt.Errorf("split supervoxel index for data %q, supervoxel %d: %v", d.DataName(), op.Supervoxel, err)
	}
	return svblocks, nil
}

// CleaveIndex modifies the label index to remove specified supervoxels and create another
// label index for this cleaved body.
func CleaveIndex(d dvid.Data, v dvid.VersionID, op labels.CleaveOp, info dvid.ModInfo) error {
	shard := op.Target % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	idx, err := getCachedLabelIndex(d, v, op.Target)
	if err != nil {
		return err
	}
	if idx == nil {
		return fmt.Errorf("cannot cleave non-existent label %d", op.Target)
	}
	idx.LastMutId = op.MutID
	idx.LastModUser = info.User
	idx.LastModTime = info.Time
	idx.LastModApp = info.App

	supervoxels := idx.GetSupervoxels()
	for _, supervoxel := range op.CleavedSupervoxels {
		if _, found := supervoxels[supervoxel]; !found {
			return fmt.Errorf("cannot cleave supervoxel %d, which does not exist in label %d", supervoxel, op.Target)
		}
		delete(supervoxels, supervoxel)
	}
	if len(supervoxels) == 0 {
		return fmt.Errorf("cannot cleave all supervoxels from the label %d", op.Target)
	}

	// create a new label index to contain the cleaved supervoxels.
	// we don't have to worry about mutex here because it's a new index.
	cidx := idx.Cleave(op.CleavedLabel, op.CleavedSupervoxels)
	if err := putCachedLabelIndex(d, v, cidx); err != nil {
		return err
	}
	return putCachedLabelIndex(d, v, idx)
}

// ChangeLabelIndex applies changes to a label's index and then stores the result.
// Supervoxel size changes for blocks should be passed into the function.  The passed
// SupervoxelDelta can contain more supervoxels than the label index.
func ChangeLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, delta labels.SupervoxelChanges) error {
	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	idx, err := getCachedLabelIndex(d, v, label)
	if err != nil {
		return err
	}
	if idx == nil {
		idx = new(labels.Index)
		idx.Label = label
	}

	if err := idx.ModifyBlocks(label, delta); err != nil {
		return err
	}

	if len(idx.Blocks) == 0 {
		return deleteCachedLabelIndex(d, v, label)
	}
	return putCachedLabelIndex(d, v, idx)
}

///////////////////////////////////////

// block-level analysis of mutation to get supervoxel changes in a block.  accumulates data for
// a given mutation into a map per mutation which will then be flushed for each supervoxel meta
// k/v pair at end of mutation.
func (d *Data) handleBlockMutate(v dvid.VersionID, ch chan blockChange, mut MutatedBlock) {
	if !d.IndexedLabels {
		return
	}
	bc := blockChange{
		bcoord: mut.BCoord,
	}
	if d.IndexedLabels {
		// if mut.Prev == nil {
		// 	dvid.Infof("block mutate %s has no previous block\n", mut.Prev)
		// } else {
		// 	dvid.Infof("block mutate %s: prev labels %v\n", mut.BCoord, mut.Prev.Labels)
		// }
		bc.delta = mut.Data.CalcNumLabels(mut.Prev)
	}
	ch <- bc
}

// block-level analysis of label ingest to do indexing
func (d *Data) handleBlockIndexing(v dvid.VersionID, ch chan blockChange, mut IngestedBlock) {
	if !d.IndexedLabels {
		return
	}
	bc := blockChange{
		bcoord: mut.BCoord,
	}
	if d.IndexedLabels {
		bc.delta = mut.Data.CalcNumLabels(nil)
	}
	ch <- bc
}

// Goroutines accepts block-level changes and segregates all changes by supervoxel, and then
// sends supervoxel-specific changes to concurrency-handling label indexing functions.

type blockChange struct {
	bcoord dvid.IZYXString
	delta  map[uint64]int32
}

// goroutine(s) that aggregates supervoxel changes across blocks for one mutation, then calls
// mutex-guarded label index mutation routine.
func (d *Data) aggregateBlockChanges(v dvid.VersionID, svmap *SVMap, ch <-chan blockChange) {
	ancestry, err := svmap.getLockedAncestry(v)
	if err != nil {
		dvid.Criticalf("unable to get ancestry for data %q, version %d: %v\n", d.DataName(), v, err)
		return
	}
	labelset := make(labels.Set)
	svChanges := make(labels.SupervoxelChanges)
	var maxLabel uint64
	for change := range ch {
		svmap.RLock()
		for supervoxel, delta := range change.delta {
			blockChanges, found := svChanges[supervoxel]
			if !found {
				blockChanges = make(map[dvid.IZYXString]int32)
				svChanges[supervoxel] = blockChanges
			}
			blockChanges[change.bcoord] += delta
			if supervoxel > maxLabel {
				maxLabel = supervoxel
			}
			label, _ := svmap.mapLabel(supervoxel, ancestry)
			labelset[label] = struct{}{}
		}
		svmap.RUnlock()
	}
	go func() {
		if err := d.updateMaxLabel(v, maxLabel); err != nil {
			dvid.Errorf("max label change during block aggregation for %q: %v\n", d.DataName(), err)
		}
	}()
	if d.IndexedLabels {
		for label := range labelset {
			if err := ChangeLabelIndex(d, v, label, svChanges); err != nil {
				dvid.Errorf("indexing label %d: %v\n", label, err)
			}
		}
	}
}

type labelBlock struct {
	index dvid.IZYXString
	data  []byte
}

type rleResult struct {
	runs          uint32
	serialization []byte
}

// goroutine to process retrieved label data and generate RLEs, could be sharded by block coordinate
func (d *Data) processBlocksToRLEs(lbls labels.Set, bounds dvid.Bounds, in chan labelBlock, out chan rleResult) {
	for {
		lb, more := <-in
		if !more {
			return
		}
		var result rleResult
		data, _, err := dvid.DeserializeData(lb.data, true)
		if err != nil {
			dvid.Errorf("could not deserialize %d bytes in block %s: %v\n", len(lb.data), lb.index, err)
			out <- result
			continue
		}
		var block labels.Block
		if err := block.UnmarshalBinary(data); err != nil {
			dvid.Errorf("unable to unmarshal label block %s: %v\n", lb.index, err)
		}
		blockData, _ := block.MakeLabelVolume()

		var newRuns uint32
		var serialization []byte
		if bounds.Exact && bounds.Voxel.IsSet() {
			serialization, newRuns, err = d.addBoundedRLEs(lb.index, blockData, lbls, bounds.Voxel)
		} else {
			serialization, newRuns, err = d.addRLEs(lb.index, blockData, lbls)
		}
		if err != nil {
			dvid.Errorf("could not process %d bytes in block %s to create RLEs: %v\n", len(blockData), lb.index, err)
		} else {
			result = rleResult{runs: newRuns, serialization: serialization}
		}
		out <- result
	}
}

func writeRLE(w io.Writer, start dvid.Point3d, run int32) error {
	rle := dvid.NewRLE(start, run)
	serialization, err := rle.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := w.Write(serialization); err != nil {
		return err
	}
	return nil
}

// Scan a block and construct RLEs that will be serialized and added to the given buffer.
func (d *Data) addRLEs(izyx dvid.IZYXString, data []byte, lbls labels.Set) (serialization []byte, newRuns uint32, err error) {
	if len(data) != int(d.BlockSize().Prod())*8 {
		err = fmt.Errorf("deserialized label block %d bytes, not uint64 size times %d block elements",
			len(data), d.BlockSize().Prod())
		return
	}
	var indexZYX dvid.IndexZYX
	indexZYX, err = izyx.IndexZYX()
	if err != nil {
		return
	}
	firstPt := indexZYX.MinPoint(d.BlockSize())
	lastPt := indexZYX.MaxPoint(d.BlockSize())

	var label uint64
	var spanStart dvid.Point3d
	var z, y, x, spanRun int32
	start := 0
	buf := new(bytes.Buffer)
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				label = binary.LittleEndian.Uint64(data[start : start+8])
				start += 8

				// If we are in labels of interest, start or extend run.
				inSpan := false
				if label != 0 {
					_, inSpan = lbls[label]
				}
				if inSpan {
					spanRun++
					if spanRun == 1 {
						spanStart = dvid.Point3d{x, y, z}
					}
				} else {
					if spanRun > 0 {
						newRuns++
						if err = writeRLE(buf, spanStart, spanRun); err != nil {
							return
						}
					}
					spanRun = 0
				}
			}
			// Force break of any runs when we finish x scan.
			if spanRun > 0 {
				if err = writeRLE(buf, spanStart, spanRun); err != nil {
					return
				}
				newRuns++
				spanRun = 0
			}
		}
	}
	serialization = buf.Bytes()
	return
}

// Scan a block and construct bounded RLEs that will be serialized and added to the given buffer.
func (d *Data) addBoundedRLEs(izyx dvid.IZYXString, data []byte, lbls labels.Set, bounds *dvid.OptionalBounds) (serialization []byte, newRuns uint32, err error) {
	if len(data) != int(d.BlockSize().Prod())*8 {
		err = fmt.Errorf("deserialized label block %d bytes, not uint64 size times %d block elements",
			len(data), d.BlockSize().Prod())
		return
	}
	var indexZYX dvid.IndexZYX
	indexZYX, err = izyx.IndexZYX()
	if err != nil {
		return
	}
	firstPt := indexZYX.MinPoint(d.BlockSize())
	lastPt := indexZYX.MaxPoint(d.BlockSize())

	var label uint64
	var spanStart dvid.Point3d
	var z, y, x, spanRun int32
	start := 0
	buf := new(bytes.Buffer)
	yskip := int(d.BlockSize().Value(0) * 8)
	zskip := int(d.BlockSize().Value(1)) * yskip
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		if bounds.OutsideZ(z) {
			start += zskip
			continue
		}
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			if bounds.OutsideY(y) {
				start += yskip
				continue
			}
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				label = binary.LittleEndian.Uint64(data[start : start+8])
				start += 8

				// If we are in labels of interest, start or extend run.
				inSpan := false
				if label != 0 {
					_, inSpan = lbls[label]
					if inSpan && bounds.OutsideX(x) {
						inSpan = false
					}
				}
				if inSpan {
					spanRun++
					if spanRun == 1 {
						spanStart = dvid.Point3d{x, y, z}
					}
				} else {
					if spanRun > 0 {
						newRuns++
						if err = writeRLE(buf, spanStart, spanRun); err != nil {
							return
						}
					}
					spanRun = 0
				}
			}
			// Force break of any runs when we finish x scan.
			if spanRun > 0 {
				if err = writeRLE(buf, spanStart, spanRun); err != nil {
					return
				}
				newRuns++
				spanRun = 0
			}
		}
	}
	serialization = buf.Bytes()
	return
}

// FoundSparseVol returns true if a sparse volume is found for the given label
// within the given bounds.
func (d *Data) FoundSparseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds, isSupervoxel bool) (bool, error) {
	idx, err := GetBoundedIndex(d, ctx.VersionID(), label, bounds, isSupervoxel)
	if err != nil {
		return false, err
	}

	if idx != nil && len(idx.Blocks) > 0 {
		dvid.Infof("Found %d blocks for label %d with constituents %s\n", len(idx.Blocks), label, idx.GetSupervoxels())
		return true, nil
	}
	return false, nil
}

// writeBinaryBlocks does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) writeBinaryBlocks(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, isSupervoxel bool, w io.Writer) (bool, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		return false, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return false, nil
	}
	supervoxels := idx.GetSupervoxels()
	if isSupervoxel {
		if _, found := supervoxels[label]; !found {
			return false, nil
		}
		supervoxels = labels.Set{label: struct{}{}}
	}

	indices, err := idx.GetProcessedBlockIndices(scale, bounds)
	if err != nil {
		return false, err
	}
	sort.Sort(indices)

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteBinaryBlocks(label, supervoxels, op, bounds)
	var preErr error
	for _, izyx := range indices {
		tk := NewBlockTKeyByCoord(scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			preErr = err
			break
		}
		if data == nil {
			preErr = fmt.Errorf("expected block %s @ scale %d to have key-value, but found none", izyx, scale)
			break
		}
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			preErr = err
			break
		}
		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			preErr = err
			break
		}
		pb := labels.PositionedBlock{
			Block:  block,
			BCoord: izyx,
		}
		op.Process(&pb)
	}
	if err = op.Finish(); err != nil {
		return false, err
	}

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, supervoxels, len(indices), len(idx.Blocks))
	return true, preErr
}

// writeStreamingRLE does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) writeStreamingRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, isSupervoxel bool, w io.Writer) (bool, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		return false, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return false, nil
	}
	supervoxels := idx.GetSupervoxels()
	if isSupervoxel {
		if _, found := supervoxels[label]; !found {
			return false, nil
		}
		supervoxels = labels.Set{label: struct{}{}}
	}

	blocks, err := idx.GetProcessedBlockIndices(scale, bounds)
	if err != nil {
		return false, err
	}
	sort.Sort(blocks)

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteRLEs(supervoxels, op, bounds)
	for _, izyx := range blocks {
		tk := NewBlockTKeyByCoord(scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return false, err
		}
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return false, err
		}
		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			return false, err
		}
		pb := labels.PositionedBlock{
			Block:  block,
			BCoord: izyx,
		}
		op.Process(&pb)
	}
	if err = op.Finish(); err != nil {
		return false, err
	}

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, supervoxels, len(blocks), len(idx.Blocks))
	return true, nil
}

func (d *Data) writeLegacyRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, b dvid.Bounds, compression string, isSupervoxel bool, w io.Writer) (found bool, err error) {
	var data []byte
	data, err = d.getLegacyRLEs(ctx, label, scale, b, isSupervoxel)
	if err != nil {
		return
	}
	if len(data) == 0 {
		found = false
		return
	}
	found = true
	switch compression {
	case "":
		_, err = w.Write(data)
	case "lz4":
		compressed := make([]byte, lz4.CompressBound(data))
		var n, outSize int
		if outSize, err = lz4.Compress(data, compressed); err != nil {
			return
		}
		compressed = compressed[:outSize]
		n, err = w.Write(compressed)
		if n != outSize {
			err = fmt.Errorf("only able to write %d of %d lz4 compressed bytes", n, outSize)
		}
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err = gw.Write(data); err != nil {
			return
		}
		err = gw.Close()
	default:
		err = fmt.Errorf("unknown compression type %q", compression)
	}
	return
}

//  The encoding has the following format where integers are little endian:
//
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    0
//    uint32    # Spans
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func (d *Data) getLegacyRLEs(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, isSupervoxel bool) ([]byte, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, nil
	}
	supervoxels := idx.GetSupervoxels()
	if isSupervoxel {
		if _, found := supervoxels[label]; !found {
			return nil, nil
		}
		supervoxels = labels.Set{label: struct{}{}}
	}

	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	blocks, err := idx.GetProcessedBlockIndices(scale, bounds)
	if err != nil {
		return nil, err
	}
	sort.Sort(blocks)

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}
	op := labels.NewOutputOp(buf)
	go labels.WriteRLEs(supervoxels, op, bounds)
	var numEmpty int
	for _, izyx := range blocks {
		tk := NewBlockTKeyByCoord(scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			numEmpty++
			if numEmpty < 10 {
				dvid.Errorf("Block %s included in blocks for labels %s but has no data (%d times)... skipping.\n", izyx, supervoxels, numEmpty)
			} else if numEmpty == 10 {
				dvid.Errorf("Over %d blocks included in blocks with no data.  Halting error stream.\n", numEmpty)
			}
			continue
		}
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return nil, err
		}
		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			return nil, err
		}
		pb := labels.PositionedBlock{
			Block:  block,
			BCoord: izyx,
		}
		op.Process(&pb)
	}
	if numEmpty < len(blocks) {
		if err = op.Finish(); err != nil {
			return nil, err
		}
	}

	serialization := buf.Bytes()
	numRuns := uint32(len(serialization)-12) >> 4
	if numRuns == 0 {
		return nil, nil // Couldn't find this out until we did voxel-level clipping
	}

	binary.LittleEndian.PutUint32(serialization[8:12], numRuns)
	dvid.Infof("label %d: sent %d blocks (%d hi-res blocks) within bounds excluding %d empty blocks, %d runs, serialized %d bytes\n", idx.Label, len(blocks), len(idx.Blocks), numEmpty, numRuns, len(serialization))
	return serialization, nil
}

// GetSparseCoarseVol returns an encoded sparse volume given a label.  This will return nil slice
// if the given label was not found.  The encoding has the following format where integers are
// little endian and blocks are returned in sorted ZYX order (small Z first):
//
// 		byte     Set to 0
// 		uint8    Number of dimensions
// 		uint8    Dimension of run (typically 0 = X)
// 		byte     Reserved (to be used later)
// 		uint32    # Blocks [TODO.  0 for now]
// 		uint32    # Spans
// 		Repeating unit of:
//     		int32   Block coordinate of run start (dimension 0)
//     		int32   Block coordinate of run start (dimension 1)
//     		int32   Block coordinate of run start (dimension 2)
//     		int32   Length of run
//
func (d *Data) GetSparseCoarseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds, isSupervoxel bool) ([]byte, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, nil
	}
	if isSupervoxel {
		idx, err = idx.LimitToSupervoxel(label)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			return nil, nil
		}
	}
	blocks, err := idx.GetProcessedBlockIndices(0, bounds)
	if err != nil {
		return nil, err
	}
	sort.Sort(blocks)

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	spans, err := blocks.WriteSerializedRLEs(buf)
	if err != nil {
		return nil, err
	}
	serialization := buf.Bytes()
	binary.LittleEndian.PutUint32(serialization[8:12], spans) // Placeholder for # spans

	return serialization, nil
}

// WriteSparseCoarseVols returns a stream of sparse volumes with blocks of the given label
// in encoded RLE format:
//
// 		uint64   label
// 		<coarse sparse vol as given below>
//
// 		uint64   label
// 		<coarse sparse vol as given below>
//
// 		...
//
// 	The coarse sparse vol has the following format where integers are little endian and the order
// 	of data is exactly as specified below:
//
// 		int32    # Spans
// 		Repeating unit of:
// 			int32   Block coordinate of run start (dimension 0)
// 			int32   Block coordinate of run start (dimension 1)
// 			int32   Block coordinate of run start (dimension 2)
// 			int32   Length of run
func (d *Data) WriteSparseCoarseVols(ctx *datastore.VersionedCtx, w io.Writer, begLabel, endLabel uint64, bounds dvid.Bounds) error {

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	begTKey := NewLabelIndexTKey(begLabel)
	endTKey := NewLabelIndexTKey(endLabel)

	err = store.ProcessRange(ctx, begTKey, endTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil {
			return nil
		}
		kv := c.TKeyValue
		if kv.V == nil {
			return nil
		}
		label, err := DecodeLabelIndexTKey(kv.K)
		if err != nil {
			return err
		}
		val, _, err := dvid.DeserializeData(kv.V, true)
		if err != nil {
			return err
		}
		var idx labels.Index
		if len(val) != 0 {
			if err := idx.Unmarshal(val); err != nil {
				return err
			}
			blocks, err := idx.GetProcessedBlockIndices(0, bounds)
			if err != nil {
				return err
			}
			sort.Sort(blocks)
			buf := new(bytes.Buffer)
			spans, err := blocks.WriteSerializedRLEs(buf)
			if err != nil {
				return err
			}
			binary.Write(w, binary.LittleEndian, label)
			binary.Write(w, binary.LittleEndian, int32(spans))
			w.Write(buf.Bytes())
		}
		return nil
	})
	return err
}

func (d *Data) countThread(f *os.File, mu *sync.Mutex, wg *sync.WaitGroup, chunkCh chan *storage.Chunk) {
	for c := range chunkCh {
		scale, idx, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			wg.Done()
			continue
		}
		if scale != 0 {
			dvid.Errorf("Counts had unexpected error: getting scale %d blocks\n", scale)
			wg.Done()
			continue
		}
		var data []byte
		data, _, err = dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", idx, d.DataName(), err)
			wg.Done()
			continue
		}
		var block labels.Block
		if err := block.UnmarshalBinary(data); err != nil {
			dvid.Errorf("Unable to unmarshal Block %s in data %q: %v\n", idx, d.DataName(), err)
			wg.Done()
			continue
		}
		counts := block.CalcNumLabels(nil)
		for supervoxel, count := range counts {
			bx, by, bz := idx.Unpack()
			line := fmt.Sprintf("%d %d %d %d %d\n", supervoxel, bz, by, bx, count)
			mu.Lock()
			_, err := f.WriteString(line)
			mu.Unlock()
			if err != nil {
				dvid.Errorf("Unable to write data for block %s, data %q: %v\n", idx, d.DataName(), err)
				break
			}
		}
		wg.Done()
	}
}

// scan all label blocks in this labelmap instance, writing supervoxel counts into a given file
func (d *Data) writeSVCounts(f *os.File, outPath string, v dvid.VersionID) {
	timedLog := dvid.NewTimeLog()

	// Start the counting goroutine
	mu := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	chunkCh := make(chan *storage.Chunk, 100)
	for i := 0; i < 100; i++ {
		go d.countThread(f, mu, wg, chunkCh)
	}

	// Iterate through all label blocks and count them.
	chunkOp := &storage.ChunkOp{Wg: wg}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("problem getting store for data %q: %v\n", d.DataName(), err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	begTKey := NewBlockTKeyByCoord(0, dvid.MinIndexZYX.ToIZYXString())
	endTKey := NewBlockTKeyByCoord(0, dvid.MaxIndexZYX.ToIZYXString())
	var numBlocks uint64
	err = store.ProcessRange(ctx, begTKey, endTKey, chunkOp, func(c *storage.Chunk) error {
		if c == nil {
			wg.Done()
			return fmt.Errorf("received nil chunk in count for data %q", d.DataName())
		}
		if c.V == nil {
			wg.Done()
			return nil
		}
		numBlocks++
		if numBlocks%10000 == 0 {
			timedLog.Infof("Now counting block %d with chunk channel at %d", numBlocks, len(chunkCh))
		}
		chunkCh <- c
		return nil
	})
	if err != nil {
		dvid.Errorf("problem during process range: %v\n", err)
	}
	close(chunkCh)
	wg.Wait()
	if err = f.Close(); err != nil {
		dvid.Errorf("problem closing file %q: %v\n", outPath, err)
	}
	timedLog.Infof("Finished counting supervoxels in %d blocks and sent to output file %q", numBlocks, outPath)
}

func (d *Data) writeFileMappings(f *os.File, outPath string, v dvid.VersionID) {
	if err := d.writeMappings(f, v); err != nil {
		dvid.Errorf("error writing mapping to file %q: %v\n", outPath, err)
		return
	}
	if err := f.Close(); err != nil {
		dvid.Errorf("problem closing file %q: %v\n", outPath, err)
	}
}

func (d *Data) writeMappings(w io.Writer, v dvid.VersionID) error {
	timedLog := dvid.NewTimeLog()

	svm, err := getMapping(d, v)
	if err != nil {
		return fmt.Errorf("unable to retrieve mappings for data %q, version %d: %v", d.DataName(), v, err)
	}
	ancestry, err := svm.getLockedAncestry(v)
	if err != nil {
		return fmt.Errorf("unable to get ancestry for data %q, version %d: %v", d.DataName(), v, err)
	}
	svm.RLock()
	defer svm.RUnlock()
	if len(svm.fm) == 0 {
		dvid.Infof("no mappings found for data %q\n", d.DataName())
		return nil
	}
	var numMappings, numErrors uint64
	for supervoxel, vm := range svm.fm {
		label, present := vm.value(ancestry)
		if present {
			numMappings++
			if supervoxel != label {
				line := fmt.Sprintf("%d %d\n", supervoxel, label)
				if _, err := w.Write([]byte(line)); err != nil {
					numErrors++
					if numErrors < 100 {
						return fmt.Errorf("unable to write data for mapping of supervoxel %d -> %d, data %q: %v", supervoxel, label, d.DataName(), err)
					}
				}
			}
		}
	}
	timedLog.Infof("Finished writing %d mappings (%d errors) for data %q, version %d", numMappings, numErrors, d.DataName(), v)
	return nil
}

func (d *Data) indexThread(f *os.File, mu *sync.Mutex, wg *sync.WaitGroup, chunkCh chan *storage.Chunk) {
	for c := range chunkCh {
		label, err := DecodeLabelIndexTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label index key %v for data %q\n", c.K, d.DataName())
			wg.Done()
			continue
		}
		var data []byte
		data, _, err = dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize label index %d in data %q: %v\n", label, d.DataName(), err)
			wg.Done()
			continue
		}
		idx := new(labels.Index)
		if err := idx.Unmarshal(data); err != nil {
			dvid.Errorf("Unable to unmarshal label index %d in data %q: %v\n", label, d.DataName(), err)
			wg.Done()
			continue
		}
		if idx.Label == 0 {
			idx.Label = label
		}
		for zyx, svc := range idx.Blocks {
			bx, by, bz := labels.DecodeBlockIndex(zyx)
			if svc != nil && len(svc.Counts) != 0 {
				for supervoxel, count := range svc.Counts {
					line := fmt.Sprintf("%d %d %d %d %d %d\n", idx.Label, supervoxel, bz, by, bx, count)
					mu.Lock()
					_, err := f.WriteString(line)
					mu.Unlock()
					if err != nil {
						dvid.Errorf("Unable to write label index %d line, data %q: %v\n", idx.Label, d.DataName(), err)
						break
					}
				}
			}
		}
		wg.Done()
	}
}

// scan all label indices in this labelmap instance, writing Blocks data into a given file
func (d *Data) writeIndices(f *os.File, outPath string, v dvid.VersionID) {
	timedLog := dvid.NewTimeLog()

	// Start the counting goroutine
	mu := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	chunkCh := make(chan *storage.Chunk, 100)
	for i := 0; i < 100; i++ {
		go d.indexThread(f, mu, wg, chunkCh)
	}

	// Iterate through all label blocks and count them.
	chunkOp := &storage.ChunkOp{Wg: wg}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("problem getting store for data %q: %v\n", d.DataName(), err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	begTKey := NewLabelIndexTKey(0)
	endTKey := NewLabelIndexTKey(math.MaxUint64)
	var numIndices uint64
	err = store.ProcessRange(ctx, begTKey, endTKey, chunkOp, func(c *storage.Chunk) error {
		if c == nil {
			wg.Done()
			return fmt.Errorf("received nil chunk in dump index for data %q", d.DataName())
		}
		if c.V == nil {
			wg.Done()
			return nil
		}
		numIndices++
		if numIndices%10000 == 0 {
			timedLog.Infof("Now dumping label index %d with chunk channel at %d", numIndices, len(chunkCh))
		}
		chunkCh <- c
		return nil
	})
	if err != nil {
		dvid.Errorf("problem during process range: %v\n", err)
	}
	close(chunkCh)
	wg.Wait()
	if err = f.Close(); err != nil {
		dvid.Errorf("problem closing file %q: %v\n", outPath, err)
	}
	timedLog.Infof("Finished dumping %d label indices to output file %q", numIndices, outPath)
}
