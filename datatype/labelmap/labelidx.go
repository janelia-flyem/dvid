package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/coocood/freecache"
	lz4 "github.com/janelia-flyem/go/golz4"
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
	if len(val) == 0 {
		return nil, err
	}

	idx := new(labels.Index)
	if err := idx.Unmarshal(val); err != nil {
		return nil, err
	}
	return idx, nil
}

func putLabelIndex(ctx *datastore.VersionedCtx, label uint64, idx *labels.Index) error {
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q PutLabelMeta had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(label)
	serialization, err := idx.Marshal()
	if err != nil {
		return fmt.Errorf("error trying to serialize index for label set %d, data %q: %v", label, ctx.Data().DataName(), err)
	}
	compressFormat, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	compressed, err := dvid.SerializeData(serialization, compressFormat, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("error trying to LZ4 compress label %d indexing in data %q", label, ctx.Data().DataName())
	}
	if err := store.Put(ctx, tk, compressed); err != nil {
		return fmt.Errorf("unable to store indices for label %d, data %s: %v", label, ctx.Data().DataName(), err)
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

///////////////////////////////////////////////////////////////////////////
// The following public functions are concurrency-safe and support caching.

// GetLabelIndex gets label set index data from storage for a given data instance
// and version. Concurrency-safe access and supports caching.
func GetLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) (*labels.Index, error) {
	mapping, err := GetMapping(d, v)
	if err != nil {
		return nil, err
	}
	if mapping != nil {
		if mapped, found := mapping.MappedLabel(label); found {
			return nil, fmt.Errorf("label %d has already been merged into label %d", label, mapped)
		}
	}

	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	shard := label % numIndexShards
	indexMu[shard].RLock()
	defer indexMu[shard].RUnlock()

	var idxBytes []byte
	if indexCache != nil {
		idxBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return nil, err
		}
	}
	if idxBytes != nil {
		idx := new(labels.Index)
		if err := idx.Unmarshal(idxBytes); err != nil {
			return nil, err
		}
		atomic.AddUint64(&metaHits, 1)
		return idx, nil
	}
	idx, err := getLabelIndex(k.VersionedCtx(), label)
	if err != nil {
		return nil, err
	}
	if indexCache != nil && idx != nil {
		idxBytes, err := idx.Marshal()
		if err != nil {
			dvid.Errorf("unable to marshal label %d index for %q: %v\n", label, d.DataName(), err)
		} else if err := indexCache.Set(k.Bytes(), idxBytes, 0); err != nil {
			dvid.Errorf("unable to set label %d index cache for %q: %v\n", label, d.DataName(), err)
		}
	}
	return idx, err
}

// GetSupervoxelBlocks gets the blocks corresponding to a supervoxel id.
func GetSupervoxelBlocks(d dvid.Data, v dvid.VersionID, supervoxel uint64) (dvid.IZYXSlice, error) {
	mapping, err := GetMapping(d, v)
	if err != nil {
		return nil, err
	}
	label := supervoxel
	if mapping != nil {
		if mapped, found := mapping.MappedLabel(supervoxel); found {
			label = mapped
		}
	}
	idx, err := GetLabelIndex(d, v, label)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	var blocks dvid.IZYXSlice
	for s, svc := range idx.Blocks {
		if svc != nil && svc.Counts != nil {
			sz, found := svc.Counts[supervoxel]
			if found && sz > 0 {
				blocks = append(blocks, dvid.IZYXString(s))
			}
		}
	}
	return blocks, nil
}

// GetBoundedIndex gets bounded label index data from storage for a given data instance.
func GetBoundedIndex(d dvid.Data, v dvid.VersionID, label uint64, bounds dvid.Bounds) (*labels.Index, error) {
	idx, err := GetLabelIndex(d, v, label)
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
		idx2, err := GetLabelIndex(d, v, label)
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
	defer indexMu[shard].Unlock()

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

// PutLabelIndex persists a label index data for a given data instance and
// version. If the given index is nil, the index is deleted.  Concurrency-safe
// and supports caching.
func PutLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, idx *labels.Index) error {
	if idx == nil {
		return DeleteLabelIndex(d, v, label)
	}
	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelIndex(ctx, label, idx); err != nil {
		return err
	}
	idxBytes, err := idx.Marshal()
	if err != nil {
		return err
	}
	if indexCache != nil {
		k := indexKey{data: d, version: v, label: label}.Bytes()
		if err := indexCache.Set(k, idxBytes, 0); err != nil {
			return err
		}
	}
	return nil
}

// SplitSupervoxelIndex modifies the label index for a given supervoxel, returning the slice of blocks
// that were modified.  NOTE: This assumes the split RLEs are accurate because they are not tested
// at the voxel level as being a subset of the supervoxel.
func SplitSupervoxelIndex(d dvid.Data, v dvid.VersionID, op labels.SplitSupervoxelOp) (dvid.IZYXSlice, error) {
	mapping, err := GetMapping(d, v)
	if err != nil {
		return nil, err
	}
	label := op.Supervoxel
	if mapping != nil {
		if mapped, found := mapping.MappedLabel(op.Supervoxel); found {
			label = mapped
		}
	}

	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	// get the index containing this supervoxel
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
		if err := idx.Unmarshal(idxBytes); err != nil {
			return nil, err
		}
		atomic.AddUint64(&metaHits, 1)
	} else {
		idx, err = getLabelIndex(k.VersionedCtx(), label)
		if err != nil {
			return nil, err
		}
	}

	// modify the index to reflect old supervoxel -> two new supervoxels.
	var svblocks dvid.IZYXSlice
	for blockStr, svc := range idx.Blocks {
		origNumVoxels, found := svc.Counts[op.Supervoxel]
		if found { // split supervoxel is in this block
			delete(svc.Counts, op.Supervoxel)
			izyx := dvid.IZYXString(blockStr)
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
	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelIndex(ctx, label, idx); err != nil {
		return nil, err
	}
	if indexCache != nil {
		idxBytes, err = idx.Marshal()
		if err != nil {
			return nil, err
		}
		k := indexKey{data: d, version: v, label: label}.Bytes()
		if err := indexCache.Set(k, idxBytes, 0); err != nil {
			return nil, err
		}
	}
	return svblocks, nil
}

// CleaveIndex modifies the label index to remove specified supervoxels and create another
// label index for this cleaved body.
func CleaveIndex(d dvid.Data, v dvid.VersionID, op labels.CleaveOp) error {
	mapping, err := GetMapping(d, v)
	if err != nil {
		return err
	}
	if mapping != nil {
		if mapped, found := mapping.MappedLabel(op.Target); found {
			return fmt.Errorf("can't cleave label %d which has already been merged into label %d", op.Target, mapped)
		}
	}

	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: op.Target}

	shard := op.Target % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	// get the index containing this supervoxel
	var idxBytes []byte
	if indexCache != nil {
		idxBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return err
		}
	}
	var idx *labels.Index
	if idxBytes != nil {
		idx = new(labels.Index)
		if err := idx.Unmarshal(idxBytes); err != nil {
			return err
		}
		atomic.AddUint64(&metaHits, 1)
	} else {
		idx, err = getLabelIndex(k.VersionedCtx(), op.Target)
		if err != nil {
			return err
		}
	}

	// modify and store the target label index after cleaving.
	cidx := idx.Cleave(op.CleavedSupervoxels)

	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelIndex(ctx, op.Target, idx); err != nil {
		return err
	}
	if indexCache != nil {
		idxBytes, err = idx.Marshal()
		if err != nil {
			return err
		}
		k := indexKey{data: d, version: v, label: op.Target}.Bytes()
		if err := indexCache.Set(k, idxBytes, 0); err != nil {
			return err
		}
	}

	// create a new label index to contain the cleaved supervoxels.
	if err := putLabelIndex(ctx, op.CleavedLabel, cidx); err != nil {
		return err
	}
	if indexCache != nil {
		idxBytes, err = cidx.Marshal()
		if err != nil {
			return err
		}
		k := indexKey{data: d, version: v, label: op.CleavedLabel}.Bytes()
		if err := indexCache.Set(k, idxBytes, 0); err != nil {
			return err
		}
	}

	return nil
}

// ChangeLabelIndex applies changes to a label's index and then stores the result.
// Supervoxel size changes for blocks should be passed into the function.  The passed
// SupervoxelDelta can contain more supervoxels than the label index.
func ChangeLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, delta labels.SupervoxelChanges) error {
	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	// get the index
	var err error
	var idxBytes []byte
	if indexCache != nil {
		idxBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return err
		}
	}
	var idx *labels.Index
	if idxBytes != nil {
		idx = new(labels.Index)
		if err := idx.Unmarshal(idxBytes); err != nil {
			return err
		}
		atomic.AddUint64(&metaHits, 1)
	} else {
		idx, err = getLabelIndex(k.VersionedCtx(), label)
		if err != nil {
			return err
		}
	}

	// Modify the label index
	if err := idx.ModifyBlocks(delta); err != nil {
		return err
	}

	// Persist the label index changes.
	ctx := datastore.NewVersionedCtx(d, v)
	if len(idx.Blocks) == 0 { // Delete this label's index
		if err := deleteLabelIndex(ctx, label); err != nil {
			return err
		}
		if indexCache != nil {
			k := indexKey{data: d, version: v, label: label}.Bytes()
			indexCache.Del(k)
		}
	} else {
		if err = putLabelIndex(ctx, label, idx); err != nil {
			return err
		}
		if indexCache != nil && idx != nil {
			idxBytes, err = idx.Marshal()
			if err != nil {
				return err
			}
			k := indexKey{data: d, version: v, label: label}.Bytes()
			if err := indexCache.Set(k, idxBytes, 0); err != nil {
				return err
			}
		}
	}
	return nil
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

// goroutine(s) that aggregates supervoxel changes across blocks, then calls
// mutex-guarded label index mutation routine.
func (d *Data) aggregateBlockChanges(v dvid.VersionID, ch <-chan blockChange) {
	svmap, err := GetMapping(d, v)
	if err != nil {
		dvid.Criticalf("couldn't get mapping for data %q, version %d: %v\n", d.DataName(), v)
		return
	}
	labelset := make(labels.Set)
	svChanges := make(labels.SupervoxelChanges)
	var maxLabel uint64
	for change := range ch {
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
			label, _ := svmap.MappedLabel(supervoxel)
			labelset[label] = struct{}{}
		}
	}
	go func() {
		if err := d.updateMaxLabel(v, maxLabel); err != nil {
			dvid.Errorf("max label change during block aggregation for %q: %v\n", d.DataName(), err)
		}
	}()

	if d.IndexedLabels {
		for label := range labelset {
			ChangeLabelIndex(d, v, label, svChanges)
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
		err = fmt.Errorf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
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
		err = fmt.Errorf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
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
func (d *Data) FoundSparseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) (bool, error) {
	idx, err := GetBoundedIndex(d, ctx.VersionID(), label, bounds)
	if err != nil {
		return false, err
	}

	if idx != nil && len(idx.Blocks) > 0 {
		dvid.Infof("Found %d blocks for label %d with constituents %s\n", len(idx.Blocks), label, idx.GetConstituentLabels())
		return true, nil
	}
	return false, nil
}

// WriteBinaryBlocks does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) WriteBinaryBlocks(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, w io.Writer) (bool, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label)
	if err != nil {
		return false, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return false, err
	}

	blocks, err := idx.GetProcessedBlockIndices(scale, bounds)
	if err != nil {
		return false, err
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	lbls := idx.GetConstituentLabels()
	go labels.WriteBinaryBlocks(label, lbls, op, bounds)
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

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, lbls, len(blocks), len(idx.Blocks))
	return true, nil
}

// WriteStreamingRLE does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) WriteStreamingRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, w io.Writer) (bool, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label)
	if err != nil {
		return false, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return false, err
	}

	blocks, err := idx.GetProcessedBlockIndices(scale, bounds)
	if err != nil {
		return false, err
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	lbls := idx.GetConstituentLabels()
	go labels.WriteRLEs(lbls, op, bounds)
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

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, lbls, len(blocks), len(idx.Blocks))
	return true, nil
}

func (d *Data) WriteLegacyRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, b dvid.Bounds, compression string, w io.Writer) (found bool, err error) {
	var data []byte
	data, err = d.GetLegacyRLE(ctx, label, scale, b)
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
			err = fmt.Errorf("only able to write %d of %d lz4 compressed bytes\n", n, outSize)
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

// GetLegacyRLE returns an encoded sparse volume given a label and an output format.
func (d *Data) GetLegacyRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds) ([]byte, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label)
	if err != nil {
		return nil, err
	}
	return d.getLegacyRLEs(ctx, idx, scale, bounds)
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
func (d *Data) getLegacyRLEs(ctx *datastore.VersionedCtx, idx *labels.Index, scale uint8, bounds dvid.Bounds) ([]byte, error) {
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

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}
	op := labels.NewOutputOp(buf)
	lbls := idx.GetConstituentLabels()
	go labels.WriteRLEs(lbls, op, bounds)
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
				dvid.Errorf("Block %s included in blocks for labels %s but has no data (%d times)... skipping.\n", izyx, lbls, numEmpty)
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
	dvid.Infof("[%s] labels %v: found %d of %d blocks within bounds excluding %d empty blocks, %d runs, serialized %d bytes\n", ctx, lbls, len(blocks), len(idx.Blocks), numEmpty, numRuns, len(serialization))
	return serialization, nil
}

// GetSparseCoarseVol returns an encoded sparse volume given a label.  This will return nil slice
// if the given label was not found.  The encoding has the following format where integers are
// little endian:
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
func (d *Data) GetSparseCoarseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) ([]byte, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, nil
	}
	blocks, err := idx.GetProcessedBlockIndices(0, bounds)
	if err != nil {
		return nil, err
	}

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
