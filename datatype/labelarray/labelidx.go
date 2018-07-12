package labelarray

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
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
	numBytes := server.CacheSize("labelarray")
	if indexCache == nil {
		if numBytes > 0 {
			indexCache = freecache.NewCache(numBytes)
			mbs := numBytes >> 20
			dvid.Infof("Created freecache of ~ %d MB for labelarray instances.\n", mbs)
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
// should not be called concurrently since label meta could have race condition
func getLabelMeta(ctx *datastore.VersionedCtx, label uint64) (*Meta, error) {
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

	var meta Meta
	if err := meta.UnmarshalBinary(val); err != nil {
		return nil, err
	}
	return &meta, nil
}

func putLabelMeta(ctx *datastore.VersionedCtx, label uint64, meta *Meta) error {
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q PutLabelMeta had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(label)
	serialization, err := meta.MarshalBinary()
	if err != nil {
		return fmt.Errorf("error trying to serialize meta for label %d, data %q: %v", label, ctx.Data().DataName(), err)
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

func deleteLabelMeta(ctx *datastore.VersionedCtx, label uint64) error {
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q DeleteLabelMeta had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(label)
	if err := store.Delete(ctx, tk); err != nil {
		return fmt.Errorf("unable to delete indices for label %d, data %s: %v", label, ctx.Data().DataName(), err)
	}
	return nil
}

// GetLabelIndex gets label index data from storage for a given data instance
// and version. Concurrency-safe access and supports caching.
func GetLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) (*Meta, error) {
	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	shard := label % numIndexShards
	indexMu[shard].RLock()
	defer indexMu[shard].RUnlock()

	var metaBytes []byte
	var err error
	if indexCache != nil {
		metaBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return nil, err
		}
	}
	if metaBytes != nil {
		var m Meta
		if err := m.UnmarshalBinary(metaBytes); err != nil {
			return nil, err
		}
		atomic.AddUint64(&metaHits, 1)
		return &m, nil
	}
	m, err := getLabelMeta(k.VersionedCtx(), label)
	if err != nil {
		return nil, err
	}
	if indexCache != nil && m != nil {
		metaBytes, err := m.MarshalBinary()
		if err != nil {
			dvid.Errorf("unable to marshal label %d index for %q: %v\n", label, d.DataName(), err)
		} else if err := indexCache.Set(k.Bytes(), metaBytes, 0); err != nil {
			dvid.Errorf("unable to set label %d index cache for %q: %v\n", label, d.DataName(), err)
		}
	}
	return m, err
}

// GetLabelProcessedIndex gets label index data from storage for a given data instance
// and version with scaling and/or bounds. Concurrency-safe access and supports caching.
func GetLabelProcessedIndex(d dvid.Data, v dvid.VersionID, label uint64, scale uint8, bounds dvid.Bounds) (*Meta, error) {
	meta, err := GetLabelIndex(d, v, label)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}
	voxels := meta.Voxels
	blocks := meta.Blocks
	if bounds.Block != nil && bounds.Block.IsSet() {
		blocks, err = meta.Blocks.FitToBounds(bounds.Block)
		if err != nil {
			return nil, err
		}
		voxels = 0
	}

	if scale > 0 {
		if blocks, err = blocks.Downres(scale); err != nil {
			return nil, err
		}
	}
	newMeta := Meta{Voxels: voxels, Blocks: blocks}
	return &newMeta, nil
}

// GetMappedLabelIndex gets index data for all labels potentially merged to the
// given label, allowing scaling and bounds. If bounds are used, the returned Meta
// will not include a voxel count.  Concurrency-safe access and supports caching.
func GetMappedLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, scale uint8, bounds dvid.Bounds) (*Meta, labels.Set, error) {
	iv := dvid.InstanceVersion{Data: d.DataUUID(), Version: v}
	mapping := labels.LabelMap(iv)
	if mapping != nil {
		// Check if this label has been merged.
		if _, found := mapping.FinalLabel(label); found {
			return nil, nil, nil
		}
	}

	// Get set of all labels that have been merged to this given label.
	var lbls labels.Set
	if mapping == nil {
		lbls = labels.Set{label: struct{}{}}
	} else {
		lbls = mapping.ConstituentLabels(label)
	}

	// Get the block indices for the set of labels.
	var voxels uint64
	var blocks dvid.IZYXSlice
	for label := range lbls {
		meta, err := GetLabelIndex(d, v, label)
		if err != nil {
			return nil, nil, err
		}
		if meta != nil {
			if len(lbls) == 1 {
				blocks = meta.Blocks
				voxels = meta.Voxels
			} else if len(meta.Blocks) > 0 {
				voxels += meta.Voxels
				blocks.Merge(meta.Blocks)
			}
		}
	}

	var err error
	if bounds.Block != nil && bounds.Block.IsSet() {
		blocks, err = blocks.FitToBounds(bounds.Block)
		if err != nil {
			return nil, nil, err
		}
		voxels = 0
	}

	if scale > 0 {
		if blocks, err = blocks.Downres(scale); err != nil {
			return nil, nil, err
		}
	}

	meta := Meta{Voxels: voxels, Blocks: blocks}
	return &meta, lbls, nil
}

// GetMappedLabelSetIndex gets index data for all labels potentially merged to the
// given labels, allowing scaling and bounds. If bounds are used, the returned Meta
// will not include a voxel count.  Concurrency-safe access and supports caching.
func GetMappedLabelSetIndex(d dvid.Data, v dvid.VersionID, lbls labels.Set, scale uint8, bounds dvid.Bounds) (*Meta, labels.Set, error) {
	iv := dvid.InstanceVersion{Data: d.DataUUID(), Version: v}
	mapping := labels.LabelMap(iv)
	if mapping != nil {
		// Check if this label has been merged.
		for label := range lbls {
			if mapped, found := mapping.FinalLabel(label); found {
				return nil, nil, fmt.Errorf("label %d has already been merged into label %d", label, mapped)
			}
		}
	}

	// Expand set of all labels based on mappings.
	var lbls2 labels.Set
	if mapping == nil {
		lbls2 = lbls
	} else {
		lbls2 = make(labels.Set)
		for label := range lbls {
			mappedlbls := mapping.ConstituentLabels(label)
			for label2 := range mappedlbls {
				lbls2[label2] = struct{}{}
			}
		}
	}

	// Get the block indices for the set of labels.
	var voxels uint64
	var blocks dvid.IZYXSlice
	for label := range lbls2 {
		meta, err := GetLabelIndex(d, v, label)
		if err != nil {
			return nil, nil, err
		}
		if meta != nil {
			if len(lbls) == 1 {
				blocks = meta.Blocks
				voxels = meta.Voxels
			} else if len(meta.Blocks) > 0 {
				voxels += meta.Voxels
				blocks.Merge(meta.Blocks)
			}
		}
	}

	var err error
	if bounds.Block != nil && bounds.Block.IsSet() {
		blocks, err = blocks.FitToBounds(bounds.Block)
		if err != nil {
			return nil, nil, err
		}
		voxels = 0
	}

	if scale > 0 {
		if blocks, err = blocks.Downres(scale); err != nil {
			return nil, nil, err
		}
	}

	meta := Meta{Voxels: voxels, Blocks: blocks}
	return &meta, lbls2, nil
}

// DeleteLabelIndex deletes the index for a given label.
func DeleteLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) error {
	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	ctx := datastore.NewVersionedCtx(d, v)
	if err := deleteLabelMeta(ctx, label); err != nil {
		return err
	}
	if indexCache != nil {
		k := indexKey{data: d, version: v, label: label}.Bytes()
		indexCache.Del(k)
	}
	return nil
}

// SetLabelIndex sets label index data from storage for a given data instance and
// version. If the given Meta is nil, the label index is deleted.  Concurrency-safe
// and supports caching.
func SetLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, m *Meta) error {
	if m == nil {
		return DeleteLabelIndex(d, v, label)
	}
	shard := label % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelMeta(ctx, label, m); err != nil {
		return err
	}
	metaBytes, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	if indexCache != nil {
		k := indexKey{data: d, version: v, label: label}.Bytes()
		if err := indexCache.Set(k, metaBytes, 0); err != nil {
			return err
		}
	}
	return nil
}

// ChangeLabelIndex applies changes to a label's index and then stores the result.
// Concurrency-safe and supports caching.
func ChangeLabelIndex(d dvid.Data, v dvid.VersionID, label uint64, delta blockDiffMap) error {
	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	shard := label % numIndexShards
	indexMu[shard].RLock()
	defer indexMu[shard].RUnlock()

	var metaBytes []byte
	var err error
	if indexCache != nil {
		metaBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			return err
		}
	}
	m := new(Meta)
	if metaBytes != nil {
		if err = m.UnmarshalBinary(metaBytes); err != nil {
			return err
		}
		atomic.AddUint64(&metaHits, 1)
	} else {
		var mret *Meta
		if mret, err = getLabelMeta(k.VersionedCtx(), label); err != nil {
			return err
		}
		if mret != nil {
			m = mret
		}
	}
	if err := m.applyChanges(delta); err != nil {
		return err
	}

	ctx := datastore.NewVersionedCtx(d, v)
	if len(m.Blocks) == 0 { // Delete this label's index
		if err := deleteLabelMeta(ctx, label); err != nil {
			return err
		}
		if indexCache != nil {
			k := indexKey{data: d, version: v, label: label}.Bytes()
			indexCache.Del(k)
		}
	} else {
		if err = putLabelMeta(ctx, label, m); err != nil {
			return err
		}
		if indexCache != nil && m != nil {
			metaBytes, err = m.MarshalBinary()
			if err != nil {
				return err
			}
			k := indexKey{data: d, version: v, label: label}.Bytes()
			if err := indexCache.Set(k, metaBytes, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// Meta gives a high-level overview of a label's voxels including a block index.
// Some properties are only used if the associated data instance has features enabled, e.g.,
// size tracking.
type Meta struct {
	Voxels uint64         // Total # of voxels in label.
	Blocks dvid.IZYXSlice // Sorted block coordinates occupied by label.

	LastModTime time.Time
	LastModUser string
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (m Meta) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(m.Blocks)*12+8)
	binary.LittleEndian.PutUint64(buf[0:8], m.Voxels)
	off := 8
	for _, izyx := range m.Blocks {
		copy(buf[off:off+12], string(izyx))
		off += 12
	}
	return buf, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (m *Meta) UnmarshalBinary(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("cannot unmarshal %d bytes into Meta", len(b))
	}
	m.Voxels = binary.LittleEndian.Uint64(b[0:8])
	numBlocks := (len(b) - 8) / 12
	if err := m.Blocks.UnmarshalBinary(b[8 : 8+numBlocks*12]); err != nil {
		return err
	}
	return nil
}

// change the block presence map
func (m *Meta) applyChanges(bdm blockDiffMap) error {
	var present dvid.IZYXSlice
	var absent dvid.IZYXSlice
	for block, diff := range bdm {
		m.Voxels = uint64(int64(m.Voxels) + int64(diff.delta))
		if diff.present {
			present = append(present, block)
		} else {
			absent = append(absent, block)
		}
	}
	if len(present) > 1 {
		sort.Sort(present)
	}
	if len(absent) > 1 {
		sort.Sort(absent)
	}

	m.Blocks.Delete(absent)
	m.Blocks.Merge(present)
	return nil
}

type sld struct { // sortable labelDiff
	bcoord dvid.IZYXString
	labelDiff
}

type slds []sld

func (s slds) Len() int {
	return len(s)
}

func (s slds) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s slds) Less(i, j int) bool {
	return s[i].bcoord < s[j].bcoord
}

const presentOld = uint8(0x01) // bit flag for label presence in old block
const presentNew = uint8(0x02) // bit flag for label presence in new block

// block-level analysis of mutation to get label changes in a block.  accumulates data for
// a given mutation into a map per mutation which will then be flushed for each label meta
// k/v pair at end of mutation.
func (d *Data) handleBlockMutate(v dvid.VersionID, ch chan blockChange, mut MutatedBlock) {
	if !d.IndexedLabels && !d.CountLabels {
		return
	}
	bc := blockChange{
		bcoord: mut.BCoord,
	}
	if d.IndexedLabels {
		bc.present = make(map[uint64]uint8)
		for _, label := range mut.Prev.Labels {
			if label != 0 {
				bc.present[label] |= presentOld
			}
		}
		for _, label := range mut.Data.Labels {
			if label != 0 {
				bc.present[label] |= presentNew
			}
		}
	}
	if d.CountLabels {
		bc.delta = mut.Data.CalcNumLabels(mut.Prev)
	}
	ch <- bc
}

// block-level analysis of label ingest to do indexing
func (d *Data) handleBlockIndexing(v dvid.VersionID, ch chan blockChange, mut IngestedBlock) {
	if !d.IndexedLabels && !d.CountLabels {
		return
	}
	bc := blockChange{
		bcoord: mut.BCoord,
	}
	if d.IndexedLabels {
		bc.present = make(map[uint64]uint8)
		for _, label := range mut.Data.Labels {
			if label != 0 {
				bc.present[label] |= presentNew
			}
		}
	}
	if d.CountLabels {
		bc.delta = mut.Data.CalcNumLabels(nil)
	}
	ch <- bc
}

// Goroutines accepts block-level changes and segregates all changes by label, and then
// sends label-specific changes to concurrency-handling label indexing functions.

type blockChange struct {
	bcoord  dvid.IZYXString
	present map[uint64]uint8
	delta   map[uint64]int32
}

// goroutine(s) that accepts label change data for a block, then consolidates it and writes label
// indexing.
func (d *Data) aggregateBlockChanges(v dvid.VersionID, ch <-chan blockChange) {
	var maxLabel uint64
	ldm := make(map[uint64]blockDiffMap)
	for change := range ch {
		for label, flag := range change.present {
			bdm, found := ldm[label]
			if !found {
				bdm = make(blockDiffMap)
				ldm[label] = bdm
			}
			var diff labelDiff
			switch flag {
			case presentOld: // we no longer have this label in the block
				diff.present = false
				bdm[change.bcoord] = diff
			case presentNew: // this is a new label in this block
				diff.present = true
				bdm[change.bcoord] = diff
				if label > maxLabel {
					maxLabel = label
				}
			case presentOld | presentNew: // no change
				diff.present = true
				bdm[change.bcoord] = diff
			}
		}
		for label, delta := range change.delta {
			bdm, found := ldm[label]
			if !found {
				bdm = make(blockDiffMap)
				ldm[label] = bdm
			}
			diff := bdm[change.bcoord]
			diff.delta += delta
		}
	}
	go func() {
		if err := d.updateMaxLabel(v, maxLabel); err != nil {
			dvid.Errorf("max label change during block aggregation for %q: %v\n", d.DataName(), err)
		}
	}()

	if d.IndexedLabels {
		for label, bdm := range ldm {
			ChangeLabelIndex(d, v, label, bdm)
		}
	}
}

type labelDiff struct {
	delta   int32 // change in # voxels
	present bool
}
type blockDiffMap map[dvid.IZYXString]labelDiff

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
	m, members, err := GetMappedLabelIndex(d, ctx.VersionID(), label, 0, bounds)
	if err != nil {
		return false, err
	}

	if m != nil && len(m.Blocks) > 0 {
		dvid.Infof("Found %d blocks for label %d with constituents %s\n", len(m.Blocks), label, members)
		return true, nil
	}
	return false, nil
}

// writeBinaryBlocks does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) writeBinaryBlocks(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, w io.Writer) (bool, error) {
	meta, lbls, err := GetMappedLabelIndex(d, ctx.VersionID(), label, scale, bounds)
	if err != nil {
		return false, err
	}
	if meta == nil || len(meta.Blocks) == 0 || len(lbls) == 0 {
		return false, err
	}

	indices, err := getBoundedBlockIndices(meta, bounds)
	if err != nil {
		return false, err
	}
	sort.Sort(indices)

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteBinaryBlocks(label, lbls, op, bounds)
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

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, lbls, len(indices), len(meta.Blocks))
	return true, preErr
}

// writeStreamingRLE does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) writeStreamingRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, compression string, w io.Writer) (bool, error) {
	meta, lbls, err := GetMappedLabelIndex(d, ctx.VersionID(), label, scale, bounds)
	if err != nil {
		return false, err
	}
	if meta == nil || len(meta.Blocks) == 0 || len(lbls) == 0 {
		return false, err
	}

	indices, err := getBoundedBlockIndices(meta, bounds)
	if err != nil {
		return false, err
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return false, err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteRLEs(lbls, op, bounds)
	for _, izyx := range indices {
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

	dvid.Infof("[%s] labels %v: streamed %d of %d blocks within bounds\n", ctx, lbls, len(indices), len(meta.Blocks))
	return true, nil
}

func (d *Data) writeLegacyRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, b dvid.Bounds, compression string, w io.Writer) (found bool, err error) {
	var data []byte
	data, err = d.getLegacyRLE(ctx, label, scale, b)
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
func (d *Data) getLegacyRLE(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds) ([]byte, error) {
	meta, lbls, err := GetMappedLabelIndex(d, ctx.VersionID(), label, scale, bounds)
	if err != nil {
		return nil, err
	}
	if meta == nil || len(meta.Blocks) == 0 || len(lbls) == 0 {
		return nil, err
	}
	indices, err := getBoundedBlockIndices(meta, bounds)
	if err != nil {
		return nil, err
	}
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	op := labels.NewOutputOp(buf)
	go labels.WriteRLEs(lbls, op, bounds)
	var numEmpty int
	for _, izyx := range indices {
		tk := NewBlockTKeyByCoord(scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			numEmpty++
			if numEmpty < 10 {
				dvid.Errorf("Block %s included in indices for labels %s but has no data (%d times)... skipping.\n", izyx, lbls, numEmpty)
			} else if numEmpty == 10 {
				dvid.Errorf("Over %d blocks included in indices with no data.  Halting error stream.\n", numEmpty)
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
	if numEmpty < len(indices) {
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
	dvid.Infof("[%s] labels %v: found %d of %d blocks within bounds excluding %d empty blocks, %d runs, serialized %d bytes\n", ctx, lbls, len(indices), len(meta.Blocks), numEmpty, numRuns, len(serialization))
	return serialization, nil
}

// apply bounds to list of block indices.
func getBoundedBlockIndices(meta *Meta, bounds dvid.Bounds) (dvid.IZYXSlice, error) {
	indices := make(dvid.IZYXSlice, len(meta.Blocks))
	totBlocks := 0
	for _, izyx := range meta.Blocks {
		if bounds.Block.BoundedX() || bounds.Block.BoundedY() || bounds.Block.BoundedZ() {
			blockX, blockY, blockZ, err := izyx.Unpack()
			if err != nil {
				return nil, fmt.Errorf("Error decoding block %v: %v\n", izyx, err)
			}
			if bounds.Block.OutsideX(blockX) || bounds.Block.OutsideY(blockY) || bounds.Block.OutsideZ(blockZ) {
				continue
			}
		}
		indices[totBlocks] = izyx
		totBlocks++
	}
	if totBlocks == 0 {
		return nil, nil
	}
	return indices[:totBlocks], nil
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
	meta, _, err := GetMappedLabelIndex(d, ctx.VersionID(), label, 0, bounds)
	if err != nil {
		return nil, err
	}

	if meta == nil || len(meta.Blocks) == 0 {
		return nil, nil
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	spans, err := meta.Blocks.WriteSerializedRLEs(buf)
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
		var meta Meta
		if len(val) != 0 {
			if err := meta.UnmarshalBinary(val); err != nil {
				return err
			}
			blocks := meta.Blocks
			if bounds.Block != nil && bounds.Block.IsSet() {
				blocks, err = blocks.FitToBounds(bounds.Block)
				if err != nil {
					return err
				}
			}

			buf := new(bytes.Buffer)
			spans, err := meta.Blocks.WriteSerializedRLEs(buf)
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
