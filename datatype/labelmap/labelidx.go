package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	pb "google.golang.org/protobuf/proto"

	"github.com/coocood/freecache"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
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

	mutcache map[dvid.UUID]storage.OrderedKeyValueDB
)

// Initialize establishes the in-memory labelmap and other supporting
// data systems if specified in the server configuration:
// - index caching if cache size is specified in the server config.
// - mutation cache for label indices if specified in server config.
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

	mutcachePath := server.MutcachePath(d.DataName())
	if mutcachePath != "" {
		// Setup database support for mutation cache for this instance
		engine := storage.GetEngine("badger")
		var cfg dvid.Config
		cfg.Set("path", mutcachePath)
		store, _, err := engine.NewStore(dvid.StoreConfig{Config: cfg, Engine: "badger"})
		if err != nil {
			dvid.Criticalf("can't initialize Badger: %v\n", err)
		} else {
			okvDB, ok := store.(storage.OrderedKeyValueDB)
			if !ok {
				dvid.Criticalf("can't get proper ordered keyvalue DB for mutation cache %q for data %q\n", mutcachePath, d.DataName())
			} else {
				if mutcache == nil {
					mutcache = make(map[dvid.UUID]storage.OrderedKeyValueDB)
				}
				mutcache[d.DataUUID()] = okvDB
			}
		}
	}

	ancestry, err := datastore.GetBranchVersions(d.RootUUID(), "")
	if err != nil {
		dvid.Criticalf("Unable to get ancestry of master branch: %v\n", err)
		return
	}
	if len(ancestry) == 0 {
		dvid.Infof("No versions in master branch -- not loading labelmap %q\n", d.DataName())
		return
	}
	masterLeafV, err := datastore.VersionFromUUID(ancestry[0])
	if err != nil {
		dvid.Criticalf("Unable to get version ID from master leaf UUID %s: %v\n", ancestry[0], err)
	}
	if _, err := getMapping(d, masterLeafV); err != nil {
		dvid.Criticalf("Unable to initialize in-memory labelmap for data %q: %v\n", d.DataName(), err)
	}
}

func (d *Data) getMutcache(v dvid.VersionID, mutID uint64, label uint64) (*labels.Index, error) {
	if mutcache == nil {
		return nil, nil
	}
	db, found := mutcache[d.DataUUID()]
	if !found {
		return nil, nil
	}
	ctx := datastore.NewVersionedCtx(d, v)
	tkey1 := newMutcacheKey(label, mutID)
	tkey2 := newMutcacheKey(label, 0)
	var idx *labels.Index
	db.ProcessRange(ctx, tkey1, tkey2, nil, func(chunk *storage.Chunk) error {
		label, curMutID, err := decodeMutcacheKey(chunk.K)
		if err != nil {
			return err
		}
		idx = new(labels.Index)
		if err := pb.Unmarshal(chunk.V, idx); err != nil {
			return err
		}
		dvid.Infof("Mutcache label %d mutid %d > %d\n", label, mutID, curMutID)
		return fmt.Errorf("found mutcache")
	})
	return idx, nil
}

func (d *Data) addMutcache(v dvid.VersionID, mutID uint64, indices ...*labels.Index) error {
	if mutcache == nil {
		return nil
	}
	db, found := mutcache[d.DataUUID()]
	if !found {
		return nil
	}
	ctx := datastore.NewVersionedCtx(d, v)
	for _, idx := range indices {
		tkey := newMutcacheKey(idx.Label, mutID)
		idxBytes, err := pb.Marshal(idx)
		if err != nil {
			return fmt.Errorf("unable to marshal index for label %d before caching: %v", idx.Label, err)
		}
		if err := db.Put(ctx, tkey, idxBytes); err != nil {
			return err
		}
	}
	return nil
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

// TODO -- allow check of the cache as well.
func (d *Data) labelIndexExists(v dvid.VersionID, label uint64) (bool, error) {
	ctx := datastore.NewVersionedCtx(d, v)
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return false, err
	}
	return store.Exists(ctx, NewLabelIndexTKey(label))
}

// returns nil if no Meta is found.
// should only call getCachedLabelIndex external to this file.
func getLabelIndex(ctx *datastore.VersionedCtx, label uint64) (*labels.Index, error) {
	// timedLog := dvid.NewTimeLog()
	store, err := datastore.GetKeyValueDB(ctx.Data())
	if err != nil {
		return nil, err
	}
	compressed, err := store.Get(ctx, NewLabelIndexTKey(label))
	if err != nil {
		return nil, err
	}
	if len(compressed) == 0 {
		// timedLog.Infof("retrieved empty index for label %d", label)
		return nil, nil
	}
	val, _, err := dvid.DeserializeData(compressed, true)
	if err != nil {
		return nil, err
	}

	idx := new(labels.Index)
	if err := pb.Unmarshal(val, idx); err != nil {
		return nil, err
	}
	if idx.Label == 0 {
		idx.Label = label
	}
	// timedLog.Infof("retrieved label %d index with %d blocks", label, len(idx.Blocks))
	return idx, nil
}

func getLabelIndexCompressed(store storage.KeyValueGetter, ctx *datastore.VersionedCtx, label uint64) ([]byte, error) {
	compressed, err := store.Get(ctx, NewLabelIndexTKey(label))
	if err != nil {
		return nil, err
	}
	if len(compressed) == 0 {
		// timedLog.Infof("retrieved empty index for label %d", label)
		return nil, nil
	}
	return compressed, nil
}

func getProtoLabelIndices(ctx *datastore.VersionedCtx, dataIn []byte) (numLabels int, dataOut []byte, err error) {
	var labelList []uint64
	if err = json.Unmarshal(dataIn, &labelList); err != nil {
		err = fmt.Errorf("expected JSON label list: %v", err)
		return
	}
	numLabels = len(labelList)
	if numLabels > 50000 {
		err = fmt.Errorf("only 50,000 label indices can be returned at a time, %d given", len(labelList))
		return
	}
	var indices proto.LabelIndices
	indices.Indices = make([]*proto.LabelIndex, len(labelList))
	for i, label := range labelList {
		var idx *labels.Index
		if idx, err = getLabelIndex(ctx, label); err != nil {
			err = fmt.Errorf("could not get label %d index in position %d: %v", label, i, err)
			return
		}
		if idx != nil {
			indices.Indices[i] = &(idx.LabelIndex)
		} else {
			index := proto.LabelIndex{
				Label: label,
			}
			indices.Indices[i] = &index
		}
	}
	if dataOut, err = pb.Marshal(&indices); err != nil {
		err = fmt.Errorf("could not serialize %d label indices: %v", len(labelList), err)
	}
	return
}

// batch mode put/delete using protobuf indices without caching
func putProtoLabelIndices(ctx *datastore.VersionedCtx, dataIn []byte) (numAdded, numDeleted int, err error) {
	data := ctx.Data()
	var store storage.KeyValueDB
	if store, err = datastore.GetKeyValueDB(data); err != nil {
		err = fmt.Errorf("data %q had error getting KeyValue store: %v", data.DataName(), err)
		return
	}
	indices := new(proto.LabelIndices)
	if err = pb.Unmarshal(dataIn, indices); err != nil {
		return
	}
	var maxLabel uint64
	for i, protoIdx := range indices.Indices {
		if protoIdx == nil {
			err = fmt.Errorf("indices included a nil index in position %d", i)
			return
		}
		if protoIdx.Label == 0 {
			err = fmt.Errorf("index %d had label 0, which is a reserved label", i)
			return
		}
		if len(protoIdx.Blocks) == 0 {
			if err = deleteLabelIndex(ctx, protoIdx.Label); err != nil {
				return
			}
			numDeleted++
			continue
		}
		numAdded++
		idx := labels.Index{LabelIndex: *protoIdx}
		if err = putLabelIndex(store, ctx, data, &idx); err != nil {
			return
		}
		if idx.Label > maxLabel {
			maxLabel = idx.Label
		}
	}
	// handle updating the max label
	d, ok := data.(*Data)
	if !ok {
		err = fmt.Errorf("unable to update max label during PUT label index due to bad data ptr for %q", data.DataName())
		return
	}
	_, err = d.updateMaxLabel(ctx.VersionID(), maxLabel)
	return
}

// puts label index and doesn't calc max label
// Note: should only call putCachedLabelIndex external to this file so recent changes get cached.
func putLabelIndex(store storage.KeyValueDB, ctx *datastore.VersionedCtx, data dvid.Data, idx *labels.Index) error {
	tk := NewLabelIndexTKey(idx.Label)
	serialization, err := pb.Marshal(idx)
	if err != nil {
		return fmt.Errorf("error trying to serialize index for label set %d, data %q: %v", idx.Label, data.DataName(), err)
	}
	compressFormat, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	compressed, err := dvid.SerializeData(serialization, compressFormat, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("error trying to LZ4 compress label %d indexing in data %q", idx.Label, data.DataName())
	}
	if err := store.Put(ctx, tk, compressed); err != nil {
		return fmt.Errorf("unable to store indices for label %d, data %s: %v", idx.Label, data.DataName(), err)
	}
	return err
}

// puts label index and stores max label.
// Note: should only call putCachedLabelIndex external to this file so recent changes get cached.
func putLabelIndexAndMax(ctx *datastore.VersionedCtx, idx *labels.Index) error {
	// timedLog := dvid.NewTimeLog()
	store, err := datastore.GetOrderedKeyValueDB(ctx.Data())
	if err != nil {
		return fmt.Errorf("data %q PutLabelMeta had error initializing store: %v", ctx.Data().DataName(), err)
	}

	tk := NewLabelIndexTKey(idx.Label)
	serialization, err := pb.Marshal(idx)
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
	d, ok := ctx.Data().(*Data)
	if !ok {
		return fmt.Errorf("Unable to update max label during PUT label index due to bad data ptr")
	}
	_, err = d.updateMaxLabel(ctx.VersionID(), idx.Label)

	// timedLog.Infof("stored label %d index with %d blocks", idx.Label, len(idx.Blocks))
	return err
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

// only returns error if we can't get index from storage, not if we can't get it from cache
func getCachedLabelIndex(d dvid.Data, v dvid.VersionID, label uint64) (*labels.Index, error) {
	atomic.AddUint64(&metaAttempts, 1)
	k := indexKey{data: d, version: v, label: label}

	var err error
	var idxBytes []byte
	if indexCache != nil {
		idxBytes, err = indexCache.Get(k.Bytes())
		if err != nil && err != freecache.ErrNotFound {
			dvid.Errorf("cache get of index for label %d failed due to error: %v\n", label, err)
			idxBytes = nil
		}
	}
	var idx *labels.Index
	if idxBytes != nil {
		idx = new(labels.Index)
		if err = pb.Unmarshal(idxBytes, idx); err != nil {
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
		idxBytes, err := pb.Marshal(idx)
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

// only returns error if we can't persist index, not if we can't cache
func putCachedLabelIndex(d dvid.Data, v dvid.VersionID, idx *labels.Index) error {
	ctx := datastore.NewVersionedCtx(d, v)
	if err := putLabelIndexAndMax(ctx, idx); err != nil {
		return err
	}
	if indexCache != nil {
		idxBytes, err := pb.Marshal(idx)
		if err != nil {
			dvid.Errorf("unable to marshal index for label %d before caching: %v\n", idx.Label, err)
			return nil
		}
		k := indexKey{data: d, version: v, label: idx.Label}.Bytes()
		if err = indexCache.Set(k, idxBytes, 0); err != nil {
			dvid.Errorf("unable to cache index for label %d: %v\n", idx.Label, err)
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

///////////////////////////////////////////////////////////////////////////
// The following public functions are concurrency-safe and support caching.

// GetSupervoxels returns the set of supervoxel ids that compose the given label.
// Used in other datatypes private interfaces (e.g., tarsupervoxels) and generally useful
// for other packages.
func (d *Data) GetSupervoxels(v dvid.VersionID, label uint64) (labels.Set, error) {
	idx, err := GetLabelIndex(d, v, label, false)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, err
	}
	return idx.GetSupervoxels(), nil
}

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
				if mapped == 0 {
					return nil, fmt.Errorf("cannot get label for supervoxel %d, which has been split and doesn't exist anymore", label)
				}
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

func getSupervoxelSizes(d dvid.Data, v dvid.VersionID, supervoxels []uint64) ([]uint64, error) {
	svmap, err := getMapping(d, v)
	if err != nil {
		return nil, fmt.Errorf("couldn't get mapping for data %q, version %d: %v", d.DataName(), v, err)
	}
	labelsets := make(map[uint64][]uint64) // maps labels -> set of supervoxels in it.
	labels, _, err := svmap.MappedLabels(v, supervoxels)
	if err != nil {
		return nil, err
	}
	for i, label := range labels {
		labelsets[label] = append(labelsets[label], supervoxels[i])
	}

	sizemap := make(map[uint64]uint64, len(supervoxels))
	for label, svlist := range labelsets {
		idx, err := GetLabelIndex(d, v, label, false)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			for _, sv := range svlist {
				sizemap[sv] = 0
			}
		} else {
			svcounts := idx.GetSupervoxelCounts()
			for _, sv := range svlist {
				sizemap[sv] = svcounts[sv]
			}
		}
	}
	sizes := make([]uint64, len(supervoxels))
	for i, sv := range supervoxels {
		sizes[i] = sizemap[sv]
	}
	return sizes, nil
}

// GetLabelSizes returns the # of voxels in the given labels.  If isSupervoxel = true, the given
// labels are interpreted as supervoxel ids and the sizes are of a supervoxel.  If a label doesn't
// exist, a zero (not error) is returned.
func GetLabelSizes(d dvid.Data, v dvid.VersionID, labels []uint64, isSupervoxel bool) (sizes []uint64, err error) {
	if isSupervoxel {
		return getSupervoxelSizes(d, v, labels)
	}
	sizes = make([]uint64, len(labels))
	for i, label := range labels {
		idx, err := GetLabelIndex(d, v, label, false)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			sizes[i] = 0
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

type proximityJSON struct {
	Block    dvid.ChunkPoint3d
	Distance int
}

// TODO: Flesh out this stub for /proximity endpoint.
func (d *Data) getProximity(ctx *datastore.VersionedCtx, idx1, idx2 *labels.Index) (jsonBytes []byte, err error) {
	// Find all blocks shared by the two indices

	// For each intersecting block, determine distance between labels and add to output.
	return
}

// CleaveIndex modifies the label index to remove specified supervoxels and create another
// label index for this cleaved body.
func (d *Data) cleaveIndex(v dvid.VersionID, op labels.CleaveOp, info dvid.ModInfo) (cleavedSize, remainSize uint64, err error) {
	shard := op.Target % numIndexShards
	indexMu[shard].Lock()
	defer indexMu[shard].Unlock()

	var idx *labels.Index
	if idx, err = getCachedLabelIndex(d, v, op.Target); err != nil {
		return
	}
	if idx == nil {
		err = fmt.Errorf("cannot cleave non-existent label %d", op.Target)
		return
	}

	if err := d.addMutcache(v, op.MutID, idx); err != nil {
		dvid.Criticalf("unable to add cleaved mutid %d index %d: %v\n", op.MutID, op.Target, err)
	}

	supervoxels := idx.GetSupervoxels()
	for _, supervoxel := range op.CleavedSupervoxels {
		if _, found := supervoxels[supervoxel]; !found {
			err = fmt.Errorf("cannot cleave supervoxel %d, which does not exist in label %d", supervoxel, op.Target)
			return
		}
		delete(supervoxels, supervoxel)
	}
	if len(supervoxels) == 0 {
		err = fmt.Errorf("cannot cleave all supervoxels from the label %d", op.Target)
		return
	}

	// create a new label index to contain the cleaved supervoxels.
	// we don't have to worry about mutex here because it's a new index.
	var cidx *labels.Index
	mutInfo := dvid.MutInfo{
		MutID:   op.MutID,
		ModInfo: info,
	}
	cleavedSize, remainSize, cidx = idx.Cleave(op.CleavedLabel, op.CleavedSupervoxels, mutInfo)
	if err = putCachedLabelIndex(d, v, cidx); err != nil {
		return
	}
	err = putCachedLabelIndex(d, v, idx)
	return
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

// getMergedIndex gets index data for all labels in a set with possible bounds.
func (d *Data) getMergedIndex(v dvid.VersionID, lbls labels.Set, mutInfo dvid.MutInfo, bounds dvid.Bounds) (*labels.Index, error) {
	if len(lbls) == 0 {
		return nil, nil
	}
	idx := new(labels.Index)
	for label := range lbls {
		idx2, err := GetLabelIndex(d, v, label, false)
		if err != nil {
			return nil, err
		}
		if err := d.addMutcache(v, mutInfo.MutID, idx2); err != nil {
			dvid.Criticalf("unable to add merge mutid %d index %d: %v\n", mutInfo.MutID, label, err)
		}
		if bounds.Block != nil && bounds.Block.IsSet() {
			if err := idx2.FitToBounds(bounds.Block); err != nil {
				return nil, err
			}
		}
		if err := idx.Add(idx2, mutInfo); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

// given supervoxels with given mapping and whether they were actually in in-memory SVMap, check
// label indices to see if supervoxels really are present in assigned bodies.
func (d *Data) verifyMappings(ctx *datastore.VersionedCtx, supervoxels, mapped []uint64, found []bool) (verified []uint64, err error) {
	numSupervoxels := len(supervoxels)
	if numSupervoxels != len(mapped) {
		return nil, fmt.Errorf("length of supervoxels list (%d) not equal to length of provided mappings (%d)", numSupervoxels, len(mapped))
	}
	if numSupervoxels != len(found) {
		return nil, fmt.Errorf("length of supervoxels list (%d) not equal to length of provided found mappings (%d)", numSupervoxels, len(found))
	}
	bodySupervoxels := make(map[uint64]labels.Set)
	for i, supervoxel := range supervoxels {
		if supervoxel != 0 && !found[i] { // we are uncertain whether this is a real or implicit supervoxel that may not exist
			bodysvs, bodyfound := bodySupervoxels[mapped[i]]
			if !bodyfound {
				bodysvs = labels.Set{supervoxel: struct{}{}}
			} else {
				bodysvs[supervoxel] = struct{}{}
			}
			bodySupervoxels[mapped[i]] = bodysvs
		}
	}

	wasVerified := make(map[uint64]bool, numSupervoxels)
	for label, bodysvs := range bodySupervoxels {
		shard := label % numIndexShards
		indexMu[shard].RLock()
		idx, err := getCachedLabelIndex(d, ctx.VersionID(), label)
		indexMu[shard].RUnlock()
		if err != nil {
			return nil, err
		}
		svpresent := idx.SupervoxelsPresent(bodysvs)
		for supervoxel, present := range svpresent {
			wasVerified[supervoxel] = present
		}
	}

	verified = make([]uint64, numSupervoxels)
	for i, label := range mapped {
		if found[i] || wasVerified[supervoxels[i]] {
			verified[i] = label
		} else {
			verified[i] = 0
		}
	}
	return
}

// computes supervoxel split on a label index, returning the split index and the slice of blocks
// that were modified.
func (d *Data) splitSupervoxelIndex(v dvid.VersionID, info dvid.ModInfo, op labels.SplitSupervoxelOp, idx *labels.Index) (dvid.IZYXSlice, error) {
	idx.LastMutid = op.MutID
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
				if splitNumVoxels > uint64(origNumVoxels) {
					return nil, fmt.Errorf("tried to split %d voxels from supervoxel %d, but label index only has %d voxels", splitNumVoxels, op.Supervoxel, origNumVoxels)
				}
				if splitNumVoxels != uint64(origNumVoxels) {
					svc.Counts[op.RemainSupervoxel] = origNumVoxels - uint32(splitNumVoxels)
				}
			} else { // part of remainder
				svc.Counts[op.RemainSupervoxel] = origNumVoxels
			}
		}
	}
	return svblocks, nil
}

// SplitIndex modifies the split label's index and creates a new index for the split portion.
func (d *Data) splitIndex(v dvid.VersionID, info dvid.ModInfo, op labels.SplitOp, idx *labels.Index, splitMap dvid.BlockRLEs, blockSplits blockSplitsMap) error {
	idx.LastMutid = op.MutID
	idx.LastModUser = info.User
	idx.LastModTime = info.Time
	idx.LastModApp = info.App

	sidx := new(labels.Index)
	sidx.LastMutid = op.MutID
	sidx.LastModUser = info.User
	sidx.LastModTime = info.Time
	sidx.LastModApp = info.App
	sidx.Label = op.NewLabel
	sidx.Blocks = make(map[uint64]*proto.SVCount, len(blockSplits))

	for zyx, indexsvc := range idx.Blocks {
		splitCount, inSplitBlock := blockSplits[zyx]
		var splitsvc *proto.SVCount
		if inSplitBlock {
			splitsvc = new(proto.SVCount)
			splitsvc.Counts = make(map[uint64]uint32, len(splitCount))
		}
		for supervoxel, svOrigCount := range indexsvc.Counts {
			var svWasSplit bool
			var svSplitCount labels.SVSplitCount
			if inSplitBlock {
				svSplitCount, svWasSplit = splitCount[supervoxel]
				if svWasSplit {
					if svSplitCount.Voxels > svOrigCount {
						return fmt.Errorf("block %s had %d voxels written over supervoxel %d in label %d split yet this supervoxel only had %d voxels from index", labels.BlockIndexToIZYXString(zyx), svSplitCount.Voxels, supervoxel, op.Target, svOrigCount)
					}
					splitsvc.Counts[svSplitCount.Split] = svSplitCount.Voxels
					if svOrigCount > svSplitCount.Voxels {
						indexsvc.Counts[svSplitCount.Remain] = svOrigCount - svSplitCount.Voxels
					}
					delete(indexsvc.Counts, supervoxel)
				}
			}
			if !svWasSplit {
				// see if split anywhere
				svsplit, found := op.SplitMap[supervoxel]
				if found {
					indexsvc.Counts[svsplit.Remain] = svOrigCount
					delete(indexsvc.Counts, supervoxel)
				}
			}
		}
		if inSplitBlock {
			sidx.Blocks[zyx] = splitsvc
		}
		if len(indexsvc.Counts) == 0 {
			delete(idx.Blocks, zyx)
		}
	}

	if err := putCachedLabelIndex(d, v, idx); err != nil {
		return fmt.Errorf("modify split index for data %q, label %d: %v", d.DataName(), op.Target, err)
	}
	if err := putCachedLabelIndex(d, v, sidx); err != nil {
		return fmt.Errorf("create new split index for data %q, label %d: %v", d.DataName(), op.NewLabel, err)
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
func (d *Data) aggregateBlockChanges(v dvid.VersionID, svmap *VCache, ch <-chan blockChange) {
	mappedVersions := svmap.getMappedVersionsDist(v)
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
			label, _ := svmap.mapLabel(supervoxel, mappedVersions)
			labelset[label] = struct{}{}
		}
	}
	go func() {
		if _, err := d.updateMaxLabel(v, maxLabel); err != nil {
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
		return true, nil
	}
	return false, nil
}

// encapsulates all we need to know about blocks & supervoxels within scaled, bounded labels
type labelBlockMetadata struct {
	label        uint64
	scale        uint8
	bounds       dvid.Bounds
	supervoxels  labels.Set
	sortedBlocks dvid.IZYXSlice
}

// use label index to return supervoxels and sorted blocks that meet criteria of scale and bounds
func (d *Data) constrainLabelIndex(ctx *datastore.VersionedCtx, label uint64, scale uint8, bounds dvid.Bounds, isSupervoxel bool) (blockMeta *labelBlockMetadata, exists bool, err error) {
	blockMeta = &labelBlockMetadata{label: label, scale: scale, bounds: bounds}
	var idx *labels.Index
	if idx, err = GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel); err != nil {
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		exists = false
		return
	}
	blockMeta.supervoxels = idx.GetSupervoxels()
	var supervoxel uint64
	if isSupervoxel {
		supervoxel = label
		if _, exists = blockMeta.supervoxels[supervoxel]; !exists {
			return
		}
		blockMeta.supervoxels = labels.Set{supervoxel: struct{}{}}
	}

	if blockMeta.sortedBlocks, err = idx.GetProcessedBlockIndices(scale, bounds, supervoxel); err != nil {
		return
	}
	if len(blockMeta.sortedBlocks) == 0 { // if no blocks are within bounds, regardless of scale
		exists = false
		return
	}
	sort.Sort(blockMeta.sortedBlocks)
	exists = true
	return
}

// writeBinaryBlocks does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label exists even if there's no data at the given scale and bounds.
func (d *Data) writeBinaryBlocks(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, compression string, w io.Writer) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteBinaryBlocks(blockMeta.label, blockMeta.supervoxels, op, blockMeta.bounds)
	var preErr error
	for _, izyx := range blockMeta.sortedBlocks {
		tk := NewBlockTKeyByCoord(blockMeta.scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			preErr = err
			break
		}
		if data == nil {
			preErr = fmt.Errorf("expected block %s @ scale %d to have key-value, but found none", izyx, blockMeta.scale)
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
		return err
	}

	dvid.Infof("labelmap %q label %d consisting of %d supervoxels: streamed %d blocks within bounds\n",
		d.DataName(), blockMeta.label, len(blockMeta.supervoxels), len(blockMeta.sortedBlocks))
	return preErr
}

// writeStreamingRLE does a streaming write of an encoded sparse volume given a label.
// It returns a bool whether the label was found in the given bounds and any error.
func (d *Data) writeStreamingRLE(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, compression string, w io.Writer) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	op := labels.NewOutputOp(w)
	go labels.WriteRLEs(blockMeta.supervoxels, op, blockMeta.bounds)
	for _, izyx := range blockMeta.sortedBlocks {
		tk := NewBlockTKeyByCoord(blockMeta.scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return err
		}
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return err
		}
		var block labels.Block
		if err := block.UnmarshalBinary(blockData); err != nil {
			return err
		}
		pb := labels.PositionedBlock{
			Block:  block,
			BCoord: izyx,
		}
		op.Process(&pb)
	}
	if err = op.Finish(); err != nil {
		return err
	}

	dvid.Infof("labelmap %q label %d consisting of %d supervoxels: streamed %d blocks within bounds\n",
		d.DataName(), blockMeta.label, len(blockMeta.supervoxels), len(blockMeta.sortedBlocks))
	return nil
}

// Write legacy RLEs to writer. Body can exist but have no data if scale is too low-res.
//
// The encoding has the following format where integers are little endian:
//
//	byte     Payload descriptor:
//	           Bit 0 (LSB) - 8-bit grayscale
//	           Bit 1 - 16-bit grayscale
//	           Bit 2 - 16-bit normal
//	           ...
//	uint8    Number of dimensions
//	uint8    Dimension of run (typically 0 = X)
//	byte     Reserved (to be used later)
//	uint32    0
//	uint32    # Spans
//	Repeating unit of:
//	    int32   Coordinate of run start (dimension 0)
//	    int32   Coordinate of run start (dimension 1)
//	    int32   Coordinate of run start (dimension 2)
//	    int32   Length of run
//	    bytes   Optional payload dependent on first byte descriptor
func (d *Data) writeLegacyRLE(ctx *datastore.VersionedCtx, blockMeta *labelBlockMetadata, compression string, w io.Writer) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	op := labels.NewOutputOp(buf)
	go labels.WriteRLEs(blockMeta.supervoxels, op, blockMeta.bounds)
	var numEmpty int
	for _, izyx := range blockMeta.sortedBlocks {
		tk := NewBlockTKeyByCoord(blockMeta.scale, izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			numEmpty++
			if numEmpty < 10 {
				dvid.Errorf("Block %s included in blocks for labels %s but has no data (%d times)... skipping.\n",
					izyx, blockMeta.supervoxels, numEmpty)
			} else if numEmpty == 10 {
				dvid.Errorf("Over %d blocks included in blocks with no data.  Halting error stream.\n", numEmpty)
			}
			continue
		}
		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			return err
		}
		var block labels.Block
		if err = block.UnmarshalBinary(blockData); err != nil {
			return err
		}
		pb := labels.PositionedBlock{
			Block:  block,
			BCoord: izyx,
		}
		op.Process(&pb)
	}
	if numEmpty < len(blockMeta.sortedBlocks) {
		if err := op.Finish(); err != nil {
			return err
		}
	}

	serialization := buf.Bytes()
	if len(serialization) == 0 {
		return nil
	}
	numRuns := uint32(len(serialization)-12) >> 4
	if numRuns == 0 {
		return nil
	}

	binary.LittleEndian.PutUint32(serialization[8:12], numRuns)
	dvid.Infof("label %d: sending %d blocks within bounds excluding %d empty blocks, %d runs, serialized %d bytes\n",
		blockMeta.label, len(blockMeta.sortedBlocks), numEmpty, numRuns, len(serialization))

	switch compression {
	case "":
		_, err = w.Write(serialization)
	case "lz4":
		compressed := make([]byte, lz4.CompressBound(serialization))
		var n, outSize int
		if outSize, err = lz4.Compress(serialization, compressed); err != nil {
			return err
		}
		compressed = compressed[:outSize]
		n, err = w.Write(compressed)
		if n != outSize {
			err = fmt.Errorf("only able to write %d of %d lz4 compressed bytes", n, outSize)
		}
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err = gw.Write(serialization); err != nil {
			return err
		}
		err = gw.Close()
	default:
		err = fmt.Errorf("unknown compression type %q", compression)
	}
	return err
}

// GetSparseCoarseVol returns an encoded sparse volume given a label.  This will return nil slice
// if the given label was not found.  The encoding has the following format where integers are
// little endian and blocks are returned in sorted ZYX order (small Z first):
//
//			byte     Set to 0
//			uint8    Number of dimensions
//			uint8    Dimension of run (typically 0 = X)
//			byte     Reserved (to be used later)
//			uint32    # Blocks [TODO.  0 for now]
//			uint32    # Spans
//			Repeating unit of:
//	    		int32   Block coordinate of run start (dimension 0)
//	    		int32   Block coordinate of run start (dimension 1)
//	    		int32   Block coordinate of run start (dimension 2)
//	    		int32   Length of run
func (d *Data) GetSparseCoarseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds, isSupervoxel bool) ([]byte, error) {
	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		return nil, err
	}
	if idx == nil || len(idx.Blocks) == 0 {
		return nil, nil
	}
	var supervoxel uint64
	if isSupervoxel {
		supervoxel = label
		idx, err = idx.LimitToSupervoxel(supervoxel)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			return nil, nil
		}
	}
	blocks, err := idx.GetProcessedBlockIndices(0, bounds, supervoxel)
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
//		uint64   label
//		<coarse sparse vol as given below>
//
//		uint64   label
//		<coarse sparse vol as given below>
//
//		...
//
//	The coarse sparse vol has the following format where integers are little endian and the order
//	of data is exactly as specified below:
//
//		int32    # Spans
//		Repeating unit of:
//			int32   Block coordinate of run start (dimension 0)
//			int32   Block coordinate of run start (dimension 1)
//			int32   Block coordinate of run start (dimension 2)
//			int32   Length of run
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
			if err := pb.Unmarshal(val, &idx); err != nil {
				return err
			}
			blocks, err := idx.GetProcessedBlockIndices(0, bounds, 0)
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
	if err := d.writeMappings(f, v, false); err != nil {
		dvid.Errorf("error writing mapping to file %q: %v\n", outPath, err)
		return
	}
	if err := f.Close(); err != nil {
		dvid.Errorf("problem closing file %q: %v\n", outPath, err)
	}
}

func (d *Data) writeMappings(w io.Writer, v dvid.VersionID, binaryFormat bool) error {
	timedLog := dvid.NewTimeLog()

	vc, err := getMapping(d, v)
	if err != nil {
		return fmt.Errorf("unable to retrieve mappings for data %q, version %d: %v", d.DataName(), v, err)
	}
	if !vc.mapUsed {
		dvid.Infof("no mappings found for data %q\n", d.DataName())
		return nil
	}
	numMappings, err := vc.writeMappings(w, v, binaryFormat)

	timedLog.Infof("Wrote %d mappings: err = %v", numMappings, err)
	return err
}

// Streams labels and optionally # voxels for the label to the http.ResponseWriter.
func (d *Data) listLabels(ctx *datastore.VersionedCtx, start, number uint64, countVoxels bool, w http.ResponseWriter) (numLabels uint64, err error) {
	if number == 0 {
		return
	}
	var store storage.OrderedKeyValueDB
	store, err = datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return 0, fmt.Errorf("problem getting store for data %q: %v", d.DataName(), err)
	}
	begTKey := NewLabelIndexTKey(start)
	endTKey := NewLabelIndexTKey(math.MaxUint64)
	haltFakeErr := fmt.Errorf("terminate range query")
	err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(c *storage.Chunk) error {
		if c == nil || c.V == nil || len(c.V) == 0 {
			return nil
		}
		numLabels++
		label, err := DecodeLabelIndexTKey(c.K)
		if err != nil {
			return err
		}
		if err = binary.Write(w, binary.LittleEndian, label); err != nil {
			return err
		}
		if countVoxels {
			var data []byte
			data, _, err = dvid.DeserializeData(c.V, true)
			if err != nil {
				return err
			}
			idx := new(labels.Index)
			if err := pb.Unmarshal(data, idx); err != nil {
				return err
			}
			var numVoxels uint64
			for _, svc := range idx.Blocks {
				if svc != nil && len(svc.Counts) != 0 {
					for _, count := range svc.Counts {
						numVoxels += uint64(count)
					}
				}
			}
			if err = binary.Write(w, binary.LittleEndian, numVoxels); err != nil {
				return err
			}
		}
		if numLabels == number {
			return haltFakeErr
		}
		return nil
	})
	if err == haltFakeErr {
		err = nil
	}
	return
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
		if err := pb.Unmarshal(data, idx); err != nil {
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

// scan all label indices for the presence of its key, writing existing key's body id
// into a stream and/or setting it in an internal map.
func (d *Data) writeExistingIndices(ctx *datastore.VersionedCtx, w http.ResponseWriter, bodymap map[uint64]struct{}) error {
	timedLog := dvid.NewTimeLog()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("problem getting store for data %q: %v", d.DataName(), err)
	}

	minTKey := NewLabelIndexTKey(0)
	maxTKey := NewLabelIndexTKey(math.MaxUint64)
	keyChan := make(storage.KeyChan, 1000)
	go func() {
		store.SendKeysInRange(ctx, minTKey, maxTKey, keyChan)
		close(keyChan)
	}()

	if w != nil {
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, "[")
	}

	var numExist uint64
	for key := range keyChan {
		if key != nil {
			tkey, err := storage.TKeyFromKey(key)
			if err != nil {
				dvid.Errorf("Unable to get TKey from key %v for data %q: %v\n", key, d.DataName(), err)
				continue
			}
			label, err := DecodeLabelIndexTKey(tkey)
			if err != nil {
				dvid.Errorf("Couldn't decode label index key %v for data %q\n", key, d.DataName())
				continue
			}
			if bodymap != nil {
				bodymap[label] = struct{}{}
			}
			if w != nil {
				if numExist > 0 {
					fmt.Fprintf(w, ",")
				}
				fmt.Fprintf(w, "%d", label)
			}
			numExist++
			if numExist%10000 == 0 {
				timedLog.Infof("Found %d existing labels, currently at %d", numExist, label)
			}
		}
	}
	if w != nil {
		fmt.Fprintf(w, "]")
	}

	timedLog.Infof("Finished writing %d existing label indices.", numExist)
	return nil
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
