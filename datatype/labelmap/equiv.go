// Equivalence maps for each version in DAG.

package labelmap

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func (d *Data) ingestMappings(ctx *datastore.VersionedCtx, mappings proto.MappingOps) error {
	m, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return err
	}
	m.Lock()
	vid, err := m.createShortVersion(ctx.VersionID())
	if err != nil {
		m.Unlock()
		return err
	}
	for _, mapOp := range mappings.Mappings {
		for _, supervoxel := range mapOp.Original {
			vm := m.fm[supervoxel]
			newvm, changed := vm.modify(vid, mapOp.Mapped)
			if changed {
				m.fm[supervoxel] = newvm
			}
		}
	}
	m.Unlock()
	return labels.LogMappings(d, ctx.VersionID(), mappings)
}

// versioned map entry for a given supervoxel.
// All versions are contained where each entry is an 8-bit version id
// followed by the uint64 mapping.  So length must be N * 9.
type vmap []byte

// returns the mapping for a given version given its ancestry
func (vm vmap) value(ancestry []uint8) (label uint64, present bool) {
	sz := len(vm)
	if sz == 0 {
		return 0, false
	}
	for _, vid := range ancestry {
		for pos := 0; pos < sz; pos += 9 {
			entryvid := uint8(vm[pos])
			if entryvid == vid {
				return binary.LittleEndian.Uint64(vm[pos+1 : pos+9]), true
			}
		}
	}
	return 0, false
}

// modify or append a new mapping given a unique version id and mapped label
func (vm vmap) modify(vid uint8, toLabel uint64) (out vmap, changed bool) {
	if len(vm) == 0 {
		out = make([]byte, 9)
		out[0] = vid
		binary.LittleEndian.PutUint64(out[1:], toLabel)
		return out, true
	}
	for pos := 0; pos < len(vm); pos += 9 {
		entryvid := uint8(vm[pos])
		if entryvid == vid {
			curLabel := binary.LittleEndian.Uint64(vm[pos+1 : pos+9])
			if curLabel == toLabel {
				return vm, false
			}
			out := make([]byte, len(vm))
			copy(out, vm)
			binary.LittleEndian.PutUint64(out[pos+1:pos+9], toLabel)
			return out, true
		}
	}
	pos := len(vm)
	out = make([]byte, pos+9)
	copy(out, vm)
	out[pos] = vid
	binary.LittleEndian.PutUint64(out[pos+1:], toLabel)
	return out, true
}

// SVMap is a version-aware supervoxel map that tries to be memory efficient and
// allows up to 256 versions per SVMap instance.
type SVMap struct {
	fm          map[uint64]vmap
	versions    map[dvid.VersionID]uint8   // versions that have been initialized
	versionsRev map[uint8]dvid.VersionID   // reverse map for byte -> version
	ancestry    map[dvid.VersionID][]uint8 // cache of ancestry other than current version
	numVersions uint8
	sync.RWMutex
}

// makes sure that current map has been initialized with all forward mappings up to
// given version.
func (svm *SVMap) initToVersion(d dvid.Data, v dvid.VersionID) error {
	svm.Lock()
	defer svm.Unlock()

	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestors {
		vid, found := svm.versions[ancestor]
		if found {
			return nil // we have already loaded this version and its ancestors
		}
		vid, err = svm.createShortVersion(ancestor)
		if err != nil {
			return fmt.Errorf("problem creating mapping version for id %d: %v\n", ancestor, err)
		}
		timedLog := dvid.NewTimeLog()
		ch := make(chan storage.LogMessage, 100)
		wg := new(sync.WaitGroup)
		go func(vid uint8, ch chan storage.LogMessage, wg *sync.WaitGroup) {
			numMsgs := 0
			for msg := range ch { // expects channel to be closed on completion
				numMsgs++
				if msg.EntryType != proto.MappingOpType {
					dvid.Errorf("received odd log message not of type Mapping for version %d\n", ancestor)
					wg.Done()
					continue
				}
				var op proto.MappingOp
				if err := op.Unmarshal(msg.Data); err != nil {
					dvid.Errorf("unable to unmarshal mapping log message for version %d: %v\n", ancestor, err)
					wg.Done()
					continue
				}
				mapped := op.GetMapped()
				for _, supervoxel := range op.GetOriginal() {
					vm := svm.fm[supervoxel]
					newvm, changed := vm.modify(vid, mapped)
					if changed {
						svm.fm[supervoxel] = newvm
					}
				}
				wg.Done()
			}
		}(vid, ch, wg)
		if err = labels.StreamMappingLog(d, ancestor, ch, wg); err != nil {
			return fmt.Errorf("problem loading mapping logs: %v", err)
		}
		wg.Wait()
		timedLog.Infof("Loaded mappings for data %q, version ID %d", d.DataName(), ancestor)
	}
	return nil
}

// getAncestry returns a slice of short version ids that actually have mappings,
// from current version to root along ancestry.  Since all ancestors are immutable,
// we can cache the ancestor slice and check if we should add current short version id.
// This possible mutation requires a Lock on the receiver from outside or use getLockedAncestry().
func (svm *SVMap) getAncestry(v dvid.VersionID) ([]uint8, error) {
	if svm.ancestry == nil {
		svm.ancestry = make(map[dvid.VersionID][]uint8)
	}
	ancestry, found := svm.ancestry[v]
	if !found {
		ancestors, err := datastore.GetAncestry(v)
		if err != nil {
			return nil, err
		}
		for _, ancestor := range ancestors[1:] {
			vid, found := svm.versions[ancestor]
			if found {
				ancestry = append(ancestry, vid)
			}
		}
		svm.ancestry[v] = ancestry
	}
	vid, found := svm.versions[v]
	if found {
		return append([]uint8{vid}, ancestry...), nil
	}
	return ancestry, nil
}

// getAncestry with a receiver lock built-in.
func (svm *SVMap) getLockedAncestry(v dvid.VersionID) (ancestry []uint8, err error) {
	svm.Lock()
	ancestry, err = svm.getAncestry(v)
	svm.Unlock()
	return
}

// returns a short version or creates one if it didn't exist before.
func (svm *SVMap) createShortVersion(v dvid.VersionID) (uint8, error) {
	vid, found := svm.versions[v]
	if !found {
		if svm.numVersions == 255 {
			return 0, fmt.Errorf("can only have 256 active versions of data instance mapping")
		}
		vid = svm.numVersions
		svm.versions[v] = vid
		svm.versionsRev[vid] = v
		svm.numVersions++
	}
	return vid, nil
}

// returns true if the given version is likely to have some mappings.
// provides receiver locking within.
func (svm *SVMap) exists(v dvid.VersionID) bool {
	svm.Lock() // need write lock due to possible caching in getAncestry()
	defer svm.Unlock()
	if len(svm.fm) == 0 {
		return false
	}
	ancestry, err := svm.getAncestry(v)
	if err != nil {
		dvid.Criticalf("unable to get ancestry for version %d: %v\n", v, err)
		return false
	}
	if len(ancestry) == 0 {
		return false
	}
	return true
}

// faster inner-loop version of mapping where ancestry should already be provided.
// receiver RLock should be provided outside.
func (svm *SVMap) mapLabel(label uint64, ancestry []uint8) (uint64, bool) {
	vm, found := svm.fm[label]
	if !found {
		return label, false
	}
	return vm.value(ancestry)
}

// MappedLabel returns the mapped label and a boolean: true if
// a mapping was found and false if none was found.  For faster mapping,
// large scale transformations, e.g. block-level output, should not use this
// routine but work directly with mapLabel() doing locking and ancestry lookup
// outside loops.
func (svm *SVMap) MappedLabel(v dvid.VersionID, label uint64) (uint64, bool) {
	if svm == nil {
		return label, false
	}
	svm.RLock()
	if len(svm.fm) == 0 {
		svm.RUnlock()
		return label, false
	}
	vm, found := svm.fm[label]
	if !found {
		svm.RUnlock()
		return label, false
	}
	svm.RUnlock()

	ancestry, err := svm.getLockedAncestry(v)
	if err != nil {
		dvid.Criticalf("unable to get ancestry for version %d: %v\n", v, err)
		return label, false
	}
	return vm.value(ancestry)
}

// MappedLabels returns an array of mapped labels, which could be the same as the passed slice.
func (svm *SVMap) MappedLabels(v dvid.VersionID, supervoxels []uint64) ([]uint64, error) {
	if svm == nil {
		return supervoxels, nil
	}
	ancestry, err := svm.getLockedAncestry(v)
	if err != nil {
		return nil, fmt.Errorf("unable to get ancestry for version %d: %v", v, err)
	}
	svm.RLock()
	if len(svm.fm) == 0 {
		svm.RUnlock()
		return supervoxels, nil
	}
	mapped := make([]uint64, len(supervoxels))
	for i, supervoxel := range supervoxels {
		label, found := svm.mapLabel(supervoxel, ancestry)
		if found {
			mapped[i] = label
		} else {
			mapped[i] = supervoxel
		}
	}
	svm.RUnlock()
	return mapped, nil
}

// GetMappedLabels returns an array of mapped labels, which could be the same as the passed slice,
// for the given version of the data instance.
func (d *Data) GetMappedLabels(v dvid.VersionID, supervoxels []uint64) ([]uint64, error) {
	svmap, err := getMapping(d, v)
	if err != nil {
		return nil, fmt.Errorf("couldn't get mapping for data %q, version %d: %v", d.DataName(), v, err)
	}
	return svmap.MappedLabels(v, supervoxels)
}

type instanceMaps struct {
	maps map[dvid.UUID]*SVMap
	sync.RWMutex
}

var (
	iMap instanceMaps
)

func init() {
	iMap.maps = make(map[dvid.UUID]*SVMap)
}

// returns or creates an SVMap so nil is never returned unless there's an error
func getMapping(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	iMap.Lock()
	m, found := iMap.maps[d.DataUUID()]
	if !found {
		m = new(SVMap)
		m.fm = make(map[uint64]vmap)
		m.versions = make(map[dvid.VersionID]uint8)
		m.versionsRev = make(map[uint8]dvid.VersionID)
		iMap.maps[d.DataUUID()] = m
	}
	iMap.Unlock()
	if err := m.initToVersion(d, v); err != nil {
		return nil, err
	}
	return m, nil
}

// adds a merge into the equivalence map for a given instance version and also
// records the mappings into the log.
func addMergeToMapping(d dvid.Data, v dvid.VersionID, mutID, toLabel uint64, mergeIdx *labels.Index) error {
	m, err := getMapping(d, v)
	if err != nil {
		return err
	}
	supervoxels := mergeIdx.GetSupervoxels()
	if len(supervoxels) == 0 {
		return nil
	}
	m.Lock()
	vid, err := m.createShortVersion(v)
	if err != nil {
		m.Unlock()
		return err
	}
	for supervoxel := range supervoxels {
		vm := m.fm[supervoxel]
		newvm, changed := vm.modify(vid, toLabel)
		if changed {
			m.fm[supervoxel] = newvm
		}
	}
	m.Unlock()
	op := labels.MappingOp{
		MutID:    mutID,
		Mapped:   toLabel,
		Original: supervoxels,
	}
	return labels.LogMapping(d, v, op)
}

// adds new cleave into the equivalence map for a given instance version and also
// records the mappings into the log.
func addCleaveToMapping(d dvid.Data, v dvid.VersionID, op labels.CleaveOp) error {
	m, err := getMapping(d, v)
	if err != nil {
		return err
	}
	if len(op.CleavedSupervoxels) == 0 {
		return nil
	}
	m.Lock()
	vid, err := m.createShortVersion(v)
	if err != nil {
		return err
	}
	supervoxelSet := make(labels.Set, len(op.CleavedSupervoxels))
	for _, supervoxel := range op.CleavedSupervoxels {
		supervoxelSet[supervoxel] = struct{}{}
		vm := m.fm[supervoxel]
		newvm, changed := vm.modify(vid, op.CleavedLabel)
		if changed {
			m.fm[supervoxel] = newvm
		}
	}
	m.Unlock()
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
	m, err := getMapping(d, v)
	if err != nil {
		return err
	}
	label := op.Supervoxel
	mapped, found := m.MappedLabel(v, op.Supervoxel)
	if found {
		label = mapped
	}

	m.Lock()
	vid, err := m.createShortVersion(v)
	if err != nil {
		return err
	}
	vm := m.fm[op.SplitSupervoxel]
	newvm, changed := vm.modify(vid, label)
	if changed {
		m.fm[op.SplitSupervoxel] = newvm
	}
	vm = m.fm[op.RemainSupervoxel]
	newvm, changed = vm.modify(vid, label)
	if changed {
		m.fm[op.RemainSupervoxel] = newvm
	}
	m.Unlock()
	original := labels.Set{
		op.SplitSupervoxel:  struct{}{},
		op.RemainSupervoxel: struct{}{},
	}
	mapOp := labels.MappingOp{
		MutID:    op.MutID,
		Mapped:   label,
		Original: original,
	}
	return labels.LogMapping(d, v, mapOp)
}
