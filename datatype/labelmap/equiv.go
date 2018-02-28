// Equivalence maps for each version in DAG.

package labelmap

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

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

// requires write lock outside
func (svm *SVMap) getAncestry(v dvid.VersionID) ([]uint8, error) {
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

// GetAncestry returns a slice of short version ids that actually have mappings,
// from current version to root along ancestry.  Since all ancestors are immutable,
// we can cache the ancestor slice and check if we should add current short version id.
// This possible mutation requires a Lock.
func (svm *SVMap) GetAncestry(v dvid.VersionID) (ancestry []uint8, err error) {
	svm.Lock()
	ancestry, err = svm.getAncestry(v)
	svm.Unlock()
	return
}

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

func (svm *SVMap) initToVersion(d dvid.Data, v dvid.VersionID) error {
	svm.Lock()
	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestors {
		mergeOps, err := labels.ReadMergeLog(d, ancestor)
		if err != nil {
			return err
		}
		if len(mergeOps) == 0 {
			continue
		}
		vid, err := svm.createShortVersion(v)
		if err != nil {
			return err
		}
		for _, mergeOp := range mergeOps {
			for supervoxel := range mergeOp.Merged {
				vm := svm.fm[supervoxel]
				newvm, changed := vm.modify(vid, mergeOp.Target)
				if changed {
					svm.fm[supervoxel] = newvm
				}
			}
		}
	}

	// TODO: Read in affinities
	svm.Unlock()
	return nil
}

// MergeIndex adjusts the mapping for all the constituent supervoxels in the given index
// to point the specified label.
func (svm *SVMap) MergeIndex(v dvid.VersionID, idx *labels.Index, toLabel uint64) error {
	supervoxels := idx.GetSupervoxels()
	if len(supervoxels) == 0 {
		return nil
	}
	svm.Lock()
	vid, err := svm.createShortVersion(v)
	if err != nil {
		return err
	}
	for supervoxel := range supervoxels {
		vm := svm.fm[supervoxel]
		newvm, changed := vm.modify(vid, toLabel)
		if changed {
			svm.fm[supervoxel] = newvm
		}
	}
	svm.Unlock()
	return nil
}

// Exists returns true if the given version is likely to have some mappings.
func (svm *SVMap) Exists(v dvid.VersionID) bool {
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

	ancestry, err := svm.GetAncestry(v)
	if err != nil {
		dvid.Criticalf("unable to get ancestry for version %d: %v\n", v, err)
		return label, false
	}
	return vm.value(ancestry)
}

type instanceMaps struct {
	maps map[dvid.UUID]*SVMap
	sync.RWMutex
}

var (
	iMap instanceMaps
)

func getMap(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	iMap.Lock()
	defer iMap.Unlock()
	m, found := iMap.maps[d.DataUUID()]
	if !found {
		m := new(SVMap)
		m.fm = make(map[uint64]vmap)
		m.versions = make(map[dvid.VersionID]uint8)
		m.versionsRev = make(map[uint8]dvid.VersionID)
		iMap.maps[d.DataUUID()] = m
	}
	if err := m.initToVersion(d, v); err != nil {
		return nil, err
	}
	return m, nil
}

func GetMapping(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	return getMap(d, v)
}

// AddMergeToMapping adds a merge into the equivalence map for a given instance version.
func AddMergeToMapping(d dvid.Data, v dvid.VersionID, mutID, toLabel uint64, mergeIdx *labels.Index) error {
	m, err := getMap(d, v)
	if err != nil {
		return err
	}
	if err := m.MergeIndex(v, mergeIdx, toLabel); err != nil {
		return err
	}
	op := labels.MergeOp{
		Target: toLabel,
		Merged: mergeIdx.GetSupervoxels(),
	}
	if err := labels.LogMerge(d, v, mutID, op); err != nil {
		return err
	}
	return nil
}
