// Equivalence maps for each version in DAG.

package labelmap

import (
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

type SVMap struct {
	fm map[uint64]uint64 // forward map
	sync.RWMutex
}

// MappedLabel returns the final mapped label and a boolean: true if
// a mapping was found and false if none was found.
func (emap *SVMap) MappedLabel(label uint64) (uint64, bool) {
	if emap == nil {
		return label, false
	}
	emap.RLock()
	if len(emap.fm) == 0 {
		return label, false
	}
	cur := label
	found := false
	for {
		l, ok := emap.fm[cur]
		if !ok {
			break
		}
		cur = l
		found = true
	}
	emap.RUnlock()
	return cur, found
}

type ivMaps struct {
	maps map[dvid.InstanceVersion]*SVMap
	sync.RWMutex
}

var (
	ivMap ivMaps
)

func initSVMap(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	ancestors, err := datastore.GetAncestorVersions(v)
	if err != nil {
		return nil, err
	}
	var emap SVMap
	emap.fm = make(map[uint64]uint64)
	for _, ancestor := range ancestors {
		mergeOps, err := labels.ReadMergeLog(d, ancestor)
		if err != nil {
			return nil, err
		}
		for _, mergeOp := range mergeOps {
			for label := range mergeOp.Merged {
				emap.fm[label] = mergeOp.Target
			}
		}
	}
	// TODO: Read in affinities
	return &emap, nil
}

func getMap(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	iv := dvid.InstanceVersion{d.DataUUID(), v}
	ivMap.Lock()
	m, found := ivMap.maps[iv]
	if !found {
		var err error
		if m, err = initSVMap(d, v); err != nil {
			return &SVMap{fm: make(map[uint64]uint64)}, err
		}
		ivMap.maps[iv] = m
	}
	ivMap.Unlock()
	return m, nil
}

func GetMapping(d dvid.Data, v dvid.VersionID) (*SVMap, error) {
	m, err := getMap(d, v)
	if err != nil {
		return &SVMap{fm: make(map[uint64]uint64)}, err
	}
	return m, err
}

// AddMerge adds a merge into the equivalence map for a given instance version.
func AddMerge(d dvid.Data, v dvid.VersionID, mutID uint64, op labels.MergeOp) error {
	m, err := getMap(d, v)
	if err != nil {
		return err
	}
	m.Lock()

	// add the merge
	for label := range op.Merged {
		m.fm[label] = op.Target
	}

	m.Unlock()

	// persist the merge into the log
	return labels.LogMerge(d, v, mutID, op)
}
