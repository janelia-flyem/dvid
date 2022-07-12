// Equivalence maps for each version in DAG.

package labelmap

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/DmitriyVTitov/size"
	"github.com/dustin/go-humanize"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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
		m.splits = make(map[uint8][]proto.SupervoxelSplitOp)
		m.versions = make(map[dvid.VersionID]uint8)
		m.versionsRev = make(map[uint8]dvid.VersionID)
		iMap.maps[d.DataUUID()] = m
	}
	iMap.Unlock()

	m.RLock()
	_, found = m.versions[v]
	m.RUnlock()
	if found {
		return m, nil // we have already loaded this version and its ancestors
	}
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
		m.setMapping(vid, supervoxel, toLabel)
	}
	m.Unlock()
	op := labels.MappingOp{
		MutID:    mutID,
		Mapped:   toLabel,
		Original: supervoxels,
	}
	return labels.LogMapping(d, v, op)
}

// // adds a renumber into the equivalence map for a given instance version and also
// // records the mappings into the log.
func addRenumberToMapping(d dvid.Data, v dvid.VersionID, mutID, origLabel, newLabel uint64, mergeIdx *labels.Index) error {
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
		m.setMapping(vid, supervoxel, newLabel)
	}
	m.setMapping(vid, newLabel, 0)
	m.Unlock()
	return labels.LogRenumber(d, v, mutID, origLabel, newLabel)
}

// adds new arbitrary split into the equivalence map for a given instance version.
func addSplitToMapping(d dvid.Data, v dvid.VersionID, op labels.SplitOp) error {
	m, err := getMapping(d, v)
	if err != nil {
		return err
	}
	m.Lock()
	vid, err := m.createShortVersion(v)
	if err != nil {
		m.Unlock()
		return err
	}
	deleteSupervoxels := make(labels.Set)
	splitSupervoxels := make(labels.Set)
	remainSupervoxels := make(labels.Set)

	splits := m.splits[vid]
	for supervoxel, svsplit := range op.SplitMap {
		deleteSupervoxels[supervoxel] = struct{}{}
		splitSupervoxels[svsplit.Split] = struct{}{}
		remainSupervoxels[svsplit.Remain] = struct{}{}

		m.setMapping(vid, svsplit.Split, op.NewLabel)
		m.setMapping(vid, svsplit.Remain, op.Target)
		m.setMapping(vid, supervoxel, 0)

		rec := proto.SupervoxelSplitOp{
			Mutid:       op.MutID,
			Supervoxel:  supervoxel,
			Remainlabel: svsplit.Remain,
			Splitlabel:  svsplit.Split,
		}
		splits = append(splits, rec)

		// TODO -- for each split, we log each supervoxel split.
	}
	m.splits[vid] = splits
	m.Unlock()

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
		m.Unlock()
		return err
	}
	supervoxelSet := make(labels.Set, len(op.CleavedSupervoxels))
	for _, supervoxel := range op.CleavedSupervoxels {
		supervoxelSet[supervoxel] = struct{}{}
		m.setMapping(vid, supervoxel, op.CleavedLabel)
	}
	m.setMapping(vid, op.CleavedLabel, 0)
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
		m.Unlock()
		return err
	}
	m.setMapping(vid, op.SplitSupervoxel, label)
	m.setMapping(vid, op.RemainSupervoxel, label)
	m.setMapping(vid, op.Supervoxel, 0)
	rec := proto.SupervoxelSplitOp{
		Mutid:       op.MutID,
		Supervoxel:  op.Supervoxel,
		Remainlabel: op.RemainSupervoxel,
		Splitlabel:  op.SplitSupervoxel,
	}
	m.splits[vid] = append(m.splits[vid], rec)
	m.Unlock()

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

// DumpMutations makes a log of all mutations from the start UUID to the end UUID.
func (d *Data) DumpMutations(startUUID, endUUID dvid.UUID, filename string) (comment string, err error) {
	rl := d.GetReadLog()
	if rl == nil {
		err = fmt.Errorf("no mutation log was available for data %q", d.DataName())
		return
	}
	var startV, endV dvid.VersionID
	startV, err = datastore.VersionFromUUID(startUUID)
	if err != nil {
		return
	}
	endV, err = datastore.VersionFromUUID(endUUID)
	if err != nil {
		return
	}
	var rootToLeaf, leafToRoot []dvid.VersionID
	leafToRoot, err = datastore.GetAncestry(endV)
	if err != nil {
		return
	}
	rootToLeaf = make([]dvid.VersionID, len(leafToRoot))

	// reverse it and screen on UUID start/stop
	startPos := len(leafToRoot) - 1
	for _, v := range leafToRoot {
		rootToLeaf[startPos] = v
		if v == startV {
			break
		}
		startPos--
	}

	// open up target log
	var f *os.File
	f, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return
	}

	// go through the ancestors from root to leaf, appending data to target log
	numLogs := 0
	var uuid dvid.UUID
	for i, ancestor := range rootToLeaf[startPos:] {
		if uuid, err = datastore.UUIDFromVersion(ancestor); err != nil {
			return
		}
		timedLog := dvid.NewTimeLog()
		var data []byte
		if data, err = rl.ReadBinary(d.DataUUID(), uuid); err != nil {
			return
		}
		if len(data) == 0 {
			timedLog.Infof("No mappings found for data %q, version %s", d.DataName(), uuid)
			continue
		}
		if _, err = f.Write(data); err != nil {
			return
		}
		timedLog.Infof("Loaded mappings #%d for data %q, version ID %s", i+1, d.DataName(), uuid)
		numLogs++
	}
	err = f.Close()
	comment = fmt.Sprintf("Completed flattening of %d mutation logs to %s\n", numLogs, filename)
	return
}

// returns true if the given newLabel does not exist as forward mapping key
// TODO? Also check if it exists anywhere in mapping, which would probably
//   full set of ids.
func (d *Data) verifyIsNewLabel(v dvid.VersionID, newLabel uint64) (bool, error) {
	m, err := getMapping(d, v)
	if err != nil {
		return false, err
	}
	m.RLock()
	_, found := m.fm[newLabel]
	m.RUnlock()
	return !found, nil
}

func (d *Data) ingestMappings(ctx *datastore.VersionedCtx, mappings *proto.MappingOps) error {
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
// allows up to 256 versions per SVMap instance.  Splits are also cached by version.
type SVMap struct {
	fm          map[uint64]vmap          // forward map from supervoxel to agglomerated (body) id
	versions    map[dvid.VersionID]uint8 // versions that have been initialized
	versionsRev map[uint8]dvid.VersionID // reverse map for byte -> version
	numVersions uint8

	ancestry   map[dvid.VersionID][]uint8 // cache of ancestry other than current version
	ancestryMu sync.RWMutex

	splits map[uint8][]proto.SupervoxelSplitOp

	sync.RWMutex
}

// makes sure that current map has been initialized with all forward mappings up to
// given version as well as split index.
func (svm *SVMap) initToVersion(d dvid.Data, v dvid.VersionID) error {
	svm.Lock()
	defer svm.Unlock()

	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestors {
		if _, found := svm.versions[ancestor]; found {
			return nil // we have already loaded this version and its ancestors
		}
		vid, err := svm.createShortVersion(ancestor)
		if err != nil {
			return fmt.Errorf("problem creating mapping version for id %d: %v", ancestor, err)
		}
		timedLog := dvid.NewTimeLog()
		ch := make(chan storage.LogMessage, 100)
		wg := new(sync.WaitGroup)
		go func(vid uint8, ch chan storage.LogMessage, wg *sync.WaitGroup) {
			numMsgs := 0
			for msg := range ch { // expects channel to be closed on completion
				numMsgs++
				switch msg.EntryType {
				case proto.MappingOpType:
					var op proto.MappingOp
					if err := pb.Unmarshal(msg.Data, &op); err != nil {
						dvid.Errorf("unable to unmarshal mapping log message for version %d: %v\n", ancestor, err)
						wg.Done()
						continue
					}
					mapped := op.GetMapped()
					for _, supervoxel := range op.GetOriginal() {
						svm.setMapping(vid, supervoxel, mapped)
					}

				case proto.SplitOpType:
					var op proto.SplitOp
					if err := pb.Unmarshal(msg.Data, &op); err != nil {
						dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", ancestor, err)
						wg.Done()
						continue
					}
					splits := svm.splits[vid]
					for supervoxel, svsplit := range op.GetSvsplits() {
						rec := proto.SupervoxelSplitOp{
							Mutid:       op.Mutid,
							Supervoxel:  supervoxel,
							Remainlabel: svsplit.Remainlabel,
							Splitlabel:  svsplit.Splitlabel,
						}
						splits = append(splits, rec)
						svm.setMapping(vid, supervoxel, 0)
					}
					svm.splits[vid] = splits

				case proto.SupervoxelSplitType:
					var op proto.SupervoxelSplitOp
					if err := pb.Unmarshal(msg.Data, &op); err != nil {
						dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", ancestor, err)
						wg.Done()
						continue
					}
					rec := proto.SupervoxelSplitOp{
						Mutid:       op.Mutid,
						Supervoxel:  op.Supervoxel,
						Remainlabel: op.Remainlabel,
						Splitlabel:  op.Splitlabel,
					}
					svm.splits[vid] = append(svm.splits[vid], rec)
					svm.setMapping(vid, op.Supervoxel, 0)

				case proto.CleaveOpType:
					var op proto.CleaveOp
					if err := pb.Unmarshal(msg.Data, &op); err != nil {
						dvid.Errorf("unable to unmarshal cleave log message for version %d: %v\n", ancestor, err)
						wg.Done()
						continue
					}
					svm.setMapping(vid, op.Cleavedlabel, 0)

				case proto.RenumberOpType:
					var op proto.RenumberOp
					if err := pb.Unmarshal(msg.Data, &op); err != nil {
						dvid.Errorf("unable to unmarshal renumber log message for version %d: %v\n", ancestor, err)
						wg.Done()
						continue
					}
					// We don't set op.Target to 0 because it could be the ID of a supervoxel.
					svm.setMapping(vid, op.Newlabel, 0)

				default:
				}
				wg.Done()
			}
		}(vid, ch, wg)
		if err = labels.StreamLog(d, ancestor, ch, wg); err != nil {
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
func (svm *SVMap) getAncestry(v dvid.VersionID) ([]uint8, error) {
	svm.ancestryMu.Lock()
	defer svm.ancestryMu.Unlock()
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

// SupervoxelSplitsJSON returns a JSON string giving all the supervoxel splits from
// this version to the root.
func (svm *SVMap) SupervoxelSplitsJSON(v dvid.VersionID) (string, error) {
	svm.RLock()
	defer svm.RUnlock()
	ancestry, err := svm.getAncestry(v)
	if err != nil {
		return "", err
	}
	var items []string
	for _, ancestor := range ancestry {
		splitops, found := svm.splits[ancestor]
		if !found || len(splitops) == 0 {
			continue
		}
		ancestorVersion, found := svm.versionsRev[ancestor]
		if !found {
			return "", fmt.Errorf("ancestor id %d has no version id", ancestor)
		}
		uuid, err := datastore.UUIDFromVersion(ancestorVersion)
		if err != nil {
			return "", err
		}
		str := fmt.Sprintf(`"%s",`, uuid)
		splitstrs := make([]string, len(splitops))
		for i, splitop := range splitops {
			splitstrs[i] = fmt.Sprintf("[%d,%d,%d,%d]", splitop.Mutid, splitop.Supervoxel, splitop.Remainlabel, splitop.Splitlabel)
		}
		str += "[" + strings.Join(splitstrs, ",") + "]"
		items = append(items, str)
	}
	return "[" + strings.Join(items, ",") + "]", nil
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
	svm.RLock()
	fmSize := len(svm.fm)
	svm.RUnlock()
	if fmSize == 0 {
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

func (svm *SVMap) setMapping(vid uint8, from, to uint64) {
	vm := svm.fm[from]
	newvm, changed := vm.modify(vid, to)
	if changed {
		svm.fm[from] = newvm
	}
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
	svm.RUnlock()
	if !found {
		return label, false
	}

	ancestry, err := svm.getAncestry(v)
	if err != nil {
		dvid.Criticalf("unable to get ancestry for version %d: %v\n", v, err)
		return label, false
	}
	return vm.value(ancestry)
}

// MappedLabels returns an array of mapped labels, which could be the same as the passed slice.
func (svm *SVMap) MappedLabels(v dvid.VersionID, supervoxels []uint64) (mapped []uint64, found []bool, err error) {
	found = make([]bool, len(supervoxels))
	mapped = make([]uint64, len(supervoxels))
	copy(mapped, supervoxels)

	if svm == nil {
		return
	}
	ancestry, err := svm.getAncestry(v)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get ancestry for version %d: %v", v, err)
	}
	svm.RLock()
	if len(svm.fm) == 0 {
		svm.RUnlock()
		return
	}
	for i, supervoxel := range supervoxels {
		label, wasMapped := svm.mapLabel(supervoxel, ancestry)
		if wasMapped {
			mapped[i] = label
			found[i] = wasMapped
		}
	}
	svm.RUnlock()
	return
}

// ApplyMappingToBlock applies label mapping (given an ancestry path) to the passed labels.Block.
func (svm *SVMap) ApplyMappingToBlock(ancestry []uint8, block *labels.Block) {
	svm.RLock()
	for i, label := range block.Labels {
		mapped, found := svm.mapLabel(label, ancestry)
		if found {
			block.Labels[i] = mapped
		}
	}
	svm.RUnlock()
}

// GetMappedLabels returns an array of mapped labels, which could be the same as the passed slice,
// for the given version of the data instance.
func (d *Data) GetMappedLabels(v dvid.VersionID, supervoxels []uint64) (mapped []uint64, found []bool, err error) {
	var svmap *SVMap
	if svmap, err = getMapping(d, v); err != nil {
		return
	}
	return svmap.MappedLabels(v, supervoxels)
}

type mapStats struct {
	MapEntries  int
	MapSize     string
	NumVersions int
	MaxVersion  int
}

// GetMapStats returns JSON describing in-memory mapping stats.
func (d *Data) GetMapStats(ctx *datastore.VersionedCtx) (jsonBytes []byte, err error) {
	stats := make(map[string]mapStats)
	for dataUUID, svm := range iMap.maps {
		var ds datastore.DataService
		if ds, err = datastore.GetDataByDataUUID(dataUUID); err != nil {
			return
		}
		svm.RLock()
		maxVersion := 0
		for _, v := range svm.versions {
			if int(v) > maxVersion {
				maxVersion = int(v)
			}
		}
		name := string(ds.DataName())
		stats[name] = mapStats{
			MapEntries:  len(svm.fm),
			MapSize:     humanize.Bytes(uint64(size.Of(svm))),
			NumVersions: len(svm.versions),
			MaxVersion:  maxVersion,
		}
		svm.RUnlock()
	}
	return json.Marshal(stats)
}
