// Equivalence maps for each version in DAG.

package labelmap

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
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
func initMapping(d dvid.Data, v dvid.VersionID) *SVMap {
	iMap.Lock()
	m, found := iMap.maps[d.DataUUID()]
	if !found {
		m = newSVMap()
		iMap.maps[d.DataUUID()] = m
	}
	iMap.Unlock()
	return m
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
	m.fmMu.Lock()
	for supervoxel := range supervoxels {
		m.setMapping(v, supervoxel, toLabel)
	}
	m.fmMu.Unlock()
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
	m.fmMu.Lock()
	for supervoxel := range supervoxels {
		m.setMapping(v, supervoxel, newLabel)
	}
	m.setMapping(v, newLabel, 0)
	m.fmMu.Unlock()
	return labels.LogRenumber(d, v, mutID, origLabel, newLabel)
}

// adds new arbitrary split into the equivalence map for a given instance version.
func addSplitToMapping(d dvid.Data, v dvid.VersionID, op labels.SplitOp) error {
	m, err := getMapping(d, v)
	if err != nil {
		return err
	}

	deleteSupervoxels := make(labels.Set)
	splitSupervoxels := make(labels.Set)
	remainSupervoxels := make(labels.Set)

	splits := m.splits[v]
	m.fmMu.Lock()
	for supervoxel, svsplit := range op.SplitMap {
		deleteSupervoxels[supervoxel] = struct{}{}
		splitSupervoxels[svsplit.Split] = struct{}{}
		remainSupervoxels[svsplit.Remain] = struct{}{}

		m.setMapping(v, svsplit.Split, op.NewLabel)
		m.setMapping(v, svsplit.Remain, op.Target)
		m.setMapping(v, supervoxel, 0)

		rec := proto.SupervoxelSplitOp{
			Mutid:       op.MutID,
			Supervoxel:  supervoxel,
			Remainlabel: svsplit.Remain,
			Splitlabel:  svsplit.Split,
		}
		splits = append(splits, rec)

		// TODO -- for each split, we log each supervoxel split.
	}
	m.fmMu.Unlock()
	m.splitsMu.Lock()
	m.splits[v] = splits
	m.splitsMu.Unlock()

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
	supervoxelSet := make(labels.Set, len(op.CleavedSupervoxels))
	m.fmMu.Lock()
	for _, supervoxel := range op.CleavedSupervoxels {
		supervoxelSet[supervoxel] = struct{}{}
		m.setMapping(v, supervoxel, op.CleavedLabel)
	}
	m.setMapping(v, op.CleavedLabel, 0)
	m.fmMu.Unlock()
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

	m.fmMu.Lock()
	m.setMapping(v, op.SplitSupervoxel, label)
	m.setMapping(v, op.RemainSupervoxel, label)
	m.setMapping(v, op.Supervoxel, 0)
	m.fmMu.Unlock()
	rec := proto.SupervoxelSplitOp{
		Mutid:       op.MutID,
		Supervoxel:  op.Supervoxel,
		Remainlabel: op.RemainSupervoxel,
		Splitlabel:  op.SplitSupervoxel,
	}
	m.splitsMu.Lock()
	m.splits[v] = append(m.splits[v], rec)
	m.splitsMu.Unlock()

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
//   full set of ids.
func (d *Data) verifyIsNewLabel(v dvid.VersionID, newLabel uint64) (bool, error) {
	m, err := getMapping(d, v)
	if err != nil {
		return false, err
	}
	m.fmMu.RLock()
	_, found := m.fm[newLabel]
	m.fmMu.RUnlock()
	return !found, nil
}

func (d *Data) ingestMappings(ctx *datastore.VersionedCtx, mappings *proto.MappingOps) error {
	m, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return err
	}
	m.fmMu.Lock()
	vid := ctx.VersionID()
	for _, mapOp := range mappings.Mappings {
		for _, label := range mapOp.Original {
			vm := m.fm[label]
			m.fm[label] = vm.modify(vid, mapOp.Mapped, true)
		}
	}
	m.fmMu.Unlock()
	return labels.LogMappings(d, ctx.VersionID(), mappings)
}

// a cache of the index of each version in an ancestry
// where the root is 1 and leaf is len(ancestry).
type distFromRoot map[dvid.VersionID]uint32

func getDistFromRoot(ancestry []dvid.VersionID) distFromRoot {
	distMap := make(distFromRoot, len(ancestry))
	for i, v := range ancestry {
		distMap[v] = uint32(len(ancestry) - i)
	}
	return distMap
}

// Versioned map entries for a given supervoxel, corresponding to
// varint encodings of (VersionID, uint64) pairs.
type vmap []byte

func createEncodedMapping(v dvid.VersionID, label uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	n += binary.PutUvarint(buf[n:], label)
	return buf[:n]
}

func readEncodedMapping(buf []byte) (v dvid.VersionID, label uint64, nbytes int) {
	vid, vlen := binary.Uvarint(buf)
	v = dvid.VersionID(vid)
	label, llen := binary.Uvarint(buf[vlen:])
	nbytes = vlen + llen
	return
}

func (vm vmap) decodeMappings() map[dvid.VersionID]uint64 {
	fm := map[dvid.VersionID]uint64{}
	n := 0
	for n < len(vm) {
		v, label, nbytes := readEncodedMapping(vm[n:])
		fm[v] = label
		n += nbytes
	}
	return fm
}

func (vm vmap) String() string {
	fm := vm.decodeMappings()
	var out string
	for v, label := range fm {
		out += fmt.Sprintf("%d:%d ", v, label)
	}
	return out
}

// Returns the mapping for a given version given its mappedVersions.
// Typically there will be fewer mappings for a given label than the
// number of versions in the mappedVersions, so we cache the priority of
// versions in the mappedVersions and iterate over the decoded mappings.
// While this is O(N), N = modified mappings and should be small.
func (vm vmap) value(mappedVersions distFromRoot) (label uint64, present bool) {
	sz := len(vm)
	if sz == 0 || len(mappedVersions) == 0 {
		return 0, false
	}
	mapping := vm.decodeMappings()
	var farthest uint32
	for v, curLabel := range mapping {
		rootDist, found := mappedVersions[v]
		if found && rootDist > farthest {
			farthest = rootDist
			label = curLabel
			present = true
		}
	}
	return
}

// Returns the vmap encoding with the given version excised.
func (vm vmap) excludeVersion(v dvid.VersionID) (out vmap) {
	var exciseStart, exciseStop int
	n := 0
	for n < len(vm) {
		vid, _, nbytes := readEncodedMapping(vm[n:])
		if v == vid {
			exciseStart = n
			exciseStop = n + nbytes
			break
		}
		n += nbytes
	}
	if exciseStop == 0 {
		return vm
	}
	if exciseStart == 0 {
		return vm[exciseStop:]
	}
	if exciseStop == len(vm) {
		return vm[0:exciseStart]
	}
	return append(vm[0:exciseStart], vm[exciseStop:]...)
}

// Adds a unique version id and mapped label. If replace is true,
// the mappings are checked for the given version and updated.
// If it is known that the mappings don't include a version, like
// when ingesting during initialization, then replace should be set
// to false.
func (vm vmap) modify(v dvid.VersionID, label uint64, replace bool) (out vmap) {
	if len(vm) == 0 {
		out = createEncodedMapping(v, label)
		return
	}
	if replace {
		out = vm.excludeVersion(v)
	} else {
		out = vm
	}
	return append(out, createEncodedMapping(v, label)...)
}

// SVMap is a version-aware supervoxel map that tries to be memory efficient.
// Splits are also cached by version.
type SVMap struct {
	// Mapping from a label to a vmap, which holds (version, label) tuples.
	// Read very frequently, written heavily on server startup and occasionally afterwards.
	fm   map[uint64]vmap // forward map from orig label to agglomerated (body) id
	fmMu sync.RWMutex

	// Cache of versions with mappings and their distance from root (necessary for quick versioned value retrieval)
	// Read/used very frequently (same as fm above), written heavily on server startup
	// but extremely limited writes (only once per new version) afterwards.
	mappedVersions   map[dvid.VersionID]distFromRoot // mapped versions' distance from root
	mappedVersionsMu sync.RWMutex

	// Cache of split ops done across versions. Read and write infrequently.
	splits   map[dvid.VersionID][]proto.SupervoxelSplitOp
	splitsMu sync.RWMutex
}

func newSVMap() (svm *SVMap) {
	svm = new(SVMap)
	svm.fm = make(map[uint64]vmap)
	svm.mappedVersions = make(map[dvid.VersionID]distFromRoot)
	svm.splits = make(map[dvid.VersionID][]proto.SupervoxelSplitOp)
	return
}

// faster inner-loop versions where mutex should be held outside call to allow
// concurrent access.

// low-level label mapping that should pass in mappedVersions from a getMappedVersionsDist().
func (svm *SVMap) mapLabel(label uint64, mappedVersions distFromRoot) (uint64, bool) {
	if len(mappedVersions) == 0 {
		return label, false
	}
	vm, found := svm.fm[label]
	if !found {
		return label, false
	}
	return vm.value(mappedVersions)
}

// set mapping with expectation that SVMap has been locked for write
func (svm *SVMap) setMapping(v dvid.VersionID, from, to uint64) {
	vm := svm.fm[from]
	svm.fm[from] = vm.modify(v, to, true)
}

// --------------------------------------------------------

func (svm *SVMap) getMappedVersionsDist(v dvid.VersionID) distFromRoot {
	svm.mappedVersionsMu.RLock()
	dist, found := svm.mappedVersions[v]
	svm.mappedVersionsMu.RUnlock()

	if !found { // We have an uncached version so cache the distFromRoot
		ancestry, err := datastore.GetAncestry(v)
		if err != nil {
			dvid.Errorf("Error getting ancestry for version %d: %v\n", v, err)
			return nil
		}
		svm.mappedVersionsMu.Lock()
		dist = getDistFromRoot(ancestry)
		svm.mappedVersions[v] = dist
		svm.mappedVersionsMu.Unlock()
	}
	return dist
}

func (svm *SVMap) exists() bool {
	svm.fmMu.RLock()
	defer svm.fmMu.RUnlock()

	return len(svm.fm) != 0
}

// goroutine-safe function for intializing the in-memory mapping with a version's mutations log
// and caching the mapped versions with the distance from the root.
func (svm *SVMap) loadVersionMapping(ancestors []dvid.VersionID, dataname dvid.InstanceName, ch chan storage.LogMessage, wg *sync.WaitGroup) {
	if len(ancestors) == 0 {
		return
	}
	timedLog := dvid.NewTimeLog()

	v := ancestors[0]
	var splits []proto.SupervoxelSplitOp
	numMsgs := map[string]uint64{
		"Mapping":         0,
		"Split":           0,
		"SupervoxelSplit": 0,
		"Cleave":          0,
		"Renumber":        0,
	}
	// svm.fmMu.Lock()
	// defer svm.fmMu.Unlock()
	for msg := range ch { // expects channel to be closed on completion
		switch msg.EntryType {
		case proto.MappingOpType:
			numMsgs["Mapping"]++
			var op proto.MappingOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal mapping log message for version %d: %v\n", v, err)
				continue
			}
			mapped := op.GetMapped()
			for _, supervoxel := range op.GetOriginal() {
				svm.setMapping(v, supervoxel, mapped)
			}

		case proto.SplitOpType:
			numMsgs["Split"]++
			var op proto.SplitOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", v, err)
				continue
			}
			for supervoxel, svsplit := range op.GetSvsplits() {
				rec := proto.SupervoxelSplitOp{
					Mutid:       op.Mutid,
					Supervoxel:  supervoxel,
					Remainlabel: svsplit.Remainlabel,
					Splitlabel:  svsplit.Splitlabel,
				}
				splits = append(splits, rec)
				svm.setMapping(v, supervoxel, 0)
			}

		case proto.SupervoxelSplitType:
			numMsgs["SupervoxelSplit"]++
			var op proto.SupervoxelSplitOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", v, err)
				continue
			}
			rec := proto.SupervoxelSplitOp{
				Mutid:       op.Mutid,
				Supervoxel:  op.Supervoxel,
				Remainlabel: op.Remainlabel,
				Splitlabel:  op.Splitlabel,
			}
			splits = append(splits, rec)
			svm.setMapping(v, op.Supervoxel, 0)

		case proto.CleaveOpType:
			numMsgs["Cleave"]++
			var op proto.CleaveOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal cleave log message for version %d: %v\n", v, err)
				continue
			}
			svm.setMapping(v, op.Cleavedlabel, 0)

		case proto.RenumberOpType:
			numMsgs["Renumber"]++
			var op proto.RenumberOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal renumber log message for version %d: %v\n", v, err)
				continue
			}
			// We don't set op.Target to 0 because it could be the ID of a supervoxel.
			svm.setMapping(v, op.Newlabel, 0)

		default:
		}
	}

	svm.splitsMu.Lock()
	svm.splits[v] = splits
	svm.splitsMu.Unlock()
	timedLog.Infof("Loaded mappings for data %q, version %d", dataname, v)
	dvid.Infof("Mutations for version %d: %v\n", v, numMsgs)
	wg.Done()
}

// makes sure that current map has been initialized with all forward mappings up to
// given version as well as split index.  Note that this function can be called
// multiple times and it won't reload formerly visited ancestors because it initializes
// the mapping from current version -> root.
func (svm *SVMap) initToVersion(d dvid.Data, v dvid.VersionID, loadMutations bool) error {
	svm.fmMu.Lock()
	defer svm.fmMu.Unlock()
	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return err
	}
	for pos, ancestor := range ancestors {
		svm.mappedVersionsMu.Lock()
		if _, found := svm.mappedVersions[ancestor]; found {
			svm.mappedVersionsMu.Unlock()
			return nil // we have already loaded this version and its ancestors
		}
		svm.mappedVersions[v] = getDistFromRoot(ancestors[pos:])
		svm.mappedVersionsMu.Unlock()

		if loadMutations {
			ch := make(chan storage.LogMessage, 1000)
			wg := new(sync.WaitGroup)
			go svm.loadVersionMapping(ancestors[pos:], d.DataName(), ch, wg)

			if err = labels.StreamLog(d, ancestor, ch, nil); err != nil {
				return fmt.Errorf("problem loading mapping logs for data %q, version %d: %v", d.DataName(), ancestor, err)
			}
			wg.Wait()
		}
	}
	return nil
}

// SupervoxelSplitsJSON returns a JSON string giving all the supervoxel splits from
// this version to the root.
func (svm *SVMap) SupervoxelSplitsJSON(v dvid.VersionID) (string, error) {
	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return "", err
	}
	var items []string
	for _, ancestor := range ancestors {
		splitops, found := svm.splits[ancestor]
		if !found || len(splitops) == 0 {
			continue
		}
		uuid, err := datastore.UUIDFromVersion(ancestor)
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

// MappedLabel returns the mapped label and a boolean: true if
// a mapping was found and false if none was found.  For faster mapping,
// large scale transformations, e.g. block-level output, should not use this
// routine but work directly with mapLabel() doing locking and ancestry lookup
// outside loops.
func (svm *SVMap) MappedLabel(v dvid.VersionID, label uint64) (uint64, bool) {
	if svm == nil {
		return label, false
	}
	svm.mappedVersionsMu.RLock()
	mappedVersions := svm.mappedVersions[v]
	svm.mappedVersionsMu.RUnlock()

	svm.fmMu.RLock()
	defer svm.fmMu.RUnlock()
	return svm.mapLabel(label, mappedVersions)
}

// MappedLabels returns an array of mapped labels, which could be the same as the passed slice.
func (svm *SVMap) MappedLabels(v dvid.VersionID, supervoxels []uint64) (mapped []uint64, found []bool, err error) {
	found = make([]bool, len(supervoxels))
	mapped = make([]uint64, len(supervoxels))
	copy(mapped, supervoxels)

	if svm == nil {
		return
	}
	dist := svm.getMappedVersionsDist(v)
	svm.fmMu.RLock()
	if len(svm.fm) == 0 {
		svm.fmMu.RUnlock()
		return
	}
	for i, supervoxel := range supervoxels {
		label, wasMapped := svm.mapLabel(supervoxel, dist)
		if wasMapped {
			mapped[i] = label
			found[i] = wasMapped
		}
	}
	svm.fmMu.RUnlock()
	return
}

// ApplyMappingToBlock applies label mapping (given an ancestry path) to the passed labels.Block.
func (svm *SVMap) ApplyMappingToBlock(mappedVersions distFromRoot, block *labels.Block) {
	svm.fmMu.RLock()
	for i, label := range block.Labels {
		mapped, found := svm.mapLabel(label, mappedVersions)
		if found {
			block.Labels[i] = mapped
		}
	}
	svm.fmMu.RUnlock()
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
		svm.mappedVersionsMu.RLock()
		maxVersion := 0
		for v, _ := range svm.mappedVersions {
			if int(v) > maxVersion {
				maxVersion = int(v)
			}
		}
		name := string(ds.DataName())
		stats[name] = mapStats{
			MapEntries:  len(svm.fm),
			MapSize:     humanize.Bytes(uint64(size.Of(svm))),
			NumVersions: len(svm.mappedVersions),
			MaxVersion:  maxVersion,
		}
		svm.mappedVersionsMu.RUnlock()
	}
	return json.Marshal(stats)
}
