package labelmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/DmitriyVTitov/size"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	pb "google.golang.org/protobuf/proto"
)

// VCache is holds in-memory versioned data for (1) version-aware, sharded label map,
// (2) distance of a version from DAG root, and (3) split ops.
// The label map tries to be memory efficient and also improves concurrency by
// using sharded maps hashed by label modulo.
type VCache struct {
	mu sync.RWMutex // mutex for all fields

	// Sharded maps of label mappings.
	numShards uint64
	mapShards []*mapShard
	mapUsed   bool // true if there's at least one mapping

	// Cache of versions with mappings and their distance from root (necessary for quick versioned value retrieval)
	// Read/used very frequently (same as fm above), written heavily on server startup
	// but extremely limited writes (only once per new version) afterwards.
	mappedVersions   map[dvid.VersionID]distFromRoot // mapped versions' distance from root
	mappedVersionsMu sync.RWMutex

	// Cache of split ops done across versions. Read and write infrequently.
	splits   map[dvid.VersionID][]proto.SupervoxelSplitOp
	splitsMu sync.RWMutex
}

// Mapping from a label to a vmap, which holds (version, label) tuples.
// Read very frequently, written heavily on server startup and occasionally afterwards.
type mapShard struct {
	fm   map[uint64]vmap // forward map from orig label to agglomerated (body) id
	fmMu sync.RWMutex
}

func newVCache(numMaps int) (vc *VCache) {
	vc = new(VCache)
	vc.numShards = uint64(numMaps)
	vc.mapShards = make([]*mapShard, numMaps)
	for i := 0; i < numMaps; i++ {
		vc.mapShards[i] = &mapShard{
			fm: make(map[uint64]vmap),
		}
	}
	vc.mappedVersions = make(map[dvid.VersionID]distFromRoot)
	vc.splits = make(map[dvid.VersionID][]proto.SupervoxelSplitOp)
	return
}

// --- Low-level mapping functions that handle locking ----

// check if label has been mapped
func (vc *VCache) hasMapping(label uint64) bool {
	shard := label % vc.numShards
	lmap := vc.mapShards[shard]

	lmap.fmMu.RLock()
	_, found := lmap.fm[label]
	lmap.fmMu.RUnlock()
	return found
}

// get mapping that should pass in mappedVersions from a getMappedVersionsDist().
func (vc *VCache) mapLabel(label uint64, mappedVersions distFromRoot) (uint64, bool) {
	if len(mappedVersions) == 0 {
		return label, false
	}
	shard := label % vc.numShards
	lmap := vc.mapShards[shard]

	lmap.fmMu.RLock()
	defer lmap.fmMu.RUnlock()

	vm, found := lmap.fm[label]
	if !found {
		return label, false
	}
	return vm.value(mappedVersions)
}

// set mapping with expectation that SVMap has been locked for write
func (vc *VCache) setMapping(v dvid.VersionID, from, to uint64) {
	vc.mapUsed = true
	shard := from % vc.numShards
	lmap := vc.mapShards[shard]

	lmap.fmMu.Lock()
	vm := lmap.fm[from]
	lmap.fm[from] = vm.modify(v, to, true)
	lmap.fmMu.Unlock()
}

// write all mappings by iterating through the shards, holding the read lock only
// for in-memory operation and writing data outside the lock.
func (vc *VCache) writeMappings(w io.Writer, v dvid.VersionID, binaryFormat bool) (numMappings uint64, err error) {
	mappedVersions := vc.getMappedVersionsDist(v)

	var outBuf bytes.Buffer
	for _, lmap := range vc.mapShards {
		lmap.fmMu.RLock()
		for fromLabel, vm := range lmap.fm {
			toLabel, present := vm.value(mappedVersions)
			if present {
				numMappings++
				if fromLabel != toLabel {
					if binaryFormat {
						err = binary.Write(&outBuf, binary.LittleEndian, fromLabel)
						if err == nil {
							err = binary.Write(&outBuf, binary.LittleEndian, toLabel)
						}
					} else {
						line := fmt.Sprintf("%d %d\n", fromLabel, toLabel)
						_, err = outBuf.WriteString(line)
					}

					if err != nil {
						lmap.fmMu.RUnlock()
						return
					}
				}
			}
		}
		lmap.fmMu.RUnlock()
		if _, err = w.Write(outBuf.Bytes()); err != nil {
			return
		}
		outBuf.Reset()
	}
	return
}

func (vc *VCache) mapStats() (entries, numBytes uint64) {
	for _, lmap := range vc.mapShards {
		lmap.fmMu.RLock()
		numBytes += uint64(size.Of(lmap))
		entries += uint64(len(lmap.fm))
		lmap.fmMu.RUnlock()
	}
	return
}

// --------------------------------------------------------

func (vc *VCache) getMappedVersionsDist(v dvid.VersionID) distFromRoot {
	vc.mappedVersionsMu.RLock()
	dist, found := vc.mappedVersions[v]
	vc.mappedVersionsMu.RUnlock()

	if !found { // We have an uncached version so cache the distFromRoot
		ancestry, err := datastore.GetAncestry(v)
		if err != nil {
			dvid.Errorf("Error getting ancestry for version %d: %v\n", v, err)
			return nil
		}
		vc.mappedVersionsMu.Lock()
		dist = getDistFromRoot(ancestry)
		vc.mappedVersions[v] = dist
		vc.mappedVersionsMu.Unlock()
	}
	return dist
}

// goroutine-safe function for intializing the in-memory mapping with a version's mutations log
// and caching the mapped versions with the distance from the root.
func (vc *VCache) loadVersionMapping(ancestors []dvid.VersionID, dataname dvid.InstanceName, ch chan storage.LogMessage, wg *sync.WaitGroup) {
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
				vc.setMapping(v, supervoxel, mapped)
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
				vc.setMapping(v, supervoxel, 0)
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
			vc.setMapping(v, op.Supervoxel, 0)

		case proto.CleaveOpType:
			numMsgs["Cleave"]++
			var op proto.CleaveOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal cleave log message for version %d: %v\n", v, err)
				continue
			}
			vc.setMapping(v, op.Cleavedlabel, 0)

		case proto.RenumberOpType:
			numMsgs["Renumber"]++
			var op proto.RenumberOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal renumber log message for version %d: %v\n", v, err)
				continue
			}
			// We don't set op.Target to 0 because it could be the ID of a supervoxel.
			vc.setMapping(v, op.Newlabel, 0)

		default:
		}
	}

	vc.splitsMu.Lock()
	vc.splits[v] = splits
	vc.splitsMu.Unlock()
	timedLog.Infof("Loaded mappings for data %q, version %d", dataname, v)
	dvid.Infof("Mutations for version %d: %v\n", v, numMsgs)
	wg.Done()
}

// makes sure that current map has been initialized with all forward mappings up to
// given version as well as split index.  Note that this function can be called
// multiple times and it won't reload formerly visited ancestors because it initializes
// the mapping from current version -> root.
func (vc *VCache) initToVersion(d dvid.Data, v dvid.VersionID, loadMutations bool) error {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return err
	}
	for pos, ancestor := range ancestors {
		vc.mappedVersionsMu.Lock()
		if _, found := vc.mappedVersions[ancestor]; found {
			vc.mappedVersionsMu.Unlock()
			return nil // we have already loaded this version and its ancestors
		}
		vc.mappedVersions[ancestor] = getDistFromRoot(ancestors[pos:])
		vc.mappedVersionsMu.Unlock()

		if loadMutations {
			ch := make(chan storage.LogMessage, 1000)
			wg := new(sync.WaitGroup)
			wg.Add(1)
			go vc.loadVersionMapping(ancestors[pos:], d.DataName(), ch, wg)

			if err = labels.StreamLog(d, ancestor, ch); err != nil {
				return fmt.Errorf("problem loading mapping logs for data %q, version %d: %v", d.DataName(), ancestor, err)
			}
			wg.Wait()
		}
	}
	return nil
}

// SupervoxelSplitsJSON returns a JSON string giving all the supervoxel splits from
// this version to the root.
func (vc *VCache) SupervoxelSplitsJSON(v dvid.VersionID) (string, error) {
	ancestors, err := datastore.GetAncestry(v)
	if err != nil {
		return "", err
	}
	var items []string
	for _, ancestor := range ancestors {
		splitops, found := vc.splits[ancestor]
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
func (vc *VCache) MappedLabel(v dvid.VersionID, label uint64) (uint64, bool) {
	if vc == nil || !vc.hasMapping(label) {
		return label, false
	}
	vc.mappedVersionsMu.RLock()
	mappedVersions := vc.mappedVersions[v]
	vc.mappedVersionsMu.RUnlock()

	return vc.mapLabel(label, mappedVersions)
}

// MappedLabels returns an array of mapped labels, which could be the same as the passed slice.
func (vc *VCache) MappedLabels(v dvid.VersionID, supervoxels []uint64) (mapped []uint64, found []bool, err error) {
	found = make([]bool, len(supervoxels))
	mapped = make([]uint64, len(supervoxels))
	copy(mapped, supervoxels)

	if vc == nil || !vc.mapUsed {
		return
	}
	dist := vc.getMappedVersionsDist(v)
	for i, supervoxel := range supervoxels {
		label, wasMapped := vc.mapLabel(supervoxel, dist)
		if wasMapped {
			mapped[i] = label
			found[i] = wasMapped
		}
	}
	return
}

// ApplyMappingToBlock applies label mapping (given an ancestry path) to the passed labels.Block.
func (vc *VCache) ApplyMappingToBlock(mappedVersions distFromRoot, block *labels.Block) {
	for i, label := range block.Labels {
		mapped, found := vc.mapLabel(label, mappedVersions)
		if found {
			block.Labels[i] = mapped
		}
	}
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
