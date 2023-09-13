package labelmap

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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

// GetLabelMutationHistory writes JSON of the mutations that were done to the given label at toUUID version,
// where we delimit the time range of interest to [fromUUID, toUUID] versions.  The mutations are written
// backwards in time toUUID -> fromUUID.
func (d *Data) GetLabelMutationHistory(w http.ResponseWriter, fromUUID, toUUID dvid.UUID, target uint64) error {
	fromV, err := datastore.VersionFromUUID(fromUUID)
	if err != nil {
		return err
	}
	toV, err := datastore.VersionFromUUID(toUUID)
	if err != nil {
		return err
	}

	// Get latest supervoxels assigned to target id.
	supervoxelSet, err := d.GetSupervoxels(toV, target)
	if err != nil {
		return err
	}
	supervoxels := make([]uint64, len(supervoxelSet))
	i := 0
	for sv := range supervoxelSet {
		supervoxels[i] = sv
	}

	// Get the starting version labels mapped to the target's supervoxels -> origBodies set
	mapped, _, err := d.GetMappedLabels(fromV, supervoxels)
	if err != nil {
		return err
	}
	origBodies := make(labels.Set, len(mapped))
	for _, label := range mapped {
		origBodies[label] = struct{}{}
	}

	// Get all UUIDs in [fromUUID, toUUID] span -> versions
	ancestors, err := datastore.GetAncestry(toV)
	if err != nil {
		return err
	}
	var record bool
	var versions []dvid.VersionID
	for i := len(ancestors) - 1; i >= 0; i-- { // work from root to toUUID
		curV := ancestors[i]
		if !record && curV == fromV {
			record = true
		}
		if record {
			versions = append(versions, curV)
		}
		if curV == toV {
			break
		}
	}

	// Write the mutations for any fromUUID label touched by supervoxels in the toUUID target label.
	for _, v := range versions {
		timedLog := dvid.NewTimeLog()
		ch := make(chan storage.LogMessage, 100)
		wg := new(sync.WaitGroup)
		wg.Add(1)
		go processMutationLogStream(w, v, ch, wg, origBodies, supervoxelSet)
		if err = labels.StreamLog(d, v, ch); err != nil {
			return fmt.Errorf("problem loading mutation log: %v", err)
		}
		wg.Wait()
		timedLog.Infof("Wrote mutation history for data %q, from UUID %s to %s", d.DataName(), fromUUID, toUUID)
	}
	return nil
}

func processMutationLogStream(w http.ResponseWriter, v dvid.VersionID, ch chan storage.LogMessage, wg *sync.WaitGroup, origBodies, supervoxelSet labels.Set) {
	numMsgs := 0
	for msg := range ch { // expects channel to be closed on completion
		numMsgs++
		var err error
		var out []byte
		switch msg.EntryType {
		case proto.MergeOpType:
			var op proto.MergeOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal cleave message for version %d: %v\n", v, err)
				wg.Done()
				continue
			}
			if len(op.Merged) == 0 {
				wg.Done()
				continue
			}
			if _, found := origBodies[op.Target]; found {
				out, err = json.Marshal(struct {
					Action string
					Target uint64
					Labels []uint64
				}{
					Action: "merge",
					Target: op.Target,
					Labels: op.Merged,
				})
				if err != nil {
					dvid.Errorf("unable to write merge message: %v\n", err)
				}
			}

		case proto.CleaveOpType:
			var op proto.CleaveOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal cleave message for version %d: %v\n", v, err)
				wg.Done()
				continue
			}
			if len(op.Cleaved) == 0 {
				wg.Done()
				continue
			}
			if _, found := origBodies[op.Target]; found {
				out, err = json.Marshal(struct {
					Action             string
					OrigLabel          uint64
					CleavedLabel       uint64
					CleavedSupervoxels []uint64
				}{
					Action:             "cleave",
					OrigLabel:          op.Target,
					CleavedLabel:       op.Cleavedlabel,
					CleavedSupervoxels: op.Cleaved,
				})
				if err != nil {
					dvid.Errorf("unable to write cleave message: %v\n", err)
				}
			}

		case proto.SplitOpType:
			var op proto.SplitOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", v, err)
				wg.Done()
				continue
			}
			if _, found := origBodies[op.Target]; found {
				out, err = json.Marshal(struct {
					Action   string
					Target   uint64
					NewLabel uint64
					Coarse   bool
				}{
					Action:   "split",
					Target:   op.Target,
					NewLabel: op.Newlabel,
					Coarse:   op.Coarse,
				})
				if err != nil {
					dvid.Errorf("unable to write split message: %v\n", err)
				}
			}

		case proto.SupervoxelSplitType:
			var op proto.SupervoxelSplitOp
			if err := pb.Unmarshal(msg.Data, &op); err != nil {
				dvid.Errorf("unable to unmarshal split log message for version %d: %v\n", v, err)
				wg.Done()
				continue
			}
			if _, found := supervoxelSet[op.Supervoxel]; found {
				out, err = json.Marshal(struct {
					Action           string
					Supervoxel       uint64
					SplitSupervoxel  uint64
					RemainSupervoxel uint64
				}{
					Action:           "split-supervoxel",
					Supervoxel:       op.Supervoxel,
					SplitSupervoxel:  op.Splitlabel,
					RemainSupervoxel: op.Remainlabel,
				})
				if err != nil {
					dvid.Errorf("unable to write split-supervoxel message: %v\n", err)
				}
			}

		default:
		}
		if len(out) != 0 {
			_, err := w.Write(out)
			if err != nil {
				dvid.Errorf("Error writing mutation history for %s: %v\n", origBodies, err)
			}
		}
	}
	wg.Done()
}
