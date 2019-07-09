package labelmap

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// GetMutationHistory writes JSON of the mutations that were done to the given label at toUUID version,
// where we delimit the time range of interest to [fromUUID, toUUID] versions.
func (d *Data) GetMutationHistory(w http.ResponseWriter, fromUUID, toUUID dvid.UUID, target uint64) error {
	fromV, err := datastore.VersionFromUUID(fromUUID)
	if err != nil {
		return err
	}
	toV, err := datastore.VersionFromUUID(toUUID)
	if err != nil {
		return err
	}
	ancestors, err := datastore.GetAncestry(toV)
	if err != nil {
		return err
	}
	supervoxelSet, err := d.GetSupervoxels(toV, target)
	if err != nil {
		return err
	}
	supervoxels := make([]uint64, len(supervoxelSet))
	i := 0
	for sv := range supervoxelSet {
		supervoxels[i] = sv
	}
	mapped, _, err := d.GetMappedLabels(fromV, supervoxels)
	origBodies := make(labels.Set, len(mapped))
	for _, label := range mapped {
		origBodies[label] = struct{}{}
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

	out := fmt.Sprintf(`echo "For label %d at version %s, there were %d supervoxels"`, target, toUUID, len(supervoxels))
	if _, err = w.Write([]byte(out)); err != nil {
		return err
	}
	out = fmt.Sprintf(`echo "The above maps to labels %v at version %s"`, origBodies, fromUUID)
	if _, err = w.Write([]byte(out)); err != nil {
		return err
	}

	for _, v := range versions {
		timedLog := dvid.NewTimeLog()
		ch := make(chan storage.LogMessage, 100)
		wg := new(sync.WaitGroup)
		go processMutationLogStream(w, v, ch, wg, origBodies, supervoxelSet)
		if err = labels.StreamLog(d, v, ch, wg); err != nil {
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
			if err := op.Unmarshal(msg.Data); err != nil {
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
			if err := op.Unmarshal(msg.Data); err != nil {
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
			if err := op.Unmarshal(msg.Data); err != nil {
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
			if err := op.Unmarshal(msg.Data); err != nil {
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
			_, err := w.Write([]byte(out))
			if err != nil {
				dvid.Errorf("Error writing mutation history for %s: %v\n", origBodies, err)
			}
		}
		wg.Done()
	}
}
