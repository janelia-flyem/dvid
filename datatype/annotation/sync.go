package annotation

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/datatype/labelarray"
	"github.com/janelia-flyem/dvid/datatype/labelmap"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type ElementPos struct {
	Label uint64
	Kind  ElementType
	Pos   dvid.Point3d
}

// DeltaModifyElements is a change in the elements assigned to a label.
// Need positions of elements because subscribers may have ROI filtering.
type DeltaModifyElements struct {
	Add []ElementPos
	Del []ElementPos
}

// DeltaSetElements is a replacement of elements assigned to a label.
type DeltaSetElements struct {
	Set []ElementPos
}

// Annotation number change event identifiers.
const (
	ModifyElementsEvent = "ANNOTATION_MOD_ELEMENTS"
	SetElementsEvent    = "ANNOTATION_SET_ELEMENTS"
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBufferSize = 100

type LabelElements map[uint64]ElementsNR

func (le LabelElements) add(label uint64, elem ElementNR) {
	if label == 0 {
		return
	}
	elems, found := le[label]
	if found {
		elems = append(elems, elem)
		le[label] = elems
	} else {
		le[label] = ElementsNR{elem}
	}
}

func (le LabelElements) applyMapping(d mappedLabelType, v dvid.VersionID, setSupervoxels bool) (LabelElements, error) {
	supervoxels := make([]uint64, len(le))
	i := 0
	for supervoxel := range le {
		supervoxels[i] = supervoxel
		i++
	}
	mapped, err := d.GetMappedLabels(v, supervoxels)
	if err != nil {
		return nil, err
	}
	mapping := make(map[uint64]uint64, len(mapped))
	for i, supervoxel := range supervoxels {
		mapping[supervoxel] = mapped[i]
	}
	mappedAdd := make(LabelElements, len(le))
	for supervoxel, elems := range le {
		elemsCopy := make(ElementsNR, len(elems))
		copy(elemsCopy, elems)
		if setSupervoxels {
			for _, elem := range elemsCopy {
				elem.Supervoxel = supervoxel
			}
		}
		mappedAdd[supervoxel] = elemsCopy
	}
	for supervoxel, elems := range le {
		body := mapping[supervoxel]
		if body != supervoxel {
			mappedElems, found := mappedAdd[body]
			if found {
				mappedAdd[body] = append(mappedElems, elems...)
			} else {
				mappedElems = elems
			}
			mappedAdd[body] = mappedElems
			delete(mappedAdd, supervoxel)
		}
	}
	return mappedAdd, nil
}

type LabelPoints map[uint64][]dvid.Point3d

func (lp LabelPoints) add(label uint64, pt dvid.Point3d) {
	if label == 0 {
		return
	}
	pts, found := lp[label]
	if found {
		pts = append(pts, pt)
		lp[label] = pts
	} else {
		lp[label] = []dvid.Point3d{pt}
	}
}

// InitDataHandlers launches goroutines to handle each labelblk instance's syncs.
func (d *Data) InitDataHandlers() error {
	if d.syncCh != nil || d.syncDone != nil {
		return nil
	}
	d.syncCh = make(chan datastore.SyncMessage, syncBufferSize)
	d.syncDone = make(chan *sync.WaitGroup)

	// Launch handlers of sync events.
	dvid.Infof("Launching sync event handler for data %q...\n", d.DataName())
	go d.processEvents()
	return nil
}

// Shutdown terminates blocks until syncs are done then terminates background goroutines processing data.
func (d *Data) Shutdown(wg *sync.WaitGroup) {
	if d.syncDone != nil {
		dwg := new(sync.WaitGroup)
		dwg.Add(1)
		d.syncDone <- dwg
		dwg.Wait() // Block until we are done.
	}
	wg.Done()
}

// GetSyncSubs implements the datastore.Syncer interface.  Returns a list of subscriptions
// to the sync data instance that will notify the receiver.
func (d *Data) GetSyncSubs(synced dvid.Data) (subs datastore.SyncSubs, err error) {
	if d.syncCh == nil {
		if err = d.InitDataHandlers(); err != nil {
			err = fmt.Errorf("unable to initialize handlers for data %q: %v", d.DataName(), err)
			return
		}
	}

	// Our syncing depends on the datatype we are syncing.
	switch synced.TypeName() {
	case "labelblk":
		subs = datastore.SyncSubs{
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.IngestBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MutateBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.DeleteBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
		}
	case "labelvol":
		subs = datastore.SyncSubs{
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitLabelEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
		}
	case "labelarray":
		subs = datastore.SyncSubs{
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.IngestBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MutateBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.DeleteBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitLabelEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
		}
	case "labelmap":
		subs = datastore.SyncSubs{
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.IngestBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MutateBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.DeleteBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.MergeBlockEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.SplitLabelEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
			datastore.SyncSub{
				Event:  datastore.SyncEvent{synced.DataUUID(), labels.CleaveLabelEvent},
				Notify: d.DataUUID(),
				Ch:     d.syncCh,
			},
		}
	default:
		err = fmt.Errorf("unable to sync %s with %s since datatype %q is not supported", d.DataName(), synced.DataName(), synced.TypeName())
	}
	return
}

// Processes each labelblk change as we get it.
func (d *Data) processEvents() {
	batcher, err := datastore.GetKeyValueBatcher(d)
	if err != nil {
		dvid.Errorf("handleBlockEvent %v\n", err)
		return
	}
	var stop bool
	var wg *sync.WaitGroup
	for {
		select {
		case wg = <-d.syncDone:
			queued := len(d.syncCh)
			if queued > 0 {
				dvid.Infof("Received shutdown signal for %q sync events (%d in queue)\n", d.DataName(), queued)
				stop = true
			} else {
				dvid.Infof("Shutting down sync event handler for instance %q...\n", d.DataName())
				wg.Done()
				return
			}
		case msg := <-d.syncCh:
			ctx := datastore.NewVersionedCtx(d, msg.Version)
			d.handleSyncMessage(ctx, msg, batcher)

			if stop && len(d.syncCh) == 0 {
				dvid.Infof("Shutting down sync even handler for instance %q after draining sync events.\n", d.DataName())
				wg.Done()
				return
			}
		}
	}
}

func (d *Data) handleSyncMessage(ctx *datastore.VersionedCtx, msg datastore.SyncMessage, batcher storage.KeyValueBatcher) {
	d.StartUpdate()
	defer d.StopUpdate()

	switch delta := msg.Delta.(type) {

	case imageblk.Block:
		chunkPt := dvid.ChunkPoint3d(*delta.Index)
		d.ingestBlock(ctx, chunkPt, delta.Data, batcher)

	case imageblk.MutatedBlock:
		chunkPt := dvid.ChunkPoint3d(*delta.Index)
		d.mutateBlock(ctx, chunkPt, delta.Prev, delta.Data, batcher)

	case labelarray.IngestedBlock:
		chunkPt, _ := delta.BCoord.ToChunkPoint3d()
		data, _ := delta.Data.MakeLabelVolume()
		d.ingestBlock(ctx, chunkPt, data, batcher)

	case labelmap.IngestedBlock:
		chunkPt, _ := delta.BCoord.ToChunkPoint3d()
		data, _ := delta.Data.MakeLabelVolume()
		d.ingestBlock(ctx, chunkPt, data, batcher)

	case labelarray.MutatedBlock:
		chunkPt, _ := delta.BCoord.ToChunkPoint3d()
		prev, _ := delta.Prev.MakeLabelVolume()
		data, _ := delta.Data.MakeLabelVolume()
		d.mutateBlock(ctx, chunkPt, prev, data, batcher)

	case labelmap.MutatedBlock:
		chunkPt, _ := delta.BCoord.ToChunkPoint3d()
		prev, _ := delta.Prev.MakeLabelVolume()
		data, _ := delta.Data.MakeLabelVolume()
		d.mutateBlock(ctx, chunkPt, prev, data, batcher)

	case labels.DeltaMergeStart:
		// ignore
	case labels.DeltaMerge:
		// process annotation type
		if err := d.mergeLabels(batcher, msg.Version, delta.MergeOp); err != nil {
			dvid.Errorf("error on merging labels for data %q: %v\n", d.DataName(), err)
			return
		}

	case labels.DeltaSplitStart:
		// ignore for now
	case labels.DeltaSplit:
		if delta.Split == nil {
			// This is a coarse split so can't be mapped data.
			if err := d.splitLabelsCoarse(batcher, msg.Version, delta); err != nil {
				dvid.Errorf("error on splitting labels for data %q: %v\n", d.DataName(), err)
			}
			return
		}
		lmapData := d.getMappedLabelData()
		if lmapData == nil {
			if err := d.splitLabels(batcher, msg.Version, delta); err != nil {
				dvid.Errorf("error on splitting labels for data %q: %v\n", d.DataName(), err)
			}
			return
		}
		if err := d.splitMappedLabels(batcher, lmapData, msg.Version, delta); err != nil {
			dvid.Errorf("error on splitting mapped labels for data %q: %v\n", d.DataName(), err)
		}

	case labels.CleaveOp:
		if err := d.cleaveLabels(batcher, msg.Version, delta); err != nil {
			dvid.Errorf("error on cleaving label for data %q: %v\n", d.DataName(), err)
			return
		}

	default:
		dvid.Criticalf("Got unexpected delta: %v\n", msg)
	}
}

func (d *Data) getMappedLabelData() mappedLabelType {
	var lmapData mappedLabelType
	labelData := d.getSyncedLabels()
	if labelData != nil {
		var ok bool
		lmapData, ok = labelData.(mappedLabelType)
		if !ok {
			lmapData = nil
		}
	}
	return lmapData
}

// If a block of labels is ingested, adjust each label's synaptic element list.
func (d *Data) ingestBlock(ctx *datastore.VersionedCtx, chunkPt dvid.ChunkPoint3d, data []byte, batcher storage.KeyValueBatcher) {
	// Get the synaptic elements for this block
	tk := NewBlockTKey(chunkPt)
	elems, err := getElements(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting elements for block %s: %v\n", chunkPt, err)
		return
	}
	if len(elems) == 0 {
		return
	}
	blockSize := d.blockSize()
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all element positions, finding corresponding label and storing elements.
	lmapData := d.getMappedLabelData()
	added := 0
	toAdd := LabelElements{}
	for n := range elems {
		pt := elems[n].Pos.Point3dInChunk(blockSize)
		i := (pt[2]*bY+pt[1])*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(data[i : i+8])
		if label != 0 {
			if lmapData != nil && elems[n].Supervoxel != label {
				elems[n].Supervoxel = label
			}
			toAdd.add(label, elems[n].ElementNR)
			added++
		}
	}
	if lmapData != nil {
		toAdd, err = toAdd.applyMapping(lmapData, ctx.VersionID(), true)
		if err != nil {
			dvid.Errorf("unable to applying label mapping from synced labelmap %q: %v\n", lmapData.DataName(), err)
		}
	}

	// Add any non-zero label elements to their respective label k/v.
	var delta DeltaModifyElements
	delta.Add = make([]ElementPos, added)
	i := 0
	for label, addElems := range toAdd {
		tk := NewLabelTKey(label)
		labelElems, err := getElementsNR(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting elements for label %d: %v\n", label, err)
			return
		}
		labelElems.add(addElems)
		val, err := json.Marshal(labelElems)
		if err != nil {
			dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)

		for _, addElem := range addElems {
			delta.Add[i] = ElementPos{Label: label, Kind: addElem.Kind, Pos: addElem.Pos}
			i++
		}
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in annotations %q after delete block: %v\n", d.DataName(), err)
		return
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

// If a block of labels is mutated, adjust any label that was either removed or added.
func (d *Data) mutateBlock(ctx *datastore.VersionedCtx, chunkPt dvid.ChunkPoint3d, prev, data []byte, batcher storage.KeyValueBatcher) {
	// Get the synaptic elements for this block
	tk := NewBlockTKey(chunkPt)
	elems, err := getElements(ctx, tk)
	if err != nil {
		dvid.Errorf("err getting elements for block %s: %v\n", chunkPt, err)
		return
	}
	if len(elems) == 0 {
		return
	}
	blockSize := d.blockSize()
	batch := batcher.NewBatch(ctx)

	// Compute the strides (in bytes)
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX

	// Iterate through all element positions, finding corresponding label and storing elements.
	var delta DeltaModifyElements
	lmapData := d.getMappedLabelData()
	labels := make(map[uint64]struct{})
	toAdd := LabelElements{}
	toDel := LabelPoints{}
	for n := range elems {
		pt := elems[n].Pos.Point3dInChunk(blockSize)
		i := pt[2]*bY + pt[1]*bX + pt[0]*8
		label := binary.LittleEndian.Uint64(data[i : i+8])
		var old uint64
		if len(prev) != 0 {
			old = binary.LittleEndian.Uint64(prev[i : i+8])
		}
		if lmapData != nil {
			if elems[n].Supervoxel != label {
				elems[n].Supervoxel = label
			}
			supervoxels := []uint64{label, old}
			mapped, err := lmapData.GetMappedLabels(ctx.VersionID(), supervoxels)
			if err != nil {
				dvid.Errorf("mutateBlock had problem with synced labelmap, annotations %q will be wrong: %v\n", d.DataName(), err)
			} else {
				label = mapped[0]
				old = mapped[1]
			}
		}
		if label != 0 {
			toAdd.add(label, elems[n].ElementNR)
			labels[label] = struct{}{}
			delta.Add = append(delta.Add, ElementPos{Label: label, Kind: elems[n].Kind, Pos: elems[n].Pos})
		}
		if old != 0 {
			toDel.add(old, elems[n].Pos)
			labels[old] = struct{}{}
			delta.Del = append(delta.Del, ElementPos{Label: old, Kind: elems[n].Kind, Pos: elems[n].Pos})
		}
	}

	// Modify any modified label k/v.
	for label := range labels {
		tk := NewLabelTKey(label)
		labelElems, err := getElementsNR(ctx, tk)
		if err != nil {
			dvid.Errorf("err getting elements for label %d: %v\n", label, err)
			return
		}
		deletions, found := toDel[label]
		if found {
			for _, pt := range deletions {
				labelElems.delete(pt)
			}
		}
		additions, found := toAdd[label]
		if found {
			labelElems.add(additions)
		}
		val, err := json.Marshal(labelElems)
		if err != nil {
			dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			return
		}
		batch.Put(tk, val)
	}
	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in annotations %q after delete block: %v\n", d.DataName(), err)
		return
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

func (d *Data) mergeLabels(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.MergeOp) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Get the target label
	targetTk := NewLabelTKey(op.Target)
	targetElems, err := getElementsNR(ctx, targetTk)
	if err != nil {
		return fmt.Errorf("get annotations for instance %q, target %d, in syncMerge: %v", d.DataName(), op.Target, err)
	}

	// Iterate through each merged label, read old elements, delete that k/v, then add it to the current target elements.
	var delta DeltaModifyElements
	elemsAdded := 0
	for label := range op.Merged {
		tk := NewLabelTKey(label)
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			return fmt.Errorf("unable to get annotation elements for instance %q, label %d in syncMerge: %v", d.DataName(), label, err)
		}
		if elems == nil || len(elems) == 0 {
			continue
		}
		batch.Delete(tk)
		elemsAdded += len(elems)
		targetElems = append(targetElems, elems...)

		// for labelsz.  TODO, only do this computation if really subscribed.
		for _, elem := range elems {
			delta.Add = append(delta.Add, ElementPos{Label: op.Target, Kind: elem.Kind, Pos: elem.Pos})
			delta.Del = append(delta.Del, ElementPos{Label: label, Kind: elem.Kind, Pos: elem.Pos})
		}
	}
	if elemsAdded > 0 {
		val, err := json.Marshal(targetElems)
		if err != nil {
			return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v", d.DataName(), err)
		}
		batch.Put(targetTk, val)
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("unable to commit merge for instance %q: %v", d.DataName(), err)
		}
	}
	d.StopUpdate()

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
	return nil
}

func (d *Data) cleaveLabels(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.CleaveOp) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Get the elements for the target label.
	oldTk := NewLabelTKey(op.Target)
	oldElems, err := getElementsNR(ctx, oldTk)
	if err != nil {
		return fmt.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v", d.DataName(), op.Target, err)
	}

	cleaved := make(labels.Set)
	for _, supervoxel := range op.CleavedSupervoxels {
		cleaved[supervoxel] = struct{}{}
	}

	// Iterate through all the old elements and if it has a cleaved supervoxel, move it to new label.
	var delta DeltaModifyElements
	toDel := make(map[int]struct{})
	toAdd := ElementsNR{}
	for i, elem := range oldElems {
		if _, found := cleaved[elem.Supervoxel]; found {
			toDel[i] = struct{}{}
			toAdd = append(toAdd, elem)

			// for downstream annotation syncs like labelsz.  TODO: only perform if subscribed.  Better: do ROI filtering here.
			delta.Del = append(delta.Del, ElementPos{Label: op.Target, Kind: elem.Kind, Pos: elem.Pos})
			delta.Add = append(delta.Add, ElementPos{Label: op.CleavedLabel, Kind: elem.Kind, Pos: elem.Pos})
		}
	}
	if len(toDel) == 0 {
		return nil
	}

	// Store cleaved elements into new label elements.
	newTk := NewLabelTKey(op.CleavedLabel)
	newElems, err := getElementsNR(ctx, newTk)
	if err != nil {
		return fmt.Errorf("unable to get annotations for instance %q, label %d in sync of cleave: %v", d.DataName(), op.CleavedLabel, err)
	}
	newElems.add(toAdd)
	val, err := json.Marshal(newElems)
	if err != nil {
		return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v", d.DataName(), err)
	}
	batch.Put(newTk, val)

	// Delete any cleave from old label elements without removing the relationships.
	// This filters without allocating, using fact that a slice shares the same backing array and
	// capacity as the original, so storage is reused.
	filtered := oldElems[:0]
	for i, elem := range oldElems {
		if _, found := toDel[i]; !found {
			filtered = append(filtered, elem)
		}
	}

	// Delete or store k/v depending on what remains.
	if len(filtered) == 0 {
		batch.Delete(oldTk)
	} else {
		val, err := json.Marshal(filtered)
		if err != nil {
			return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v", d.DataName(), err)
		}
		batch.Put(oldTk, val)
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in annotations %q after split: %v", d.DataName(), err)
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
	return nil
}

func (d *Data) splitLabelsCoarse(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Get the elements for the old label.
	oldTk := NewLabelTKey(op.OldLabel)
	oldElems, err := getElementsNR(ctx, oldTk)
	if err != nil {
		return fmt.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v", d.DataName(), op.OldLabel, err)
	}

	// Create a map to test each point.
	splitBlocks := make(map[dvid.IZYXString]struct{})
	for _, zyxStr := range op.SortedBlocks {
		splitBlocks[zyxStr] = struct{}{}
	}

	// Move any elements that are within the split blocks.
	var delta DeltaModifyElements
	toDel := make(map[int]struct{})
	toAdd := ElementsNR{}
	blockSize := d.blockSize()
	for i, elem := range oldElems {
		zyxStr := elem.Pos.ToBlockIZYXString(blockSize)
		if _, found := splitBlocks[zyxStr]; found {
			toDel[i] = struct{}{}
			toAdd = append(toAdd, elem)

			// for downstream annotation syncs like labelsz.  TODO: only perform if subscribed.  Better: do ROI filtering here.
			delta.Del = append(delta.Del, ElementPos{Label: op.OldLabel, Kind: elem.Kind, Pos: elem.Pos})
			delta.Add = append(delta.Add, ElementPos{Label: op.NewLabel, Kind: elem.Kind, Pos: elem.Pos})
		}
	}
	if len(toDel) == 0 {
		return nil
	}

	// Store split elements into new label elements.
	newTk := NewLabelTKey(op.NewLabel)
	newElems, err := getElementsNR(ctx, newTk)
	if err != nil {
		return fmt.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v", d.DataName(), op.NewLabel, err)
	}
	newElems.add(toAdd)
	val, err := json.Marshal(newElems)
	if err != nil {
		return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v", d.DataName(), err)
	}
	batch.Put(newTk, val)

	// Delete any split from old label elements without removing the relationships.
	// This filters without allocating, using fact that a slice shares the same backing array and
	// capacity as the original, so storage is reused.
	filtered := oldElems[:0]
	for i, elem := range oldElems {
		if _, found := toDel[i]; !found {
			filtered = append(filtered, elem)
		}
	}

	// Delete or store k/v depending on what remains.
	if len(filtered) == 0 {
		batch.Delete(oldTk)
	} else {
		val, err := json.Marshal(filtered)
		if err != nil {
			return fmt.Errorf("couldn't serialize annotation elements in instance %q: %v", d.DataName(), err)
		}
		batch.Put(oldTk, val)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in annotations %q after split: %v", d.DataName(), err)
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
	return nil
}

func (d *Data) splitLabels(batcher storage.KeyValueBatcher, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	var delta DeltaModifyElements
	toAdd := ElementsNR{}
	toDel := make(map[string]struct{})

	// Iterate through each split block, get the elements, and then modify the previous and new label k/v.
	for izyx, rles := range op.Split {
		// Get the elements for this block.
		blockPt, err := izyx.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(blockPt)
		elems, err := getElements(ctx, tk)
		if err != nil {
			dvid.Errorf("getting annotations for block %s on split of %d from %d: %v\n", blockPt, op.NewLabel, op.OldLabel, err)
			continue
		}

		// For any element within the split RLEs, add to the delete and addition lists.
		for n, elem := range elems {
			for _, rle := range rles {
				if rle.Within(elem.Pos) {
					toAdd = append(toAdd, elems[n].ElementNR)
					toDel[elem.Pos.String()] = struct{}{}

					// for downstream annotation syncs like labelsz.  TODO: only perform if subscribed.  Better: do ROI filtering here.
					delta.Del = append(delta.Del, ElementPos{Label: op.OldLabel, Kind: elem.Kind, Pos: elem.Pos})
					delta.Add = append(delta.Add, ElementPos{Label: op.NewLabel, Kind: elem.Kind, Pos: elem.Pos})
					break
				}
			}
		}
	}

	// Modify the old label k/v
	if len(toDel) != 0 {
		tk := NewLabelTKey(op.OldLabel)
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			dvid.Errorf("unable to get annotations for instance %q, old label %d in syncSplit: %v\n", d.DataName(), op.OldLabel, err)
		} else {
			filtered := elems[:0]
			for _, elem := range elems {
				if _, found := toDel[elem.Pos.String()]; !found {
					filtered = append(filtered, elem)
				}
			}
			if len(filtered) == 0 {
				batch.Delete(tk)
			} else {
				val, err := json.Marshal(filtered)
				if err != nil {
					dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
				} else {
					batch.Put(tk, val)
				}
			}
		}
	}

	// Modify the new label k/v
	if len(toAdd) != 0 {
		tk := NewLabelTKey(op.NewLabel)
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			dvid.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.NewLabel, err)
		} else {
			elems.add(toAdd)
			val, err := json.Marshal(elems)
			if err != nil {
				dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			} else {
				batch.Put(tk, val)
			}
		}
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in annotations %q after split: %v", d.DataName(), err)
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
	return nil
}

func (d *Data) splitMappedLabels(batcher storage.KeyValueBatcher, lmapData mappedLabelType, v dvid.VersionID, op labels.DeltaSplit) error {
	d.Lock()
	defer d.Unlock()

	d.StartUpdate()
	defer d.StopUpdate()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// get the elements for label being split
	oldTk := NewLabelTKey(op.OldLabel)
	elems, err := getElementsNR(ctx, oldTk)
	if err != nil {
		return err
	}

	// for each element, see if it's in the split area and if so, change supervoxel to split id.
	var delta DeltaModifyElements
	toAdd := ElementsNR{}
	toDel := make(map[string]struct{})
	blockSize := d.blockSize()
	for n, elem := range elems {
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)
		rles, found := op.Split[izyxStr]
		if found {
			for _, rle := range rles {
				if rle.Within(elem.Pos) {
					elems[n].Supervoxel = op.NewLabel // label = supervoxel for a mapped split
					toAdd = append(toAdd, elems[n])
					toDel[elem.Pos.String()] = struct{}{}

					// for downstream annotation syncs like labelsz.  TODO: only perform if subscribed.  Better: do ROI filtering here.
					delta.Del = append(delta.Del, ElementPos{Label: op.OldLabel, Kind: elem.Kind, Pos: elem.Pos})
					delta.Add = append(delta.Add, ElementPos{Label: op.NewLabel, Kind: elem.Kind, Pos: elem.Pos})
					break
				}
			}
		}
	}
	// Modify the old label k/v
	if len(toDel) != 0 {
		elems, err := getElementsNR(ctx, oldTk)
		if err != nil {
			dvid.Errorf("unable to get annotations for instance %q, old label %d in syncSplit: %v\n", d.DataName(), op.OldLabel, err)
		} else {
			filtered := elems[:0]
			for _, elem := range elems {
				if _, found := toDel[elem.Pos.String()]; !found {
					filtered = append(filtered, elem)
				}
			}
			if len(filtered) == 0 {
				batch.Delete(oldTk)
			} else {
				val, err := json.Marshal(filtered)
				if err != nil {
					dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
				} else {
					batch.Put(oldTk, val)
				}
			}
		}
	}

	// Modify the new label k/v
	if len(toAdd) != 0 {
		tk := NewLabelTKey(op.NewLabel)
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			dvid.Errorf("unable to get annotations for instance %q, label %d in syncSplit: %v\n", d.DataName(), op.NewLabel, err)
		} else {
			elems.add(toAdd)
			val, err := json.Marshal(elems)
			if err != nil {
				dvid.Errorf("couldn't serialize annotation elements in instance %q: %v\n", d.DataName(), err)
			} else {
				batch.Put(tk, val)
			}
		}
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad commit in annotations %q after split: %v", d.DataName(), err)
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
	return nil
}
