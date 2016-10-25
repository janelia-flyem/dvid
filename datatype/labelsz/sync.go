package labelsz

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/annotation"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBufferSize = 100

// InitDataHandlers launches goroutines to handle each labelblk instance's syncs.
func (d *Data) InitDataHandlers() error {
	if d.syncCh != nil || d.syncDone != nil {
		return nil
	}
	d.syncCh = make(chan datastore.SyncMessage, syncBufferSize)
	d.syncDone = make(chan *sync.WaitGroup)

	// Launch handlers of sync events.
	fmt.Printf("Launching sync event handler for data %q...\n", d.DataName())
	go d.processEvents()
	return nil
}

// Shutdown terminates blocks until syncs are done then terminates background goroutines processing data.
func (d *Data) Shutdown() {
	if d.syncDone != nil {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		d.syncDone <- wg
		wg.Wait() // Block until we are done.
	}
}

// GetSyncSubs implements the datastore.Syncer interface.  Returns a list of subscriptions
// to the sync data instance that will notify the receiver.
func (d *Data) GetSyncSubs(synced dvid.Data) (datastore.SyncSubs, error) {
	if d.syncCh == nil {
		if err := d.InitDataHandlers(); err != nil {
			return nil, fmt.Errorf("unable to initialize handlers for data %q: %v\n", d.DataName(), err)
		}
	}

	subs := datastore.SyncSubs{
		datastore.SyncSub{
			Event:  datastore.SyncEvent{synced.DataUUID(), annotation.ModifyElementsEvent},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		},
		// datastore.SyncSub{
		// 	Event:  datastore.SyncEvent{synced.DataUUID(), annotation.SetElementsEvent},
		// 	Notify: d.DataUUID(),
		// 	Ch:     d.SyncCh,
		// },
	}
	return subs, nil
}

// If annotation elements are added or deleted, adjust the label counts.
func (d *Data) processEvents() {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		dvid.Errorf("Exiting sync goroutine for labelsz %q after annotation modifications: %v\n", d.DataName(), err)
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
			d.StartUpdate()
			ctx := datastore.NewVersionedCtx(d, msg.Version)
			switch delta := msg.Delta.(type) {
			case annotation.DeltaModifyElements:
				d.modifyElements(ctx, delta, batcher)
			default:
				dvid.Criticalf("Cannot sync annotations from modify element.  Got unexpected delta: %v\n", msg)
			}
			d.StopUpdate()

			if stop && len(d.syncCh) == 0 {
				dvid.Infof("Shutting down sync even handler for instance %q after draining sync events.\n", d.DataName())
				wg.Done()
				return
			}
		}
	}
}

// returned map will only include labels that had previously been seen (has key)
func (d *Data) getCounts(ctx *datastore.VersionedCtx, labels map[indexedLabel]int32) (counts map[indexedLabel]uint32, err error) {
	var store storage.OrderedKeyValueDB
	store, err = d.GetOrderedKeyValueDB()
	if err != nil {
		return
	}

	counts = make(map[indexedLabel]uint32, len(labels))
	var i IndexType
	var label uint64
	var val []byte
	for il := range labels {
		i, label, err = decodeIndexedLabel(il)
		if err != nil {
			return
		}

		val, err = store.Get(ctx, NewTypeLabelTKey(i, label))
		if err != nil {
			return
		}
		if val == nil {
			continue
		}
		if len(val) != 4 {
			err = fmt.Errorf("bad size in value for index type %s, label %d: value has length %d", i, label, len(val))
			return
		}
		counts[il] = binary.LittleEndian.Uint32(val)
	}
	return
}

func (d *Data) modifyElements(ctx *datastore.VersionedCtx, delta annotation.DeltaModifyElements, batcher storage.KeyValueBatcher) {
	mods := make(map[indexedLabel]int32)
	for _, elemPos := range delta.Add {
		if d.inROI(elemPos) {
			i := toIndexedLabel(elemPos)
			mods[i]++
			if elemPos.Kind.IsSynaptic() {
				i = newIndexedLabel(AllSyn, elemPos.Label)
				mods[i]++
			}
		}
	}
	for _, elemPos := range delta.Del {
		if d.inROI(elemPos) {
			i := toIndexedLabel(elemPos)
			mods[i]--
			if elemPos.Kind.IsSynaptic() {
				i = newIndexedLabel(AllSyn, elemPos.Label)
				mods[i]--
			}
		}
	}

	d.Lock()
	defer d.Unlock()

	// Get old counts for the modified labels.
	counts, err := d.getCounts(ctx, mods)
	if err != nil {
		dvid.Errorf("couldn't get counts for modified labels: %v\n", err)
		return
	}

	// Modify the keys based on the change in counts, then delete or store.
	batch := batcher.NewBatch(ctx)
	for il, change := range mods {
		if change == 0 {
			continue
		}
		i, label, err := decodeIndexedLabel(il)
		if err != nil {
			dvid.Criticalf("couldn't decode indexedLabel %s for modify elements sync of %s: %v\n", il, d.DataName(), err)
			continue
		}

		// check if we had prior key that needs to be deleted.
		count, found := counts[il]
		if found {
			batch.Delete(NewTypeSizeLabelTKey(i, count, label))
		}

		// add new count
		if change < 0 && -change > int32(count) {
			dvid.Criticalf("received element mod that would subtract %d with only count %d!  Setting floor at 0.\n", -change, count)
			change = int32(-count)
		}
		newcount := uint32(int32(count) + change)

		// If it's at zero, we've merged or removed it so delete the count.
		if newcount == 0 {
			batch.Delete(NewTypeLabelTKey(i, label))
			batch.Delete(NewTypeSizeLabelTKey(i, newcount, label))
			continue
		}

		// store the data.
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, newcount)
		batch.Put(NewTypeLabelTKey(i, label), buf)
		batch.Put(NewTypeSizeLabelTKey(i, newcount, label), nil)
	}

	if err := batch.Commit(); err != nil {
		dvid.Criticalf("bad commit in labelsz %q during sync of modify elements: %v\n", d.DataName(), err)
		return
	}
}

/*
func (d *Data) syncSet(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		dvid.Errorf("Exiting sync goroutine for labelsz %q after annotation sets: %v\n", d.DataName(), err)
		return
	}
	for msg := range in {
		select {
		case <-done:
			return
		default:
		}
	}
}
*/
