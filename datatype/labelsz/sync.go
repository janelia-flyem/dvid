/*
	This file supports creation of labelsz data from the associated labelvol data.
*/

package labelsz

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBuffer = 100

// InitSyncGraph implements the datastore.Syncer interface
func (d *Data) InitSyncGraph() []datastore.SyncSubscribe {
	syncCh := make(chan datastore.SyncMessage, syncBuffer)
	doneCh := make(chan struct{})

	go d.handleSizeEvent(syncCh, doneCh)

	subs := []datastore.SyncSubscribe{
		datastore.SyncSubscribe{
			Src:   d.Source,
			Dst:   d.DataName(),
			Event: labels.ChangeSizeEvent,
			Ch:    syncCh,
			Done:  doneCh,
		},
	}
	return subs
}

func (d *Data) handleSizeEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	for msg := range in {
		deltas, ok := msg.Delta.(labels.DeltaSizes)
		if !ok {
			dvid.Criticalf("Cannot sync labelsz.  Got unexpected delta: %v", msg)
			return
		}
		ctx := datastore.NewVersionedContext(d, msg.Version)
		batch := batcher.NewBatch(ctx)
		for _, delta := range deltas {
			oldKey := NewIndex(delta.OldSize, label)
			newKey := NewIndex(delta.NewSize, label)
			batch.Put(newKey, dvid.EmptyValue())
			batch.Delete(oldKey)
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
		}
		select {
		case <-done:
			return
		}
	}
}
