/*
	This file supports creation of labelsz data from the associated labelvol data.
*/

package labelsz

import (
	"math"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Number of change messages we can buffer before blocking on sync channel.
const syncBuffer = 100

// InitSync implements the datastore.Syncer interface
func (d *Data) InitSync(name dvid.InstanceName) []datastore.SyncSub {
	// This should only be called once for any synced instance.
	if d.IsSyncEstablished(name) {
		return nil
	}
	d.SyncEstablished(name)

	syncCh := make(chan datastore.SyncMessage, syncBuffer)
	doneCh := make(chan struct{})

	go d.handleSizeEvent(syncCh, doneCh)

	evt := datastore.SyncEvent{name, labels.ChangeSizeEvent}
	subs := []datastore.SyncSub{
		datastore.SyncSub{
			Event:  evt,
			Notify: d.DataName(),
			Ch:     syncCh,
			Done:   doneCh,
		},
	}
	return subs
}

func (d *Data) handleSizeEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	store, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	for msg := range in {
		ctx := datastore.NewVersionedContext(d, msg.Version)

		select {
		case <-done:
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaNewSize:
				batch := batcher.NewBatch(ctx)
				newKey := NewSizeLabelIndex(delta.Size, delta.Label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeIndex(delta.Label, delta.Size)
				batch.Put(newKey, dvid.EmptyValue())
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
				}

			case labels.DeltaDeleteSize:
				if delta.OldKnown {
					batch := batcher.NewBatch(ctx)
					oldKey := NewSizeLabelIndex(delta.OldSize, delta.Label)
					batch.Delete(oldKey)
					oldKey = NewLabelSizeIndex(delta.Label, delta.OldSize)
					batch.Delete(oldKey)
					if err := batch.Commit(); err != nil {
						dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
					}
				} else {
					// TODO -- Make transactional or force sequentially to label-specific goroutine
					begKey := NewLabelSizeIndex(delta.Label, 0)
					endKey := NewLabelSizeIndex(delta.Label, math.MaxUint64)
					if err := store.DeleteRange(ctx, begKey, endKey); err != nil {
						dvid.Errorf("Error on trying to delete label+size index for label %d\n", delta.Label)
					}
					begKey = NewSizeLabelIndex(0, delta.Label)
					endKey = NewSizeLabelIndex(math.MaxUint64, delta.Label)
					if err := store.DeleteRange(ctx, begKey, endKey); err != nil {
						dvid.Errorf("Error on trying to delete size+label index for label %d\n", delta.Label)
					}
					continue
				}

			case labels.DeltaModSize:
				// TODO -- Make transactional or force sequentially to label-specific goroutine
				// Get old label size
				begKey := NewLabelSizeIndex(delta.Label, 0)
				endKey := NewLabelSizeIndex(delta.Label, math.MaxUint64)
				keys, err := store.KeysInRange(ctx, begKey, endKey)
				if err != nil {
					dvid.Errorf("Unable to get size keys for label %d: %s\n", delta.Label, err.Error())
					continue
				}
				if len(keys) != 1 {
					dvid.Errorf("Cannot modify size of label %d when no prior size recorded!", delta.Label)
					continue
				}

				// Modify label size
				label, oldSize, err := DecodeLabelSizeKey(keys[0])
				if label != delta.Label {
					dvid.Errorf("Requested size of label %d and got key for label %d!\n", delta.Label, label)
					continue
				}
				// Assumes 63 bits is sufficient for any label
				size := int64(oldSize)
				size += delta.SizeChange
				newSize := uint64(size)

				// Delete old label size keys
				batch := batcher.NewBatch(ctx)
				oldKey := NewSizeLabelIndex(oldSize, label)
				batch.Delete(oldKey)
				oldKey = NewLabelSizeIndex(label, oldSize)
				batch.Delete(oldKey)

				// Add new one
				newKey := NewSizeLabelIndex(newSize, label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeIndex(label, newSize)
				batch.Put(newKey, dvid.EmptyValue())

				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
				}

			case labels.DeltaReplaceSize:
				batch := batcher.NewBatch(ctx)
				oldKey := NewSizeLabelIndex(delta.OldSize, delta.Label)
				batch.Delete(oldKey)
				oldKey = NewLabelSizeIndex(delta.Label, delta.OldSize)
				batch.Delete(oldKey)
				newKey := NewSizeLabelIndex(delta.NewSize, delta.Label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeIndex(delta.Label, delta.NewSize)
				batch.Put(newKey, dvid.EmptyValue())
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
				}

			default:
				dvid.Criticalf("Cannot sync labelsz using unexpected delta: %v", msg)
			}
		}
	}
}
