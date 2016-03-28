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

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(syncData dvid.Data) []datastore.SyncSub {
	syncCh := make(chan datastore.SyncMessage, syncBuffer)
	doneCh := make(chan struct{})

	go d.handleSizeEvent(syncCh, doneCh)

	evt := datastore.SyncEvent{syncData.DataName(), labels.ChangeSizeEvent}
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
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	for msg := range in {
		ctx := datastore.NewVersionedCtx(d, msg.Version)

		select {
		case <-done:
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaNewSize:
				batch := batcher.NewBatch(ctx)
				newKey := NewSizeLabelTKey(delta.Size, delta.Label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeTKey(delta.Label, delta.Size)
				batch.Put(newKey, dvid.EmptyValue())
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %v\n", ctx, err)
				}

			case labels.DeltaDeleteSize:
				if delta.OldKnown {
					batch := batcher.NewBatch(ctx)
					oldKey := NewSizeLabelTKey(delta.OldSize, delta.Label)
					batch.Delete(oldKey)
					oldKey = NewLabelSizeTKey(delta.Label, delta.OldSize)
					batch.Delete(oldKey)
					if err := batch.Commit(); err != nil {
						dvid.Errorf("Error on updating label sizes on %s: %v\n", ctx, err)
					}
				} else {
					// TODO -- Make transactional or force sequentially to label-specific goroutine
					begKey := NewLabelSizeTKey(delta.Label, 0)
					endKey := NewLabelSizeTKey(delta.Label, math.MaxUint64)
					keys, err := store.KeysInRange(ctx, begKey, endKey)
					if err != nil {
						dvid.Errorf("Unable to get size keys for label %d: %v\n", delta.Label, err)
						continue
					}
					if len(keys) != 1 {
						dvid.Errorf("Cannot modify size of label %d when no prior size recorded!", delta.Label)
						continue
					}

					// Modify label size
					label, oldSize, err := DecodeLabelSizeTKey(keys[0])
					if label != delta.Label {
						dvid.Errorf("Requested size of label %d and got key for label %d!\n", delta.Label, label)
						continue
					}
					batch := batcher.NewBatch(ctx)
					oldKey := NewSizeLabelTKey(oldSize, delta.Label)
					batch.Delete(oldKey)
					oldKey = NewLabelSizeTKey(delta.Label, oldSize)
					batch.Delete(oldKey)
					if err := batch.Commit(); err != nil {
						dvid.Errorf("Error on updating label sizes on %s: %v\n", ctx, err)
					}
				}

			case labels.DeltaModSize:
				// TODO -- Make transactional or force sequentially to label-specific goroutine
				// Get old label size
				begKey := NewLabelSizeTKey(delta.Label, 0)
				endKey := NewLabelSizeTKey(delta.Label, math.MaxUint64)
				keys, err := store.KeysInRange(ctx, begKey, endKey)
				if err != nil {
					dvid.Errorf("Unable to get size keys for label %d: %v\n", delta.Label, err)
					continue
				}
				if len(keys) != 1 {
					dvid.Errorf("Cannot modify size of label %d when no prior size recorded!", delta.Label)
					continue
				}

				// Modify label size
				label, oldSize, err := DecodeLabelSizeTKey(keys[0])
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
				oldKey := NewSizeLabelTKey(oldSize, label)
				batch.Delete(oldKey)
				oldKey = NewLabelSizeTKey(label, oldSize)
				batch.Delete(oldKey)

				// Add new one
				newKey := NewSizeLabelTKey(newSize, label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeTKey(label, newSize)
				batch.Put(newKey, dvid.EmptyValue())

				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %v\n", ctx, err)
				}

			case labels.DeltaReplaceSize:
				batch := batcher.NewBatch(ctx)
				oldKey := NewSizeLabelTKey(delta.OldSize, delta.Label)
				batch.Delete(oldKey)
				oldKey = NewLabelSizeTKey(delta.Label, delta.OldSize)
				batch.Delete(oldKey)
				newKey := NewSizeLabelTKey(delta.NewSize, delta.Label)
				batch.Put(newKey, dvid.EmptyValue())
				newKey = NewLabelSizeTKey(delta.Label, delta.NewSize)
				batch.Put(newKey, dvid.EmptyValue())
				if err := batch.Commit(); err != nil {
					dvid.Errorf("Error on updating label sizes on %s: %v\n", ctx, err)
				}

			default:
				dvid.Criticalf("Cannot sync labelsz using unexpected delta: %v", msg)
			}
		}
	}
}
