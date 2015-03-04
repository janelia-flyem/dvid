/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelblk

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs() []datastore.SyncSub {
	mergeCh := make(chan datastore.SyncMessage, 100)
	mergeDone := make(chan struct{})

	splitCh := make(chan datastore.SyncMessage, 10) // Splits can be a lot bigger due to sparsevol
	splitDone := make(chan struct{})

	subs := []datastore.SyncSub{
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.ChangeSparsevolEvent},
			Notify: d.DataName(),
			Ch:     make(chan datastore.SyncMessage, 100),
			Done:   make(chan struct{}),
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.MergeStartEvent},
			Notify: d.DataName(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.MergeEndEvent},
			Notify: d.DataName(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.MergeBlockEvent},
			Notify: d.DataName(),
			Ch:     mergeCh,
			Done:   mergeDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.SplitStartEvent},
			Notify: d.DataName(),
			Ch:     splitCh,
			Done:   splitDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.SplitEndEvent},
			Notify: d.DataName(),
			Ch:     splitCh,
			Done:   splitDone,
		},
		datastore.SyncSub{
			Event:  datastore.SyncEvent{d.Link, labels.SplitBlockEvent},
			Notify: d.DataName(),
			Ch:     splitCh,
			Done:   splitDone,
		},
	}

	// Launch go routines to handle sync events.
	go d.syncSparsevolChange(subs[0].Ch, subs[0].Done)
	go d.syncMerge(mergeCh, mergeDone)
	go d.syncSplit(splitCh, splitDone)

	return subs
}

func (d *Data) syncSparsevolChange(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	/*
		for msg := range in {
			select {
			case <-done:
				return
			default:
			}
		}
	*/
}

func hashStr(s dvid.IZYXString, n int) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(s))
	if err != nil {
		dvid.Criticalf("Could not write to fnv hash in labelblk.hashStr()")
		return 0
	}
	return int(hash.Sum32()) % n
}

type mergeOp struct {
	labels.MergeOp
	ctx  datastore.VersionedContext
	izyx dvid.IZYXString
}

func (d *Data) syncMerge(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	// Start N goroutines to process blocks.  Don't need transactional support for
	// GET-PUT combo if each block only is handled serially by a given goroutine.
	const numprocs = 32
	const mergeBufSize = 100
	var pch [numprocs]chan mergeOp
	for i := 0; i < numprocs; i++ {
		pch[i] = make(chan mergeOp, mergeBufSize)
		go d.relabelBlock(pch[i])
	}

	// Process incoming merge messages
	const batchSize = 100
	for msg := range in {
		select {
		case <-done:
			for i := 0; i < numprocs; i++ {
				close(pch[i])
			}
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaMerge:
				ctx := datastore.NewVersionedContext(d, msg.Version)
				for izyxStr := range delta.Blocks {
					n := hashStr(izyxStr, numprocs)
					pch[n] <- mergeOp{delta.MergeOp, *ctx, izyxStr}
				}
			case labels.DeltaMergeStart:
			case labels.DeltaMergeEnd:
			default:
				dvid.Criticalf("bad delta in merge event: %v\n", delta)
				continue
			}
		}
	}
}

func (d *Data) syncSplit(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	for msg := range in {
		select {
		case <-done:
			return
		default:
			switch delta := msg.Delta.(type) {
			case labels.DeltaSplit:
				//ctx := datastore.NewVersionedContext(d, msg.Version)
			case labels.DeltaSplitStart:
			case labels.DeltaSplitEnd:
			default:
				dvid.Criticalf("bad delta in split event: %v\n", delta)
				continue
			}
		}
	}
}

// Goroutine that handles relabeling of blocks that form a part of the spatial index space.
// Since the same block coordinate always gets mapped to the same goroutine, we handle
// concurrency by serializing GET/PUT for a particular block coordinate.
// TODO -- Block-level ops should be handled by one goroutine for a particular block across all ops.
func (d *Data) relabelBlock(in <-chan mergeOp) {
	blockBytes := int(d.BlockSize().Prod() * 8)

	for op := range in {
		k := NewIndexByCoord(op.izyx)
		data, err := store.Get(op.ctx, k)
		if err != nil {
			dvid.Errorf("Error on GET of labelblk with coord string %q\n", op.izyx)
			return
		}
		if data == nil {
			dvid.Errorf("nil label block where merge was done!\n")
			return
		}

		blockData, _, err := dvid.DeserializeData(data, true)
		if err != nil {
			dvid.Criticalf("unable to deserialize label block in '%s': %s\n", d.DataName(), err.Error())
			return
		}
		if len(blockData) != blockBytes {
			dvid.Criticalf("After labelblk deserialization got back %d bytes, expected %d bytes\n", len(blockData), blockBytes)
			return
		}

		// Iterate through this block of labels and relabel if label in merge.
		for i := 0; i < blockBytes; i += 8 {
			label := binary.LittleEndian.Uint64(blockData[i : i+8])
			if _, merged := op.Merged[label]; merged {
				binary.LittleEndian.PutUint64(blockData[i:i+8], op.Target)
			}
		}

		// Store this block.
		serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
		if err != nil {
			dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
			return
		}
		if err := store.Put(op.ctx, k, serialization); err != nil {
			dvid.Errorf("Error in putting key %v: %s\n", k, err.Error())
		}
	}
}
