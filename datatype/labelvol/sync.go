package labelvol

import (
	"encoding/binary"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// ---- Subscribing to changes in other data types ------

// Number of change messages we can buffer before blocking on sync channel.
const syncBuffer = 100

// InitSyncGraph implements the datastore.Syncer interface
func (d *Data) InitSyncGraph() []datastore.SyncSubscribe {
	syncCh := make(chan datastore.SyncMessage, syncBuffer)
	doneCh := make(chan struct{})

	go d.handleBlockEvent(syncCh, doneCh)

	subs := []datastore.SyncSubscribe{
		datastore.SyncSubscribe{
			Src:   d.Link,
			Dst:   d.DataName(),
			Event: labels.ChangeBlockEvent,
			Ch:    syncCh,
			Done:  doneCh,
		},
	}
	return subs
}

// Processes each change as we get it.
// TODO -- accumulate larger # of changes before committing to prevent
// excessive compaction time?  This assumes LSM storage engine, which
// might not always hold in future, so stick with incremental update
// until proven to be a bottleneck.
func (d *Data) handleBlockEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(keyLabelBlockRLE)

	for msg := range in {
		block, ok := msg.Delta.(labels.Block)
		if !ok {
			dvid.Criticalf("Cannot sync labelvol from %q block event.  Got unexpected delta: %v", d.Link, msg)
			continue
		}

		// Iterate through this block of labels and create RLEs for each label.
		blockBytes := len(block.Data)
		if blockBytes != int(d.BlockSize.Prod())*8 {
			dvid.Criticalf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
				blockBytes, d.BlockSize.Prod())
			continue
		}
		labelRLEs := make(map[uint64]dvid.RLEs, 10)
		firstPt := block.Index.MinPoint(d.BlockSize)
		lastPt := block.Index.MaxPoint(d.BlockSize)

		var curStart dvid.Point3d
		var voxelLabel, curLabel uint64
		var z, y, x, curRun int32
		start := 0
		for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
			for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
				for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
					voxelLabel = binary.LittleEndian.Uint64(block.Data[start : start+8])
					start += 8

					// If we hit background or have switched label, save old run and start new one.
					if voxelLabel == 0 || voxelLabel != curLabel {
						// Save old run
						if curRun > 0 {
							labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
						}
						// Start new one if not zero label.
						if voxelLabel != 0 {
							curStart = dvid.Point3d{x, y, z}
							curRun = 1
						} else {
							curRun = 0
						}
						curLabel = voxelLabel
					} else {
						curRun++
					}
				}
				// Force break of any runs when we finish x scan.
				if curRun > 0 {
					labelRLEs[curLabel] = append(labelRLEs[curLabel], dvid.NewRLE(curStart, curRun))
					curLabel = 0
					curRun = 0
				}
			}
		}

		// Store the label RLEs
		ctx := datastore.NewVersionedContext(d, msg.Version)
		batch := batcher.NewBatch(ctx)

		copy(bsIndex[9:9+dvid.IndexZYXSize], block.Index.Bytes())
		for b, rles := range labelRLEs {
			binary.BigEndian.PutUint64(bsIndex[1:9], b)
			key := dvid.IndexBytes(bsIndex)
			runsBytes, err := rles.MarshalBinary()
			if err != nil {
				dvid.Errorf("Bad encoding labelvol keys for label %d: %s\n", b, err.Error())
				continue
			}
			batch.Put(key, runsBytes)
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Batch PUT during sync of %q with %q block event: %s\n", d.DataName(), d.Link, err.Error())
		}

		// Check if we should end this goroutine
		select {
		case <-done:
			return
		}
	}
}

// CreateChunkRLEs processes a chunk of labels data and stores the RLEs for each label.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) CreateChunkRLEs(chunk *storage.Chunk) error {
	<-server.HandlerToken
	go func() {
		defer func() {
			// After processing a chunk, return the token.
			server.HandlerToken <- 1

			// Notify the requestor that this chunk is done.
			if chunk.Wg != nil {
				chunk.Wg.Done()
			}
		}()

		// Get the spatial index associated with this chunk.
		_, zyxBytes, err := DecodeKey(chunk.K)
		if err != nil {
			dvid.Errorf("Error in %s.denormalizeChunk(): %s", d.DataName(), err.Error())
			return
		}
		var index dvid.IndexZYX
		if err := index.UnmarshalBinary(zyxBytes); err != nil {
			dvid.Errorf("Unable to deserialize 3d block coord in %q: %s\n", d.DataName(), err.Error())
			return
		}
		op := chunk.Op.(*denormOp)
		// This data needs to be uncompressed and deserialized.
		blockData, _, err := dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block in %q: %s\n", d.DataName(), err.Error())
			return
		}
		d.createChunkRLEs(op.versionID, index, blockData)
	}()
	return nil
}
