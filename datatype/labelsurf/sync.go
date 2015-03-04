/*
	This file supports creation of labelsurf data from the associated labelvol data.
*/

package labelsurf

import "github.com/janelia-flyem/dvid/datastore"

// Number of change messages we can buffer before blocking on sync channel.
const syncBuffer = 100

// GetSyncSubs implements the datastore.Syncer interface and allows syncing of
// label surfaces with changes to linked labelvol (sparse volume) representations.
func (d *Data) GetSyncSubs() []datastore.SyncSub {
	/*
		mergeCh := make(chan datastore.SyncMessage, syncBuffer)
		done1Ch := make(chan struct{})

		go d.handleMergeEvent(mergeCh, done1Ch)

		splitCh := make(chan datastore.SyncMessage, syncBuffer)
		done2Ch := make(chan struct{})

		go d.handleSplitEvent(splitCh, done2Ch)

		subs := []datastore.SyncSub{
			datastore.SyncSub{
				Src:   d.Link,
				Dst:   d.DataName(),
				Event: labels.MergeEvent,
				Ch:    mergeCh,
				Done:  done1Ch,
			},
			datastore.SyncSub{
				Src:   d.Link,
				Dst:   d.DataName(),
				Event: labels.SplitEvent,
				Ch:    splitCh,
				Done:  done2Ch,
			},
		}
		return subs
	*/
	return nil
}

/*
func (d *Data) handleMergeEvent(in <-chan datastore.SyncMessage, done <-chan struct{}) {
	// Delete the fromLabel surface.
	// TODO -- Move to labelsurf

		surfaceIndex := imageblk.NewLabelSurfaceIndex(fromLabel)
		if err := bigdata.Delete(ctx, surfaceIndex); err != nil {
			return fmt.Errorf("Can't delete label %d surface: %s", fromLabel, err.Error())
		}

	// Recompute the toLabel surface
	// TODO -- Move to labelsurf
	// go d.recomputeSurface(ctx, toLabel, toLabelRLEs)
	for msg := range in {
		deltas, ok := msg.Delta.(labels.DeltaReplaceSize)
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
*/

/*

// Handle merge of one or more labels into a target label.

// Handle split of some sparsevol from a previous label.

// TODO -- sync from labelvol changes
// recomputeSurface refreshes the computed surface from a label's RLEs.
func (d *Data) recomputeSurface(ctx *datastore.VersionedContext, label uint64, rles labels.BlockRLEs) {
	var curVol dvid.SparseVol
	curVol.SetLabel(label)
	for _, rle := range rles {
		curVol.AddRLE(rle)
	}
	if err := d.computeAndSaveSurface(ctx, &curVol); err != nil {
		dvid.Errorf("Error on computing surface and normals for label %d: %s\n", label, err.Error())
	}
}

type denormOp struct {
	source    *Data
	versionID dvid.VersionID
}

// Iterate through all linked label sparse volumes (labelvol) and sync the associated labelblk.
func (d *Data) SyncOnNodeLock(uuid dvid.UUID) {
	dvid.Infof("Adding spatial information from label volume %s ...\n", d.DataName())

	versionID, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		dvid.Errorf("Illegal UUID %q with no corresponding version ID!  Aborting.", uuid)
		return
	}

	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
		return
	}
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
		return
	}
	ctx := datastore.NewVersionedContext(d, versionID)

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	timedLog := dvid.NewTimeLog()
	wg := new(sync.WaitGroup)
	op := &denormOp{d, versionID}

	extents := d.Extents()
	minIndexZ := extents.MinIndex.(*dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(*dvid.IndexZYX)[2]
	for z := minIndexZ; z <= maxIndexZ; z++ {
		server.BlockOnInteractiveRequests("labelblk [load layer]")
		layerLog := dvid.NewTimeLog()

		minIndexZYX := dvid.IndexZYX{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], z}
		maxIndexZYX := dvid.IndexZYX{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], z}
		begIndex := NewIndex(&minIndexZYX)
		endIndex := NewIndex(&maxIndexZYX)

		// Process the labels chunks for this Z
		chunkOp := &storage.ChunkOp{op, wg}
		err = bigdata.ProcessRange(ctx, begIndex, endIndex, chunkOp, storage.ChunkProcessor(d.CreateChunkRLEs))
		wg.Wait()

		layerLog.Debugf("Processed all %q blocks for layer %d/%d", d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	timedLog.Infof("Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	timedLog = dvid.NewTimeLog()

	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go ComputeSizes(ctx, sizeCh, wg)

	// Create a number of label-specific surface calculation jobs
	size := extents.MaxPoint.Sub(extents.MinPoint)
	goroutineMB := size.Value(0) * size.Value(1) * (2*d.BlockSize().Value(2) + 1) / dvid.Mega
	numSurfCalculators := dvid.EstimateGoroutines(0.5, goroutineMB)
	surfaceCh := make([]chan *storage.Chunk, numSurfCalculators, numSurfCalculators)
	for i := 0; i < numSurfCalculators; i++ {
		<-server.HandlerToken
		surfaceCh[i] = make(chan *storage.Chunk, 10000)
		wg.Add(1)
		go ComputeSurface(ctx, d, surfaceCh[i], wg)
	}

	// Wait for results then set Updating.
	go func() {
		wg.Wait()
		timedLog.Debugf("Finished processing all RLEs for labels '%s'", d.DataName())
		d.Ready = true
		if err := datastore.SaveRepo(uuid); err != nil {
			dvid.Errorf("Could not save READY state to data '%s', uuid %s: %s",
				d.DataName(), uuid, err.Error())
		}
	}()

	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		// Get label associated with this sparse volume.
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Could not get %q index bytes from chunk key: %s\n", d.DataName(), err.Error())
		}
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%uint64(numSurfCalculators)] <- chunk

		server.BlockOnInteractiveRequests("labelblk [size/surface compute]")
		return nil
	}

	begIndex := NewIndex(0, dvid.MinIndexZYX.Bytes())
	endIndex := NewIndex(math.MaxUint64, dvid.MaxIndexZYX.Bytes())
	err = smalldata.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, f)
	if err != nil {
		dvid.Errorf("Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	for i := 0; i < numSurfCalculators; i++ {
		surfaceCh[i] <- nil
	}
	timedLog.Infof("Finished reading all RLEs for labels '%s'", d.DataName())
}

// denormFunc handles denormalization for label blocks sent down a channel.  When channel is
// closed, batch denormalizations are handled.
// On return from this function, block-level RLEs have been written but size and surface
// data are handled asynchronously.
func (d *Data) denormFunc(versionID dvid.VersionID, mods imageblk.BlockChannel) {
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
		return
	}

	// Keep track of all labels that are within these blocks since we will
	// redo denormalization for all of them.
	// TODO: Limit re-denormalization on actually modified labels, but figure merge/split
	// is primary calls for these more specific edits.
	labels := make(map[uint64]bool, 1000)

	// Accept modified label blocks and change LabelSpatialMapIndex key/values.
	for {
		block, more := <-mods
		if !more {
			break
		}
		if len(block.Data)%8 != 0 {
			dvid.Errorf("Received block label data with size not-aligned with uint64: %d bytes\n",
				len(block.Data))
			return
		}
		for i := 0; i < len(block.Data); i += 8 {
			label := binary.LittleEndian.Uint64(block.Data[i : i+8])
			if label != 0 {
				labels[label] = true
			}
		}
		d.createChunkRLEs(versionID, block.Index, block.Data)
	}

	// Setup goroutines for processing label size and surface.
	ctx := datastore.NewVersionedContext(d, versionID)
	wg := new(sync.WaitGroup)
	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go ComputeSizes(ctx, sizeCh, wg)
	surfaceCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)
	go ComputeSurface(ctx, d, surfaceCh, wg)

	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		// Get label associated with this sparse volume.
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Could not get %q index bytes from chunk key: %s\n", d.DataName(), err.Error())
		}
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh <- chunk

		server.BlockOnInteractiveRequests("labelblk [size/surface compute]")
		return nil
	}

	// Given all blocks modified, process body RLEs for label sizes and surfaces.
	for label := range labels {
		begIndex := NewIndex(label, dvid.MinIndexZYX.Bytes())
		endIndex := NewIndex(label, dvid.MaxIndexZYX.Bytes())
		err := smalldata.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, f)
		if err != nil {
			dvid.Errorf("Error denormalizing %s: %s\n", d.DataName(), err.Error())
			return
		}
	}

	sizeCh <- nil
	surfaceCh <- nil

	// Wait for results then set Updating.
	go func() {
		wg.Wait()
		dvid.Debugf("Finished processing denormalization for labels '%s'\n", d.DataName())
		d.Ready = true
		// TODO -- should use metadata store for this kind of mutex
	}()
}

// ComputeSurface computes and stores a label surface.
// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func ComputeSurface(ctx storage.Context, data *Data, ch chan *storage.Chunk, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		server.HandlerToken <- 1
	}()

	// Sequentially process all the sparse volume data for each label coming down channel.
	var curVol dvid.SparseVol
	var curLabel uint64
	notFirst := false
	for {
		chunk := <-ch
		if chunk == nil {
			if notFirst {
				if err := data.computeAndSaveSurface(ctx, &curVol); err != nil {
					dvid.Errorf("Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			return
		}
		label := chunk.ChunkOp.Op.(uint64)
		if label != curLabel || label == 0 {
			if notFirst {
				if err := data.computeAndSaveSurface(ctx, &curVol); err != nil {
					dvid.Errorf("Error on computing surface and normals: %s\n", err.Error())
					return
				}
			}
			curVol.Clear()
			curVol.SetLabel(label)
		}

		if err := curVol.AddSerializedRLEs(chunk.V); err != nil {
			dvid.Errorf("Error adding RLE for label %d: %s\n", label, err.Error())
			return
		}
		curLabel = label
		notFirst = true
	}
}

func (d *Data) computeAndSaveSurface(ctx storage.Context, vol *dvid.SparseVol) error {
	surfaceBytes, err := vol.SurfaceSerialization(d.BlockSize().Value(2), d.Resolution.VoxelSize)
	if err != nil {
		return err
	}
	store, err := storage.BigDataStore()
	if err != nil {
		return err
	}

	// Surface blobs are always stored using gzip with best compression, trading off time
	// during the store for speed during interactive GETs.
	compression, _ := dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
	serialization, err := dvid.SerializeData(surfaceBytes, compression, dvid.NoChecksum)
	if err != nil {
		return fmt.Errorf("Unable to serialize data in surface computation: %s\n", err.Error())
	}
	key := imageblk.NewLabelSurfaceIndex(vol.Label())
	return store.Put(ctx, key, serialization)
}
*/
