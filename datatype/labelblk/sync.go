/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelblk

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numBlockHandlers = 32

	DownsizeBlockEvent  = "LABELBLK_DOWNSIZE_ADD"
	DownsizeCommitEvent = "LABELBLK_DOWNSIZE_COMMIT"
)

type deltaBlock struct {
	block dvid.IZYXString // block coordinate of the originating labelblk resolution.
	data  []byte
}

type procMsg struct {
	op interface{}
	v  dvid.VersionID
	wg *sync.WaitGroup
}

const (
	blockIngest blockOpType = iota
	blockMutate
	blockDelete
)

type blockOpType uint8

type blockOp struct {
	blockOpType
	delta interface{}
}

type mergeOp struct {
	labels.MergeOp
	block dvid.IZYXString
}

type splitOp struct {
	oldLabel uint64
	newLabel uint64
	rles     dvid.RLEs
	block    dvid.IZYXString
}

// cache of all blocks modified where the ZYX index is the lower-res
// (down-res by 2x) coordinate.
type blockCache map[dvid.IZYXString]octant

type octant [8][]byte // octant has nil []byte if not modified.

// serializes octant contents.  requires preallocated block buffer that may have old data.
func (d *Data) serializeOctants(oct octant, blockBuf []byte) ([]byte, error) {
	blockSize := d.BlockSize()
	nx := blockSize.Value(0)
	nxy := blockSize.Value(1) * nx

	halfx := blockSize.Value(0) >> 1
	halfy := blockSize.Value(1) >> 1
	halfz := blockSize.Value(2) >> 1
	octantBytes := halfx * halfy * halfz * 8

	for sector, data := range oct {
		if len(data) > 0 {
			// Get the corner voxel (in block coordinates) for this sector.
			iz := sector >> 2
			sector -= iz * 4
			iy := sector >> 1
			ix := sector % 2

			ox := int32(ix) * halfx
			oy := int32(iy) * halfy
			oz := int32(iz) * halfz

			// Copy data from octant into larger block buffer.
			oct[sector] = make([]byte, octantBytes)
			xbytes := halfx * 8

			var oi int32
			for z := oz; z < oz+halfz; z++ {
				for y := oy; y < oy+halfy; y++ {
					di := (z*nxy + y*nx + ox) * 8
					copy(oct[sector][oi:oi+xbytes], blockBuf[di:di+xbytes])
					oi += xbytes
				}
			}
		}
	}

	return dvid.SerializeData(blockBuf, d.Compression(), d.Checksum())
}

// Shutdown terminates background goroutines processing data and fullfills the Shutdowner interface.
func (d *Data) Shutdown() {
	if d.syncDone != nil {
		d.syncDone <- struct{}{}
	}
}

// InitDataHandlers launches goroutines to handle each labelblk instance's syncs.
func (d *Data) InitDataHandlers() error {
	d.syncCh = make(chan datastore.SyncMessage, 100)
	d.syncDone = make(chan struct{})

	// Start N goroutines to process mutations for each block that will be consistently
	// assigned to one of the N goroutines.
	for i := 0; i < numBlockHandlers; i++ {
		d.procCh[i] = make(chan procMsg, 100)
		go d.processBlock(d.procCh[i])
	}

	dvid.Infof("Launching sync event handler for data %q...\n", d.DataName())
	go d.processEvents()
	return nil
}

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(synced dvid.Data) datastore.SyncSubs {
	// Our syncing depends on the datatype we are syncing.
	var evts []string
	switch synced.TypeName() {
	case "labelblk":
		evts = []string{DownsizeBlockEvent, DownsizeCommitEvent}
		// TODO -- If multiscale syncs should also handle labelblk POST and other mutations instead
		//         of just handling split/merge ops, we need to get those events.
		// evts = []string{labels.IngestBlockEvent, labels.MutateBlockEvent, labels.DeleteBlockEvent}
	case "labelvol":
		evts = []string{labels.MergeStartEvent, labels.MergeBlockEvent, labels.SplitStartEvent, labels.SplitLabelEvent}
	default:
		dvid.Errorf("Unable to sync %s with %s since datatype %q is not supported.", d.DataName(), synced.DataName(), synced.TypeName())
		return nil
	}

	subs := make(datastore.SyncSubs, len(evts))
	for i, evt := range evts {
		subs[i] = datastore.SyncSub{
			Event:  datastore.SyncEvent{synced.DataUUID(), evt},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		}
	}
	return subs
}

// Store the cache then relay changes to any downstream instance.
// If we are getting these events, this particular data instance's goroutines
// should only be occupied processing the downsize events.
func (d *Data) downsizeCommit(v dvid.VersionID) {
	if d.vcache == nil {
		dvid.Criticalf("downsize commit for %q attempted when no prior blocks were sent!\n", d.DataName())
		return
	}
	bc, found := d.vcache[v]
	if !found {
		dvid.Criticalf("downsize commit for %q sent when no cache for version %d was present!\n", d.DataName(), v)
		return
	}

	// Allocate block buffer for writing so that it gets reused instead of reallocated over loop.
	blockSize := d.BlockSize()
	blockBytes := blockSize.Prod()
	blockData := make([]byte, blockBytes)

	// Do GET/PUT for each block, unless we have all 8 octants and can just do a PUT.
	// For each block, send to downstream if any.
	store, err := d.GetKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	for block, oct := range bc {
		tk := NewTKeyByCoord(block)

		// Are all 8 octants set?
		partial := false
		for _, data := range oct {
			if data == nil {
				partial = true
				break
			}
		}

		// If not, GET the previous block data for reintegration and insert into nil octants.
		if partial {
			serialization, err := store.Get(ctx, tk)
			if err != nil {
				dvid.Errorf("unable to get data for %q, block %s: %v\n", d.DataName(), block, err)
				continue
			}
			uncompress := true
			deserialized, _, err := dvid.DeserializeData(serialization, uncompress)
			if err != nil {
				dvid.Criticalf("Unable to deserialize data for %q, block %s: %v", d.DataName(), block, err)
				continue
			}
			copy(blockData, deserialized)
		}

		// Write the data.
		serialization, err := d.serializeOctants(oct, blockData)
		if err != nil {
			dvid.Errorf("unable to serialize octant data in %q, block %s: %v\n", d.DataName(), block, err)
			continue
		}
		if err := store.Put(ctx, tk, serialization); err != nil {
			dvid.Errorf("unable to write downsized data in %q, block %s: %v\n", d.DataName(), block, err)
			continue
		}
	}
}

// Handle upstream mods on a labelblk we are downresing.
func (d *Data) downsizeAdd(v dvid.VersionID, delta deltaBlock) {
	// initialize the cache
	var bc blockCache
	if d.vcache == nil {
		d.vcache = make(map[dvid.VersionID]blockCache)
	} else {
		var found bool
		bc, found = d.vcache[v]
		if !found {
			bc = nil
		}
	}
	if bc == nil {
		bc = make(blockCache)
		d.vcache[v] = bc
	}

	// Setup the octant buffer and block cache.
	downresBlock, err := delta.block.Downres()
	if err != nil {
		dvid.Criticalf("unable to downres labelblk %q block: %v\n", d.DataName(), err)
		return
	}
	oct := bc[downresBlock]
	sector, ox, oy, oz, err := d.getOctantInfo(delta.block)
	if err != nil {
		dvid.Criticalf("unable to interpret block in labelblk %q downres: %v\n", d.DataName(), err)
		return
	}
	blockSize := d.BlockSize()
	nbytes := blockSize.Value(0) * blockSize.Value(1) * blockSize.Value(2) // actually / 8 (downres 2^3) then * 8 bytes for label
	if oct[sector] == nil {
		oct[sector] = make([]byte, nbytes)
	}

	// Compute the downres for each group of 8 contiguous voxels by finding most frequent label.
	nx := blockSize.Value(0) * 8
	nxy := blockSize.Value(1) * nx
	dnx := nx >> 1
	dnxy := (blockSize.Value(1) >> 1) * dnx
	for z := oz; z < oz+blockSize.Value(2); z += 2 {
		for y := oy; y < oy+blockSize.Value(1); y += 2 {
			cornerOff := z*nxy + y*nx + ox*8             // byte offset into corner of higher-res block data
			off := cornerOff                             // we use this to move in the 8 voxel neighborhood
			doff := (z>>1)*dnxy + (y>>1)*dnx + (ox>>1)*8 // offset into lower-res block data
			for x := ox; x < ox+blockSize.Value(0); x += 2 {
				counts := make(map[uint64]int)

				label := binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off += 8
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off = off - 8 + nx
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off += 8
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				// next z neighbors
				off = off - 8 - nx + nxy
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off += 8
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off = off - 8 + nx
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				off += 8
				label = binary.LittleEndian.Uint64(delta.data[off : off+8])
				counts[label]++

				// get best label and if there's a tie use smaller label
				var most int
				var best uint64
				for label, count := range counts {
					if count > most {
						best = label
					} else if count == most && label < best {
						best = label
					}
				}

				// store into downres cache
				binary.LittleEndian.PutUint64(oct[sector][doff:doff+8], best)

				// Move to next corner of 8 block voxels
				doff += 8
				cornerOff += 16 // 2 * label byte size
				off = cornerOff
			}
		}
	}
}

// (ox, oy, oz) is the high-res voxel offset for the corner of the block.
func (d *Data) getOctantInfo(block dvid.IZYXString) (sector, ox, oy, oz int32, err error) {
	var chunkPt dvid.ChunkPoint3d
	chunkPt, err = block.ToChunkPoint3d()
	if err != nil {
		return
	}

	// determine which down-res sector (0-7 where it's x, then y, then z ordering) in 2x2x2 block
	// the given block will sit.
	nx := chunkPt[0] % 2
	ny := chunkPt[1] % 2
	nz := chunkPt[2] % 2
	sector = (nz >> 2) + (ny >> 1) + nx

	offset := chunkPt.MinPoint(d.BlockSize())
	ox = offset.Value(0)
	oy = offset.Value(1)
	oz = offset.Value(2)
	return
}

// gets all the changes relevant to labelblk, then breaks up any multi-block op into
// separate block ops and puts them onto channels to index-specific handlers.
func (d *Data) processEvents() {
	for {
		select {
		case <-d.syncDone:
			dvid.Infof("Received shutdown signal.  Closing goroutine for processing %q sync events...\n", d.DataName())
			return
		case msg := <-d.syncCh:
			if msg.Event == DownsizeCommitEvent {
				d.downsizeCommit(msg.Version)
				continue
			}

			switch delta := msg.Delta.(type) {
			case deltaBlock:
				d.downsizeAdd(msg.Version, delta)

			case labels.DeltaMergeStart:
				// Add this merge into the cached blockRLEs
				iv := dvid.InstanceVersion{d.DataUUID(), msg.Version}
				d.StartUpdate()
				labels.MergeStart(iv, delta.MergeOp)
				d.StopUpdate()

			case labels.DeltaMerge:
				d.processMerge(msg.Version, delta)

			case labels.DeltaSplit:
				d.processSplit(msg.Version, delta)

			/* TODO -- Use if we begin to handle labelblk POST for multiscale
			case imageblk.Block:
				n := delta.Index.Hash(numBlockHandlers)
				procCh[n] <- procMsg{op: blockOp{blockIngest, delta}, v: msg.Version}

			case imageblk.MutatedBlock:
				n := delta.Index.Hash(numBlockHandlers)
				procCh[n] <- procMsg{op: blockOp{blockMutate, delta}, v: msg.Version}

			case labels.DeleteBlock:
				n := delta.Index.Hash(numBlockHandlers)
				procCh[n] <- procMsg{op: blockOp{blockDelete, delta}, v: msg.Version}
			*/

			default:
			}
		}
	}
}

func (d *Data) savePotentialDownres(v dvid.VersionID) {
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: DownsizeCommitEvent}
	msg := datastore.SyncMessage{Event: DownsizeCommitEvent, Version: v}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

func (d *Data) processMerge(v dvid.VersionID, delta labels.DeltaMerge) {
	wg := new(sync.WaitGroup)
	for izyxStr := range delta.Blocks {
		n := getHandlerNum(izyxStr)
		wg.Add(1)
		op := mergeOp{MergeOp: delta.MergeOp, block: izyxStr}
		d.procCh[n] <- procMsg{op: op, v: v, wg: wg}
	}
	// When we've processed all the delta blocks, we can remove this merge op
	// from the merge cache since all labels will have completed.
	iv := dvid.InstanceVersion{d.DataUUID(), v}
	go func() {
		wg.Wait()
		labels.MergeStop(iv, delta.MergeOp)
		d.savePotentialDownres(v)
	}()
}

func (d *Data) processSplit(v dvid.VersionID, delta labels.DeltaSplit) {
	timedLog := dvid.NewTimeLog()
	splitOpStart := labels.DeltaSplitStart{delta.OldLabel, delta.NewLabel}
	splitOpEnd := labels.DeltaSplitEnd{delta.OldLabel, delta.NewLabel}
	iv := dvid.InstanceVersion{d.DataUUID(), v}
	labels.SplitStart(iv, splitOpStart)

	wg := new(sync.WaitGroup)
	d.StartUpdate()
	if delta.Split == nil {
		// Coarse Split
		for _, izyxStr := range delta.SortedBlocks {
			n := getHandlerNum(izyxStr)
			wg.Add(1)
			op := splitOp{
				oldLabel: delta.OldLabel,
				newLabel: delta.NewLabel,
				block:    izyxStr,
			}
			d.procCh[n] <- procMsg{op: op, v: v, wg: wg}
		}
	} else {
		// Fine Split
		for izyxStr, blockRLEs := range delta.Split {
			n := getHandlerNum(izyxStr)
			wg.Add(1)
			op := splitOp{
				oldLabel: delta.OldLabel,
				newLabel: delta.NewLabel,
				rles:     blockRLEs,
				block:    izyxStr,
			}
			d.procCh[n] <- procMsg{op: op, v: v, wg: wg}
		}
	}
	// Wait for all blocks to be split then mark end of split op.
	go func() {
		wg.Wait()
		labels.SplitStop(iv, splitOpEnd)
		timedLog.Debugf("labelblk sync complete for split of %d -> %d", delta.OldLabel, delta.NewLabel)
		d.StopUpdate()
		d.savePotentialDownres(v)
	}()
}

func getHandlerNum(s dvid.IZYXString) int {
	hash := fnv.New32()
	_, err := hash.Write([]byte(s))
	if err != nil {
		dvid.Criticalf("Could not write to fnv hash in labelblk.hashStr()")
		return 0
	}
	return int(hash.Sum32()) % numBlockHandlers
}

// Handles a stream of block operations for a unique shard of block coordinates.
// Since the same block coordinate always gets mapped to the same goroutine we can
// do a GET/PUT without worrying about interleaving PUT from other goroutines, as
// long as there is only one DVID server.
func (d *Data) processBlock(ch <-chan procMsg) {
	for msg := range ch {
		ctx := datastore.NewVersionedCtx(d, msg.v)
		switch op := msg.op.(type) {
		case mergeOp:
			d.mergeBlock(ctx, op)
			msg.wg.Done()

		case splitOp:
			d.splitBlock(ctx, op)
			msg.wg.Done()

		default:
			dvid.Criticalf("Received unknown processing msg in processBlock: %v\n", msg)
		}
	}
}

// Notify any downstream downres instance of block change.
func (d *Data) publishBlockChange(v dvid.VersionID, block dvid.IZYXString, blockData []byte) {
	evt := datastore.SyncEvent{d.DataUUID(), DownsizeBlockEvent}
	delta := deltaBlock{
		block: block,
		data:  blockData,
	}
	msg := datastore.SyncMessage{DownsizeBlockEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

// handles relabeling of blocks during a merge operation.
func (d *Data) mergeBlock(ctx *datastore.VersionedCtx, op mergeOp) {
	store, err := d.GetKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}

	tk := NewTKeyByCoord(op.block)
	data, err := store.Get(ctx, tk)
	if err != nil {
		dvid.Errorf("Error on GET of labelblk with coord string %q\n", op.block)
		return
	}
	if data == nil {
		dvid.Errorf("nil label block where merge was done!\n")
		return
	}

	blockData, _, err := dvid.DeserializeData(data, true)
	if err != nil {
		dvid.Criticalf("unable to deserialize label block in '%s': %v\n", d.DataName(), err)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)
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
		dvid.Criticalf("Unable to serialize block in %q: %v\n", d.DataName(), err)
		return
	}
	if err := store.Put(ctx, tk, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %v\n", tk, err)
	}

	// Notify any downstream downres instance.
	d.publishBlockChange(ctx.VersionID(), op.block, blockData)
}

// Goroutine that handles splits across a lot of blocks for one label.
func (d *Data) splitBlock(ctx *datastore.VersionedCtx, op splitOp) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labelblk had error initializing store: %v\n", err)
		return
	}

	// Read the block.
	tk := NewTKeyByCoord(op.block)
	data, err := store.Get(ctx, tk)
	if err != nil {
		dvid.Errorf("Error on GET of labelblk with coord string %v\n", []byte(op.block))
		return
	}
	if data == nil {
		dvid.Errorf("nil label block where split was done, coord %v\n", []byte(op.block))
		return
	}
	blockData, _, err := dvid.DeserializeData(data, true)
	if err != nil {
		dvid.Criticalf("unable to deserialize label block in '%s' key %v: %v\n", d.DataName(), []byte(op.block), err)
		return
	}
	blockBytes := int(d.BlockSize().Prod() * 8)
	if len(blockData) != blockBytes {
		dvid.Criticalf("splitBlock: coord %v got back %d bytes, expected %d bytes\n", []byte(op.block), len(blockData), blockBytes)
		return
	}

	// Modify the block using either voxel-level changes or coarser block-level mods.
	if op.rles != nil {
		if err := d.storeRLEs(blockData, op.newLabel, op.block, op.rles); err != nil {
			dvid.Errorf("can't store label %d RLEs into block %s: %v\n", op.newLabel, op.block, err)
			return
		}
	} else {
		// We are doing coarse split and will replace all
		if err := d.replaceLabel(blockData, op.oldLabel, op.newLabel); err != nil {
			dvid.Errorf("can't replace label %d with %d in block %s: %v\n", op.oldLabel, op.newLabel, op.block, err)
			return
		}
	}

	// Write the modified block.
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Criticalf("Unable to serialize block %s in %q: %v\n", op.block, d.DataName(), err)
		return
	}
	if err := store.Put(ctx, tk, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %v\n", tk, err)
	}

	// Notify any downstream downres instance.
	d.publishBlockChange(ctx.VersionID(), op.block, blockData)
}

// Replace a label in a block.
func (d *Data) replaceLabel(data []byte, fromLabel, toLabel uint64) error {
	n := len(data)
	if n%8 != 0 {
		return fmt.Errorf("label data in block not aligned to uint64: %d bytes", n)
	}
	for i := 0; i < n; i += 8 {
		label := binary.LittleEndian.Uint64(data[i : i+8])
		if label == fromLabel {
			binary.LittleEndian.PutUint64(data[i:i+8], toLabel)
		}
	}
	return nil
}

// Store a label into a block using RLEs.
func (d *Data) storeRLEs(data []byte, toLabel uint64, zyxStr dvid.IZYXString, rles dvid.RLEs) error {
	// Get the block coordinate
	bcoord, err := zyxStr.ToChunkPoint3d()
	if err != nil {
		return err
	}

	// Get the first voxel offset
	blockSize := d.BlockSize()
	offset := bcoord.MinPoint(blockSize)

	// Iterate through rles, getting span for this block of bytes.
	nx := blockSize.Value(0) * 8
	nxy := nx * blockSize.Value(1)
	for _, rle := range rles {
		p := rle.StartPt().Sub(offset)
		i := p.Value(2)*nxy + p.Value(1)*nx + p.Value(0)*8
		for n := int32(0); n < rle.Length(); n++ {
			binary.LittleEndian.PutUint64(data[i:i+8], toLabel)
			i += 8
		}
	}
	return nil
}
