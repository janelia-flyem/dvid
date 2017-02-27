/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labels64

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numBlockHandlers = 8 // goroutines used to process block changes
	numLabelHandlers = 8 // goroutines used to do get/put tx on label indices

	DownsizeBlockEvent  = "LABELBLK_DOWNSIZE_ADD"
	DownsizeCommitEvent = "LABELBLK_DOWNSIZE_COMMIT"
)

type procMsg struct {
	v  dvid.VersionID
	op interface{}
}

type downsizeOp struct {
	mutID uint64
	block dvid.IZYXString // block coordinate of the originating labels64 resolution.
	data  []byte
}

type mergeOp struct {
	mutID uint64
	labels.MergeOp
	block dvid.IZYXString
}

type splitOp struct {
	mutID       uint64
	oldLabel    uint64
	newLabel    uint64
	rles        dvid.RLEs
	block       dvid.IZYXString
	deleteBlkCh chan dvid.IZYXString
}

type octant [8][]byte // octant has nil []byte if not modified.

// cache of all blocks modified where the ZYX index is the lower-res
// (down-res by 2x) coordinate.
type blockCache map[dvid.IZYXString]octant

// Returns the slice to which any down-resolution data should be written for the given higher-res block coord.
func (d *Data) getLoresCache(v dvid.VersionID, block dvid.IZYXString) ([]byte, error) {
	// Setup the octant buffer and block cache.
	downresBlock, err := block.Downres()
	if err != nil {
		return nil, fmt.Errorf("unable to downres labels64 %q block: %v\n", d.DataName(), err)
	}

	var chunkPt dvid.ChunkPoint3d
	chunkPt, err = block.ToChunkPoint3d()
	if err != nil {
		return nil, err
	}

	// determine which down-res sector (0-7 where it's x, then y, then z ordering) in 2x2x2 block
	// the given block will sit.
	nx := chunkPt[0] % 2
	ny := chunkPt[1] % 2
	nz := chunkPt[2] % 2
	sector := (nz * 4) + (ny * 2) + nx

	// Get the sector slice from the octant corresponding to the downres block coord.
	// Initialize blockCache if necessary.
	d.vcacheMu.Lock()
	defer d.vcacheMu.Unlock()

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

	// Get the relevant slice.
	oct := bc[downresBlock]
	if oct[sector] == nil {
		nbytes := d.BlockSize().Prod() // actually / 8 (downres 2^3) then * 8 bytes for label
		oct[sector] = make([]byte, nbytes)
	}
	d.vcache[v][downresBlock] = oct
	return oct[sector], nil
}

func (d *Data) getReadOnlyBlockCache(v dvid.VersionID) (blockCache, error) {
	if d.vcache == nil {
		return nil, fmt.Errorf("downsize commit for %q attempted when no prior blocks were sent!\n", d.DataName())
	}

	bc, found := d.vcache[v]
	if !found {
		return nil, fmt.Errorf("downsize commit for %q sent when no cache for version %d was present!\n", d.DataName(), v)
	}
	return bc, nil
}

// serializes octant contents.  Writes octants into preallocated block buffer that may have old data,
// and then returns a serialized data slice suitable for storage.
func (d *Data) serializeOctants(oct octant, blockBuf []byte) ([]byte, error) {
	blockSize := d.BlockSize()
	nx := blockSize.Value(0)
	nxy := blockSize.Value(1) * nx

	halfx := blockSize.Value(0) >> 1
	halfy := blockSize.Value(1) >> 1
	halfz := blockSize.Value(2) >> 1
	sectorbytes := int(halfx * halfy * halfz * 8)
	xbytes := halfx * 8

	for sector, data := range oct {
		if len(data) > 0 {
			if len(data) != sectorbytes {
				dvid.Criticalf("Expected %d bytes in octant for %d, instead got %d bytes.\n", sectorbytes, d.DataName(), len(data))
			}
			// Get the corner voxel (in block coordinates) for this sector.
			iz := sector >> 2
			sector -= iz * 4
			iy := sector >> 1
			ix := sector % 2

			ox := int32(ix) * halfx
			oy := int32(iy) * halfy
			oz := int32(iz) * halfz

			// Copy data from octant into larger block buffer.
			var oi int32
			for z := oz; z < oz+halfz; z++ {
				for y := oy; y < oy+halfy; y++ {
					di := (z*nxy + y*nx + ox) * 8
					copy(blockBuf[di:di+xbytes], data[oi:oi+xbytes])
					oi += xbytes
				}
			}
		}
	}

	return dvid.SerializeData(blockBuf, d.Compression(), d.Checksum())
}

// InitDataHandlers launches goroutines to handle each labels64 instance's syncs.
func (d *Data) InitDataHandlers() error {
	if d.syncCh != nil || d.syncDone != nil {
		return nil
	}
	d.syncCh = make(chan datastore.SyncMessage, 100)
	d.syncDone = make(chan *sync.WaitGroup)

	// Start N goroutines to process mutations for each block that will be consistently
	// assigned to one of the N goroutines.
	for i := 0; i < numBlockHandlers; i++ {
		d.downresCh[i] = make(chan procMsg, 8)
		go d.downresBlock(d.downresCh[i])
		d.mutateCh[i] = make(chan procMsg, 10)
		go d.mutateBlock(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		d.indexCh[i] = make(chan labelChange, 10)
		go d.indexLabels(d.indexCh[i])
	}

	dvid.Infof("Launching sync event handler for data %q...\n", d.DataName())
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
	for i := 0; i < numBlockHandlers; i++ {
		close(d.downresCh[i])
		close(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		close(d.indexCh[i])
	}
}

// GetSyncSubs implements the datastore.Syncer interface
func (d *Data) GetSyncSubs(synced dvid.Data) (datastore.SyncSubs, error) {
	if d.syncCh == nil {
		if err := d.InitDataHandlers(); err != nil {
			return nil, fmt.Errorf("unable to initialize handlers for data %q: %v\n", d.DataName(), err)
		}
	}

	var evts []string
	switch synced.TypeName() {
	case "labels64":
		evts = []string{
			// For down-res support
			DownsizeBlockEvent, DownsizeCommitEvent,
			labels.IngestBlockEvent, labels.MutateBlockEvent, labels.DeleteBlockEvent,
		}
	default:
		return nil, fmt.Errorf("Unable to sync %s with %s since datatype %q is not supported.", d.DataName(), synced.DataName(), synced.TypeName())
	}

	subs := make(datastore.SyncSubs, len(evts))
	for i, evt := range evts {
		subs[i] = datastore.SyncSub{
			Event:  datastore.SyncEvent{synced.DataUUID(), evt},
			Notify: d.DataUUID(),
			Ch:     d.syncCh,
		}
	}
	return subs, nil
}

// gets all the changes relevant to labels64, then breaks up any multi-block op into
// separate block ops and puts them onto channels to index-specific handlers.
func (d *Data) processEvents() {
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
			switch msg.Event {
			case DownsizeCommitEvent:
				mutID := msg.Delta.(uint64)
				go func(v dvid.VersionID, mutID uint64) {
					d.downsizeCommit(v, mutID) // async since we will wait on any in waitgroup.
					d.StopUpdate()
				}(msg.Version, mutID)

			default:
				d.handleEvent(msg)
			}

			if stop && len(d.syncCh) == 0 {
				dvid.Infof("Shutting down sync even handler for instance %q after draining sync events.\n", d.DataName())
				wg.Done()
				return
			}
		}
	}
}

func (d *Data) handleEvent(msg datastore.SyncMessage) {
	switch delta := msg.Delta.(type) {
	case downsizeOp: // received downres processing from upstream
		// NOTE: need to add wait here since there will be delay through channel compared to commit event.
		if d.MutAdd(delta.mutID) {
			d.StartUpdate() // stopped when the upstream instance issues a DownsizeCommitEvent: see processEvents()
		}
		n := delta.block.Hash(numBlockHandlers)
		d.downresCh[n] <- procMsg{op: delta, v: msg.Version}

	case imageblk.Block:
		if d.MutAdd(delta.MutID) {
			d.StartUpdate()
		}
		n := delta.Index.Hash(numBlockHandlers)
		block := delta.Index.ToIZYXString()
		d.downresCh[n] <- procMsg{op: downsizeOp{delta.MutID, block, delta.Data}, v: msg.Version}

	case imageblk.MutatedBlock:
		if d.MutAdd(delta.MutID) {
			d.StartUpdate()
		}
		n := delta.Index.Hash(numBlockHandlers)
		block := delta.Index.ToIZYXString()
		d.downresCh[n] <- procMsg{op: downsizeOp{delta.MutID, block, delta.Data}, v: msg.Version}

	case labels.DeleteBlock:
		if d.MutAdd(delta.MutID) {
			d.StartUpdate()
		}
		n := delta.Index.Hash(numBlockHandlers)
		block := delta.Index.ToIZYXString()
		d.downresCh[n] <- procMsg{op: downsizeOp{delta.MutID, block, nil}, v: msg.Version}

	default:
		dvid.Criticalf("Received unknown delta in labels64.processEvents(): %v\n", msg)
	}
}

func (d *Data) publishDownresCommit(v dvid.VersionID, mutID uint64) {
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: DownsizeCommitEvent}
	msg := datastore.SyncMessage{Event: DownsizeCommitEvent, Version: v, Delta: mutID}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

// Notify any downstream downres instance of block change.
func (d *Data) publishBlockChange(v dvid.VersionID, mutID uint64, block dvid.IZYXString, blockData []byte) {
	evt := datastore.SyncEvent{d.DataUUID(), DownsizeBlockEvent}
	delta := downsizeOp{
		mutID: mutID,
		block: block,
		data:  blockData,
	}
	msg := datastore.SyncMessage{DownsizeBlockEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
	}
}

// Handles a stream of block operations for a unique shard of block coordinates.
// Since the same block coordinate always gets mapped to the same goroutine we can
// do a GET/PUT without worrying about interleaving PUT from other goroutines, as
// long as there is only one DVID server.
func (d *Data) downresBlock(ch <-chan procMsg) {
	for {
		msg, more := <-ch
		if !more {
			return
		}
		switch op := msg.op.(type) {
		case downsizeOp:
			d.downsizeAdd(msg.v, op)

		default:
			dvid.Criticalf("Received unknown processing msg in processBlock: %v\n", msg)
		}
	}
}

// Store the cache then relay changes to any downstream instance.
// If we are getting these events, this particular data instance's goroutines
// should only be occupied processing the downsize events.
func (d *Data) downsizeCommit(v dvid.VersionID, mutID uint64) {
	// block until we have all of the operation completed.
	d.MutWait(mutID)
	d.MutDelete(mutID)

	d.vcacheMu.RLock()
	defer d.vcacheMu.RUnlock()

	bc, err := d.getReadOnlyBlockCache(v)
	if err != nil {
		dvid.Criticalf("downsize commit for %q: %v\n", d.DataName(), err)
	}

	// Allocate block buffer for writing so that it gets reused instead of reallocated over loop.
	blockSize := d.BlockSize()
	blockBytes := blockSize.Prod() * 8

	// Do GET/PUT for each block, unless we have all 8 octants and can just do a PUT.
	// For each block, send to downstream if any.
	store, err := d.GetKeyValueDB()
	if err != nil {
		dvid.Errorf("Data type labels64 had error initializing store: %v\n", err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	for downresBlock, oct := range bc {
		blockData := make([]byte, blockBytes)
		tk := NewBlockTKeyByCoord(downresBlock)

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
				dvid.Errorf("unable to get data for %q, block %s: %v\n", d.DataName(), downresBlock, err)
				continue
			}
			uncompress := true
			compressed, _, err := dvid.DeserializeData(serialization, uncompress)
			if err != nil {
				dvid.Criticalf("Unable to deserialize data for %q, block %s: %v", d.DataName(), downresBlock, err)
				continue
			}
			deserialized, err := labels.Decompress(compressed, d.BlockSize())
			if err != nil {
				dvid.Errorf("Unable to decompress google compression in %q: %v\n", d.DataName(), err)
				return
			}
			copy(blockData, deserialized)
		}

		// Write the data.
		serialization, err := d.serializeOctants(oct, blockData)
		if err != nil {
			dvid.Errorf("unable to serialize octant data in %q, block %s: %v\n", d.DataName(), downresBlock, err)
			continue
		}

		if err := store.Put(ctx, tk, serialization); err != nil {
			dvid.Errorf("unable to write downsized data in %q, block %s: %v\n", d.DataName(), downresBlock, err)
			continue
		}

		// Notify any downstream downres that we've just modified a block at this level.
		d.publishBlockChange(v, mutID, downresBlock, blockData)
	}

	// Notify and downstream downres that we're done and can commit.
	d.publishDownresCommit(v, mutID)
}

// Handle upstream mods on a labels64 we are downresing.
func (d *Data) downsizeAdd(v dvid.VersionID, delta downsizeOp) {
	defer d.MutDone(delta.mutID)

	lobuf, err := d.getLoresCache(v, delta.block)
	if err != nil {
		dvid.Criticalf("unable to initialize block cache for labels64 %q: %v\n", d.DataName(), err)
		return
	}

	// Offsets from corner of 2x2x2 voxel neighborhood to neighbor in highres block.
	blockSize := d.BlockSize()
	bx := blockSize.Value(0) * 8
	bxy := blockSize.Value(1) * bx

	var off [8]int32
	off[0] = 0
	off[1] = 8
	off[2] = bx
	off[3] = bx + 8
	off[4] = bxy
	off[5] = bxy + off[1]
	off[6] = bxy + off[2]
	off[7] = bxy + off[3]

	var lo int32 // lores byte offset
	for z := int32(0); z < blockSize.Value(2); z += 2 {
		for y := int32(0); y < blockSize.Value(1); y += 2 {
			hi := z*bxy + y*bx // hires byte offset to 2^3 neighborhood corner
			for x := int32(0); x < blockSize.Value(0); x += 2 {
				counts := make(map[uint64]int)
				for n := 0; n < 8; n++ {
					i := hi + off[n]
					label := binary.LittleEndian.Uint64(delta.data[i : i+8])
					counts[label]++
				}

				// get best label and if there's a tie use smaller label
				var most int
				var best uint64
				for label, count := range counts {
					if count > most {
						best = label
						most = count
					} else if count == most && label > best {
						best = label
					}
				}

				// store into downres cache
				//dvid.Infof("Data %q: best %d for (%d,%d,%d)\n", d.DataName(), best, x/2, y/2, z/2)
				binary.LittleEndian.PutUint64(lobuf[lo:lo+8], best)

				// Move to next corner of 8 block voxels
				lo += 8
				hi += 16 // 2 * label byte size
			}
		}
	}
}
