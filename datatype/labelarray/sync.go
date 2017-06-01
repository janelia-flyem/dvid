/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelarray

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numBlockHandlers = 8 // goroutines used to process block changes
	numLabelHandlers = 8 // goroutines used to do get/put tx on label indices

	DownsizeBlockEvent  = "LABELS64_DOWNSIZE_ADD"
	DownsizeCommitEvent = "LABELS64_DOWNSIZE_COMMIT"

	IngestBlockEvent = "LABELS64_BLOCK_INGEST"
	MutateBlockEvent = "LABELS64_BLOCK_MUTATE"
	DeleteBlockEvent = "LABELS64_BLOCK_DELETE"
)

// IngestedBlock is the unit of delta for a IngestBlockEvent.
type IngestedBlock struct {
	MutID  uint64
	BCoord dvid.IZYXString
	Data   *labels.Block
}

// MutatedBlock tracks previous and updated block data.
// It is the unit of delta for a MutateBlockEvent.
type MutatedBlock struct {
	MutID  uint64
	BCoord dvid.IZYXString
	Prev   *labels.Block
	Data   *labels.Block
}

type procMsg struct {
	v  dvid.VersionID
	op interface{}
}

type downresOp struct {
	mutID  uint64
	bcoord dvid.IZYXString // the high-res block coord corresponding to the Block
	block  *labels.Block
}

type mergeOp struct {
	labels.MergeOp
	mutID  uint64
	bcoord dvid.IZYXString
}

type splitOp struct {
	labels.SplitOp
	mutID       uint64
	bcoord      dvid.IZYXString
	deleteBlkCh chan dvid.IZYXString
}

func (d *Data) publishDownsizeBlock(v dvid.VersionID, mutID uint64, bcoord dvid.IZYXString, block *labels.Block) error {
	evt := datastore.SyncEvent{d.DataUUID(), DownsizeBlockEvent}
	msg := datastore.SyncMessage{DownsizeBlockEvent, v, downresOp{mutID, bcoord, block}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return fmt.Errorf("can't publish downsize block %s event: %v\n", bcoord, err)
	}
	return nil
}

func (d *Data) publishDownsizeCommit(v dvid.VersionID, mutID uint64) error {
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: DownsizeCommitEvent}
	msg := datastore.SyncMessage{Event: DownsizeCommitEvent, Version: v, Delta: mutID}
	return datastore.NotifySubscribers(evt, msg)
}

// cache of all blocks modified where the ZYX index is the lower-res
// (down-res by 2x) coordinate.
type blockCache struct {
	cache map[dvid.IZYXString]*labels.Block
	sync.RWMutex
}

type mutationCache struct {
	cache map[uint64]blockCache
	sync.RWMutex
}

// Returns Block to which any down-resolution data should be written for a higher-res Block coord.
func (d *Data) getDownresBlock(mutID uint64, hiresIndex dvid.IZYXString) (*labels.Block, error) {
	loresIndex, err := hiresIndex.Halfres()
	if err != nil {
		return nil, fmt.Errorf("unable to downres labelarray %q block %d: %v\n", d.DataName(), hiresIndex, err)
	}

	bc := d.getDownresCache(mutID)

	bc.Lock()
	block, found := bc.cache[loresIndex]
	if !found {
		block = new(labels.Block)
		bc.cache[loresIndex] = block
	}
	bc.Unlock()

	return block, nil
}

// returns entire block cache associated with a mutation operation, possibly creating one if none exists.
func (d *Data) getDownresCache(mutID uint64) blockCache {
	d.mutcache.Lock()
	defer d.mutcache.Unlock()

	// handle blockCache for each mutation of this data
	if d.mutcache.cache == nil {
		d.mutcache.cache = make(map[uint64]blockCache)
	}
	bc, found := d.mutcache.cache[mutID]
	if !found {
		bc.cache = make(map[dvid.IZYXString]*labels.Block)
		d.mutcache.cache[mutID] = bc
	}
	return bc
}

func (d *Data) deleteDownresCache(mutID uint64) {
	d.mutcache.Lock()
	delete(d.mutcache.cache, mutID)
	d.mutcache.Unlock()
}

// InitDataHandlers launches goroutines to handle each labelarray instance's syncs.
func (d *Data) InitDataHandlers() error {
	if d.syncCh != nil || d.syncDone != nil {
		return nil
	}
	d.syncCh = make(chan datastore.SyncMessage, 100)
	d.syncDone = make(chan *sync.WaitGroup)

	// Start N goroutines to process mutations for each block that will be consistently
	// assigned to one of the N goroutines.
	for i := 0; i < numBlockHandlers; i++ {
		d.mutateCh[i] = make(chan procMsg, 10)
		go d.mutateBlock(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		d.indexCh[i] = make(chan labelChange, 100)
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
	case "labelarray":
		evts = []string{
			DownsizeBlockEvent, DownsizeCommitEvent,
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

// gets all the changes relevant to labelarray, then breaks up any multi-block op into
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
			case DownsizeBlockEvent:
				op := msg.Delta.(downresOp)
				if d.MutAdd(op.mutID) {
					d.StartUpdate()
				}
				go d.downsizeAdd(op)

			case DownsizeCommitEvent:
				mutID := msg.Delta.(uint64)
				go func(v dvid.VersionID, mutID uint64) {
					d.downsizeCommit(v, mutID) // async since we will wait on any in waitgroup.
					d.StopUpdate()
				}(msg.Version, mutID)
			}

			if stop && len(d.syncCh) == 0 {
				dvid.Infof("Shutting down sync even handler for instance %q after draining sync events.\n", d.DataName())
				wg.Done()
				return
			}
		}
	}
}

// Handle upstream mods on a labelarray we are downresing.
func (d *Data) downsizeAdd(op downresOp) {
	defer d.MutDone(op.mutID)

	loresBlock, err := d.getDownresBlock(op.mutID, op.bcoord)
	if err != nil {
		dvid.Criticalf("unable to initialize block cache for labelarray %q: %v\n", d.DataName(), err)
		return
	}

	if err := loresBlock.ModifyHighres(op.bcoord, op.block); err != nil {
		dvid.Criticalf("unable to modify lowres block after change of higher res one %s: %v\n", op.bcoord, err)
		return
	}
}

// Store the cache then relay changes to any downstream instance.
// If we are getting these events, this particular data instance's goroutines
// should only be occupied processing the downsize events.
func (d *Data) downsizeCommit(v dvid.VersionID, mutID uint64) {
	// block until we have all of the operation completed.
	d.MutWait(mutID)
	d.MutDelete(mutID)

	bc := d.getDownresCache(mutID)
	bc.Lock() // we should be last function using this blockCache.
	defer func() {
		bc.cache = nil
		bc.Unlock()
		d.deleteDownresCache(mutID)
	}()

	ctx := datastore.NewVersionedCtx(d, v)
	for downresBCoord, newBlock := range bc.cache {
		oldBlock, err := d.getLabelBlock(ctx, downresBCoord)
		if err != nil {
			dvid.Errorf("unable to get %q label block %s: %v\n", d.DataName(), downresBCoord, err)
			continue
		}

		if err := newBlock.FillUninitialized(&(oldBlock.Block)); err != nil {
			dvid.Errorf("unable to fill uninitialized portions of block %s in data %q: %v\n", downresBCoord, d.DataName(), err)
			continue
		}

		// Write the data.
		pb := labels.PositionedBlock{Block: *newBlock, BCoord: downresBCoord}
		if err := d.putLabelBlock(ctx, &pb); err != nil {
			dvid.Errorf("unable to PUT downres block %s for data %q: %v\n", downresBCoord, d.DataName(), err)
			continue
		}

		// Notify any downstream downres that we've just modified a block at this level.
		evt := datastore.SyncEvent{d.DataUUID(), DownsizeBlockEvent}
		msg := datastore.SyncMessage{DownsizeBlockEvent, v, downresOp{mutID, downresBCoord, newBlock}}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Criticalf("unable to notify subscribers of event %s: %v\n", evt, err)
		}
	}

	// Notify and downstream downres that we're done and can commit.
	if err := d.publishDownsizeCommit(v, mutID); err != nil {
		dvid.Criticalf("error publishing downsize commit data %q: %v\n", d.DataName(), err)
	}
}
