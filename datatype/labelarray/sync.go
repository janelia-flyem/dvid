/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelarray

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
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
	mutID      uint64
	bcoord     dvid.IZYXString
	downresMut *downres.Mutation
}

type splitOp struct {
	labels.SplitOp
	mutID       uint64
	bcoord      dvid.IZYXString
	deleteBlkCh chan dvid.IZYXString
	downresMut  *downres.Mutation
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
