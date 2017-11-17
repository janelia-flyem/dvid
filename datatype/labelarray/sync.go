/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelarray

import (
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numMutateHandlers = 16  // goroutines used to process mutations on blocks
	numLabelHandlers  = 256 // goroutines used to do get/put tx on label indices

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
	if d.mutateCh[0] != nil {
		return nil
	}

	// Start N goroutines to process mutations for each block that will be consistently
	// assigned to one of the N goroutines.
	for i := 0; i < numMutateHandlers; i++ {
		d.mutateCh[i] = make(chan procMsg, 10)
		go d.mutateBlock(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		d.indexCh[i] = make(chan labelChange, 100)
		go d.indexLabels(d.indexCh[i])
	}

	dvid.Infof("Launched mutation handlers for data %q...\n", d.DataName())
	return nil
}

func (d *Data) queuedSize() int {
	var queued int
	for i := 0; i < numMutateHandlers; i++ {
		queued += len(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		queued += len(d.indexCh[i])
	}
	return queued
}

// Shutdown terminates blocks until syncs are done then terminates background goroutines processing data.
func (d *Data) Shutdown(wg *sync.WaitGroup) {
	var elapsed int
	for {
		queued := d.queuedSize()
		if queued > 0 {
			if elapsed >= datastore.DataShutdownTime {
				dvid.Infof("Timed out after %d seconds waiting for data %q mutations: %d still to be processed", elapsed, d.DataName(), queued)
				break
			}
			dvid.Infof("After %d seconds, data %q has %d mutations in queue pending.", elapsed, d.DataName(), queued)
			time.Sleep(1 * time.Second)
			elapsed++
		} else {
			break
		}
	}
	for i := 0; i < numMutateHandlers; i++ {
		close(d.mutateCh[i])
	}
	for i := 0; i < numLabelHandlers; i++ {
		close(d.indexCh[i])
	}
	hitrate := (float64(d.metaHits) / float64(d.metaAttempts)) * 100.0
	dvid.Infof("Closing index handler for data %q: got %d meta cache hits on %d attempts (%5.2f)\n", d.DataName(), d.metaHits, d.metaAttempts, hitrate)
	wg.Done()
}
