/*
	This file supports interactive syncing between data instances.  It is different
	from ingestion syncs that can more effectively batch changes.
*/

package labelmap

import (
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	numLabelHandlers = 256 // goroutines used to do get/put tx on label indices
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

type splitSupervoxelOp struct {
	labels.SplitSupervoxelOp
	mutID      uint64
	bcoord     dvid.IZYXString
	downresMut *downres.Mutation
}

// InitDataHandlers launches goroutines to handle each labelmap instance's syncs.
func (d *Data) InitDataHandlers() error {
	return nil
}

// Shutdown terminates blocks until syncs are done then terminates background goroutines processing data.
func (d *Data) Shutdown(wg *sync.WaitGroup) {
	var elapsed int
	for {
		if d.Updating() {
			if elapsed >= datastore.DataShutdownTime {
				dvid.Infof("Timed out after %d seconds waiting for data %q updating", elapsed, d.DataName())
				break
			}
			dvid.Infof("After %d seconds, data %q is still updating", elapsed, d.DataName())
			time.Sleep(1 * time.Second)
			elapsed++
		} else {
			break
		}
	}
	if indexCache != nil {
		var hitrate float64
		if metaAttempts > 0 {
			hitrate = (float64(metaHits) / float64(metaAttempts)) * 100.0
		}
		dvid.Infof("Cache for data %q: got %d meta cache hits on %d attempts (%5.2f)\n", d.DataName(), metaHits, metaAttempts, hitrate)
	}
	wg.Done()
}
