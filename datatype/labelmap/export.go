package labelmap

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// ExportData exports the labelmap data via Arrow Flight to python clients already running and connected
// to the DVID server's Arrow Flight server port. See python worker code in the "export" directory
// of the DVID repository.
func (d *Data) ExportData(ctx *datastore.VersionedCtx, w http.ResponseWriter) {
	// POST <api URL>/node/<UUID>/<data name>/export

	// Start the process to read blocks from storage and send them to the workers.
	go d.readBlocks(ctx)

	comment := fmt.Sprintf("Exporting labelmap %q data to workers via Arrow Flight server...\n", d.DataName())
	dvid.Infof(comment)

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, comment)
}

// readBlocks reads blocks from the labelmap data and sends them to the blocks channel.
func (d *Data) readBlocks(ctx *datastore.VersionedCtx) {
	// Read blocks from the labelmap data and send them to the blocks channel.
	// This is a long-running operation, so it should not block the main thread.
	timedLog := dvid.NewTimeLog()

	// Get the number of workers that are connected to the Arrow Flight server via DoExchange.
	// TODO: Contact the Arrow Flight server to get the number of workers.
	workers, err := d.GetArrowFlightWorkers(ctx)
	if err != nil {
		dvid.Errorf("Error getting Arrow Flight workers for data %q: %v\n", d.DataName(), err)
		return
	}
	if workers == 0 {
		dvid.Errorf("No Arrow Flight workers connected for data %q. Cannot export data.\n", d.DataName())
		return
	}

	wg := new(sync.WaitGroup)
	chunkCh := make(chan *storage.Chunk, 100) // Buffered channel to hold chunks read from storage.
	for i := 0; i < len(workers); i++ {
		wg.Add(1)
		go d.exportWorker(wg, chunkCh)
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("export from %q had error initializing store: %v", d.DataName(), err)
		return
	}
	begTKey := NewBlockTKeyByCoord(0, dvid.MinIndexZYX.ToIZYXString())
	endTKey := NewBlockTKeyByCoord(0, dvid.MaxIndexZYX.ToIZYXString())
	var numBlocks uint64
	chunkOp := &storage.ChunkOp{Wg: wg}

	// Go through all labelmap blocks and send them to the workers.
	// TODO: Block keys are in ZYX order, but we will compute the morton code of block coordinates
	// and shard id, using the latter to determine which worker to send the block to.
	// Worker = shard ID % num workers.
	// VastDB indexing order will be (shard ID, morton code).
	err = store.ProcessRange(ctx, begTKey, endTKey, chunkOp, func(c *storage.Chunk) error {
		if c == nil {
			wg.Done()
			return fmt.Errorf("received nil chunk in count for data %q", d.DataName())
		}
		if c.V == nil {
			wg.Done()
			return nil
		}
		numBlocks++
		if numBlocks%10000 == 0 {
			timedLog.Infof("Read up to block %d with chunk channel at %d", numBlocks, len(chunkCh))
		}
		chunkCh <- c
		return nil
	})
	if err != nil {
		dvid.Errorf("problem during process range: %v\n", err)
	}
	close(chunkCh)
	wg.Wait()
	if err != nil {
		dvid.Errorf("Error in reading labelmap %q blocks: %v\n", d.DataName(), err)
	}

	timedLog.Infof("Finished reading labelmap %q %d blocks to processing workers", d.DataName(), numBlocks)
}

// exportWorker processes blocks read from the labelmap data and sends them to the workers via Arrow Flight server.
func (d *Data) exportWorker(wg *sync.WaitGroup, chunkCh chan *storage.Chunk) {
	for c := range chunkCh {
		scale, idx, err := DecodeBlockTKey(c.K)
		if err != nil {
			dvid.Errorf("Couldn't decode label block key %v for data %q\n", c.K, d.DataName())
			wg.Done()
			continue
		}
		if scale != 0 {
			dvid.Errorf("Counts had unexpected error: getting scale %d blocks\n", scale)
			wg.Done()
			continue
		}
		var data []byte
		data, _, err = dvid.DeserializeData(c.V, true)
		if err != nil {
			dvid.Errorf("Unable to deserialize block %s in data %q: %v\n", idx, d.DataName(), err)
			wg.Done()
			continue
		}
		var block labels.Block
		if err := block.UnmarshalBinary(data); err != nil {
			dvid.Errorf("Unable to unmarshal Block %s in data %q: %v\n", idx, d.DataName(), err)
			wg.Done()
			continue
		}

		// Transform block data into Arrow with supervoxel and label index followed by
		// the recompressed label data using zstd into a format suitable for Arrow Flight.

		// Send the data down channel to the python workers.
		// ...
		wg.Done()
	}
}
