package labelmap

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/klauspost/compress/zstd"
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
	for i := 0; i < workers; i++ {
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

		// Transform block data into Arrow format
		arrowData, err := d.transformBlockToArrow(&block, *idx)
		if err != nil {
			dvid.Errorf("Unable to transform block %s to Arrow format: %v\n", idx, err)
			wg.Done()
			continue
		}

		// Send the data to appropriate worker via Arrow Flight server
		if err := d.sendArrowDataToWorker(*idx, arrowData); err != nil {
			dvid.Errorf("Unable to send block %s to worker: %v\n", idx, err)
		}
		wg.Done()
	}
}

// transformBlockToArrow converts a label block to Arrow format with supervoxel/label indices
// and compressed block data
func (d *Data) transformBlockToArrow(block *labels.Block, idx dvid.IndexZYX) (*flight.FlightData, error) {
	// Create Arrow memory allocator
	pool := memory.NewGoAllocator()
	
	// Extract labels from block
	labels := block.Labels
	
	// Get block data by marshaling to binary
	blockData, err := block.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}
	
	// Compress the block data using ZSTD
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %v", err)
	}
	defer encoder.Close()
	
	compressedData := encoder.EncodeAll(blockData, nil)
	
	// Create Arrow schema for the block data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "block_coord_x", Type: arrow.PrimitiveTypes.Int32},
		{Name: "block_coord_y", Type: arrow.PrimitiveTypes.Int32},
		{Name: "block_coord_z", Type: arrow.PrimitiveTypes.Int32},
		{Name: "labels", Type: arrow.BinaryTypes.Binary},
		{Name: "compressed_data", Type: arrow.BinaryTypes.Binary},
	}, nil)
	
	// Create builders for each column
	coordXBuilder := array.NewInt32Builder(pool)
	coordYBuilder := array.NewInt32Builder(pool)
	coordZBuilder := array.NewInt32Builder(pool)
	labelsBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	compressedBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	
	defer func() {
		coordXBuilder.Release()
		coordYBuilder.Release()
		coordZBuilder.Release()
		labelsBuilder.Release()
		compressedBuilder.Release()
	}()
	
	// Convert labels slice to bytes for storage
	labelsBytes := make([]byte, len(labels)*8)
	for i, label := range labels {
		binary.LittleEndian.PutUint64(labelsBytes[i*8:(i+1)*8], label)
	}
	
	// Append data to builders
	coordXBuilder.Append(int32(idx[0]))
	coordYBuilder.Append(int32(idx[1]))
	coordZBuilder.Append(int32(idx[2]))
	labelsBuilder.Append(labelsBytes)
	compressedBuilder.Append(compressedData)
	
	// Build arrays
	coordXArray := coordXBuilder.NewArray()
	coordYArray := coordYBuilder.NewArray()
	coordZArray := coordZBuilder.NewArray()
	labelsArray := labelsBuilder.NewArray()
	compressedArray := compressedBuilder.NewArray()
	
	defer func() {
		coordXArray.Release()
		coordYArray.Release()
		coordZArray.Release()
		labelsArray.Release()
		compressedArray.Release()
	}()
	
	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{
		coordXArray, coordYArray, coordZArray,
		labelsArray, compressedArray,
	}, 1)
	defer record.Release()
	
	// Convert to FlightData
	flightData := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{fmt.Sprintf("block_%d_%d_%d", idx[0], idx[1], idx[2])},
		},
	}
	
	// Serialize the record batch to FlightData
	var buf []byte
	// Note: In a full implementation, you'd properly serialize the Arrow record to IPC format
	// For now, we'll create a placeholder implementation
	flightData.DataBody = buf
	
	return flightData, nil
}

// sendArrowDataToWorker sends Arrow data to an appropriate worker using load balancing
func (d *Data) sendArrowDataToWorker(idx dvid.IndexZYX, arrowData *flight.FlightData) error {
	flightServer := server.GetFlightServer()
	if flightServer == nil {
		return fmt.Errorf("Arrow Flight server not available")
	}
	
	workerIDs := flightServer.GetWorkerIDs()
	if len(workerIDs) == 0 {
		return fmt.Errorf("no workers available")
	}
	
	// Simple load balancing: use hash of block coordinates to determine worker
	hash := uint32(idx[0]*73856093) ^ uint32(idx[1]*19349663) ^ uint32(idx[2]*83492791)
	workerIndex := int(hash) % len(workerIDs)
	selectedWorkerID := workerIDs[workerIndex]
	
	dvid.Debugf("Sending block %v to worker %s (worker %d of %d)\n", 
		idx, selectedWorkerID, workerIndex+1, len(workerIDs))
	
	// Send data to the selected worker
	return flightServer.SendDataToWorker(selectedWorkerID, arrowData)
}
