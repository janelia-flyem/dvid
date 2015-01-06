/*
	This file contains code for denormalized representations of label data, e.g., indices
	for fast queries of all labels meeting given size restrictions, or sparse volume
	representations for a label.
*/

package labels64

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const CommandLabels64Denorm = "LABELS64_DENORM"

func init() {
	// Register post-processing actions that can be performed on dvid push/pull
	message.RegisterPostProcessing(CommandLabels64Denorm, postProcDenorm)
}

type postProcData struct {
	Name dvid.DataString
	UUID dvid.UUID
}

func postProcDenorm(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var data postProcData
	if err := dec.Decode(&data); err != nil {
		return err
	}
	dvid.Debugf("Running post-processing denormalization on repo %s for data %s\n", data.UUID, data.Name)

	// Get the Data from its name and the UUID
	repo, err := datastore.RepoFromUUID(data.UUID)
	if err != nil {
		return fmt.Errorf("Can't get Repo from transmitted uuid (%s) in %s post-proc command: %s",
			data.UUID, CommandLabels64Denorm, err.Error())
	}
	dataservice, err := repo.GetDataByName(data.Name)
	if err != nil {
		return fmt.Errorf("Can't get data instance %q in %s post-proc command: %s",
			data.Name, CommandLabels64Denorm, err.Error())
	}
	d, ok := dataservice.(*Data)
	if !ok {
		return fmt.Errorf("Data instance %q is not *labels64.Data in %s post-proc command",
			data.Name, CommandLabels64Denorm)
	}

	// Call the denormalization
	d.ProcessSpatially(data.UUID)
	return nil
}

type denormOp struct {
	source    *Data
	versionID dvid.VersionID
}

// Iterate through all blocks in the associated label volume, computing the spatial indices
// for bodies and the mappings for each spatial index.
func (d *Data) ProcessSpatially(uuid dvid.UUID) {
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
		server.BlockOnInteractiveRequests("labels64 [load layer]")
		layerLog := dvid.NewTimeLog()

		minIndexZYX := dvid.IndexZYX{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], z}
		maxIndexZYX := dvid.IndexZYX{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], z}
		begIndex := voxels.NewVoxelBlockIndex(&minIndexZYX)
		endIndex := voxels.NewVoxelBlockIndex(&maxIndexZYX)

		// Process the labels chunks for this Z
		chunkOp := &storage.ChunkOp{op, wg}
		err = bigdata.ProcessRange(ctx, begIndex, endIndex, chunkOp, d.CreateChunkRLEs)
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

	begIndex := voxels.NewLabelSpatialMapIndex(0, dvid.MinIndexZYX.Bytes())
	endIndex := voxels.NewLabelSpatialMapIndex(math.MaxUint64, dvid.MaxIndexZYX.Bytes())
	err = smalldata.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		// Get label associated with this sparse volume.
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Could not get %q index bytes from chunk key: %s\n", d.DataName(), err.Error())
			return
		}
		label := binary.BigEndian.Uint64(indexBytes[1:9])
		chunk.ChunkOp = &storage.ChunkOp{label, nil}

		// Send RLE of label to size indexer and surface calculator.
		sizeCh <- chunk
		surfaceCh[label%uint64(numSurfCalculators)] <- chunk

		server.BlockOnInteractiveRequests("labels64 [size/surface compute]")
	})
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
func (d *Data) denormFunc(versionID dvid.VersionID, mods voxels.BlockChannel) {
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

	// Given all blocks modified, process body RLEs for label sizes and surfaces.
	for label := range labels {
		begIndex := voxels.NewLabelSpatialMapIndex(label, dvid.MinIndexZYX.Bytes())
		endIndex := voxels.NewLabelSpatialMapIndex(label, dvid.MaxIndexZYX.Bytes())
		err := smalldata.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
			// Get label associated with this sparse volume.
			indexBytes, err := ctx.IndexFromKey(chunk.K)
			if err != nil {
				dvid.Errorf("Could not get %q index bytes from chunk key: %s\n", d.DataName(), err.Error())
				return
			}
			label := binary.BigEndian.Uint64(indexBytes[1:9])
			chunk.ChunkOp = &storage.ChunkOp{label, nil}

			// Send RLE of label to size indexer and surface calculator.
			sizeCh <- chunk
			surfaceCh <- chunk

			server.BlockOnInteractiveRequests("labels64 [size/surface compute]")
		})
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

// CreateChunkRLEs processes a chunk of labels data and stores the RLEs for each label.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) CreateChunkRLEs(chunk *storage.Chunk) {
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
		zyx, err := voxels.DecodeVoxelBlockKey(chunk.K)
		if err != nil {
			dvid.Errorf("Error in %s.denormalizeChunk(): %s", d.DataName(), err.Error())
			return
		}
		op := chunk.Op.(*denormOp)
		// This data needs to be uncompressed and deserialized.
		blockData, _, err := dvid.DeserializeData(chunk.V, true)
		if err != nil {
			dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
			return
		}
		d.createChunkRLEs(op.versionID, zyx, blockData)
	}()
}

func (d *Data) createChunkRLEs(versionID dvid.VersionID, zyx *dvid.IndexZYX, blockData []byte) {
	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := zyx.MinPoint(d.BlockSize())
	lastPt := zyx.MaxPoint(d.BlockSize())

	var curStart dvid.Point3d
	var voxelLabel, curLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				voxelLabel = d.Properties.ByteOrder.Uint64(blockData[start : start+8])
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

	// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
	db, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Error in %s.createChunkRLEs(): %s\n", d.DataName(), err.Error())
		return
	}
	batcher, ok := db.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Database doesn't support Batch ops in %s.denormalizeChunk()", d.DataName())
		return
	}
	StoreKeyLabelSpatialMap(versionID, d, batcher, zyx.Bytes(), labelRLEs)
}
