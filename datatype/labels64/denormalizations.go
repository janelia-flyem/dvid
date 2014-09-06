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
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const NanoLabels64Denorm = "LABELS64_DENORM"

func init() {
	// Register post-processing actions that need to be performed on dvid push/pull
	message.RegisterPostProcessing(NanoLabels64Denorm, postProcDenorm)
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
	// Get the Data from its name and the UUID
	repo, err := datastore.RepoFromUUID(data.UUID)
	if err != nil {
		return fmt.Errorf("Can't get Repo from transmitted uuid (%s) in %s post-proc command: %s",
			data.UUID, NanoLabels64Denorm, err.Error())
	}
	dataservice, err := repo.GetDataByName(data.Name)
	if err != nil {
		return fmt.Errorf("Can't get data instance %q in %s post-proc command: %s",
			data.Name, NanoLabels64Denorm, err.Error())
	}
	d, ok := dataservice.(*Data)
	if !ok {
		return fmt.Errorf("Data instance %q is not *labels64.Data in %s post-proc command",
			data.Name, NanoLabels64Denorm)
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
		layerLog := dvid.NewTimeLog()

		minI := dvid.IndexZYX{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], z}
		maxI := dvid.IndexZYX{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], z}

		// Process the labels chunks for this Z
		chunkOp := &storage.ChunkOp{op, wg}
		err = bigdata.ProcessRange(ctx, minI.Bytes(), maxI.Bytes(), chunkOp, d.DenormalizeChunk)
		wg.Wait()

		layerLog.Debugf("Processed all %q blocks for layer %d/%d", d.DataName(), z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	timedLog.Infof("Processed spatial information from %s", d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	timedLog = dvid.NewTimeLog()

	sizeCh := make(chan *storage.Chunk, 1000)
	wg.Add(1)

	go ComputeSizes(ctx, sizeCh, smalldata, wg)

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

	begIndex := NewLabelSpatialMapIndex(0, dvid.MinIndexZYX)
	endIndex := NewLabelSpatialMapIndex(math.MaxUint64, dvid.MaxIndexZYX)
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

// DenormalizeChunk processes a chunk of labels data.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) DenormalizeChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.denormalizeChunk(chunk)
}

func (d *Data) denormalizeChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*denormOp)

	// Get the spatial index associated with this chunk.
	zyx, err := storage.KeyToIndexZYX(chunk.K)
	if err != nil {
		dvid.Errorf("Error in %s.denormalizeChunk(): %s", d.DataName(), err.Error())
		return
	}
	zyxBytes := zyx.Bytes()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}

	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	labelRLEs := make(map[uint64]dvid.RLEs, 10)
	firstPt := zyx.MinPoint(op.source.BlockSize())
	lastPt := zyx.MaxPoint(op.source.BlockSize())

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
	db, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Error in %s.denormalizeChunk(): %s\n", d.DataName(), err.Error())
		return
	}
	batcher, ok := db.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Database doesn't support Batch ops in %s.denormalizeChunk()", d.DataName())
		return
	}
	StoreKeyLabelSpatialMap(op.versionID, d, batcher, zyxBytes, labelRLEs)
}
