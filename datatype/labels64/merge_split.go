/*
	This file contains code that manages long-lived merge/split operations using small
	amount of globally-coordinated metadata.
*/

package labels64

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type MergeTuple []uint64

type MergeTuples []MergeTuple

func (mt *MergeTuples) addMerge(fromLabel, toLabel uint64) {
	for i, merges := range *mt {
		if (*mt)[i][0] != toLabel {
			continue
		}
		merges = append(merges, fromLabel)
		(*mt)[i] = merges
	}
}

type sizeChange struct {
	oldSize, newSize uint64
}

// MergeLabels handles merging of any number of labels throughout the various label data
// structures.  It assumes that the merges aren't cascading, e.g., there is no attempt
// to merge label 3 into 4 and also 4 into 5.  The caller should have flattened the merges.
// TODO: Provide some indication that subset of labels are under evolution, returning
//   an "unavailable" status or 203 for non-authoritative response.  This might not be
//   feasible for clustered DVID front-ends due to coordination issues.
func (d *Data) MergeLabels(ctx *datastore.VersionedContext, tuples MergeTuples) error {
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
	}
	smallBatcher, ok := smalldata.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Database doesn't support Batch ops in MergeLabels()")
	}

	// Global remapping where key = label to be merged; value = new label
	remapping := make(map[uint64]uint64)

	// Key = modified label
	sizeMods := make(map[uint64]sizeChange)

	// Key = string of block index
	blocksChanged := make(map[string]bool)

	// Iterate through all the merge ops to get targeted blocks and the necessary relabeling
	for _, tuple := range tuples {

		// Get the block-level RLEs for the toLabel
		var toLabelSize uint64
		toLabel := tuple[0]
		toLabelRLEs, err := getLabelRLEs(ctx, toLabel)
		if err != nil {
			return fmt.Errorf("Can't get block-level RLEs for label %d: %s", toLabel, err.Error())
		}
		change, found := sizeMods[toLabel]
		if found {
			toLabelSize = change.newSize
		} else {
			toLabelSize = toLabelRLEs.numVoxels()
		}
		changedRLEs := blockRLEs{}

		var addedVoxels uint64
		for _, fromLabel := range tuple[1:] {
			minIndex := voxels.NewLabelSpatialMapIndex(fromLabel, dvid.MinIndexZYX.Bytes())
			maxIndex := voxels.NewLabelSpatialMapIndex(fromLabel, dvid.MaxIndexZYX.Bytes())

			remapping[fromLabel] = toLabel

			fromLabelRLEs, err := getLabelRLEs(ctx, fromLabel)
			if err != nil {
				return fmt.Errorf("Can't get block-level RLEs for label %d: %s", fromLabel, err.Error())
			}
			fromLabelSize := fromLabelRLEs.numVoxels()

			sizeMods[fromLabel] = sizeChange{fromLabelSize, 0}
			addedVoxels += fromLabelSize

			// Append or insert RLE runs for fromLabel blocks into toLabel blocks.
			for blockStr, fromRLEs := range fromLabelRLEs {
				// Mark the fromLabel blocks as modified
				blocksChanged[blockStr] = true

				// Get the toLabel RLEs for this block and add the fromLabel RLEs
				toRLEs, found := toLabelRLEs[blockStr]
				if found {
					toRLEs.Add(fromRLEs)
				} else {
					toRLEs = fromRLEs
				}
				changedRLEs[blockStr] = toRLEs
			}

			// Delete all fromLabel RLEs since they are all integrated into toLabel RLEs
			if err := smalldata.DeleteRange(ctx, minIndex, maxIndex); err != nil {
				return fmt.Errorf("Can't delete label %d RLEs: %s", fromLabel, err.Error())
			}
		}

		// Update all toLabel RLEs that were changed
		batch := smallBatcher.NewBatch(ctx)
		for blockStr, toRLEs := range changedRLEs {
			toLabelRLEsIndex := voxels.NewLabelSpatialMapIndex(toLabel, []byte(blockStr))
			serialization, err := toRLEs.MarshalBinary()
			if err != nil {
				dvid.Errorf("Error serializing RLEs for label %d: %s\n", toLabel, err.Error())
				continue
			}
			batch.Put(toLabelRLEsIndex, serialization)
		}
		if err := batch.Commit(); err != nil {
			dvid.Errorf("Error on updating RLEs for label %d: %s\n", toLabel, err.Error())
		}
		sizeMods[toLabel] = sizeChange{toLabelSize, toLabelSize + addedVoxels}
	}

	// Update all label size data (key: sz + b)
	go updateLabelSizes(ctx, sizeMods)

	// Iterate through all the label blocks and perform the actual relabeling.
	go d.relabelBlocks(ctx, blocksChanged, remapping)

	return nil
}

// Update all label size data (key: sz + b)
func updateLabelSizes(ctx *datastore.VersionedContext, sizeMods map[uint64]sizeChange) {
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Cannot get datastore that handles small data: %s\n", err.Error())
		return
	}
	smallBatcher, ok := smalldata.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Database doesn't support Batch ops in updateLabelSizes()")
		return
	}
	// For every label key, delete the current label size and add the new one.
	batch := smallBatcher.NewBatch(ctx)
	for label, change := range sizeMods {
		oldKey := voxels.NewLabelSizesIndex(change.oldSize, label)
		newKey := voxels.NewLabelSizesIndex(change.newSize, label)
		batch.Put(newKey, dvid.EmptyValue())
		batch.Delete(oldKey)
	}
	if err := batch.Commit(); err != nil {
		dvid.Errorf("Error on updating label sizes on %s: %s\n", ctx, err.Error())
	}
}

// Iterate through all the label blocks and perform the actual relabeling.
func (d *Data) relabelBlocks(ctx *datastore.VersionedContext, blocksChanged map[string]bool,
	remapping map[uint64]uint64) {

	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("In relabeling, can't get big datastore: %s\n", err.Error())
		return
	}

	// Iterate through all modified blocks
	wg := new(sync.WaitGroup)
	for blockStr, _ := range blocksChanged {
		blockKey := voxels.NewVoxelBlockIndexByCoord(blockStr)
		value, err := bigdata.Get(ctx, blockKey)
		if err != nil {
			dvid.Errorf("Error in getting block of labels with block %v: %s\n",
				[]byte(blockStr), err.Error())
			return
		}
		<-server.HandlerToken
		go d.relabelChunk(ctx, blockKey, value, remapping, wg)
	}
	wg.Wait()
}

func (d *Data) relabelChunk(ctx *datastore.VersionedContext, k, v []byte,
	remapping map[uint64]uint64, wg *sync.WaitGroup) {

	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		wg.Done()
	}()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(v, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}
	numElements := int32(d.BlockSize().Prod())
	if int32(len(blockData)) != numElements*8 {
		dvid.Errorf("Received block with %d bytes instead of bytes for %d labels\n",
			len(blockData), numElements)
		return
	}

	// Iterate through this block of labels and relabel if label in remapping.
	for i := int32(0); i < numElements*8; i += 8 {
		label := d.Properties.ByteOrder.Uint64(blockData[i : i+8])
		toLabel, found := remapping[label]
		if found {
			d.Properties.ByteOrder.PutUint64(blockData[i:i+8], toLabel)
		}
	}

	// Store this block.
	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Unable to obtain BigData store in %q: %s\n", d.DataName(), err.Error())
		return
	}
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
		return
	}
	if err := bigdata.Put(ctx, k, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %s\n", k, err.Error())
	}
}
