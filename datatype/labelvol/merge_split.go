/*
	This file contains code that manages long-lived merge/split operations using small
	amount of globally-coordinated metadata.
*/

package labelvol

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
)

type sizeChange struct {
	oldSize, newSize uint64
}

// MergeLabels handles merging of any number of labels throughout the various label data
// structures.  It assumes that the merges aren't cascading, e.g., there is no attempt
// to merge label 3 into 4 and also 4 into 5.  The caller should have flattened the merges.
// TODO: Provide some indication that subset of labels are under evolution, returning
//   an "unavailable" status or 203 for non-authoritative response.  This might not be
//   feasible for clustered DVID front-ends due to coordination issues.
func (d *Data) MergeLabels(v dvid.VersionID, tuples labels.MergeTuples) error {
	ctx := datastore.NewVersionedContext(d, v)

	// Global remapping where key = label to be merged; value = new label
	remapping := make(map[uint64]uint64)

	// Key = modified label
	sizeMods := make(map[uint64]sizeChange)

	// All blocks that have changed during this merge.  Key = string of block index
	blocksChanged := make(map[string]bool)

	// Iterate through all the merge ops to get targeted blocks and the necessary relabeling
	for _, tuple := range tuples {

		fmt.Printf("Processing merge list: %v\n", tuple)

		// Get the block-level RLEs for the toLabel
		var toLabelSize uint64
		toLabel := tuple[0]
		toLabelRLEs, err := d.GetLabelRLEs(v, toLabel)
		if err != nil {
			return fmt.Errorf("Can't get block-level RLEs for label %d: %s", toLabel, err.Error())
		}
		change, found := sizeMods[toLabel]
		if found {
			toLabelSize = change.newSize
		} else {
			toLabelSize = toLabelRLEs.NumVoxels()
		}
		blocksChangedForLabel := make(map[string]bool)

		var addedVoxels uint64
		for _, fromLabel := range tuple[1:] {
			remapping[fromLabel] = toLabel

			fmt.Printf("Processing label %d to label %d...\n", fromLabel, toLabel)

			fromLabelRLEs, err := d.GetLabelRLEs(v, fromLabel)
			if err != nil {
				return fmt.Errorf("Can't get block-level RLEs for label %d: %s", fromLabel, err.Error())
			}
			fromLabelSize := fromLabelRLEs.NumVoxels()

			sizeMods[fromLabel] = sizeChange{fromLabelSize, 0}
			addedVoxels += fromLabelSize

			// Append or insert RLE runs for fromLabel blocks into toLabel blocks.
			for blockStr, fromRLEs := range fromLabelRLEs {
				// Mark the fromLabel blocks as modified
				blocksChanged[blockStr] = true
				blocksChangedForLabel[blockStr] = true

				// Get the toLabel RLEs for this block and add the fromLabel RLEs
				toRLEs, found := toLabelRLEs[blockStr]
				if found {
					toRLEs.Add(fromRLEs)
				} else {
					toRLEs = fromRLEs
				}
				toLabelRLEs[blockStr] = toRLEs
			}

			// Delete all fromLabel RLEs since they are all integrated into toLabel RLEs
			minIndex := NewIndex(fromLabel, dvid.MinIndexZYX.Bytes())
			maxIndex := NewIndex(fromLabel, dvid.MaxIndexZYX.Bytes())
			if err := store.DeleteRange(ctx, minIndex, maxIndex); err != nil {
				return fmt.Errorf("Can't delete label %d RLEs: %s", fromLabel, err.Error())
			}

			// Delete the fromLabel surface.
			// TODO -- Move to labelsurf
			/*
				surfaceIndex := imageblk.NewLabelSurfaceIndex(fromLabel)
				if err := bigdata.Delete(ctx, surfaceIndex); err != nil {
					return fmt.Errorf("Can't delete label %d surface: %s", fromLabel, err.Error())
				}
			*/
		}

		// Update datastore with all toLabel RLEs that were changed
		batch := batcher.NewBatch(ctx)
		for blockStr := range blocksChangedForLabel {
			toLabelRLEsIndex := NewIndex(toLabel, []byte(blockStr))
			serialization, err := toLabelRLEs[blockStr].MarshalBinary()
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

		// Recompute the toLabel surface
		// TODO -- Move to labelsurf
		// go d.recomputeSurface(ctx, toLabel, toLabelRLEs)
	}

	// Update all label size data (key: sz + b)
	// TODO -- Move to labelsz
	// go updateLabelSizes(ctx, sizeMods)

	// Iterate through all the label blocks and perform the actual relabeling.
	// TODO -- Move to labelblk
	// go d.relabelBlocks(ctx, blocksChanged, remapping)

	return nil
}
