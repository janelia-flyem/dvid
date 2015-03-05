/*
	This file contains code that manages long-lived merge/split operations using small
	amount of globally-coordinated metadata.
*/

package labelvol

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
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
//
// EVENTS
//
// labels.MergeStartEvent occurs at very start of merge and transmits labels.DeltaMergeStart struct.
//
// labels.MergeBlockEvent occurs for every block of a merged label and transmits labels.DeltaMerge struct.
//
// labels.MergeEndEvent occurs at end of merge and transmits labels.DeltaMergeEnd struct.
//
func (d *Data) MergeLabels(v dvid.VersionID, m labels.MergeOp) error {
	store, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	dvid.Debugf("Merging %s into label %d ...\n", m.Merged, m.Target)

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataName(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{v, labels.DeltaMergeStart{m}}
	repo, err := datastore.RepoFromVersionID(v)
	if err != nil {
		return fmt.Errorf("Could not get repo for version %s", v)
	}
	repo.NotifySubscribers(evt, msg)

	// All blocks that have changed during this merge.  Key = string of block index
	blocksChanged := make(map[dvid.IZYXString]struct{})

	// Get the block-level RLEs for the toLabel
	toLabel := m.Target
	toLabelRLEs, err := d.GetLabelRLEs(v, toLabel)
	if err != nil {
		return fmt.Errorf("Can't get block-level RLEs for label %d: %s", toLabel, err.Error())
	}
	toLabelSize := toLabelRLEs.NumVoxels()

	// Iterate through all labels to be merged.
	var addedVoxels uint64
	for fromLabel := range m.Merged {
		dvid.Debugf("Merging label %d to label %d...\n", fromLabel, toLabel)

		fromLabelRLEs, err := d.GetLabelRLEs(v, fromLabel)
		if err != nil {
			return fmt.Errorf("Can't get block-level RLEs for label %d: %s", fromLabel, err.Error())
		}
		fromLabelSize := fromLabelRLEs.NumVoxels()
		addedVoxels += fromLabelSize

		// Notify linked labelsz instances
		delta := labels.DeltaDeleteSize{
			Label:    fromLabel,
			OldSize:  fromLabelSize,
			OldKnown: true,
		}
		evt := datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{v, delta}
		repo.NotifySubscribers(evt, msg)

		// Append or insert RLE runs from fromLabel blocks into toLabel blocks.
		for blockStr, fromRLEs := range fromLabelRLEs {
			// Mark the fromLabel blocks as modified
			blocksChanged[blockStr] = struct{}{}

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
		ctx := datastore.NewVersionedContext(d, v)
		if err := store.DeleteRange(ctx, minIndex, maxIndex); err != nil {
			return fmt.Errorf("Can't delete label %d RLEs: %s", fromLabel, err.Error())
		}
	}

	// Publish block-level merge
	evt = datastore.SyncEvent{d.DataName(), labels.MergeBlockEvent}
	msg = datastore.SyncMessage{v, labels.DeltaMerge{m, blocksChanged}}
	repo.NotifySubscribers(evt, msg)

	// Update datastore with all toLabel RLEs that were changed
	ctx := datastore.NewVersionedContext(d, v)
	batch := batcher.NewBatch(ctx)
	for blockStr := range blocksChanged {
		index := NewIndex(toLabel, []byte(blockStr))
		serialization, err := toLabelRLEs[blockStr].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Error serializing RLEs for label %d: %s\n", toLabel, err.Error())
		}
		batch.Put(index, serialization)
	}
	if err := batch.Commit(); err != nil {
		dvid.Errorf("Error on updating RLEs for label %d: %s\n", toLabel, err.Error())
	}
	delta := labels.DeltaReplaceSize{
		Label:   toLabel,
		OldSize: toLabelSize,
		NewSize: toLabelSize + addedVoxels,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	repo.NotifySubscribers(evt, msg)

	evt = datastore.SyncEvent{d.DataName(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{v, labels.DeltaMergeEnd{m}}
	repo.NotifySubscribers(evt, msg)

	return nil
}

// SplitLabels splits a portion of a label's voxels into a new label, which is returned.
// The input is a binary sparse volume and should preferably be the smaller portion of a labeled region.
// In other words, the caller should chose to submit for relabeling the smaller portion of any split.
//
// EVENTS
//
// labels.SplitStartEvent occurs at very start of split and transmits labels.DeltaSplitStart struct.
//
// labels.SplitBlockEvent occurs for every block of a split label and transmits labels.DeltaSplit struct.
//
// labels.SplitEndEvent occurs at end of split and transmits labels.DeltaSplitEnd struct.
//
func (d *Data) SplitLabels(v dvid.VersionID, fromLabel uint64, r io.ReadCloser) (toLabel uint64, err error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// Create a new label id for this version that will persist to store
	toLabel, err = d.NewLabel(v)
	if err != nil {
		return
	}
	dvid.Debugf("Splitting subset of label %s into label %d ...\n", fromLabel, toLabel)

	// Signal that we are starting a split.
	evt := datastore.SyncEvent{d.DataName(), labels.SplitStartEvent}
	msg := datastore.SyncMessage{v, labels.DeltaSplitStart{fromLabel, toLabel}}
	var repo datastore.Repo
	repo, err = datastore.RepoFromVersionID(v)
	if err != nil {
		err = fmt.Errorf("Could not get repo for version %s", v)
	}
	repo.NotifySubscribers(evt, msg)

	// Read the sparse volume from reader.
	header := make([]byte, 12)
	if _, err = io.ReadFull(r, header); err != nil {
		return
	}
	if header[0] != dvid.EncodingBinary {
		err = fmt.Errorf("sparse vol for split has unknown encoding format: %v", header[0])
		return
	}
	var numSpans uint32
	if err = binary.Read(r, binary.LittleEndian, &numSpans); err != nil {
		return
	}
	var split dvid.RLEs
	if err = split.UnmarshalBinaryReader(r, numSpans); err != nil {
		return
	}
	toLabelSize, _ := split.Stats()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	splitmap, err = split.Partition(d.BlockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	// Write the split sparse vol.
	if err = d.writeLabelVol(v, toLabel, splitblks, splitmap); err != nil {
		return
	}

	// Restrict original block scan to split Z range.
	minZYX := dvid.MinIndexZYX
	maxZYX := dvid.MaxIndexZYX
	minZYX[2], err = splitblks[0].Z()
	if err != nil {
		return
	}
	maxZYX[2], err = splitblks[len(splitblks)-1].Z()
	if err != nil {
		return
	}
	begIndex := NewIndex(fromLabel, minZYX.Bytes())
	endIndex := NewIndex(fromLabel, maxZYX.Bytes())

	// Iterate block-by-block through the split, which is a subset of the original
	// sparse volume, read original until we have same block or are past it.
	// For blocks within split and original, if the two are identical, delete the
	// original, and if not, modify the original.  The latter modification op
	// should be transactional since it's GET-PUT, therefore use hash on block coord
	// to direct it to block-specific goroutine; we serialize requests to handle
	// concurrency.
	ctx := datastore.NewVersionedContext(d, v)
	batch := batcher.NewBatch(ctx)

	pos := 0
	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		_, origblkbytes, err := DecodeKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Error decoding sparse volume key (%v): %s\n", chunk.K, err.Error())
		}
		origblk := dvid.IZYXString(origblkbytes)
		splitblk := splitblks[pos]
		if origblk < splitblk || pos >= len(splitblks) {
			return nil // Seek forward
		}
		if origblk == splitblk {
			var rles dvid.RLEs
			if err := rles.UnmarshalBinary(chunk.V); err != nil {
				return fmt.Errorf("Unable to unmarshal RLE for label in block %v", chunk.K)
			}
			modified, dup, err := d.diffBlock(splitmap[splitblk], rles)
			if err != nil {
				return err
			}
			ibytes := NewIndex(fromLabel, []byte(origblk))
			if dup {
				batch.Delete(ibytes)
			} else {
				rleBytes, err := modified.MarshalBinary()
				if err != nil {
					return fmt.Errorf("can't serialize modified RLEs for split of %d: %s\n", fromLabel, err.Error())
				}
				batch.Put(ibytes, rleBytes)
			}
		} else {
			pos++
		}
		return nil
	}
	if err := store.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, f); err != nil {
		return toLabel, err
	}

	if err := batch.Commit(); err != nil {
		dvid.Errorf("Batch PUT during split of %q label %d: %s\n", d.DataName(), fromLabel, err.Error())
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	repo.NotifySubscribers(evt, msg)

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta2}
	repo.NotifySubscribers(evt, msg)

	// Publish split end
	evt = datastore.SyncEvent{d.DataName(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{v, labels.DeltaSplitEnd{fromLabel, toLabel}}
	repo.NotifySubscribers(evt, msg)

	return toLabel, nil
}

func (d *Data) diffBlock(split, orig dvid.RLEs) (modified dvid.RLEs, dup bool, err error) {
	return
}

func (d *Data) writeLabelVol(v dvid.VersionID, label uint64, blks []dvid.IZYXString, brles dvid.BlockRLEs) error {
	store, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	ctx := datastore.NewVersionedContext(d, v)
	batch := batcher.NewBatch(ctx)
	for _, s := range blks {
		serialization, err := brles[s].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Error serializing RLEs for label %d: %s\n", label, err.Error())
		}
		batch.Put(NewIndex(label, []byte(s)), serialization)
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Error on updating RLEs for label %d: %s\n", label, err.Error())
	}
	return nil
}
