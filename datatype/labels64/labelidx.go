package labels64

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
)

// Meta key-value pairs are sharded by label although they can be mutated by any
// mutation at the label block level.  Since we could have concurrency at the block level,
// we need to enforce block indices mutation at the label level.  This was not necessary
// in labelvol datatype because RLEs were kept at a block level as well.  Here, we are
// aggregating all changes by label.  Send mods of form (label, delta voxels, block, action)
// where action is add or delete block from the label index.

// TODO: Make sure # voxels is correct even if people ingest multiple times, i.e., make it
// an idempotent op.  Or mutate should be standard so there is no ingest.

// Meta gives a high-level overview of all voxels in a label including the # voxels,
// block index.
type Meta struct {
	Voxels uint64         // Total # of voxels in label.
	Blocks dvid.IZYXSlice // Sorted block coordinates occupied by label.

	// MinBlock dvid.ChunkPoint3d // Minimum block coordinate for label.
	// MaxBlock dvid.ChunkPoint3d // Maximum block coordinate for label.

	// BlockVoxels []uint32       // Number of voxels for each block in Blocks, respectively.
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (m Meta) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(m.Blocks)*12+8)
	binary.LittleEndian.PutUint64(buf[0:8], m.Voxels)
	off := 8
	for _, izyx := range m.Blocks {
		copy(buf[off:off+12], string(izyx))
		off += 12
	}
	return buf, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (m *Meta) UnmarshalBinary(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("cannot unmarshal %d bytes into Meta", len(b))
	}
	m.Voxels = binary.LittleEndian.Uint64(b[0:8])
	return m.Blocks.UnmarshalBinary(b[8:])
}

type sld struct { // sortable labelDiff
	block dvid.IZYXString
	labelDiff
}

type slds []sld

func (s slds) Len() int {
	return len(s)
}

func (s slds) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s slds) Less(i, j int) bool {
	return s[i].block < s[j].block
}

// change the block presence map depending on changes.
func (m *Meta) applyChanges(bdm *blockDiffMap) error {
	// apply # voxel changes, then sort the changes in ascending block order
	bdm.RLock()
	s := make(slds, len(bdm.m))
	i := 0
	for block, diff := range bdm.m {
		m.Voxels = uint64(int64(m.Voxels) + int64(diff.delta))
		s[i] = sld{block: block, labelDiff: diff}
		i++
	}
	bdm.RUnlock()
	sort.Sort(s)

	if m.Blocks == nil {
		m.Blocks = dvid.IZYXSlice{}
	}

	// step through the changes, inserting them when necessary into meta
	mpos := 0 // position in sorted label blocks
	dpos := 0 // position in diff blocks
	for _, diff := range s {
		var block dvid.IZYXString
		for {
			if mpos >= len(m.Blocks) {
				goto Complete
			}
			block = m.Blocks[mpos]
			if diff.block > block {
				mpos++
			} else {
				break
			}
		}
		if diff.block < block {
			if diff.present {
				// insert
				m.Blocks = append(m.Blocks, "")
				copy(m.Blocks[mpos+1:], m.Blocks[mpos:])
				m.Blocks[mpos] = diff.block
				mpos++
			} else {
				dvid.Criticalf("trying to unset block %s that wasn't in label meta to begin with\n", diff.block)
			}
		} else if diff.block == block {
			if !diff.present {
				// unset
				copy(m.Blocks[mpos:], m.Blocks[mpos+1:])
				m.Blocks[len(m.Blocks)-1] = ""
				m.Blocks = m.Blocks[:len(m.Blocks)-1]
			}
		}
		dpos++
	}
Complete:
	if dpos < len(s) {
		// we may have blocks to append
		for i := dpos; i < len(s); i++ {
			if s[i].present {
				m.Blocks = append(m.Blocks, s[i].block)
			} else {
				dvid.Criticalf("trying to unset block %s that was larger than current blocks in meta\n", s[i].block)
			}
		}
	}
	return nil
}

type labelDiff struct {
	delta   int32 // change in # voxels
	present bool
}
type blockDiffMap struct {
	m map[dvid.IZYXString]labelDiff
	sync.RWMutex
}

type labelDiffMap struct {
	id uint64
	m  map[uint64]*blockDiffMap
	sync.RWMutex
}

func (ldm *labelDiffMap) getBlockDiffMap(label uint64, block ...dvid.IZYXString) *blockDiffMap {
	ldm.Lock()
	defer ldm.Unlock()
	if ldm.m == nil {
		ldm.m = make(map[uint64]*blockDiffMap)
	}
	bdm, found := ldm.m[label]
	if !found {
		bdm = &blockDiffMap{m: make(map[dvid.IZYXString]labelDiff)}
		ldm.m[label] = bdm
	}
	return bdm
}

// block-level analysis of mutation to get label changes in a block.  accumulates data for
// a given mutation into a map per mutation which will then be flushed for each label meta
// k/v pair at end of mutation.
func (d *Data) changeIndices(v dvid.VersionID, ldm *labelDiffMap, mut imageblk.MutatedBlock) {
	presentOld := uint8(0x01)
	presentNew := uint8(0x02)
	present := make(map[uint64]uint8)

	// NOTE: can use ldm directly as opposed to making it concurrency safe because for each
	// mutation we are doing this serially, not concurrently UNTIL PutChunk() is made
	// concurrent.
	block := mut.Index.ToIZYXString()
	blockBytes := int(d.BlockSize().Prod() * 8)
	for i := 0; i < blockBytes; i += 8 {
		old := binary.LittleEndian.Uint64(mut.Prev[i : i+8])
		cur := binary.LittleEndian.Uint64(mut.Data[i : i+8])
		present[old] |= presentOld
		present[cur] |= presentNew
		if old != cur {
			curbdm := ldm.getBlockDiffMap(cur)
			curbdm.Lock()
			diff, found := curbdm.m[block]
			if !found {
				diff = labelDiff{}
			}
			diff.delta++
			curbdm.m[block] = diff
			curbdm.Unlock()

			oldbdm := ldm.getBlockDiffMap(old)
			oldbdm.Lock()
			diff2, found := oldbdm.m[block]
			if !found {
				diff2 = labelDiff{}
			}
			diff2.delta--
			oldbdm.m[block] = diff2
			oldbdm.Unlock()
		}
	}
	ldm.Lock()
	delete(ldm.m, 0)
	ldm.Unlock()

	for label, flag := range present {
		bdm := ldm.getBlockDiffMap(label)
		bdm.Lock()
		var diff labelDiff
		switch flag {
		case 0x01: // we no longer have this label in the block
			diff.present = false
			bdm.m[block] = diff
		case 0x02: // this is a new label in this block
			diff.present = true
			bdm.m[block] = diff
		case 0x03: // no change
			diff.present = true
			bdm.m[block] = diff
		}
		bdm.Unlock()
	}
}

// block-level analysis of ingest to get set labels in a block.
func (d *Data) setIndices(v dvid.VersionID, ldm *labelDiffMap, mut imageblk.Block) {
	block := mut.Index.ToIZYXString()
	blockBytes := int(d.BlockSize().Prod() * 8)
	fmt.Printf("setIndices on %s\n", block)
	for i := 0; i < blockBytes; i += 8 {
		label := binary.LittleEndian.Uint64(mut.Data[i : i+8])
		bdm := ldm.getBlockDiffMap(label, block)
		bdm.Lock()
		diff, found := bdm.m[block]
		if !found {
			diff = labelDiff{}
		}
		diff.delta++
		diff.present = true
		bdm.m[block] = diff
		bdm.Unlock()
	}
	ldm.Lock()
	delete(ldm.m, 0)
	ldm.Unlock()
}

type indexMsg struct {
	v     dvid.VersionID
	label uint64
	diff  *blockDiffMap
}

// at end of mutation, write the accumulated changes for the labels in terms of block indices
// and # voxels then delete the mutations labelDiffMap.
func (d *Data) writeIndexMutation(v dvid.VersionID, ldm labelDiffMap) {
	ldm.Lock()
	for label, diff := range ldm.m {
		diff.RLock()
		fmt.Printf("Applying %d block changes to label %d\n", len(diff.m), label)
		diff.RUnlock()
		d.indexCh <- indexMsg{v, label, diff}
	}
	ldm.Unlock()
}

// goroutine(s) that accept map of label changes per block and accumulates into mutationID-specific
// change sets.
func (d *Data) indexLabel(ch <-chan indexMsg) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Criticalf("Data %q merge had error initializing store: %v\n", d.DataName(), err)
		return
	}
	for msg := range ch {
		ctx := datastore.NewVersionedCtx(d, msg.v)

		meta, err := d.GetLabelMeta(ctx, labels.NewSet(msg.label), dvid.Bounds{})
		if err != nil {
			dvid.Criticalf("Error trying to read label %d meta for data %q: %v\n", msg.label, d.DataName(), err)
			continue
		}
		oldVoxels := meta.Voxels
		oldNumBlocks := len(meta.Blocks)
		if err := meta.applyChanges(msg.diff); err != nil {
			dvid.Criticalf("Error on applying mutation changes to label %d meta: %v\n", msg.label, err)
			continue
		}
		fmt.Printf("Label %d went from %d -> %d voxels, and %d -> %d blocks\n", msg.label, oldVoxels, meta.Voxels, oldNumBlocks, len(meta.Blocks))

		tk := NewLabelIndexTKey(msg.label)
		data, err := meta.MarshalBinary()
		if err != nil {
			dvid.Criticalf("Error trying to serialize meta for label %d, data %q: %v", msg.label, d.DataName(), err)
			continue
		}
		if err := store.Put(ctx, tk, data); err != nil {
			dvid.Criticalf("Error trying to put meta for label %d, data %q: %v", msg.label, d.DataName(), err)
		}
	}
}

func writeRLE(w io.Writer, start dvid.Point3d, run int32) error {
	rle := dvid.NewRLE(start, run)
	serialization, err := rle.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := w.Write(serialization); err != nil {
		return err
	}
	return nil
}

// Scan a block and construct RLEs that will be serialized and added to the given buffer.
func (d *Data) addRLEs(w io.Writer, izyx dvid.IZYXString, data []byte, lbls labels.Set) (newRuns uint32, err error) {
	if len(data) != int(d.BlockSize().Prod())*8 {
		err = fmt.Errorf("Deserialized label block %d bytes, not uint64 size times %d block elements\n",
			len(data), d.BlockSize().Prod())
	}
	indexZYX, err := izyx.IndexZYX()
	if err != nil {
		return
	}
	firstPt := indexZYX.MinPoint(d.BlockSize())
	lastPt := indexZYX.MaxPoint(d.BlockSize())

	var voxelLabel uint64
	var spanStart dvid.Point3d
	var z, y, x, spanRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				voxelLabel = binary.LittleEndian.Uint64(data[start : start+8])
				start += 8

				// If we hit background or are no longer in labels of interest, save old run and start new one.
				spanHalted := voxelLabel == 0
				if !spanHalted {
					_, found := lbls[voxelLabel]
					if !found {
						spanHalted = true
					}
				}
				if spanHalted {
					// Save old run if we care about this label.
					if spanRun > 0 {
						newRuns++
						if err = writeRLE(w, spanStart, spanRun); err != nil {
							return
						}
					}
					// Start new one if not zero label.
					if voxelLabel != 0 {
						spanStart = dvid.Point3d{x, y, z}
						spanRun = 1
					} else {
						spanRun = 0
					}
				} else {
					spanRun++
				}
			}
			// Force break of any runs when we finish x scan.
			if spanRun > 0 {
				if err = writeRLE(w, spanStart, spanRun); err != nil {
					return
				}
				newRuns++
				spanRun = 0
			}
		}
	}
	return
}

// FoundSparseVol returns true if a sparse volume is found for the given label
// within the given bounds.
func (d *Data) FoundSparseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) (bool, error) {
	// Scan through all keys coming from label blocks to see if we have any hits.
	var constituents labels.Set
	mapping := labels.LabelMap(ctx.InstanceVersion())

	if mapping == nil {
		constituents = labels.Set{label: struct{}{}}
	} else {
		// Check if this label has been merged.
		if _, found := mapping.Get(label); found {
			return false, nil
		}

		// If not, see if labels have been merged into it.
		constituents = mapping.ConstituentLabels(label)
	}

	// See if any constituent label is within bounds.
	store, err := d.GetKeyValueDB()
	if err != nil {
		return false, err
	}
	for label := range constituents {
		val, err := store.Get(ctx, NewLabelIndexTKey(label))
		if err != nil {
			return false, err
		}
		if !bounds.Block.IsSet() && len(val) != 0 {
			return true, nil
		}
		// Check bounds if one was supplied.
		var meta Meta
		if err := meta.UnmarshalBinary(val); err != nil {
			return false, err
		}
		if bounds.Block.IsSet() {
			for _, izyx := range meta.Blocks {
				chunkPt, err := izyx.ToChunkPoint3d()
				if err != nil {
					return false, err
				}
				if !(bounds.Block.OutsideX(chunkPt[0]) || bounds.Block.OutsideY(chunkPt[1]) || bounds.Block.OutsideZ(chunkPt[2])) {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

// GetLabelMeta returns a sorted list of ZYX blocks that contain the given labels.
// If block bounds are set, the number of voxels is unknown and set to zero.
func (d *Data) GetLabelMeta(ctx *datastore.VersionedCtx, lbls labels.Set, bounds dvid.Bounds) (*Meta, error) {
	store, err := d.GetKeyValueDB()
	if err != nil {
		return nil, err
	}
	var voxels uint64
	var blocks dvid.IZYXSlice
	for label := range lbls {
		val, err := store.Get(ctx, NewLabelIndexTKey(label))
		if err != nil {
			return nil, err
		}
		var meta Meta
		if val != nil {
			if err := meta.UnmarshalBinary(val); err != nil {
				return nil, err
			}
			if len(lbls) == 1 {
				blocks = meta.Blocks
				voxels = meta.Voxels
			} else if len(meta.Blocks) > 0 {
				voxels += meta.Voxels
				blocks = blocks.Merge(meta.Blocks)
			}
		}
	}

	if bounds.Block != nil && bounds.Block.IsSet() {
		blocks, err = blocks.FitToBounds(bounds.Block)
		if err != nil {
			return nil, err
		}
		voxels = 0
	}
	meta := Meta{Voxels: voxels, Blocks: blocks}
	return &meta, nil
}

// WriteSparseVol does a streaming write of an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    0
//    uint32    0
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func (d *Data) WriteSparseVol(w io.Writer, ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) error {
	mapping := labels.LabelMap(ctx.InstanceVersion())
	if mapping != nil {
		// Check if this label has been merged.
		if mapped, found := mapping.FinalLabel(label); found {
			dvid.Debugf("Label %d has already been merged into label %d.  Skipping sparse vol retrieval.\n", label, mapped)
			return nil
		}
	}

	// Get set of all labels that have been merged to this given label.
	var lbls labels.Set
	if mapping == nil {
		lbls = labels.Set{label: struct{}{}}
	} else {
		lbls = mapping.ConstituentLabels(label)
	}

	// Get the block indices for the set of labels.
	meta, err := d.GetLabelMeta(ctx, lbls, bounds)
	if err != nil {
		return err
	}

	// Write the sparse volume header
	buf := make([]byte, 4)
	buf[0] = dvid.EncodingBinary
	buf[1] = 3 // # of dimensions
	buf[2] = 0 // dimension of run (X = 0)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	// binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	// binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return err
	}

	var numRuns, numBlocks uint32
	for _, izyx := range meta.Blocks {
		tk := NewBlockTKeyByCoord(izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return err
		}
		numBlocks++
		newRuns, err := d.addRLEs(w, izyx, data, lbls)
		if err != nil {
			return err
		}
		numRuns += newRuns
	}

	dvid.Debugf("[%s] labels %v: found %d blocks, %d runs\n", ctx, lbls, numBlocks, numRuns)
	return nil
}

// GetSparseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    0
//    uint32    0
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func (d *Data) GetSparseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) ([]byte, error) {
	mapping := labels.LabelMap(ctx.InstanceVersion())
	if mapping != nil {
		// Check if this label has been merged.
		if mapped, found := mapping.FinalLabel(label); found {
			dvid.Debugf("Label %d has already been merged into label %d.  Skipping sparse vol retrieval.\n", label, mapped)
			return nil, nil
		}
	}

	// Get set of all labels that have been merged to this given label.
	var lbls labels.Set
	if mapping == nil {
		lbls = labels.Set{label: struct{}{}}
	} else {
		lbls = mapping.ConstituentLabels(label)
	}

	// Get the block indices for the set of labels.
	meta, err := d.GetLabelMeta(ctx, lbls, bounds)
	if err != nil {
		return nil, err
	}

	// Write the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return nil, err
	}

	var numRuns, numBlocks uint32
	for _, izyx := range meta.Blocks {
		tk := NewBlockTKeyByCoord(izyx)
		data, err := store.Get(ctx, tk)
		if err != nil {
			return nil, err
		}
		numBlocks++
		newRuns, err := d.addRLEs(buf, izyx, data, lbls)
		if err != nil {
			return nil, err
		}
		numRuns += newRuns
	}

	dvid.Debugf("[%s] labels %v: found %d blocks, %d runs\n", ctx, lbls, numBlocks, numRuns)
	return buf.Bytes(), nil
}

// GetSparseCoarseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
// 		byte     Set to 0
// 		uint8    Number of dimensions
// 		uint8    Dimension of run (typically 0 = X)
// 		byte     Reserved (to be used later)
// 		uint32    # Blocks [TODO.  0 for now]
// 		uint32    # Spans
// 		Repeating unit of:
//     		int32   Block coordinate of run start (dimension 0)
//     		int32   Block coordinate of run start (dimension 1)
//     		int32   Block coordinate of run start (dimension 2)
//     		int32   Length of run
//
func (d *Data) GetSparseCoarseVol(ctx *datastore.VersionedCtx, label uint64, bounds dvid.Bounds) ([]byte, error) {
	mapping := labels.LabelMap(ctx.InstanceVersion())
	if mapping != nil {
		// Check if this label has been merged.
		if mapped, found := mapping.FinalLabel(label); found {
			dvid.Debugf("Label %d has already been merged into label %d.  Skipping sparse vol retrieval.\n", label, mapped)
			return nil, nil
		}
	}

	// Get set of all labels that have been merged to this given label.
	var lbls labels.Set
	if mapping == nil {
		lbls = labels.Set{label: struct{}{}}
	} else {
		lbls = mapping.ConstituentLabels(label)
	}

	// Get the block indices for the set of labels.
	meta, err := d.GetLabelMeta(ctx, lbls, bounds)
	if err != nil {
		return nil, err
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	spans, err := meta.Blocks.WriteSerializedRLEs(buf)
	if err != nil {
		return nil, err
	}
	serialization := buf.Bytes()
	binary.LittleEndian.PutUint32(serialization[8:12], spans) // Placeholder for # spans

	return serialization, nil
}
