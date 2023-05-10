package annotation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// RecreateDenormalizations will recreate label and tag denormalizations from
// the block-based elements.
func (d *Data) RecreateDenormalizations(ctx *datastore.VersionedCtx, inMemory, check bool) {
	if inMemory {
		go d.resyncInMemory(ctx, check)
	} else {
		go d.resyncLowMemory(ctx)
	}
}

func (d *Data) storeTags(batcher storage.KeyValueBatcher, ctx *datastore.VersionedCtx, tagE map[Tag]Elements) error {
	batch := batcher.NewBatch(ctx)
	if err := d.storeTagElements(ctx, batch, tagE); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad batch commit in reload for data %q: %v", d.DataName(), err)
	}
	return nil
}

func (d *Data) storeLabels(batcher storage.KeyValueBatcher, ctx *datastore.VersionedCtx, blockE Elements) error {
	batch := batcher.NewBatch(ctx)
	if err := d.storeLabelElements(ctx, batch, blockE); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("bad batch commit in reload for data %q: %v", d.DataName(), err)
	}
	return nil
}

func (d *Data) deleteDenormalizations(ctx *datastore.VersionedCtx) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("annotation %q had error initializing store: %v", d.DataName(), err)
	}

	timedLog := dvid.NewTimeLog()
	dvid.Infof("Deleting label kv denormalizations for annotation %q...\n", d.DataName())
	minLabelTKey := storage.MinTKey(keyLabel)
	maxLabelTKey := storage.MaxTKey(keyLabel)
	if err := store.DeleteRange(ctx, minLabelTKey, maxLabelTKey); err != nil {
		return fmt.Errorf("unable to delete label denormalization for annotations %q: %v", d.DataName(), err)
	}
	timedLog.Infof("Finished deletion of label kv denormalizations for annotation %q", d.DataName())

	timedLog = dvid.NewTimeLog()
	dvid.Infof("Deleting tag kv denormalizations for annotation %q...\n", d.DataName())
	minTagTKey := storage.MinTKey(keyTag)
	maxTagTKey := storage.MaxTKey(keyTag)
	if err := store.DeleteRange(ctx, minTagTKey, maxTagTKey); err != nil {
		return fmt.Errorf("unable to delete tag denormalization for annotations %q: %v", d.DataName(), err)
	}
	timedLog.Infof("Finished deletion of tag kv denormalizations for annotation %q", d.DataName())
	return nil
}

type denormElems struct {
	tk    storage.TKey
	elems ElementsNR
}

// Do in-memory resync of all keyBlock kv pairs, forcing the label and tag denormalizations.
// If check is true, checks denormalizations, logging any issues, and only replaces denormalizations
// when they are incorrect.
func (d *Data) resyncInMemory(ctx *datastore.VersionedCtx, check bool) {
	d.Lock()
	d.denormOngoing = true
	d.Unlock()
	defer func() {
		d.Lock()
		d.denormOngoing = false
		d.Unlock()
	}()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("Annotation %q had error initializing store: %v\n", d.DataName(), err)
		return
	}
	if !check {
		if err := d.deleteDenormalizations(ctx); err != nil {
			dvid.Errorf("Can't delete denormalizations: %v\n", err)
			return
		}
	}

	var totBlocks, totElemErrs, totLabelE, totTagE int

	labelE := LabelElements{}
	tagE := make(map[Tag]ElementsNR)

	minTKey := storage.MinTKey(keyBlock)
	maxTKey := storage.MaxTKey(keyBlock)

	timedLog := dvid.NewTimeLog()
	err = store.ProcessRange(ctx, minTKey, maxTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil {
			return fmt.Errorf("received nil chunk in reload for data %q", d.DataName())
		}
		if c.V == nil {
			return nil
		}
		chunkPt, err := DecodeBlockTKey(c.K)
		if err != nil {
			return fmt.Errorf("couldn't decode chunk key %v for data %q", c.K, d.DataName())
		}
		totBlocks++
		var elems Elements
		if err := json.Unmarshal(c.V, &elems); err != nil {
			return fmt.Errorf("couldn't unmarshal elements for data %q", d.DataName())
		}
		if len(elems) == 0 {
			return nil
		}

		blockSize := d.blockSize()
		for _, elem := range elems {
			// Check element is in correct block
			elemChunkPt := elem.Pos.Chunk(blockSize).(dvid.ChunkPoint3d)
			if !chunkPt.Equals(elemChunkPt) {
				var keyBlockSize [3]int32
				for i := uint8(0); i < 3; i++ {
					keyIndex := chunkPt.Value(i)
					if keyIndex != 0 {
						keyBlockSize[i] = elem.Pos.Value(i) / keyIndex
					}

				}
				dvid.Errorf("Element at %s found in incorrect block %s (using block size %s) instead of block key of %s (requires block size %d x %d x %d): %v\n", elem.Pos, elemChunkPt, blockSize, chunkPt, keyBlockSize[0], keyBlockSize[1], keyBlockSize[2], elem)
				totElemErrs++
			}
			// Add to Tag elements
			if len(elem.Tags) > 0 {
				for _, tag := range elem.Tags {
					te := tagE[tag]
					te = append(te, elem.ElementNR)
					totTagE++
					tagE[tag] = te
				}
			}
		}
		elemsAdded, err := d.addLabelElements(ctx.VersionID(), labelE, chunkPt, elems)
		if err != nil {
			return err
		}
		totLabelE += elemsAdded

		if totBlocks%1000 == 0 {
			timedLog.Infof("Loaded %d blocks of annotations (%d elements in %d labels / %d elements in %d tags), errors %d", totBlocks, totLabelE, len(labelE), totTagE, len(tagE), totElemErrs)
		}
		return nil
	})
	if err != nil {
		dvid.Errorf("Error in reload of data %q: %v\n", d.DataName(), err)
	}
	timedLog.Infof("Completed loading %d blocks of annotations (%d elements in %d labels / %d elements in %d tags), errors %d", totBlocks, totLabelE, len(labelE), totTagE, len(tagE), totElemErrs)

	// Get a sorted list of the labels so we can sequentially write them.
	labels := make([]uint64, len(labelE))
	i := 0
	for label := range labelE {
		labels[i] = label
		i++
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i] < labels[j] })

	if check {
		d.write_denorms_with_check(ctx, store, labelE, tagE, labels)
	} else {
		d.write_denorms(ctx, store, labelE, tagE, labels)
	}

}

func (d *Data) write_denorms_with_check(ctx *datastore.VersionedCtx, store storage.OrderedKeyValueDB,
	labelE LabelElements, tagE map[Tag]ElementsNR, labels []uint64) {

	timedLog := dvid.NewTimeLog()

	// Write denormalizations
	var wg sync.WaitGroup
	var numErrs, numProcessed, numChanged int64
	numTags := int64(len(tagE))
	ch := make(chan denormElems, 1000)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for de := range ch {
				changed := true
				correctNormalized := de.elems.Normalize()
				old, err := getElementsNR(ctx, de.tk)
				if err != nil {
					atomic.AddInt64(&numErrs, 1)
					continue
				}
				oldNormalized := old.Normalize()
				if reflect.DeepEqual(correctNormalized, oldNormalized) {
					changed = false
				}
				if changed {
					atomic.AddInt64(&numChanged, 1)
					val, err := json.Marshal(de.elems)
					if err != nil {
						atomic.AddInt64(&numErrs, 1)
						continue
					}
					if err := store.Put(ctx, de.tk, val); err != nil {
						atomic.AddInt64(&numErrs, 1)
					}
				}
				atomic.AddInt64(&numProcessed, 1)
				if numProcessed%100000 == 0 {
					pct := float64(numProcessed) / float64(len(labels)) * 100.0
					timedLog.Infof("Processed %6.3f%% of %d labels", pct, len(labels))
				}
			}
			wg.Done()
		}()
	}

	dvid.Infof("Writing elements using checks for %d labels, %d tags ...\n", len(labels), numTags)
	for _, label := range labels {
		ch <- denormElems{tk: NewLabelTKey(label), elems: labelE[label]}
	}
	for tag, elems := range tagE {
		tk, err := NewTagTKey(tag)
		if err != nil {
			dvid.Errorf("problem with tag key tkey for tag %q: %v\n", tag, err)
			atomic.AddInt64(&numErrs, 1)
			continue
		}
		ch <- denormElems{tk: tk, elems: elems}
	}
	close(ch)
	wg.Wait()
	timedLog.Infof("Finished checked denormalization of %d kvs, %d changed (%d errors)", numProcessed, numChanged, numErrs)
}

type denormJSON struct {
	tk        storage.TKey
	elemsJSON []byte
}

func (d *Data) write_denorms(ctx *datastore.VersionedCtx, store storage.OrderedKeyValueDB,
	labelE LabelElements, tagE map[Tag]ElementsNR, labels []uint64) {

	timedLog := dvid.NewTimeLog()

	// Write denormalizations
	var wg sync.WaitGroup
	var numErrs, numProcessed int64
	numTags := int64(len(tagE))
	ch := make(chan denormJSON, 1000)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for de := range ch {
				if err := store.Put(ctx, de.tk, de.elemsJSON); err != nil {
					atomic.AddInt64(&numErrs, 1)
				}
				atomic.AddInt64(&numProcessed, 1)
				if numProcessed%100000 == 0 {
					pct := float64(numProcessed) / float64(len(labels)) * 100.0
					timedLog.Infof("Processed %6.3f%% of %d labels", pct, len(labels))
				}
			}
			wg.Done()
		}()
	}

	dvid.Infof("Writing elements for %d labels, %d tags ...\n", len(labels), numTags)
	for _, label := range labels {
		val, err := json.Marshal(labelE[label])
		delete(labelE, label) // once copied to JSON, we don't need the original
		if err != nil {
			atomic.AddInt64(&numErrs, 1)
			continue
		}
		ch <- denormJSON{tk: NewLabelTKey(label), elemsJSON: val}
	}
	for tag, elems := range tagE {
		tk, err := NewTagTKey(tag)
		if err != nil {
			dvid.Errorf("problem with tag key tkey for tag %q: %v\n", tag, err)
			atomic.AddInt64(&numErrs, 1)
			continue
		}
		val, err := json.Marshal(elems)
		if err != nil {
			atomic.AddInt64(&numErrs, 1)
			continue
		}
		ch <- denormJSON{tk: tk, elemsJSON: val}
	}
	close(ch)
	wg.Wait()
	timedLog.Infof("Finished denormalization of %d kvs (%d errors)", numProcessed, numErrs)
}

// Get all keyBlock kv pairs, forcing the label and tag denormalizations.
func (d *Data) resyncLowMemory(ctx *datastore.VersionedCtx) {
	d.Lock()
	d.denormOngoing = true
	d.Unlock()
	defer func() {
		d.Lock()
		d.denormOngoing = false
		d.Unlock()
	}()

	timedLog := dvid.NewTimeLog()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		dvid.Errorf("Annotation %q had error initializing store: %v\n", d.DataName(), err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		dvid.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
		return
	}

	if err := d.deleteDenormalizations(ctx); err != nil {
		dvid.Errorf("Can't delete denormalizations: %v\n", err)
		return
	}

	var numBlocks, numBlockE, numTagE int
	var totMoved, totBlockE, totTagE int

	var blockE Elements
	tagE := make(map[Tag]Elements)

	minTKey := storage.MinTKey(keyBlock)
	maxTKey := storage.MaxTKey(keyBlock)

	err = store.ProcessRange(ctx, minTKey, maxTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil {
			return fmt.Errorf("received nil chunk in reload for data %q", d.DataName())
		}
		if c.V == nil {
			return nil
		}
		chunkPt, err := DecodeBlockTKey(c.K)
		if err != nil {
			return fmt.Errorf("couldn't decode chunk key %v for data %q", c.K, d.DataName())
		}

		var elems Elements
		if err := json.Unmarshal(c.V, &elems); err != nil {
			return fmt.Errorf("couldn't unmarshal elements for data %q", d.DataName())
		}
		if len(elems) == 0 {
			return nil
		}
		numBlocks++

		// Iterate through elements, organizing them into blocks and tags.
		// Note: we do not check for redundancy and guarantee uniqueness at this stage.
		blockFixBatch := batcher.NewBatch(ctx)
		deleteElems := make(map[int]struct{})
		for i, elem := range elems {
			// Check element is in correct block
			elemChunkPt := elem.Pos.Chunk(d.blockSize()).(dvid.ChunkPoint3d)
			if !chunkPt.Equals(elemChunkPt) {
				dvid.Criticalf("Bad element at %s found in block %s: %v\n", elem.Pos, elemChunkPt, elem)
				deleteElems[i] = struct{}{}
			}
			// Append to tags if present
			if len(elem.Tags) > 0 {
				for _, tag := range elem.Tags {
					te := tagE[tag]
					te = append(te, elem)
					numTagE++
					tagE[tag] = te
				}
			}
		}
		if len(deleteElems) > 0 {
			fixed := elems[:0]
			for i, elem := range elems {
				if _, found := deleteElems[i]; !found {
					fixed = append(fixed, elem)
				}
			}
			if err := putBatchElements(blockFixBatch, c.K, fixed); err != nil {
				return err
			}
			if err := blockFixBatch.Commit(); err != nil {
				return fmt.Errorf("bad batch commit in fixing block keyvalues for data %q: %v", d.DataName(), err)
			}
			elems = fixed
			totMoved += len(deleteElems)
		}
		blockE = append(blockE, elems...)
		numBlockE += len(elems)

		if numTagE > 1000 {
			if err := d.storeTags(batcher, ctx, tagE); err != nil {
				return err
			}
			totTagE += numTagE
			numTagE = 0
			tagE = make(map[Tag]Elements)
		}
		if numBlockE > 1000 {
			if err := d.storeLabels(batcher, ctx, blockE); err != nil {
				return err
			}
			totBlockE += numBlockE
			numBlockE = 0
			blockE = Elements{}
			timedLog.Infof("Loaded %d blocks of annotations (%d elements), moved %d", numBlocks, totBlockE, totMoved)
		}

		return nil
	})
	if err != nil {
		dvid.Errorf("Error in reload of data %q: %v\n", d.DataName(), err)
	}
	if numTagE > 0 {
		totTagE += numTagE
		if err := d.storeTags(batcher, ctx, tagE); err != nil {
			dvid.Errorf("Error writing final set of tags of data %q: %v", d.DataName(), err)
		}
	}
	if numBlockE > 0 {
		totBlockE += numBlockE
		if err := d.storeLabels(batcher, ctx, blockE); err != nil {
			dvid.Errorf("Error writing final set of label elements of data %q: %v", d.DataName(), err)
		}
	}

	timedLog.Infof("Completed asynchronous annotation %q reload of %d block and %d tag elements.", d.DataName(), totBlockE, totTagE)
}
