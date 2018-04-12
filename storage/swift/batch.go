package swift

import (
	"archive/tar"
	"fmt"
	"io"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/ncw/swift"
)

// Batch implements the storage.Batch interface for the Swift storage engine.
type Batch struct {
	sync.Mutex

	// A reference to the parent store which provided this batch.
	store *Store

	// The context for the batch operations.
	context          storage.Context      // Original context.
	versionedContext storage.VersionedCtx // The versioned context (nil if the original context is not versioned).

	// The "put" operations.
	puts map[string][]byte // Maps a Swift object name to its content.

	// The "delete" operations.
	deletes map[string]struct{} // A set of Swift objet names.
}

// newBatch returns a new batch.
func newBatch(store *Store, context storage.Context) *Batch {
	batch := &Batch{
		store:   store,
		context: context,
		puts:    make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}
	if context.Versioned() {
		var ok bool
		batch.versionedContext, ok = context.(storage.VersionedCtx)
		if !ok {
			dvid.Criticalf("Context is marked as versioned but isn't actually versioned, will revert to unversioned: %s\n", context)
		}
	}
	return batch
}

// Delete removes from the batch a put using the given key.
func (b *Batch) Delete(typeKey storage.TKey) {
	b.Lock()
	defer b.Unlock()

	name := encodeKey(b.context.ConstructKey(typeKey))

	delete(b.puts, name)
	b.deletes[name] = struct{}{}

	if b.versionedContext != nil {
		// Add a tombstone.
		tombstone := encodeKey(b.versionedContext.TombstoneKey(typeKey))
		b.puts[tombstone] = dvid.EmptyValue()
	}
}

// Put adds to the batch a put using the given key-value.
func (b *Batch) Put(typeKey storage.TKey, value []byte) {
	b.Lock()
	defer b.Unlock()

	name := encodeKey(b.context.ConstructKey(typeKey))

	// In a versioned context, we delete the tombstone.
	if b.versionedContext != nil {
		tombstone := encodeKey(b.versionedContext.TombstoneKey(typeKey))
		b.deletes[tombstone] = struct{}{}
		delete(b.puts, tombstone)
	}

	b.puts[name] = value
}

// Commits a batch of operations and closes the write batch.
func (b *Batch) Commit() error {
	b.Lock()
	defer b.Unlock()
	b.store.lockBatch(b)

	// Clean up at the end.
	defer func() {
		b.store.unlockBatch(b)
		b.puts = make(map[string][]byte)
		b.deletes = make(map[string]struct{})
	}()

	// Run the bulk deletes first.
	deletes := make([]string, 0, len(b.deletes))
	for name := range b.deletes {
		deletes = append(deletes, name)
	}
	if result, err := b.store.conn.BulkDelete(b.store.container, deletes); err != nil && err != swift.Forbidden {
		// "Not found" is an error in Swift. Only stop if there was a different error.
		for name, e := range result.Errors {
			if e != nil && e != swift.ObjectNotFound {
				return fmt.Errorf(`Swift bulk deletes failed, e.g. "%s" on "%s", skipping bulk uploads`, e, name)
			}
		}
	} else if err == swift.Forbidden {
		// Bulk operations are not supported. Go to fallback.
		return b.commitNonBulk()
	}

	// Run the bulk puts.
	reader, writer := io.Pipe()
	var tarErr error
	go func() {
		defer writer.Close()

		// Send a "tar" file to the writer.
		tw := tar.NewWriter(writer)
		for name, value := range b.puts {
			// Write header.
			if tarErr = tw.WriteHeader(&tar.Header{
				Name: name,
				Mode: 0600,
				Size: int64(len(value)),
			}); tarErr != nil {
				return
			}

			// Write content.
			if _, tarErr = tw.Write(value); tarErr != nil {
				return
			}
			storage.StoreValueBytesWritten <- len(value)
		}
		tarErr = tw.Close()
	}()
	if result, err := b.store.conn.BulkUpload(b.store.container, reader, swift.UploadTar, nil); err != nil {
		return fmt.Errorf(`Swift bulk uploads failed (%d of %d succeeded): %s / %s`, result.NumberCreated, len(b.puts), err, tarErr)
	}
	if tarErr != nil {
		return fmt.Errorf("Error generating tar file for Swift bulk upload: %s", tarErr)
	}

	return nil
}

// commitNonBulk commits the batches using individual, non-bulk transactions.
func (b *Batch) commitNonBulk() error {
	// Run the deletes.
	for name := range b.deletes {
		if err := b.store.conn.ObjectDelete(b.store.container, name); err != nil && err != swift.ObjectNotFound {
			return fmt.Errorf(`Non-bulk Swift delete failed on "%s": %s`, name, err)
		}
	}

	// Run the uploads.
	for name, value := range b.puts {
		if err := b.store.conn.ObjectPutBytes(b.store.container, name, value, "application/octet-stream"); err != nil {
			return fmt.Errorf(`Non-bulk Swift upload failed on "%s": %s`, name, err)
		}
	}

	return nil
}
