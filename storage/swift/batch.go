package swift

import (
	"fmt"
	"sync"
	"time"

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
	if name == "" {
		return
	}

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
	if name == "" {
		return
	}

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

	// Make active queues from the jobs.
	deletes := make([]string, 0, len(b.deletes))
	for name := range b.deletes {
		deletes = append(deletes, name)
	}
	puts := make([]string, 0, len(b.puts))
	for name := range b.puts {
		puts = append(puts, name)
	}

	// Helper functions to access the queues.
	var (
		mutex         sync.Mutex
		delay         time.Duration
		tooManyDelays bool
	)
	nextJob := func() (name string, isDelete bool) {
		mutex.Lock()
		defer mutex.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}

		if len(deletes) > 0 {
			isDelete = true
			name = deletes[len(deletes)-1]
			deletes = deletes[:len(deletes)-1]
			return
		}

		if len(puts) > 0 {
			name = puts[len(puts)-1]
			puts = puts[:len(puts)-1]
			return
		}

		return // name is empty if there are no more jobs.
	}
	requeue := func(name string, isDelete bool) {
		mutex.Lock()
		defer mutex.Unlock()

		if isDelete {
			if len(deletes) > 0 {
				deletes = append(deletes, deletes[0])
				deletes[0] = name
			} else {
				deletes = append(deletes, name)
			}
		} else {
			if len(puts) > 0 {
				puts = append(puts, puts[0])
				puts[0] = name
			} else {
				puts = append(puts, name)
			}
		}

		if delay == 0 {
			delay = initialDelay
		} else {
			delay *= 2
			if delay > maximumDelay {
				tooManyDelays = true
			}
		}
	}
	resetDelay := func() {
		mutex.Lock()
		defer mutex.Unlock()
		delay = 0
	}

	// Start the worker queues.
	start := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < maxConcurrentOperations; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if tooManyDelays {
					break // We can't continue.
				}

				// Get the next job from the queues.
				name, isDelete := nextJob()
				if name == "" {
					return // Nothing more to do.
				}

				// Perform the operation.
				rateLimit <- struct{}{}
				if isDelete {
					// Delete.
					if err := b.store.conn.ObjectDelete(b.store.container, name); err != nil && err != swift.ObjectNotFound {
						dvid.Errorf(`Individual bulk Swift delete failed on "%s": %s (will repeat)`+"\n", name, err)
						requeue(name, isDelete)
					} else {
						resetDelay()
					}
				} else {
					// Upload.
					value := b.puts[name]
					if err := b.store.conn.ObjectPutBytes(b.store.container, name, value, "application/octet-stream"); err != nil {
						dvid.Errorf(`Individual bulk Swift upload failed on "%s": %s (will repeat)`+"\n", name, err)
						requeue(name, isDelete)
					} else {
						resetDelay()
					}
					storage.StoreValueBytesWritten <- len(value)
				}
				<-rateLimit
			}
		}()
	}
	wg.Wait()

	// Do we finish with an error?
	if tooManyDelays {
		return fmt.Errorf(`Too many errors during bulk upload to Swift, %d of %d deletes / %d of %d puts not completed`, len(deletes), len(b.deletes), len(puts), len(b.puts))
	}

	return nil
}
