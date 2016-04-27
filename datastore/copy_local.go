// +build !clustered,!gcloud

/*
	This file contains local server code supporting local data instance cloning with
	optional delimiting using datatype-specific filters.
*/

package datastore

import (
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/go-humanize"
)

// PropertyCopier are types that can copy data instance properties from another (typically identically typed)
// data instance with an optional filter.  This is used to create copies of data instances locally or
// when pushing to a remote DVID.
type PropertyCopier interface {
	CopyPropertiesFrom(DataService, storage.FilterSpec) error
}

type txStats struct {
	// num key-value pairs
	numKV uint64

	// stats on value sizes on logarithmic scale to 10 MB
	numV0, numV1, numV10, numV100, numV1k, numV10k, numV100k, numV1m, numV10m uint64

	// some stats for timing
	lastTime   time.Time
	lastBytes  uint64 // bytes received since lastTime
	totalBytes uint64
}

// record stats on size of values
func (t *txStats) addKV(k, v []byte) {
	t.numKV++

	vBytes := len(v)
	kBytes := len(k)
	curBytes := uint64(kBytes + vBytes)
	t.lastBytes += curBytes
	t.totalBytes += curBytes

	switch {
	case vBytes == 0:
		t.numV0++
	case vBytes < 10:
		t.numV1++
	case vBytes < 100:
		t.numV10++
	case vBytes < 1000:
		t.numV100++
	case vBytes < 10000:
		t.numV1k++
	case vBytes < 100000:
		t.numV10k++
	case vBytes < 1000000:
		t.numV100k++
	case vBytes < 10000000:
		t.numV1m++
	default:
		t.numV10m++
	}

	// Print progress?
	if elapsed := time.Since(t.lastTime); elapsed > time.Minute {
		mb := float64(t.lastBytes) / 1000000
		sec := elapsed.Seconds()
		throughput := mb / sec
		dvid.Debugf("Transfer throughput: %5.2f MB/s (%s in %4.1f seconds).  Total %s\n", throughput, humanize.Bytes(t.lastBytes), sec, humanize.Bytes(t.totalBytes))

		t.lastTime = time.Now()
		t.lastBytes = 0
	}
}

func (t *txStats) printStats() {
	dvid.Infof("Total size: %s\n", humanize.Bytes(t.totalBytes))
	dvid.Infof("# kv pairs: %d\n", t.numKV)
	dvid.Infof("Size of values transferred (bytes):\n")
	dvid.Infof(" key only:   %d", t.numV0)
	dvid.Infof(" [1,9):      %d", t.numV1)
	dvid.Infof(" [10,99):    %d\n", t.numV10)
	dvid.Infof(" [100,999):  %d\n", t.numV100)
	dvid.Infof(" [1k,10k):   %d\n", t.numV1k)
	dvid.Infof(" [10k,100k): %d\n", t.numV10k)
	dvid.Infof(" [100k,1m):  %d\n", t.numV100k)
	dvid.Infof(" [1m,10m):   %d\n", t.numV1m)
	dvid.Infof("  >= 10m:    %d\n", t.numV10m)
}

// CopyInstance copies a data instance locally, perhaps to a different storage
// engine if the new instance uses a different backend per a data instance-specific configuration.
// (See sample config.example.toml file in root dvid source directory.)
func CopyInstance(uuid dvid.UUID, source, target dvid.InstanceName, c dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	if source == "" || target == "" {
		return fmt.Errorf("both source and cloned name must be provided")
	}

	// Get any filter spec
	fstxt, found, err := c.GetString("filter")
	if err != nil {
		return err
	}
	var fs storage.FilterSpec
	if found {
		fs = storage.FilterSpec(fstxt)
	}

	// Get flatten or not
	transmit, found, err := c.GetString("transmit")
	if err != nil {
		return err
	}
	var flatten bool
	if transmit == "flatten" {
		flatten = true
	}

	// Get the source data instance.
	d1, err := manager.getDataByUUID(uuid, source)
	if err != nil {
		return err
	}

	// Create the target instance.
	t, err := TypeServiceByName(d1.TypeName())
	if err != nil {
		return err
	}
	d2, err := manager.newData(uuid, t, target, c)
	if err != nil {
		return err
	}

	// Populate the new data instance properties from source.
	copier, ok := d2.(PropertyCopier)
	if ok {
		if err := copier.CopyPropertiesFrom(d1, fs); err != nil {
			return err
		}
		if err := SaveDataByUUID(uuid, d2); err != nil {
			return err
		}
	}

	// Copy data with optional datatype-specific filtering.
	return CopyData(d1, d2, uuid, fs, flatten)
}

// CopyData copies all key-value pairs pertinent to the given data instance.
// Each datatype can implement filters that can restrict the transmitted key-value pairs
// based on the given FilterSpec.
func CopyData(d, d2 dvid.Data, uuid dvid.UUID, fs storage.FilterSpec, flatten bool) error {
	// We should be able to get the backing store (only ordered kv for now)
	storer, ok := d.(storage.Accessor)
	if !ok {
		return fmt.Errorf("unable to push data %q: unable to access backing store", d.DataName())
	}
	store, err := storer.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v\n", d.DataName(), err)
	}
	storer, ok = d2.(storage.Accessor)
	if !ok {
		return fmt.Errorf("unable to push data %q: unable to access backing store", d2.DataName())
	}
	store2, err := storer.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v\n", d2.DataName(), err)
	}

	dvid.Infof("Copying data %q (%s) to data %q (%s)...\n", d.DataName(), store, d2.DataName(), store2)

	// See if this data instance implements a Send filter.
	var filter storage.Filter
	filterer, ok := d.(storage.Filterer)
	if ok && fs != "" {
		var err error
		filter, err = filterer.NewFilter(fs)
		if err != nil {
			return err
		}
	}

	// Get data context for this UUID.
	v, err := VersionFromUUID(uuid)
	if err != nil {
		return err
	}
	srcCtx := NewVersionedCtx(d, v)
	dstCtx := NewVersionedCtx(d2, v)

	// Send this instance's key-value pairs
	var wg sync.WaitGroup
	wg.Add(1)

	stats := new(txStats)
	stats.lastTime = time.Now()

	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64
	keysOnly := false
	if flatten {
		// Start goroutine to receive flattened key-value pairs and store them.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					wg.Done()
					dvid.Infof("Copied %d %q key-value pairs (%s, out of %d kv pairs, %s) [flattened]\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					stats.printStats()
					return
				}
				kvTotal++
				curBytes := uint64(len(tkv.V) + len(tkv.K))
				bytesTotal += curBytes
				if filter != nil {
					skip, err := filter.Check(tkv)
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if err := store2.Put(dstCtx, tkv.K, tkv.V); err != nil {
					dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
				}
				stats.addKV(tkv.K, tkv.V)
			}
		}()

		begKey, endKey := srcCtx.TKeyRange()
		err := store.ProcessRange(srcCtx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("received nil chunk in flatten push for data %s", d.DataName())
			}
			ch <- c.TKeyValue
			return nil
		})
		ch <- nil
		if err != nil {
			return fmt.Errorf("error in flatten push for data %q: %v", d.DataName(), err)
		}
	} else {
		// Start goroutine to receive all key-value pairs and store them.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s)\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					stats.printStats()
					return
				}
				tkey, err := storage.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("couldn't get %q TKey from Key %v: %v\n", d.DataName(), kv.K, err)
					continue
				}

				kvTotal++
				curBytes := uint64(len(kv.V) + len(kv.K))
				bytesTotal += curBytes
				if filter != nil {
					skip, err := filter.Check(&storage.TKeyValue{K: tkey, V: kv.V})
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if err := dstCtx.UpdateInstance(kv.K); err != nil {
					dvid.Errorf("can't update raw key to new data instance %q: %v\n", d2.DataName(), err)
				}
				if err := store2.RawPut(kv.K, kv.V); err != nil {
					dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
				}
				stats.addKV(kv.K, kv.V)
			}
		}()

		begKey, endKey := srcCtx.KeyRange()
		if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch); err != nil {
			return fmt.Errorf("push voxels %q range query: %v", d.DataName(), err)
		}
	}
	wg.Wait()
	return nil
}
