// +build !clustered,!gcloud

/*
	This file contains local server code supporting local data instance cloning with
	optional delimiting using datatype-specific filters.
*/

package datastore

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/go-humanize"
)

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

	// Copy data with optional datatype-specific filtering.
	if err := CopyData(d1, d2, uuid, fs, flatten); err != nil {
		dvid.Errorf("Aborting send of instance %q data\n", d1.DataName())
		return err
	}

	return nil
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
	srcCtx := storage.NewDataContext(d, v)
	dstCtx := storage.NewDataContext(d2, v)

	// Send this instance's key-value pairs
	var wg sync.WaitGroup
	wg.Add(1)

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
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s) [flattened]\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
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
				if err := store2.Put(dstCtx, tkey, kv.V); err != nil {
					dvid.Errorf("can't put k/v pair to destination instance %q: %v\n", d2.DataName(), err)
				}
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
