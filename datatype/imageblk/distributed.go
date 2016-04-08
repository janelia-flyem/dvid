package imageblk

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
)

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s rpc.Session, transmit rpc.Transmit, filter dvid.Filter, versions map[dvid.VersionID]struct{}) error {
	// if there's no filter, just use base Data send.
	roiIterator, found, err := roi.NewIteratorBySpec(filter, d)
	if err != nil {
		dvid.Debugf("No filter found that was parsable: %s\n", filter)
		return err
	}
	if !found || roiIterator == nil {
		dvid.Debugf("No ROI found so using generic data push.\n")
		return d.Data.Send(s, transmit, filter, versions)
	}

	// pick any version because flatten transmit will only have one version, and all or branch transmit will
	// be looking at all versions anyway.
	if len(versions) == 0 {
		return fmt.Errorf("need at least one version to send")
	}
	var v dvid.VersionID
	for v = range versions {
		break
	}
	ctx := storage.NewDataContext(d, v)

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	// Send the initial data instance start message
	dmsg := datastore.DataTxInit{
		Session:    s.ID(),
		DataName:   d.DataName(),
		TypeName:   d.TypeName(),
		InstanceID: d.InstanceID(),
	}
	if _, err := s.Call()(datastore.StartDataMsg, dmsg); err != nil {
		dvid.Errorf("couldn't send data instance start: %v\n", err)
	}

	// Send this instance's voxel blocks down the socket
	var wg sync.WaitGroup
	wg.Add(1)

	var blocksTotal, blocksSent int
	keysOnly := false
	if transmit == rpc.TransmitFlatten {
		// Start goroutine to receive key-value pairs and transmit to remote.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					endmsg := datastore.KVMessage{Session: s.ID(), Terminate: true}
					if _, err := s.Call()(datastore.PutKVMsg, endmsg); err != nil {
						dvid.Errorf("couldn't send data instance termination: %v\n", err)
					}
					wg.Done()
					dvid.Infof("Sent %d %s voxel blocks (out of %d total) [flattened] with filter %q\n",
						blocksSent, d.DataName(), blocksTotal, filter)
					return
				}
				blocksTotal++
				indexZYX, err := DecodeTKey(tkv.K)
				if err != nil {
					dvid.Errorf("key (%v) cannot be decoded as block coord: %v", tkv.K, err)
					continue
				}
				if !roiIterator.InsideFast(*indexZYX) {
					continue
				}
				blocksSent++
				kv := storage.KeyValue{
					K: ctx.ConstructKey(tkv.K),
					V: tkv.V,
				}
				kvmsg := datastore.KVMessage{Session: s.ID(), KV: kv, Terminate: false}
				if _, err := s.Call()(datastore.PutKVMsg, kvmsg); err != nil {
					dvid.Errorf("Error sending voxel block to remote: %v", err)
				}
			}
		}()

		begKey, endKey := ctx.TKeyRange()
		err := store.ProcessRange(ctx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("received nil chunk in flatten push for imageblk")
			}
			ch <- c.TKeyValue
			return nil
		})
		ch <- nil
		if err != nil {
			return fmt.Errorf("error in flatten push for data %q: %v", d.DataName(), err)
		}
	} else {
		// Start goroutine to receive key-value pairs and transmit to remote.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					endmsg := datastore.KVMessage{Session: s.ID(), Terminate: true}
					if _, err := s.Call()(datastore.PutKVMsg, endmsg); err != nil {
						dvid.Errorf("couldn't send data instance termination: %v\n", err)
					}
					wg.Done()
					dvid.Infof("Sent %d %s voxel blocks (out of %d total) with filter %q\n",
						blocksSent, d.DataName(), blocksTotal, filter)
					return
				}
				if !ctx.ValidKV(kv, versions) {
					continue
				}
				blocksTotal++
				tkey, err := ctx.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("Unable to decode TKey from key (%v): %v\n", kv.K, err)
					continue
				}
				indexZYX, err := DecodeTKey(tkey)
				if err != nil {
					dvid.Errorf("key (%v) cannot be decoded as block coord: %v", kv.K, err)
					continue
				}
				if !roiIterator.InsideFast(*indexZYX) {
					continue
				}
				blocksSent++
				kvmsg := datastore.KVMessage{Session: s.ID(), KV: *kv, Terminate: false}
				if _, err := s.Call()(datastore.PutKVMsg, kvmsg); err != nil {
					dvid.Errorf("Error sending voxel block to remote: %v", err)
				}
			}
		}()

		begKey, endKey := ctx.KeyRange()
		if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch); err != nil {
			return fmt.Errorf("error in push voxels %q range query: %v", d.DataName(), err)
		}
	}
	wg.Wait()
	return nil
}
