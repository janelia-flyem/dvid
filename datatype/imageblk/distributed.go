package imageblk

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/storage"
)

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return fmt.Errorf("Data type imageblk had error initializing store: %v\n", err)
	}

	// Get the ROI
	var roiIterator *roi.Iterator
	if len(roiname) != 0 {
		versionID, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			return err
		}
		roiIterator, err = roi.NewIterator(dvid.InstanceName(roiname), versionID, d)
		if err != nil {
			return err
		}
	}

	// Start goroutine to receive key-value pairs and transmit to remote.
	ctx := storage.NewDataContext(d, 0)
	ch := make(chan *storage.KeyValue, 1000)
	var blocksTotal, blocksSent int
	go func() {
		for {
			kv := <-ch
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
			if roiIterator != nil && !roiIterator.InsideFast(*indexZYX) {
				continue
			}
			blocksSent++
			if err := s.SendKeyValue("voxels", kv); err != nil {
				dvid.Errorf("Error sending voxel block through socket: %v", err)
			}
		}
	}()

	// Send this instance's voxel blocks down the socket
	keysOnly := false
	begKey, endKey := ctx.KeyRange()
	if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch); err != nil {
		return fmt.Errorf("Error in voxels %q range query: %v", d.DataName(), err)
	}

	if roiIterator == nil {
		dvid.Infof("Sent %d %s voxel blocks\n", blocksTotal, d.DataName())
	} else {
		dvid.Infof("Sent %d %s voxel blocks (out of %d total) within ROI %q\n",
			blocksSent, d.DataName(), blocksTotal, roiname)
	}
	return nil
}
