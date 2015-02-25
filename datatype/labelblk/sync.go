/*
	Support for syncing labelblk from a linked labelvol or labeltiles instance.
*/

package labelblk

import (
	"encoding/binary"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// Iterate through all the label blocks and perform the actual relabeling.
func (d *Data) relabelBlocks(ctx *datastore.VersionedContext, blocksChanged map[string]bool,
	remapping map[uint64]uint64) {

	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("In relabeling, can't get big datastore: %s\n", err.Error())
		return
	}

	// Iterate through all modified blocks
	timedLog := dvid.NewTimeLog()
	wg := new(sync.WaitGroup)
	for blockStr, _ := range blocksChanged {
		blockKey := NewIndexByCoord(blockStr)
		value, err := bigdata.Get(ctx, blockKey)
		if err != nil {
			dvid.Errorf("Error in getting block of labels with block %v: %s\n",
				[]byte(blockStr), err.Error())
			return
		}
		<-server.HandlerToken
		wg.Add(1)
		go d.relabelChunk(ctx, blockKey, value, remapping, wg)
	}
	wg.Wait()
	timedLog.Infof("Completed relabeling of %d blocks", len(blocksChanged))
}

func (d *Data) relabelChunk(ctx *datastore.VersionedContext, k, v []byte,
	remapping map[uint64]uint64, wg *sync.WaitGroup) {

	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		wg.Done()
	}()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(v, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %s\n", d.DataName(), err.Error())
		return
	}
	numElements := int32(d.BlockSize().Prod())
	if int32(len(blockData)) != numElements*8 {
		dvid.Errorf("Received block with %d bytes instead of bytes for %d labels\n",
			len(blockData), numElements)
		return
	}

	// Iterate through this block of labels and relabel if label in remapping.
	for i := int32(0); i < numElements*8; i += 8 {
		label := binary.LittleEndian.Uint64(blockData[i : i+8])
		toLabel, found := remapping[label]
		if found {
			binary.LittleEndian.PutUint64(blockData[i:i+8], toLabel)
		}
	}

	// Store this block.
	bigdata, err := storage.BigDataStore()
	if err != nil {
		dvid.Errorf("Unable to obtain BigData store in %q: %s\n", d.DataName(), err.Error())
		return
	}
	serialization, err := dvid.SerializeData(blockData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize block in %q: %s\n", d.DataName(), err.Error())
		return
	}
	if err := bigdata.Put(ctx, k, serialization); err != nil {
		dvid.Errorf("Error in putting key %v: %s\n", k, err.Error())
	}
}
