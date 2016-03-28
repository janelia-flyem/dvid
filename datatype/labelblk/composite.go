/*
	Functinos that support compositing label and image blocks into a pseudo-color representation.
*/

package labelblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

type blockOp struct {
	grayscale *imageblk.Data
	composite *imageblk.Data
	versionID dvid.VersionID
}

// CreateComposite creates a new rgba8 image by combining hash of labels + the grayscale
func (d *Data) CreateComposite(request datastore.Request, reply *datastore.Response) error {
	timedLog := dvid.NewTimeLog()

	// Parse the request
	var uuidStr, dataName, cmdStr, grayscaleName, destName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &grayscaleName, &destName)

	// Get the version
	uuid, v, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Log request
	if err = datastore.AddToNodeLog(uuid, []string{request.Command.String()}); err != nil {
		return err
	}

	// Get the grayscale data.
	dataservice, err := datastore.GetDataByUUID(uuid, dvid.InstanceName(grayscaleName))
	if err != nil {
		return err
	}
	grayscale, ok := dataservice.(*imageblk.Data)
	if !ok {
		return fmt.Errorf("%s is not the name of uint8 data", grayscaleName)
	}

	// Create a new rgba8blk data.
	var compservice datastore.DataService
	compservice, err = datastore.GetDataByUUID(uuid, dvid.InstanceName(destName))
	if err == nil {
		return fmt.Errorf("Data instance with name %q already exists", destName)
	}
	typeService, err := datastore.TypeServiceByName("rgba8blk")
	if err != nil {
		return fmt.Errorf("Could not get rgba8 type service from DVID")
	}
	config := dvid.NewConfig()
	compservice, err = datastore.NewData(uuid, typeService, dvid.InstanceName(destName), config)
	if err != nil {
		return err
	}
	composite, ok := compservice.(*imageblk.Data)
	if !ok {
		return fmt.Errorf("Error: %s was unable to be set to rgba8 data", destName)
	}

	// Iterate through all labels and grayscale chunks incrementally in Z, a layer at a time.
	wg := new(sync.WaitGroup)
	op := &blockOp{grayscale, composite, v}
	chunkOp := &storage.ChunkOp{op, wg}

	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return err
	}
	ctx := datastore.NewVersionedCtx(d, v)
	extents := d.Extents()
	blockBeg := imageblk.NewTKey(extents.MinIndex)
	blockEnd := imageblk.NewTKey(extents.MaxIndex)
	err = store.ProcessRange(ctx, blockBeg, blockEnd, chunkOp, storage.ChunkFunc(d.CreateCompositeChunk))
	wg.Wait()

	// Set new mapped data to same extents.
	composite.Properties.Extents = grayscale.Properties.Extents
	if err := datastore.SaveDataByUUID(uuid, composite); err != nil {
		dvid.Infof("Could not save new data '%s': %v\n", destName, err)
	}

	timedLog.Infof("Created composite of %s and %s", grayscaleName, destName)
	return nil
}

// CreateCompositeChunk processes each chunk of labels and grayscale data,
// saving the composited result into an rgba8.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) CreateCompositeChunk(chunk *storage.Chunk) error {
	<-server.HandlerToken
	go d.createCompositeChunk(chunk)
	return nil
}

var curZ int32
var curZMutex sync.Mutex

func (d *Data) createCompositeChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1

		// Notify the requestor that this chunk is done.
		if chunk.Wg != nil {
			chunk.Wg.Done()
		}
	}()

	op := chunk.Op.(*blockOp)

	// Get the spatial index associated with this chunk.
	zyx, err := imageblk.DecodeTKey(chunk.K)
	if err != nil {
		dvid.Errorf("Error in %s.ChunkApplyMap(): %v", d.Data.DataName(), err)
		return
	}

	// Initialize the label buffers.  For voxels, this data needs to be uncompressed and deserialized.
	curZMutex.Lock()
	if zyx[2] > curZ {
		curZ = zyx[2]
		minZ := zyx.MinPoint(d.BlockSize()).Value(2)
		maxZ := zyx.MaxPoint(d.BlockSize()).Value(2)
		dvid.Debugf("Now creating composite blocks for Z %d to %d\n", minZ, maxZ)
	}
	curZMutex.Unlock()

	labelData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Infof("Unable to deserialize block in '%s': %v\n", d.DataName(), err)
		return
	}
	blockBytes := len(labelData)
	if blockBytes%8 != 0 {
		dvid.Infof("Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}

	// Get the corresponding grayscale block.
	store, err := op.grayscale.GetOrderedKeyValueDB()
	if err != nil {
		dvid.Errorf("Unable to retrieve store for %q: %v\n", op.grayscale.DataName(), err)
		return
	}
	grayscaleCtx := datastore.NewVersionedCtx(op.grayscale, op.versionID)
	blockData, err := store.Get(grayscaleCtx, chunk.K)
	if err != nil {
		dvid.Errorf("Error getting grayscale block for index %s\n", zyx)
		return
	}
	grayscaleData, _, err := dvid.DeserializeData(blockData, true)
	if err != nil {
		dvid.Errorf("Unable to deserialize block in '%s': %v\n", op.grayscale.DataName(), err)
		return
	}

	// Compute the composite block.
	// TODO -- Exploit run lengths, use cache of hash?
	compositeBytes := blockBytes / 2
	compositeData := make([]byte, compositeBytes, compositeBytes)
	compositeI := 0
	labelI := 0
	hashBuf := make([]byte, 4, 4)
	for _, grayscale := range grayscaleData {
		//murmurhash3(labelData[labelI:labelI+8], hashBuf)
		//hashBuf[3] = grayscale
		writePseudoColor(grayscale, labelData[labelI:labelI+8], hashBuf)
		copy(compositeData[compositeI:compositeI+4], hashBuf)
		compositeI += 4
		labelI += 8
	}

	// Store the composite block into the rgba8 data.
	serialization, err := dvid.SerializeData(compositeData, d.Compression(), d.Checksum())
	if err != nil {
		dvid.Errorf("Unable to serialize composite block %s: %v\n", zyx, err)
		return
	}
	compositeCtx := datastore.NewVersionedCtx(op.composite, op.versionID)
	err = store.Put(compositeCtx, chunk.K, serialization)
	if err != nil {
		dvid.Errorf("Unable to PUT composite block %s: %v\n", zyx, err)
		return
	}
}

func writePseudoColor(grayscale uint8, in64bits, out32bits []byte) {
	murmurhash3(in64bits, out32bits)
	var t uint64
	t = uint64(out32bits[0]) * uint64(grayscale)
	t >>= 8
	out32bits[0] = uint8(t)
	t = uint64(out32bits[1]) * uint64(grayscale)
	t >>= 8
	out32bits[1] = uint8(t)
	t = uint64(out32bits[2]) * uint64(grayscale)
	t >>= 8
	out32bits[2] = uint8(t)
	out32bits[3] = 255
}

func murmurhash3(in64bits, out32bits []byte) {
	length := len(in64bits)
	var c1, c2 uint32 = 0xcc9e2d51, 0x1b873593
	nblocks := length / 4
	var h, k uint32
	buf := bytes.NewBuffer(in64bits)
	for i := 0; i < nblocks; i++ {
		binary.Read(buf, binary.LittleEndian, &k)
		k *= c1
		k = (k << 15) | (k >> (32 - 15))
		k *= c2
		h ^= k
		h = (h << 13) | (h >> (32 - 13))
		h = (h * 5) + 0xe6546b64
	}
	k = 0
	tailIndex := nblocks * 4
	switch length & 3 {
	case 3:
		k ^= uint32(in64bits[tailIndex+2]) << 16
		fallthrough
	case 2:
		k ^= uint32(in64bits[tailIndex+1]) << 8
		fallthrough
	case 1:
		k ^= uint32(in64bits[tailIndex])
		k *= c1
		k = (k << 15) | (k >> (32 - 15))
		k *= c2
		h ^= k
	}
	h ^= uint32(length)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	binary.BigEndian.PutUint32(out32bits, h)
}
