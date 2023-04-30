package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

func writeBlock(w http.ResponseWriter, bcoord dvid.ChunkPoint3d, out []byte) error {
	if err := binary.Write(w, binary.LittleEndian, bcoord[0]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bcoord[1]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bcoord[2]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(out))); err != nil {
		return err
	}
	if written, err := w.Write(out); err != nil || written != int(len(out)) {
		if err != nil {
			dvid.Errorf("error writing value: %v\n", err)
			return err
		}
		return fmt.Errorf("could not write %d bytes of block %s: only %d bytes written", len(out), bcoord, written)
	}
	return nil
}

type blockTiming struct {
	readT, transcodeT, writeT time.Duration
	readN, transcodeN         int64
	sync.RWMutex
}

func (bt *blockTiming) writeDone(t0 time.Time) {
	bt.Lock()
	bt.writeT += time.Since(t0)
	bt.Unlock()
}

func (bt *blockTiming) readDone(t0 time.Time) {
	bt.Lock()
	bt.readT += time.Since(t0)
	bt.readN++
	bt.Unlock()
}

func (bt *blockTiming) transcodeDone(t0 time.Time) {
	bt.Lock()
	bt.transcodeT += time.Since(t0)
	bt.transcodeN++
	bt.Unlock()
}

func (bt *blockTiming) String() string {
	var readAvgT, transcodeAvgT, writeAvgT time.Duration
	if bt.readN == 0 {
		readAvgT = 0
	} else {
		readAvgT = bt.readT / time.Duration(bt.readN)
	}
	if bt.transcodeN == 0 {
		transcodeAvgT = 0
	} else {
		transcodeAvgT = bt.transcodeT / time.Duration(bt.transcodeN)
	}
	if bt.readN == 0 {
		writeAvgT = 0
	} else {
		writeAvgT = bt.writeT / time.Duration(bt.readN)
	}
	return fmt.Sprintf("read %s (%s), transcode %s (%s), write %s (%s)", bt.readT, readAvgT, bt.transcodeT, transcodeAvgT, bt.writeT, writeAvgT)
}

type blockData struct {
	bcoord      dvid.ChunkPoint3d
	compression string
	supervoxels bool
	v           dvid.VersionID
	data        []byte
}

// transcodes a block of data by doing any data modifications necessary to meet requested
// compression compared to stored compression as well as raw supervoxels versus mapped labels.
func (d *Data) transcodeBlock(b blockData) (out []byte, err error) {
	formatIn, checksum := dvid.DecodeSerializationFormat(dvid.SerializationFormat(b.data[0]))

	var start int
	if checksum == dvid.CRC32 {
		start = 5
	} else {
		start = 1
	}

	var outsize uint32

	switch formatIn {
	case dvid.LZ4:
		outsize = binary.LittleEndian.Uint32(b.data[start : start+4])
		out = b.data[start+4:]
		if len(out) != int(outsize) {
			err = fmt.Errorf("block %s was corrupted lz4: supposed size %d but had %d bytes", b.bcoord, outsize, len(out))
			return
		}
	case dvid.Uncompressed, dvid.Gzip:
		outsize = uint32(len(b.data[start:]))
		out = b.data[start:]
	default:
		err = fmt.Errorf("labelmap data was stored in unknown compressed format: %s", formatIn)
		return
	}

	var formatOut dvid.CompressionFormat
	switch b.compression {
	case "", "lz4":
		formatOut = dvid.LZ4
	case "blocks":
		formatOut = formatIn
	case "gzip":
		formatOut = dvid.Gzip
	case "uncompressed":
		formatOut = dvid.Uncompressed
	default:
		err = fmt.Errorf("unknown compression %q requested for blocks", b.compression)
		return
	}

	var doMapping bool
	var mapping *VCache
	if !b.supervoxels {
		if mapping, err = getMapping(d, b.v); err != nil {
			return
		}
		if mapping != nil && mapping.mapUsed {
			doMapping = true
		}
	}

	// Need to do uncompression/recompression if we are changing compression or mapping
	var uncompressed, recompressed []byte
	if formatIn != formatOut || b.compression == "gzip" || doMapping {
		switch formatIn {
		case dvid.LZ4:
			uncompressed = make([]byte, outsize)
			if err = lz4.Uncompress(out, uncompressed); err != nil {
				return
			}
		case dvid.Uncompressed:
			uncompressed = out
		case dvid.Gzip:
			gzipIn := bytes.NewBuffer(out)
			var zr *gzip.Reader
			zr, err = gzip.NewReader(gzipIn)
			if err != nil {
				return
			}
			uncompressed, err = ioutil.ReadAll(zr)
			if err != nil {
				return
			}
			zr.Close()
		}

		var block labels.Block
		if err = block.UnmarshalBinary(uncompressed); err != nil {
			err = fmt.Errorf("unable to deserialize label block %s: %v", b.bcoord, err)
			return
		}

		if !b.supervoxels {
			modifyBlockMapping(b.v, &block, mapping)
		}

		if b.compression == "blocks" { // send native DVID block compression with gzip
			out, err = block.CompressGZIP()
			if err != nil {
				return nil, err
			}
		} else { // we are sending raw block data
			uint64array, size := block.MakeLabelVolume()
			expectedSize := d.BlockSize().(dvid.Point3d)
			if !size.Equals(expectedSize) {
				err = fmt.Errorf("deserialized label block size %s does not equal data %q block size %s", size, d.DataName(), expectedSize)
				return
			}

			switch formatOut {
			case dvid.LZ4:
				recompressed = make([]byte, lz4.CompressBound(uint64array))
				var size int
				if size, err = lz4.Compress(uint64array, recompressed); err != nil {
					return nil, err
				}
				outsize = uint32(size)
				out = recompressed[:outsize]
			case dvid.Uncompressed:
				out = uint64array
			case dvid.Gzip:
				var gzipOut bytes.Buffer
				zw := gzip.NewWriter(&gzipOut)
				if _, err = zw.Write(uint64array); err != nil {
					return nil, err
				}
				zw.Flush()
				zw.Close()
				out = gzipOut.Bytes()
			}
		}
	}
	return
}

// try to write a single block either by streaming (allows for termination) or by writing
// with a simplified pipeline compared to subvolumes larger than a block.
func (d *Data) writeBlockToHTTP(ctx *datastore.VersionedCtx, w http.ResponseWriter, subvol *dvid.Subvolume, compression string, supervoxels bool, scale uint8, roiname dvid.InstanceName) (done bool, err error) {
	// Can't handle ROI for now.
	if roiname != "" {
		return
	}

	// Can only handle 3d requests.
	blockSize, okBlockSize := d.BlockSize().(dvid.Point3d)
	subvolSize, okSubvolSize := subvol.Size().(dvid.Point3d)
	startPt, okStartPt := subvol.StartPoint().(dvid.Point3d)
	if !okBlockSize || !okSubvolSize || !okStartPt {
		return
	}

	// Can only handle single block for now.
	if subvolSize != blockSize {
		return
	}

	// Can only handle aligned block for now.
	chunkPt, aligned := dvid.GetChunkPoint3d(startPt, blockSize)
	if !aligned {
		return
	}

	if compression != "" {
		err = d.sendCompressedBlock(ctx, w, subvol, compression, chunkPt, scale, supervoxels)
	} else {
		err = d.streamRawBlock(ctx, w, chunkPt, scale, supervoxels)
	}
	if err != nil {
		return
	}

	return true, nil
}

// send a single aligned block of data via HTTP.
func (d *Data) sendCompressedBlock(ctx *datastore.VersionedCtx, w http.ResponseWriter, subvol *dvid.Subvolume, compression string, chunkPt dvid.ChunkPoint3d, scale uint8, supervoxels bool) error {
	bcoordStr := chunkPt.ToIZYXString()
	block, err := d.getLabelBlock(ctx, scale, bcoordStr)
	if err != nil {
		return err
	}
	if block == nil {
		return fmt.Errorf("unable to get label block %s", bcoordStr)
	}
	vc, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return err
	}
	if !supervoxels {
		modifyBlockMapping(ctx.VersionID(), block, vc)
	}
	data, _ := block.MakeLabelVolume()
	if err := writeCompressedToHTTP(compression, data, subvol, w); err != nil {
		return err
	}

	dvid.Infof("Sent single block with compression %q, supervoxels %t, scale %d\n", compression, supervoxels, scale)
	return nil
}

// writes a block of data as uncompressed ZYX uint64 to the writer in streaming fashion, allowing
// for possible termination / error at any point.
func (d *Data) streamRawBlock(ctx *datastore.VersionedCtx, w http.ResponseWriter, bcoord dvid.ChunkPoint3d, scale uint8, supervoxels bool) error {
	bcoordStr := bcoord.ToIZYXString()
	block, err := d.getLabelBlock(ctx, scale, bcoordStr)
	if err != nil {
		return err
	}
	mapping, err := getMapping(d, ctx.VersionID())
	if err != nil {
		return err
	}
	if !supervoxels {
		modifyBlockMapping(ctx.VersionID(), block, mapping)
	}
	if err := block.WriteLabelVolume(w); err != nil {
		return err
	}
	dvid.Infof("Streaming label block %s with supervoxels %t, scale %d\n", bcoord, supervoxels, scale)
	return nil
}

// returns nil block if no block is at the given block coordinate
func (d *Data) getLabelBlock(ctx *datastore.VersionedCtx, scale uint8, bcoord dvid.IZYXString) (*labels.Block, error) {
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("labelmap getLabelBlock() had error initializing store: %v", err)
	}
	tk := NewBlockTKeyByCoord(scale, bcoord)
	val, err := store.Get(ctx, tk)
	if err != nil {
		return nil, fmt.Errorf("error on GET of labelmap %q label block @ %s", d.DataName(), bcoord)
	}
	if val == nil {
		return nil, nil
	}
	data, _, err := dvid.DeserializeData(val, true)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize label block in %q: %v", d.DataName(), err)
	}
	block := new(labels.Block)
	if err := block.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return block, nil
}

func (d *Data) getLabelPositionedBlock(ctx *datastore.VersionedCtx, scale uint8, bcoord dvid.IZYXString) (*labels.PositionedBlock, error) {
	block, err := d.getLabelBlock(ctx, scale, bcoord)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return &labels.PositionedBlock{Block: *block, BCoord: bcoord}, nil
}

func (d *Data) putLabelBlock(ctx *datastore.VersionedCtx, scale uint8, pblock *labels.PositionedBlock) error {
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("labelmap putLabelBlock() had error initializing store: %v", err)
	}
	tk := NewBlockTKeyByCoord(scale, pblock.BCoord)

	data, err := pblock.MarshalBinary()
	if err != nil {
		return err
	}

	val, err := dvid.SerializeData(data, d.Compression(), d.Checksum())
	if err != nil {
		return fmt.Errorf("unable to serialize block %s in %q: %v", pblock.BCoord, d.DataName(), err)
	}
	return store.Put(ctx, tk, val)
}

type blockSend struct {
	bcoord dvid.ChunkPoint3d
	value  []byte
	err    error
}

// convert a slice of 3 integer strings into a coordinate
func strArrayToBCoord(coordarray []string) (bcoord dvid.ChunkPoint3d, err error) {
	var xloc, yloc, zloc int
	if xloc, err = strconv.Atoi(coordarray[0]); err != nil {
		return
	}
	if yloc, err = strconv.Atoi(coordarray[1]); err != nil {
		return
	}
	if zloc, err = strconv.Atoi(coordarray[2]); err != nil {
		return
	}
	return dvid.ChunkPoint3d{int32(xloc), int32(yloc), int32(zloc)}, nil
}

// sendBlocksSpecific writes data to the blocks specified -- best for non-ordered backend
func (d *Data) sendBlocksSpecific(ctx *datastore.VersionedCtx, w http.ResponseWriter, supervoxels bool, compression, blockstring string, scale uint8) (numBlocks int, err error) {
	timedLog := dvid.NewTimeLog()
	switch compression {
	case "":
		compression = "blocks"
	case "lz4", "gzip", "blocks", "uncompressed":
		break
	default:
		err = fmt.Errorf(`compression must be "lz4" (default), "gzip", "blocks" or "uncompressed"`)
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")

	// extract querey string
	if blockstring == "" {
		return
	}
	coordarray := strings.Split(blockstring, ",")
	if len(coordarray)%3 != 0 {
		return 0, fmt.Errorf("block query string should be three coordinates per block")
	}

	var store storage.KeyValueDB
	if store, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}

	// launch goroutine that will stream blocks to client
	numBlocks = len(coordarray) / 3
	wg := new(sync.WaitGroup)

	ch := make(chan blockSend, numBlocks)
	var sendErr error
	var startBlock dvid.ChunkPoint3d
	var timing blockTiming
	go func() {
		for data := range ch {
			if data.err != nil && sendErr == nil {
				sendErr = data.err
			} else if len(data.value) > 0 {
				t0 := time.Now()
				err := writeBlock(w, data.bcoord, data.value)
				if err != nil && sendErr == nil {
					sendErr = err
				}
				timing.writeDone(t0)
			}
			wg.Done()
		}
		timedLog.Infof("labelmap %q specificblocks - finished sending %d blocks starting with %s", d.DataName(), numBlocks, startBlock)
	}()

	// iterate through each block, get data from store, and transcode based on request parameters
	for i := 0; i < len(coordarray); i += 3 {
		var bcoord dvid.ChunkPoint3d
		if bcoord, err = strArrayToBCoord(coordarray[i : i+3]); err != nil {
			return
		}
		if i == 0 {
			startBlock = bcoord
		}
		wg.Add(1)
		t0 := time.Now()
		indexBeg := dvid.IndexZYX(bcoord)
		keyBeg := NewBlockTKey(scale, &indexBeg)

		var value []byte
		value, err = store.Get(ctx, keyBeg)
		timing.readDone(t0)

		if err != nil {
			ch <- blockSend{err: err}
			return
		}

		if len(value) > 0 {
			go func(bcoord dvid.ChunkPoint3d, value []byte) {
				b := blockData{
					bcoord:      bcoord,
					v:           ctx.VersionID(),
					data:        value,
					compression: compression,
					supervoxels: supervoxels,
				}
				t0 := time.Now()
				out, err := d.transcodeBlock(b)
				timing.transcodeDone(t0)
				ch <- blockSend{bcoord: bcoord, value: out, err: err}
			}(bcoord, value)
		} else {
			ch <- blockSend{value: nil}
		}
	}
	timedLog.Infof("labelmap %q specificblocks - launched concurrent reads of %d blocks starting with %s", d.DataName(), numBlocks, startBlock)
	wg.Wait()
	close(ch)
	dvid.Infof("labelmap %q specificblocks - %d blocks starting with %s: %s\n", d.DataName(), numBlocks, startBlock, &timing)
	return numBlocks, sendErr
}

// sendBlocksVolume writes a series of blocks covering the given block-aligned subvolume to a HTTP response.
func (d *Data) sendBlocksVolume(ctx *datastore.VersionedCtx, w http.ResponseWriter, supervoxels bool, scale uint8, subvol *dvid.Subvolume, compression string) error {
	w.Header().Set("Content-type", "application/octet-stream")

	switch compression {
	case "", "lz4", "gzip", "blocks", "uncompressed":
	default:
		return fmt.Errorf(`compression must be "lz4" (default), "gzip", "blocks" or "uncompressed"`)
	}

	// convert x,y,z coordinates to block coordinates for this scale
	blocksdims := subvol.Size().Div(d.BlockSize())
	blocksoff := subvol.StartPoint().Div(d.BlockSize())

	timedLog := dvid.NewTimeLog()
	defer timedLog.Infof("SendBlocks %s, span x %d, span y %d, span z %d", blocksoff, blocksdims.Value(0), blocksdims.Value(1), blocksdims.Value(2))

	numBlocks := int(blocksdims.Prod())
	wg := new(sync.WaitGroup)

	// launch goroutine that will stream blocks to client
	ch := make(chan blockSend, numBlocks)
	var sendErr error
	go func() {
		for data := range ch {
			if data.err != nil && sendErr == nil {
				sendErr = data.err
			} else {
				err := writeBlock(w, data.bcoord, data.value)
				if err != nil && sendErr == nil {
					sendErr = err
				}
			}
			wg.Done()
		}
	}()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelmap had error initializing store: %v", err)
	}

	okv := store.(storage.BufferableOps)
	// extract buffer interface
	req, hasbuffer := okv.(storage.KeyValueRequester)
	if hasbuffer {
		okv = req.NewBuffer(ctx)
	}

	for ziter := int32(0); ziter < blocksdims.Value(2); ziter++ {
		for yiter := int32(0); yiter < blocksdims.Value(1); yiter++ {
			beginPoint := dvid.ChunkPoint3d{blocksoff.Value(0), blocksoff.Value(1) + yiter, blocksoff.Value(2) + ziter}
			endPoint := dvid.ChunkPoint3d{blocksoff.Value(0) + blocksdims.Value(0) - 1, blocksoff.Value(1) + yiter, blocksoff.Value(2) + ziter}

			indexBeg := dvid.IndexZYX(beginPoint)
			sx, sy, sz := indexBeg.Unpack()
			begTKey := NewBlockTKey(scale, &indexBeg)
			indexEnd := dvid.IndexZYX(endPoint)
			endTKey := NewBlockTKey(scale, &indexEnd)

			// Send the entire range of key-value pairs to chunk processor
			err = okv.ProcessRange(ctx, begTKey, endTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
				if c == nil || c.TKeyValue == nil {
					return nil
				}
				kv := c.TKeyValue
				if kv.V == nil {
					return nil
				}

				// Determine which block this is.
				_, indexZYX, err := DecodeBlockTKey(kv.K)
				if err != nil {
					return err
				}
				x, y, z := indexZYX.Unpack()
				if z != sz || y != sy || x < sx || x >= sx+int32(blocksdims.Value(0)) {
					return nil
				}
				b := blockData{
					bcoord:      dvid.ChunkPoint3d{x, y, z},
					compression: compression,
					supervoxels: supervoxels,
					v:           ctx.VersionID(),
					data:        kv.V,
				}
				wg.Add(1)
				go func(b blockData) {
					out, err := d.transcodeBlock(b)
					ch <- blockSend{bcoord: b.bcoord, value: out, err: err}
				}(b)
				return nil
			})

			if err != nil {
				return fmt.Errorf("unable to GET data %s: %v", ctx, err)
			}
		}
	}

	wg.Wait()
	close(ch)

	if hasbuffer {
		// submit the entire buffer to the DB
		err = okv.(storage.RequestBuffer).Flush()
		if err != nil {
			return fmt.Errorf("unable to GET data %s: %v", ctx, err)
		}
	}

	return sendErr
}

// getSupervoxelBlock returns a compressed supervoxel Block of the given block coordinate.
func (d *Data) getSupervoxelBlock(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8) (*labels.Block, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// Retrieve the block of labels
	ctx := datastore.NewVersionedCtx(d, v)
	index := dvid.IndexZYX(bcoord)
	serialization, err := store.Get(ctx, NewBlockTKey(scale, &index))
	if err != nil {
		return nil, fmt.Errorf("error getting '%s' block for index %s", d.DataName(), bcoord)
	}
	if serialization == nil {
		blockSize, ok := d.BlockSize().(dvid.Point3d)
		if !ok {
			return nil, fmt.Errorf("block size for data %q should be 3d, not: %s", d.DataName(), d.BlockSize())
		}
		return labels.MakeSolidBlock(0, blockSize), nil
	}
	deserialization, _, err := dvid.DeserializeData(serialization, true)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize block %s in '%s': %v", bcoord, d.DataName(), err)
	}
	var block labels.Block
	if err = block.UnmarshalBinary(deserialization); err != nil {
		return nil, err
	}
	return &block, nil
}

// getBlockLabels returns a block of labels at given scale in packed little-endian uint64 format.
func (d *Data) getBlockLabels(v dvid.VersionID, bcoord dvid.ChunkPoint3d, scale uint8, supervoxels bool) ([]byte, error) {
	block, err := d.getSupervoxelBlock(v, bcoord, scale)
	if err != nil {
		return nil, err
	}
	var mapping *VCache
	if !supervoxels {
		if mapping, err = getMapping(d, v); err != nil {
			return nil, err
		}
	}
	if mapping != nil {
		err = modifyBlockMapping(v, block, mapping)
		if err != nil {
			return nil, fmt.Errorf("unable to modify block %s mapping: %v", bcoord, err)
		}
	}
	labelData, _ := block.MakeLabelVolume()
	return labelData, nil
}
