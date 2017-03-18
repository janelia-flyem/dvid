/*
	Functions that support ingestion of data into persistent label blocks.

	TODO: DRY this up compared to imageblk ingest once that gets converted to more general nD.
*/

package labelarray

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// LoadImages bulk loads images using different techniques if it is a multidimensional
// file like HDF5 or a sequence of PNG/JPG/TIF images.
func (d *Data) LoadImages(v dvid.VersionID, offset dvid.Point, filenames []string) error {
	if len(filenames) == 0 {
		return nil
	}
	timedLog := dvid.NewTimeLog()

	// We only want one PUT on given version for given data to prevent interleaved
	// chunk PUTs that could potentially overwrite slice modifications.
	ctx := storage.NewDataContext(d, v)
	loadMutex := ctx.Mutex()
	loadMutex.Lock()

	// Handle cleanup given multiple goroutines still writing data.
	load := &bulkLoadInfo{filenames: filenames, versionID: v, offset: offset}
	defer func() {
		loadMutex.Unlock()

		if load.extentChanged.Value() {
			err := datastore.SaveDataByVersion(v, d)
			if err != nil {
				dvid.Errorf("Error in trying to save repo for voxel extent change: %v\n", err)
			}
		}
	}()

	// Use different loading techniques if we have a potentially multidimensional HDF5 file
	// or many 2d images.
	var err error
	if dvid.Filename(filenames[0]).HasExtensionPrefix("hdf", "h5") {
		err = d.loadHDF(load)
	} else {
		err = d.loadXYImages(load)
	}

	if err != nil {
		timedLog.Infof("RPC load of %d files had error: %v\n", err)
	} else {
		timedLog.Infof("RPC load of %d files completed.\n", len(filenames))
	}
	return err
}

// Optimized bulk loading of XY images by loading all slices for a block before processing.
// Trades off memory for speed.
func (d *Data) loadXYImages(load *bulkLoadInfo) error {
	// Load first slice, get dimensions, allocate blocks for whole slice.
	// Note: We don't need to lock the block slices because goroutines do NOT
	// access the same elements of a slice.
	const numLayers = 2
	var numBlocks int
	var blocks [numLayers]storage.TKeyValues
	var layerTransferred, layerWritten [numLayers]sync.WaitGroup
	var waitForWrites sync.WaitGroup

	curBlocks := 0
	blockSize := d.BlockSize()
	blockBytes := blockSize.Prod() * int64(d.Values.BytesPerElement())

	// Iterate through XY slices batched into the Z length of blocks.
	fileNum := 1
	errs := make(chan error, 10) // keep track of async errors.
	for _, filename := range load.filenames {
		server.BlockOnInteractiveRequests("imageblk.loadXYImages")

		timedLog := dvid.NewTimeLog()

		zInBlock := load.offset.Value(2) % blockSize.Value(2)
		firstSlice := fileNum == 1
		lastSlice := fileNum == len(load.filenames)
		firstSliceInBlock := firstSlice || zInBlock == 0
		lastSliceInBlock := lastSlice || zInBlock == blockSize.Value(2)-1
		lastBlocks := fileNum+int(blockSize.Value(2)) > len(load.filenames)

		// Load images synchronously
		vox, err := d.loadXYImage(filename, load.offset)
		if err != nil {
			return err
		}

		// Allocate blocks and/or load old block data if first/last XY blocks.
		// Note: Slices are only zeroed out on first and last slice with assumption
		// that ExtData is packed in XY footprint (values cover full extent).
		// If that is NOT the case, we need to zero out blocks for each block layer.
		if fileNum == 1 || (lastBlocks && firstSliceInBlock) {
			numBlocks = dvid.GetNumBlocks(vox, blockSize)
			if fileNum == 1 {
				for layer := 0; layer < numLayers; layer++ {
					blocks[layer] = make(storage.TKeyValues, numBlocks, numBlocks)
					for b := 0; b < numBlocks; b++ {
						blocks[layer][b].V = d.BackgroundBlock()
					}
				}
				var bufSize uint64 = uint64(blockBytes) * uint64(numBlocks) * uint64(numLayers) / 1000000
				dvid.Debugf("Allocated %d MB for buffers.\n", bufSize)
			} else {
				blocks[curBlocks] = make(storage.TKeyValues, numBlocks, numBlocks)
				for b := 0; b < numBlocks; b++ {
					blocks[curBlocks][b].V = d.BackgroundBlock()
				}
			}
			err = d.LoadOldBlocks(load.versionID, vox, blocks[curBlocks])
			if err != nil {
				return err
			}
		}

		// Transfer data between external<->internal blocks asynchronously
		layerTransferred[curBlocks].Add(1)
		go func(vox *imageblk.Voxels, curBlocks int) {
			// Track point extents
			if d.Extents().AdjustPoints(vox.StartPoint(), vox.EndPoint()) {
				load.extentChanged.SetTrue()
			}

			// Process an XY image (slice).
			changed, err := d.writeXYImage(load.versionID, vox, blocks[curBlocks])
			if err != nil {
				err = fmt.Errorf("Error writing XY image: %v\n", err)
				if len(errs) < 10 {
					errs <- err
				}
				return
			}
			if changed {
				load.extentChanged.SetTrue()
			}
			layerTransferred[curBlocks].Done()
		}(vox, curBlocks)

		// If this is the end of a block (or filenames), wait until all goroutines complete,
		// then asynchronously write blocks.
		if lastSliceInBlock {
			waitForWrites.Add(1)
			layerWritten[curBlocks].Add(1)
			go func(curBlocks int) {
				layerTransferred[curBlocks].Wait()
				dvid.Debugf("Writing block buffer %d using %s and %s...\n",
					curBlocks, d.Compression(), d.Checksum())
				err := d.writeBlocks(load.versionID, blocks[curBlocks], &layerWritten[curBlocks], &waitForWrites)
				if err != nil {
					err = fmt.Errorf("Error in async write of voxel blocks: %v", err)
					if len(errs) < 10 {
						errs <- err
					}
				}
			}(curBlocks)
			// We can't move to buffer X until all blocks from buffer X have already been written.
			curBlocks = (curBlocks + 1) % numLayers
			dvid.Debugf("Waiting for layer %d to be written before reusing layer %d blocks\n",
				curBlocks, curBlocks)
			layerWritten[curBlocks].Wait()
			dvid.Debugf("Using layer %d...\n", curBlocks)
		}

		fileNum++
		load.offset = load.offset.Add(dvid.Point3d{0, 0, 1})
		timedLog.Infof("Loaded %s slice %s", d.DataName(), vox)
	}
	waitForWrites.Wait()
	var firsterr error
	if len(errs) > 0 {
		dvid.Errorf("Had at least %d errors in image loading:\n", len(errs))
		for err := range errs {
			dvid.Errorf("  Error: %v\n", err)
			if firsterr == nil {
				firsterr = err
			}
		}
	}
	return firsterr
}

// Loads a XY oriented image at given offset, returning Voxels.
func (d *Data) loadXYImage(filename string, offset dvid.Point) (*imageblk.Voxels, error) {
	img, _, err := dvid.GoImageFromFile(filename)
	if err != nil {
		return nil, err
	}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, dvid.RectSize(img.Bounds()))
	if err != nil {
		return nil, fmt.Errorf("Unable to determine slice: %v", err)
	}
	vox, err := d.NewVoxels(slice, img)
	if err != nil {
		return nil, err
	}
	storage.FileBytesRead <- len(vox.Data())
	return vox, nil
}

func (d *Data) loadHDF(load *bulkLoadInfo) error {
	return fmt.Errorf("DVID currently does not support HDF5 image import.")
}
