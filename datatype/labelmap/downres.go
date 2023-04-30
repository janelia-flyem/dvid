package labelmap

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// For any lores block, divide it into octants and see if we have mutated the corresponding higher-res blocks.
type octantMap map[dvid.IZYXString][8]*labels.Block

// Group hires blocks by octants so we see when we actually need to GET a lower-res block.
func (d *Data) getHiresChanges(hires downres.BlockMap) (octantMap, error) {
	octants := make(octantMap)

	for hiresZYX, value := range hires {
		block, ok := value.(*labels.Block)
		if !ok {
			return nil, fmt.Errorf("bad changing block %s: expected *labels.Block got %v", hiresZYX, value)
		}
		hresCoord, err := hiresZYX.ToChunkPoint3d()
		if err != nil {
			return nil, err
		}
		downresX := hresCoord[0] >> 1
		downresY := hresCoord[1] >> 1
		downresZ := hresCoord[2] >> 1
		loresZYX := dvid.ChunkPoint3d{downresX, downresY, downresZ}.ToIZYXString()
		octidx := ((hresCoord[2] % 2) << 2) + ((hresCoord[1] % 2) << 1) + (hresCoord[0] % 2)
		oct, found := octants[loresZYX]
		if !found {
			oct = [8]*labels.Block{}
		}
		oct[octidx] = block
		octants[loresZYX] = oct
	}

	return octants, nil
}

type octantMsg struct {
	loresZYX dvid.IZYXString
	octant   [8]*labels.Block
}

func (d *Data) downresOctant(v dvid.VersionID, hiresScale uint8, mu *sync.Mutex, downresBMap downres.BlockMap, batch storage.Batch, octantCh chan octantMsg, errCh chan error) {
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		errCh <- fmt.Errorf("block size for data %q is not 3d: %v", d.DataName(), d.BlockSize())
		return
	}
	for msg := range octantCh {
		var numBlocks int
		for _, block := range msg.octant {
			if block != nil {
				numBlocks++
			}
		}

		var loresBlock *labels.Block
		if numBlocks < 8 {
			chunkPt, err := (msg.loresZYX).ToChunkPoint3d()
			if err != nil {
				errCh <- err
				return
			}
			loresBlock, err = d.getSupervoxelBlock(v, chunkPt, hiresScale+1)
			if err != nil {
				errCh <- err
				return
			}
		} else {
			loresBlock = labels.MakeSolidBlock(0, blockSize)
		}
		if err := loresBlock.Downres(msg.octant); err != nil {
			errCh <- err
			return
		}
		mu.Lock()
		downresBMap[msg.loresZYX] = loresBlock
		mu.Unlock()

		compressed, _ := loresBlock.MarshalBinary()
		serialization, err := dvid.SerializeData(compressed, d.Compression(), d.Checksum())
		if err != nil {
			errCh <- fmt.Errorf("unable to serialize downres block in %q: %v", d.DataName(), err)
			return
		}
		tk := NewBlockTKeyByCoord(hiresScale+1, msg.loresZYX)
		dvid.Infof("Storing downres block %s (%d bytes) at scale %d\n", msg.loresZYX, len(compressed), hiresScale+1)
		mu.Lock()
		batch.Put(tk, serialization)
		mu.Unlock()
		errCh <- nil
	}
}

// StoreDownres computes a downscale representation of a set of mutated blocks.
func (d *Data) StoreDownres(v dvid.VersionID, hiresScale uint8, hires downres.BlockMap) (downres.BlockMap, error) {
	timedLog := dvid.NewTimeLog()
	if hiresScale >= d.MaxDownresLevel {
		return nil, fmt.Errorf("can't downres %q scale %d since max downres scale is %d", d.DataName(), hiresScale, d.MaxDownresLevel)
	}
	octants, err := d.getHiresChanges(hires)
	if err != nil {
		return nil, err
	}

	batcher, err := datastore.GetKeyValueBatcher(d)
	if err != nil {
		return nil, err
	}
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	mu := new(sync.Mutex)
	downresBMap := make(downres.BlockMap)
	octantCh := make(chan octantMsg, len(octants))
	errCh := make(chan error, len(octants))
	defer func() {
		close(octantCh)
		close(errCh)
	}()

	numProcessors := runtime.NumCPU()
	for i := 0; i < numProcessors; i++ {
		go d.downresOctant(v, hiresScale, mu, downresBMap, batch, octantCh, errCh)
	}
	for loresZYX, octant := range octants {
		octantCh <- octantMsg{loresZYX, octant}
	}
	for i := 0; i < len(octants); i++ {
		err := <-errCh
		if err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("error on trying to write downres batch of scale %d->%d: %v", hiresScale, hiresScale+1, err)
	}
	timedLog.Infof("Computed down-resolution of %d octants at scale %d", len(octants), hiresScale)
	return downresBMap, nil
}
