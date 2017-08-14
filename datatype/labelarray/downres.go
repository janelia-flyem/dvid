package labelarray

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
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

func (d *Data) StoreDownres(v dvid.VersionID, hiresScale uint8, hires downres.BlockMap) (downres.BlockMap, error) {
	if hiresScale >= d.MaxDownresLevel {
		return nil, fmt.Errorf("can't downres %q scale %d since max downres scale is %d", d.DataName(), hiresScale, d.MaxDownresLevel)
	}
	octants, err := d.getHiresChanges(hires)
	if err != nil {
		return nil, err
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		return nil, fmt.Errorf("block size for data %q is not 3d: %v\n", d.DataName(), d.BlockSize())
	}

	batcher, err := d.GetKeyValueBatcher()
	if err != nil {
		return nil, err
	}
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	downresBMap := make(downres.BlockMap)
	for loresZYX, octant := range octants {
		var numBlocks int
		for _, block := range octant {
			if block != nil {
				numBlocks++
			}
		}

		var loresBlock *labels.Block
		if numBlocks < 8 {
			chunkPt, err := loresZYX.ToChunkPoint3d()
			if err != nil {
				return nil, err
			}
			loresBlock, err = d.GetLabelBlock(v, hiresScale+1, chunkPt)
			if err != nil {
				return nil, err
			}
		} else {
			loresBlock = labels.MakeSolidBlock(0, blockSize)
		}
		if err := loresBlock.Downres(octant); err != nil {
			return nil, err
		}
		downresBMap[loresZYX] = loresBlock

		compressed, _ := loresBlock.MarshalBinary()
		serialization, err := dvid.SerializeData(compressed, d.Compression(), d.Checksum())
		if err != nil {
			return nil, fmt.Errorf("Unable to serialize downres block in %q: %v\n", d.DataName(), err)
		}
		tk := NewBlockTKeyByCoord(hiresScale+1, loresZYX)
		batch.Put(tk, serialization)
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("Error on trying to write downres batch of scale %d->%d: %v\n", hiresScale, hiresScale+1, err)
	}
	return downresBMap, nil
}
