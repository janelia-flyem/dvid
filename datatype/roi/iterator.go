package roi

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

// Iterator is optimized for detecting whether given keys are within an ROI.
// It exploits the key, and in particular IndexZYX, ordering so that checks
// across a volume can be done quickly.
type Iterator struct {
	spans   []Span
	curSpan int32
}

func NewIterator(roiName dvid.DataString, versionID dvid.VersionID, b dvid.Bounder) (*Iterator, error) {
	dataservice, err := datastore.GetData(versionID, roiName)
	if err != nil {
		return nil, fmt.Errorf("Can't get ROI with name %q: %s", roiName, err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		return nil, fmt.Errorf("Data name %q was not of roi data type\n", roiName)
	}

	// Convert voxel extents to block Z extents
	minPt := b.StartPoint().(dvid.Chunkable)
	maxPt := b.EndPoint().(dvid.Chunkable)

	minBlockCoord := minPt.Chunk(data.BlockSize)
	maxBlockCoord := maxPt.Chunk(data.BlockSize)

	minIndex := minIndexByBlockZ(minBlockCoord.Value(2))
	maxIndex := maxIndexByBlockZ(maxBlockCoord.Value(2))

	ctx := datastore.NewVersionedContext(data, versionID)
	it := new(Iterator)
	it.spans, err = getSpans(ctx, minIndex, maxIndex)
	return it, err
}

func (it *Iterator) Reset() {
	it.curSpan = 0
}

// Returns true if the index is inside the ROI volume.  Note that this optimized
// function maintains state and is not concurrency safe; it assumes sequential
// calls where the considered indexZYX is increasing in Z, Y, and X after either
// NewIterator() or Reset().
func (it *Iterator) InsideFast(indexZYX dvid.IndexZYX) bool {
	// Fast forward through spans to make sure we are either in span or past all
	// smaller spans.
	numSpans := int32(len(it.spans))
	for {
		if it.curSpan >= numSpans {
			return false
		}
		span := it.spans[it.curSpan]
		if span[0] > indexZYX[2] { // check z
			return false
		}
		if span[0] < indexZYX[2] {
			it.curSpan++
			continue
		}
		if span[1] > indexZYX[1] { // check y
			return false
		}
		if span[1] < indexZYX[1] {
			it.curSpan++
			continue
		}
		if span[2] > indexZYX[0] { // check x0
			return false
		}
		if span[3] >= indexZYX[0] { // check x1
			return true
		}
		// We are in correct z,y but current span is before key's coordinate, so iterate.
		it.curSpan++
	}
}
