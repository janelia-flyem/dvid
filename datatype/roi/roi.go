/*
	Package roi implements DVID support for Region-Of-Interest operations.
*/
package roi

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/roi"
	TypeName = "roi"

	DefaultBlockSize = 32
)

const HelpMessage = `
API for 'roi' datatype (github.com/janelia-flyem/dvid/datatype/roi)
===================================================================

Command-line:

$ dvid repo <UUID> new roi <data name> <settings...>

	Adds newly named roi data to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new roi medulla

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "medulla"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %d)
	
    ------------------

HTTP API (Level 2 REST):

Note that browsers support HTTP PUT and DELETE via javascript but only GET/POST are
included in HTML specs.  For ease of use in constructing clients, HTTP POST is used
to create or modify resources in an idempotent fashion.

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET <api URL>/node/3f8c/stuff/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of roi data.


GET  <api URL>/node/<UUID>/<data name>/roi
POST <api URL>/node/<UUID>/<data name>/roi
DEL  <api URL>/node/<UUID>/<data name>/roi 

    Performs operations on an ROI depending on the HTTP verb.

    Example: 

    GET <api URL>/node/3f8c/medulla/roi

    Returns the data associated with the "medulla" ROI at version 3f8c.

    The "Content-type" of the HTTP response (and usually the request) are
    "application/json" for arbitrary binary data.  Returns a list of 4-tuples:

  	"[[0, 0, 0, 1], [0, 2, 3, 5], [0, 2, 8, 9], [1, 2, 3, 4]]"

	Each element is expressed as [z, y, x0, x1], which represents blocks with the block indices
	(x0, y, z) to (x1, y, z)

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of ROI data to save/modify or get.

GET <api URL>/node/<UUID>/<data name>/mask/0_1_2/<size>/<offset>

	Returns a binary volume in ZYX order (increasing X is contiguous in array) same as format of
	the nD voxels GET request.  The returned payload is marked as "octet-stream".

	The request must have size and offset arguments (both must be given if included) similar
	to the nD voxels GET request.  Currently, only the 3d GET is implemented, although in the
	future this endpoint will parallel voxel GET request.

	Example:

	GET <api URL>/node/3f8c/myroi/mask/0_1_2/512_512_256/100_200_300

	Returns a binary volume with non-zero elements for voxels within ROI.  The binary volume
	has size 512 x 512 x 256 voxels and an offset of (100, 200, 300).


POST <api URL>/node/<UUID>/<data name>/ptquery

	Determines with a list of 3d points in JSON format sent by POST is within the ROI.
	Returns a list of true/false answers for each point in the same sequence as the POSTed list.

    The "Content-type" of the HTTP response (and usually the request) are
    "application/json" for arbitrary binary data.  Returns a list of 4-tuples:

  	Sent: "[[0, 100, 910], [0, 121, 900]]"

  	Returned: "[false, true]"

GET <api URL>/node/<UUID>/<data name>/partition?batchsize=8

	Returns JSON of subvolumes that are batchsize^3 blocks in volume and cover the ROI.

    Query-string Options:

    batchsize	Number of blocks along each axis to batch to make one subvolume (default = 8)
    optimized   If "true" or "on", partioning returns non-fixed sized subvolumes where the coverage
                  is better in terms of subvolumes having more active blocks.
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// Type embeds the datastore's Type to create a unique type for keyvalue functions.
type Type struct {
	datastore.Type
}

// NewType returns a pointer to a new keyvalue Type with default values set.
func NewType() *Type {
	dtype := new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}
	return dtype
}

// --- TypeService interface ---

// NewData returns a pointer to new ROI data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	s, found, err := c.GetString("BlockSize")
	if err != nil {
		return nil, err
	}
	var blockSize dvid.Point3d
	if found {
		pt, err := dvid.StringToPoint(s, ",")
		if err != nil {
			return nil, err
		}
		if pt.NumDims() != 3 {
			return nil, fmt.Errorf("BlockSize must be 3d, not %dd", pt.NumDims())
		}
		blockSize, _ = pt.(dvid.Point3d)
	} else {
		blockSize = dvid.Point3d{DefaultBlockSize, DefaultBlockSize, DefaultBlockSize}
	}
	return &Data{basedata, Properties{blockSize, math.MaxInt32, math.MinInt32}}, nil
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	BlockSize dvid.Point3d

	// Minimum Block Coord Z for ROI
	MinZ int32

	// Maximum Block Coord Z for ROI
	MaxZ int32
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data
	Properties
}

// GetByUUID returns a pointer to ROI data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.DataString) (*Data, error) {
	repo, err := datastore.RepoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	source, err := repo.GetDataByName(name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a ROI datatype!", name)
	}
	return data, nil
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type ZRange struct {
	MinZ, MaxZ int32
}

var (
	minIndexRLE = indexRLE{dvid.MinIndexZYX, 0}
	maxIndexRLE = indexRLE{dvid.MaxIndexZYX, math.MaxUint32}
)

// indexRLE is the key component for block indices included in an ROI.
// Because we use dvid.IndexZYX for index byte slices, we know
// the key ordering will be Z, then Y, then X0 (and then X1).
type indexRLE struct {
	start dvid.IndexZYX
	span  uint32 // the span along X
}

func (i *indexRLE) Bytes() []byte {
	buf := new(bytes.Buffer)
	_, err := buf.Write(i.start.Bytes())
	if err != nil {
		dvid.Errorf("Error in roi.go, indexRLE.Bytes(): %s\n", err.Error())
	}
	binary.Write(buf, binary.BigEndian, i.span)
	return buf.Bytes()
}

func (i *indexRLE) IndexFromBytes(b []byte) error {
	if len(b) != 16 {
		return fmt.Errorf("Illegal byte length (%d) for ROI RLE Index", len(b))
	}
	if err := i.start.IndexFromBytes(b[0:12]); err != nil {
		return err
	}
	i.span = binary.BigEndian.Uint32(b[12:])
	return nil
}

func minIndexByBlockZ(z int32) indexRLE {
	return indexRLE{dvid.IndexZYX{math.MinInt32, math.MinInt32, z}, 0}
}

func maxIndexByBlockZ(z int32) indexRLE {
	return indexRLE{dvid.IndexZYX{math.MaxInt32, math.MaxInt32, z}, math.MaxUint32}
}

// Tuples are (Z, Y, X0, X1)
type Span [4]int32

func (s Span) less(block dvid.ChunkPoint3d) bool {
	if s[0] < block[2] {
		return true
	}
	if s[0] > block[2] {
		return false
	}
	if s[1] < block[1] {
		return true
	}
	if s[1] > block[1] {
		return false
	}
	if s[3] < block[0] {
		return true
	}
	return false
}

func (s Span) includes(block dvid.ChunkPoint3d) bool {
	if s[0] != block[2] {
		return false
	}
	if s[1] != block[1] {
		return false
	}
	if s[2] > block[0] || s[3] < block[0] {
		return false
	}
	return true
}

// Returns all (z, y, x0, x1) Spans in sorted order: z, then y, then x0.
func getSpans(ctx storage.VersionedContext, minIndex, maxIndex indexRLE) ([]Span, error) {
	db, err := storage.SmallDataStore()
	if err != nil {
		return nil, err
	}
	spans := []Span{}
	err = db.ProcessRange(ctx, minIndex.Bytes(), maxIndex.Bytes(), &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Unable to recover roi RLE from chunk key %v: %s\n", chunk.K, err.Error())
			return
		}
		index := new(indexRLE)
		if err = index.IndexFromBytes(indexBytes); err != nil {
			dvid.Errorf("Unable to get indexRLE out of []byte encoding: %s\n", err.Error())
		}
		z := index.start.Value(2)
		y := index.start.Value(1)
		x0 := index.start.Value(0)
		x1 := x0 + int32(index.span) - 1
		spans = append(spans, Span{z, y, x0, x1})
	})
	return spans, nil
}

// Returns all (z, y, x0, x1) Spans in sorted order: z, then y, then x0.
func GetSpans(ctx storage.VersionedContext) ([]Span, error) {
	return getSpans(ctx, minIndexRLE, maxIndexRLE)
}

// Get returns a JSON-encoded byte slice of the ROI in the form of 4-Spans,
// where each Span is [z, y, xstart, xend]
func Get(ctx storage.VersionedContext) ([]byte, error) {
	spans, err := GetSpans(ctx)
	if err != nil {
		return nil, err
	}
	jsonBytes, err := json.Marshal(spans)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}

// Deletes an ROI.
func (d *Data) Delete(ctx storage.VersionedContext) error {
	smalldata, err := storage.SmallDataStore()
	if err != nil {
		return err
	}

	// Make sure our small data store can do batching.
	batcher, ok := smalldata.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Unable to store ROI: small data store can't do batching!")
	}

	// We only want one PUT on given version for given data to prevent interleaved PUTs.
	putMutex := ctx.Mutex()
	putMutex.Lock()
	defer func() {
		putMutex.Unlock()
	}()

	d.MinZ = 0
	d.MaxZ = 0
	if err := datastore.SaveRepoByVersionID(ctx.VersionID()); err != nil {
		return fmt.Errorf("Error in trying to save repo on roi extent change: %s\n", err.Error())
	}

	// Iterate through all keys for ROI, deleting them in batches.
	const BATCH_SIZE = 10000
	batch := batcher.NewBatch(nil)
	begKey := minIndexRLE.Bytes()
	endKey := maxIndexRLE.Bytes()
	var curSize int
	err = smalldata.ProcessRange(ctx, begKey, endKey, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		batch.Delete(chunk.K)
		if (curSize+1)%BATCH_SIZE == 0 {
			if err := batch.Commit(); err != nil {
				dvid.Errorf("Error on batch delete of ROI %q\n", d.DataName())
				return
			}
			curSize = 0
		}
		curSize++
	})
	if curSize != 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("Error on batch delete of ROI %q\n", d.DataName())
		}
	}
	return nil
}

// PutSpans saves a slice of spans representing an ROI into the datastore.
func (d *Data) PutSpans(ctx storage.VersionedContext, spans []Span) error {
	db, err := storage.SmallDataStore()
	if err != nil {
		return err
	}

	// Delete the old key/values
	if err := d.Delete(ctx); err != nil {
		return err
	}

	// Make sure our small data store can do batching.
	batcher, ok := db.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Unable to store ROI: small data store can't do batching!")
	}

	// We only want one PUT on given version for given data to prevent interleaved PUTs.
	putMutex := ctx.Mutex()
	putMutex.Lock()

	// Save new extents after finished.
	defer func() {
		err := datastore.SaveRepoByVersionID(ctx.VersionID())
		if err != nil {
			dvid.Errorf("Error in trying to save repo on roi extent change: %s\n", err.Error())
		}
		putMutex.Unlock()
	}()

	// Put the new key/values
	const BATCH_SIZE = 10000
	batch := batcher.NewBatch(ctx)
	for i, span := range spans {
		if span[0] < d.MinZ {
			d.MinZ = span[0]
		}
		if span[0] > d.MaxZ {
			d.MaxZ = span[0]
		}
		if span[3] < span[2] {
			return fmt.Errorf("Got weird span %v.  span[3] (X1) < span[2] (X0)", span)
		}
		index := indexRLE{
			start: dvid.IndexZYX{span[2], span[1], span[0]},
			span:  uint32(span[3] - span[2] + 1),
		}

		batch.Put(index.Bytes(), dvid.EmptyValue())
		if (i+1)%BATCH_SIZE == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("Error on batch PUT at span %d: %s\n", i, err.Error())
			}
			batch = batcher.NewBatch(ctx)
		}
	}
	if len(spans)%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("Error on last batch PUT: %s\n", err.Error())
		}
	}
	return nil
}

// PutJSON saves JSON-encoded data representing an ROI into the datastore.
func (d *Data) PutJSON(ctx storage.VersionedContext, jsonBytes []byte) error {
	spans := []Span{}
	err := json.Unmarshal(jsonBytes, &spans)
	if err != nil {
		return fmt.Errorf("Error trying to parse POSTed JSON: %s", err.Error())
	}
	return d.PutSpans(ctx, spans)
}

// Returns the voxel range normalized to begVoxel offset and constrained by block span.
func voxelRange(blockSize, begBlock, endBlock, begVoxel, endVoxel int32) (int32, int32) {
	v0 := begBlock * blockSize
	if v0 < begVoxel {
		v0 = begVoxel
	}
	v1 := (endBlock+1)*blockSize - 1
	if v1 > endVoxel {
		v1 = endVoxel
	}
	v0 -= begVoxel
	v1 -= begVoxel
	return v0, v1
}

// GetMask returns a binary volume of subvol size where each element is 1 if inside the ROI
// and 0 if outside the ROI.
func (d *Data) GetMask(ctx storage.VersionedContext, subvol *dvid.Subvolume) ([]byte, error) {
	pt0 := subvol.StartPoint()
	pt1 := subvol.EndPoint()
	minBlockZ := pt0.Value(2) / d.BlockSize[2]
	maxBlockZ := pt1.Value(2) / d.BlockSize[2]
	minBlockY := pt0.Value(1) / d.BlockSize[1]
	maxBlockY := pt1.Value(1) / d.BlockSize[1]
	minBlockX := pt0.Value(0) / d.BlockSize[0]
	maxBlockX := pt1.Value(0) / d.BlockSize[0]

	minIndex := minIndexByBlockZ(minBlockZ)
	maxIndex := maxIndexByBlockZ(maxBlockZ)

	spans, err := getSpans(ctx, minIndex, maxIndex)
	if err != nil {
		return nil, err
	}

	// Allocate the mask volume.
	data := make([]uint8, subvol.NumVoxels())
	size := subvol.Size()
	nx := size.Value(0)
	nxy := size.Value(1) * nx

	// Fill the mask volume
	for _, span := range spans {
		// Handle out of range blocks
		if span[0] < minBlockZ {
			continue
		}
		if span[0] > maxBlockZ {
			break
		}
		if span[1] < minBlockY || span[1] > maxBlockY {
			continue
		}
		if span[3] < minBlockX || span[2] > maxBlockX {
			continue
		}

		// Get the voxel range for this span, including limits based on subvolume.
		x0, x1 := voxelRange(d.BlockSize[0], span[2], span[3], pt0.Value(0), pt1.Value(0))
		y0, y1 := voxelRange(d.BlockSize[1], span[1], span[1], pt0.Value(1), pt1.Value(1))
		z0, z1 := voxelRange(d.BlockSize[2], span[0], span[0], pt0.Value(2), pt1.Value(2))

		// Write the mask
		for z := z0; z <= z1; z++ {
			for y := y0; y <= y1; y++ {
				i := z*nxy + y*nx + x0
				for x := x0; x <= x1; x++ {
					data[i] = 1
					i++
				}
			}
		}
	}
	return data, nil
}

// Returns the current span index and whether given point is included in span.
func (d *Data) seekSpan(pt dvid.Point3d, spans []Span, curSpanI int) (int, bool) {
	numSpans := len(spans)
	if curSpanI >= numSpans {
		return curSpanI, false
	}

	// Determine current block index of point.
	chunkPt, _ := pt.Chunk(d.BlockSize).(dvid.ChunkPoint3d)

	// Keep going through spans until we are equal to or past the chunk point.
	for {
		curSpan := spans[curSpanI]
		if curSpan.less(chunkPt) {
			curSpanI++
		} else {
			if curSpan.includes(chunkPt) {
				return curSpanI, true
			} else {
				return curSpanI, false
			}
		}
		if curSpanI >= numSpans {
			return curSpanI, false
		}
	}
}

// PointQuery checks if a JSON-encoded list of voxel points are within an ROI.
// It returns a JSON list of bools, each corresponding to the original list of points.
func (d *Data) PointQuery(ctx storage.VersionedContext, jsonBytes []byte) ([]byte, error) {
	// Convert given set of JSON-encoded points to a sorted list of points.
	var pts dvid.ListPoint3d
	if err := json.Unmarshal(jsonBytes, &pts); err != nil {
		return nil, err
	}
	sort.Sort(dvid.ByZYX(pts))

	// Get the ROI.  The spans are ordered in z, y, then x0.
	spans, err := GetSpans(ctx)
	if err != nil {
		return nil, err
	}

	// Iterate through each query point, using the ordering to make the search more efficient.
	inclusions := make([]bool, len(pts))
	var included bool
	curSpan := 0
	for i := 0; i < len(pts); i++ {
		curSpan, included = d.seekSpan(pts[i], spans, curSpan)
		inclusions[i] = included
	}

	// Convert to JSON
	inclusionsJSON, err := json.Marshal(inclusions)
	if err != nil {
		return nil, err
	}
	return inclusionsJSON, nil
}

type subvolumesT struct {
	NumTotalBlocks  int32
	NumActiveBlocks int32
	NumSubvolumes   int32
	ROI             dvid.ChunkExtents3d
	Subvolumes      []subvolumeT
}

type subvolumeT struct {
	dvid.Extents3d
	dvid.ChunkExtents3d
	TotalBlocks  int32
	ActiveBlocks int32
}

type layerT struct {
	activeBlocks []*indexRLE
	minX, maxX   int32
	minY, maxY   int32
	minZ, maxZ   int32
}

func (d *Data) newLayer(z0, z1 int32) *layerT {
	return &layerT{
		[]*indexRLE{},
		math.MaxInt32, math.MinInt32,
		math.MaxInt32, math.MinInt32,
		z0, z1,
	}
}

func (layer *layerT) extend(rle *indexRLE) {
	layer.activeBlocks = append(layer.activeBlocks, rle)

	y := rle.start.Value(1)
	x0 := rle.start.Value(0)
	x1 := x0 + int32(rle.span) - 1

	if layer.minX > x0 {
		layer.minX = x0
	}
	if layer.maxX < x1 {
		layer.maxX = x1
	}
	if layer.minY > y {
		layer.minY = y
	}
	if layer.maxY < y {
		layer.maxY = y
	}
}

func getPadding(x0, x1, batchsize int32) (leftPad, rightPad int32) {
	var padding int32
	overage := (x1 - x0 + 1) % batchsize
	if overage == 0 {
		padding = 0
	} else {
		padding = batchsize - overage
	}
	leftPad = padding / 2
	rightPad = padding - leftPad
	return
}

// For a slice of RLEs, return the min and max block Y coordinate
func getYRange(blocks []*indexRLE) (minY, maxY int32, found bool) {
	minY = math.MaxInt32
	maxY = math.MinInt32
	for _, rle := range blocks {
		if rle.start[1] < minY {
			minY = rle.start[1]
		}
		if rle.start[1] > maxY {
			maxY = rle.start[1]
		}
		found = true
	}
	return
}

// Return range of x for all spans within the given range of y
func getXRange(blocks []*indexRLE, minY, maxY int32) (minX, maxX int32, actives []*indexRLE) {
	minX = math.MaxInt32
	maxX = math.MinInt32
	actives = []*indexRLE{}
	for i, rle := range blocks {
		if rle.start[1] >= minY && rle.start[1] <= maxY {
			if rle.start[0] < minX {
				minX = rle.start[0]
			}
			x1 := rle.start[0] + int32(rle.span) - 1
			if x1 > maxX {
				maxX = x1
			}
			actives = append(actives, blocks[i])
		}
	}
	return
}

func findActives(blocks []*indexRLE, minX, maxX int32) int32 {
	var numActive int32
	for _, rle := range blocks {
		spanBeg := rle.start[0]
		if spanBeg > maxX {
			continue
		}
		spanEnd := spanBeg + int32(rle.span) - 1
		if spanEnd < minX {
			continue
		}
		x0 := dvid.MaxInt32(minX, spanBeg)
		x1 := dvid.MinInt32(maxX, spanEnd)
		numActive += x1 - x0 + 1
	}
	return numActive
}

func findXHoles(blocks []*indexRLE, minX, maxX int32) (bestBeg, bestEnd int32, found bool) {
	nx := maxX - minX + 1
	used := make([]bool, nx, nx)

	for _, rle := range blocks {
		spanBeg := rle.start[0]
		if spanBeg > maxX {
			continue
		}
		spanEnd := spanBeg + int32(rle.span) - 1
		if spanEnd < minX {
			continue
		}
		x0 := dvid.MaxInt32(minX, spanBeg)
		x1 := dvid.MinInt32(maxX, spanEnd)

		for x := x0; x <= x1; x++ {
			i := x - minX
			used[i] = true
		}
	}

	// See if there are holes.
	var holeSize, bestSize int32
	var holeBeg, holeEnd int32
	var inHole bool
	for x := minX; x <= maxX; x++ {
		i := x - minX
		if !used[i] {
			if inHole {
				holeEnd = x
				holeSize++
			} else {
				inHole = true
				holeBeg = x
				holeEnd = x
				holeSize = 1
			}
		} else {
			inHole = false
		}
		if holeSize > bestSize {
			bestSize = holeSize
			bestBeg = holeBeg
			bestEnd = holeEnd
		}
	}
	if bestSize > 0 {
		found = true
	}
	return
}

func totalBlocks(minCorner, maxCorner dvid.ChunkPoint3d) int32 {
	dx := maxCorner[0] - minCorner[0] + 1
	dy := maxCorner[1] - minCorner[1] + 1
	dz := maxCorner[2] - minCorner[2] + 1
	return dx * dy * dz
}

// Adds subvolumes based on given extents for a layer.
func (d *Data) addSubvolumes(layer *layerT, subvolumes *subvolumesT, batchsize int32, merge bool) {
	// mergeThreshold := batchsize * batchsize * batchsize / 10
	minY, maxY, found := getYRange(layer.activeBlocks)
	if !found {
		return
	}
	subvolumes.ROI.ExtendDim(1, minY)
	subvolumes.ROI.ExtendDim(1, maxY)
	dy := maxY - minY + 1
	yleft := dy % batchsize

	begY := minY
	for {
		if begY > maxY {
			break
		}
		endY := begY + batchsize - 1
		if yleft > 0 {
			endY++
			yleft--
		}
		minX, maxX, actives := getXRange(layer.activeBlocks, begY, endY)
		if len(actives) > 0 {
			subvolumes.ROI.ExtendDim(0, minX)
			subvolumes.ROI.ExtendDim(0, maxX)

			dx := maxX - minX + 1
			xleft := dx % batchsize

			// Create subvolumes along this row.
			begX := minX
			for {
				if begX > maxX {
					break
				}
				endX := begX + batchsize - 1
				if xleft > 0 {
					endX++
					xleft--
				}
				minCorner := dvid.ChunkPoint3d{begX, begY, layer.minZ}
				maxCorner := dvid.ChunkPoint3d{endX, endY, layer.maxZ}
				holeBeg, holeEnd, found := findXHoles(actives, begX, endX)
				var numActive, numTotal int32
				if found && merge {
					// MinCorner stays same since we are extended in X
					if holeBeg-1 >= begX {
						lastI := len(subvolumes.Subvolumes) - 1
						subvolume := subvolumes.Subvolumes[lastI]
						lastCorner := dvid.ChunkPoint3d{holeBeg - 1, endY, layer.maxZ}
						subvolume.MaxPoint = lastCorner.MinPoint(d.BlockSize).(dvid.Point3d)
						subvolume.MaxChunk = lastCorner
						numTotal = totalBlocks(minCorner, lastCorner)
						numActive = findActives(actives, begX, holeBeg-1)
						subvolume.TotalBlocks += numTotal
						subvolume.ActiveBlocks += numActive
						subvolumes.Subvolumes[lastI] = subvolume
					}
					begX = holeEnd + 1
				} else {
					numTotal = totalBlocks(minCorner, maxCorner)
					numActive = findActives(actives, begX, endX)
					subvolume := subvolumeT{
						Extents3d: dvid.Extents3d{
							minCorner.MinPoint(d.BlockSize).(dvid.Point3d),
							maxCorner.MaxPoint(d.BlockSize).(dvid.Point3d),
						},
						ChunkExtents3d: dvid.ChunkExtents3d{minCorner, maxCorner},
						TotalBlocks:    numTotal,
						ActiveBlocks:   numActive,
					}
					subvolumes.Subvolumes = append(subvolumes.Subvolumes, subvolume)
					begX = endX + 1
				}
				subvolumes.NumActiveBlocks += numActive
				subvolumes.NumTotalBlocks += numTotal
			}
		}
		begY = endY + 1
	}
}

// Partition returns JSON of differently sized subvolumes that attempt to distribute
// the number of active blocks per subvolume.
func (d *Data) Partition(ctx storage.Context, batchsize int32) ([]byte, error) {
	// Partition Z as perfectly as we can.
	dz := d.MaxZ - d.MinZ + 1
	zleft := dz % batchsize

	// Adjust Z range
	layerBegZ := d.MinZ
	layerEndZ := layerBegZ + batchsize - 1

	// Iterate through blocks in ascending Z, calculating active extents and subvolume coverage.
	// Keep track of current layer = batchsize of blocks in Z.
	var subvolumes subvolumesT
	subvolumes.Subvolumes = []subvolumeT{}
	subvolumes.ROI.MinChunk[2] = d.MinZ
	subvolumes.ROI.MaxChunk[2] = d.MaxZ

	layer := d.newLayer(layerBegZ, layerEndZ)

	db, err := storage.SmallDataStore()
	if err != nil {
		return nil, err
	}
	merge := true
	err = db.ProcessRange(ctx, minIndexRLE.Bytes(), maxIndexRLE.Bytes(), &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Unable to recover roi RLE from chunk key %v: %s\n", chunk.K, err.Error())
			return
		}
		index := new(indexRLE)
		if err = index.IndexFromBytes(indexBytes); err != nil {
			dvid.Errorf("Unable to get indexRLE out of []byte encoding: %s\n", err.Error())
		}

		// If we are in new layer, process last one.
		z := index.start.Value(2)
		if z > layerEndZ {
			// Process last layer
			dvid.Debugf("Computing subvolumes in layer with Z %d -> %d (dz %d)\n",
				layer.minZ, layer.maxZ, layer.maxZ-layer.minZ+1)
			d.addSubvolumes(layer, &subvolumes, batchsize, merge)

			// Init variables for next layer
			layerBegZ = layerEndZ + 1
			layerEndZ += batchsize
			if zleft > 0 {
				layerEndZ++
				zleft--
			}
			layer = d.newLayer(layerBegZ, layerEndZ)
		}

		// Check this block against current layer extents
		layer.extend(index)
	})

	// Process last incomplete layer if there is one.
	if len(layer.activeBlocks) > 0 {
		dvid.Debugf("Computing subvolumes for final layer Z %d -> %d (dz %d)\n",
			layer.minZ, layer.maxZ, layer.maxZ-layer.minZ+1)
		d.addSubvolumes(layer, &subvolumes, batchsize, merge)
	}
	subvolumes.NumSubvolumes = int32(len(subvolumes.Subvolumes))

	// Encode as JSON
	jsonBytes, err := json.MarshalIndent(subvolumes, "", "    ")
	if err != nil {
		return nil, err
	}
	return jsonBytes, err
}

// Adds subvolumes using simple algorithm centers fixed-sized subvolumes across active blocks.
func (d *Data) addSubvolumesGrid(layer *layerT, subvolumes *subvolumesT, batchsize int32) {
	minY, maxY, found := getYRange(layer.activeBlocks)
	if !found {
		return
	}
	subvolumes.ROI.ExtendDim(1, minY)
	subvolumes.ROI.ExtendDim(1, maxY)
	dy := maxY - minY + 1
	yleft := dy % batchsize

	addYtoBeg := yleft / 2
	addYtoEnd := yleft - addYtoBeg
	begY := minY - addYtoBeg
	endY := maxY + addYtoEnd

	// Iterate through Y, block by block, and determine the X range.  Then center subvolumes
	// along that X.
	for y0 := begY; y0 <= endY; y0 += batchsize {
		y1 := y0 + batchsize - 1
		minX, maxX, actives := getXRange(layer.activeBlocks, y0, y1)
		if len(actives) == 0 {
			continue
		}
		subvolumes.ROI.ExtendDim(0, minX)
		subvolumes.ROI.ExtendDim(0, maxX)

		// For this row of subvolumes along X, position them to encompass the X range.
		dx := maxX - minX + 1
		xleft := dx % batchsize

		addXtoBeg := xleft / 2
		addXtoEnd := xleft - addXtoBeg
		begX := minX - addXtoBeg
		endX := maxX + addXtoEnd

		// Create the subvolumes along X
		for x0 := begX; x0 <= endX; x0 += batchsize {
			x1 := x0 + batchsize - 1
			minCorner := dvid.ChunkPoint3d{x0, y0, layer.minZ}
			maxCorner := dvid.ChunkPoint3d{x1, y1, layer.maxZ}

			numTotal := totalBlocks(minCorner, maxCorner)
			numActive := findActives(actives, x0, x1)
			if numActive > 0 {
				subvolume := subvolumeT{
					Extents3d: dvid.Extents3d{
						minCorner.MinPoint(d.BlockSize).(dvid.Point3d),
						maxCorner.MaxPoint(d.BlockSize).(dvid.Point3d),
					},
					ChunkExtents3d: dvid.ChunkExtents3d{minCorner, maxCorner},
					TotalBlocks:    numTotal,
					ActiveBlocks:   numActive,
				}
				subvolumes.Subvolumes = append(subvolumes.Subvolumes, subvolume)
			}
			subvolumes.NumActiveBlocks += numActive
			subvolumes.NumTotalBlocks += numTotal
		}
	}
}

// SimplePartition returns JSON of identically sized subvolumes arranged over ROI
func (d *Data) SimplePartition(ctx storage.Context, batchsize int32) ([]byte, error) {
	// Partition Z as perfectly as we can.
	dz := d.MaxZ - d.MinZ + 1
	zleft := dz % batchsize

	// Adjust Z range
	addZtoTop := zleft / 2
	layerBegZ := d.MinZ - addZtoTop
	layerEndZ := layerBegZ + batchsize - 1

	// Iterate through blocks in ascending Z, calculating active extents and subvolume coverage.
	// Keep track of current layer = batchsize of blocks in Z.
	var subvolumes subvolumesT
	subvolumes.Subvolumes = []subvolumeT{}
	subvolumes.ROI.MinChunk[2] = d.MinZ
	subvolumes.ROI.MaxChunk[2] = d.MaxZ

	layer := d.newLayer(layerBegZ, layerEndZ)

	db, err := storage.SmallDataStore()
	if err != nil {
		return nil, err
	}
	err = db.ProcessRange(ctx, minIndexRLE.Bytes(), maxIndexRLE.Bytes(), &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Unable to recover roi RLE from chunk key %v: %s\n", chunk.K, err.Error())
			return
		}
		index := new(indexRLE)
		if err = index.IndexFromBytes(indexBytes); err != nil {
			dvid.Errorf("Unable to get indexRLE out of []byte encoding: %s\n", err.Error())
		}

		// If we are in new layer, process last one.
		z := index.start.Value(2)
		if z > layerEndZ {
			// Process last layer
			dvid.Debugf("Computing subvolumes in layer with Z %d -> %d (dz %d)\n", layer.minZ, layer.maxZ, layer.maxZ-layer.minZ+1)
			d.addSubvolumesGrid(layer, &subvolumes, batchsize)

			// Init variables for next layer
			layerBegZ = layerEndZ + 1
			layerEndZ += batchsize
			layer = d.newLayer(layerBegZ, layerEndZ)
		}

		// Extend current layer by the current block
		layer.extend(index)
	})

	// Process last incomplete layer if there is one.
	if len(layer.activeBlocks) > 0 {
		dvid.Debugf("Computing subvolumes for final layer Z %d -> %d (dz %d)\n",
			layer.minZ, layer.maxZ, layer.maxZ-layer.minZ+1)
		d.addSubvolumesGrid(layer, &subvolumes, batchsize)
	}
	subvolumes.NumSubvolumes = int32(len(subvolumes.Subvolumes))

	// Encode as JSON
	jsonBytes, err := json.MarshalIndent(subvolumes, "", "    ")
	if err != nil {
		return nil, err
	}
	return jsonBytes, err
}

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprintf(HelpMessage, DefaultBlockSize)
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Criticalf("roi.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("Unknown command.  Data '%s' [%s] does not support '%s' command.",
		d.DataName(), d.TypeName(), request.TypeCommand())
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	_, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n", d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API specification")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return
	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return
	default:
	}

	// Get the key and process request
	var comment string
	command := parts[3]
	method := strings.ToLower(r.Method)
	switch command {
	case "roi":
		switch method {
		case "get":
			jsonBytes, err := Get(storeCtx)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
			comment = fmt.Sprintf("HTTP GET ROI %q: %d bytes\n", d.DataName(), len(jsonBytes))
		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			err = d.PutJSON(storeCtx, data)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			comment = fmt.Sprintf("HTTP POST ROI %q: %d bytes\n", d.DataName(), len(data))
		case "delete":
			if err := d.Delete(storeCtx); err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			comment = fmt.Sprintf("HTTP DELETE ROI %q\n", d.DataName())
		}
	case "mask":
		if method != "get" {
			server.BadRequest(w, r, "ROI mask only supports GET")
			return
		}
		if len(parts) < 7 {
			server.BadRequest(w, r, "%q must be followed by shape/size/offset", command)
			return
		}
		shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
		planeStr := dvid.DataShapeString(shapeStr)
		plane, err := planeStr.DataShape()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		switch plane.ShapeDimensions() {
		case 3:
			subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			data, err := d.GetMask(storeCtx, subvol)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-type", "application/octet-stream")
			_, err = w.Write(data)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
		default:
			server.BadRequest(w, r, "Currently only 3d masks ('0_1_2' shape) is supported")
			return
		}
	case "ptquery":
		switch method {
		case "get":
			server.BadRequest(w, r, "ptquery requires POST with list of points")
			return
		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			jsonBytes, err := d.PointQuery(storeCtx, data)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
			comment = fmt.Sprintf("HTTP POST ptquery '%s'\n", d.DataName())
		}
	case "partition":
		if method != "get" {
			server.BadRequest(w, r, "partition only supports GET request")
			return
		}
		queryValues := r.URL.Query()
		batchsizeStr := queryValues.Get("batchsize")
		batchsize, err := strconv.Atoi(batchsizeStr)
		if err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error reading batchsize query string: %s", err.Error()))
			return
		}

		var jsonBytes []byte
		optimizedStr := queryValues.Get("optimized")
		if optimizedStr == "true" || optimizedStr == "on" {
			dvid.Infof("Perform optimized partitioning into subvolumes using batchsize %d\n", batchsize)
			jsonBytes, err = d.Partition(storeCtx, int32(batchsize))
		} else {
			dvid.Infof("Performing simple partitioning into subvolumes using batchsize %d\n", batchsize)
			jsonBytes, err = d.SimplePartition(storeCtx, int32(batchsize))
		}
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		comment = fmt.Sprintf("HTTP partition '%s' with batch size %d\n",
			d.DataName(), batchsize)
	default:
		w.Header().Set("Content-Type", "text/plain")
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	timedLog.Infof(comment)
}
