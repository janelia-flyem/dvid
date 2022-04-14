package ngprecomputed

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"io/ioutil"
	"math"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/semver"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"gocloud.dev/gcp"
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in ngprecomputed: %v\n", err)
	}
	e := Engine{"ngprecomputed", "Neuroglancer precomputed", ver}
	storage.RegisterEngine(e)
}

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) IsDistributed() bool {
	return false
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

// NewStore returns a Neuroglancer Precomputed store. The passed Config must contain "path" setting.
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newStore(config)
}

// ---- TestableEngine interface implementation -------

// AddTestConfig sets a public ng precomputed volume (requires internet access).
func (e Engine) AddTestConfig(backend *storage.Backend) (storage.Alias, error) {
	alias := storage.Alias("ngprecomputed")
	if backend.Stores == nil {
		backend.Stores = make(map[storage.Alias]dvid.StoreConfig)
	}
	tc := map[string]interface{}{
		"ref":     "gs://neuroglancer-janelia-flyem-hemibrain/emdata/raw/jpeg",
		"testing": true,
	}
	var c dvid.Config
	c.SetAll(tc)
	backend.Stores[alias] = dvid.StoreConfig{Config: c, Engine: "ngprecomputed"}
	return alias, nil
}

// Delete implements the TestableEngine interface by providing a way to dispose
// of the testable ngprecomputed.
func (e Engine) Delete(config dvid.StoreConfig) error {
	return nil
}

func parseConfig(config dvid.StoreConfig) (ref string, testing bool, instance, uuid string, err error) {
	c := config.GetAll()

	v, found := c["ref"]
	if !found {
		err = fmt.Errorf("%q must be specified for ngprecomputed configuration", "ref")
		return
	}
	var ok bool
	ref, ok = v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string (%v)", "ref", v)
		return
	}
	v, found = c["instance"]
	if found {
		instance, ok = v.(string)
		if !ok {
			err = fmt.Errorf("%q setting must be a string (%v)", "instance", v)
			return
		}
	}
	v, found = c["uuid"]
	if found {
		uuid, ok = v.(string)
		if !ok {
			err = fmt.Errorf("%q setting must be a string (%v)", "uuid", v)
			return
		}
	}
	v, found = c["testing"]
	if found {
		testing, ok = v.(bool)
		if !ok {
			err = fmt.Errorf("%q setting must be a bool (%v)", "testing", v)
			return
		}
	}
	return
}

func (e Engine) newStore(config dvid.StoreConfig) (*ngStore, bool, error) {
	ref, _, instance, uuid, err := parseConfig(config)
	if err != nil {
		return nil, false, err
	}

	// opt, err := getOptions(config.Config)
	// if err != nil {
	// 	return nil, false, err
	// }

	dvid.Infof("Trying to open NG-Precomputed store @ %q ...\n", ref)
	ctx := context.Background()
	var bucket *blob.Bucket

	if strings.HasPrefix(ref, "s3://") {
		// This relies on the non-GCS-specific blob API
		// and requires that the user:
		// A: Have set up AWS credentials in ways gocloud can find them (see the "aws config" command)
		// B: Have set the AWS_REGION environment variable (usually to us-east-2)
		var err error
		bucket, err = blob.OpenBucket(ctx, ref)
		if err != nil {
			fmt.Printf("Can't open NG precomputed @ %q: %v\n", ref, err)
			return nil, false, err
		}
		pathpart := strings.TrimPrefix(ref, "s3://")
		pathpart = strings.SplitN(pathpart, "/", 2)[1] // Remove the bucket name
		bucket = blob.PrefixedBucket(bucket, pathpart)
	} else	{
		// In this case default to Google Store authentication as DVID did before
		// See https://cloud.google.com/docs/authentication/production
		// for more info on alternatives.
		var err error
		creds, err := gcp.DefaultCredentials(ctx)
		if err != nil {
			return nil, false, err
		}

		// Create an HTTP client.
		// This example uses the default HTTP transport and the credentials
		// created above.
		client, err := gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, false, err
		}

		// Create a *blob.Bucket.
		bucket, err = gcsblob.OpenBucket(ctx, client, ref, nil)
		if err != nil {
			fmt.Printf("Can't open NG precomputed @ %q: %v\n", ref, err)
			return nil, false, err
		}
	}

	data, err := bucket.ReadAll(ctx, "info")
	if err != nil {
		return nil, false, err
	}

	// Remove URI protocol if present so path-parsing logic won't need to deal with it
	var ngref string
	if strings.HasPrefix(ref, "s3://") {
		ngref = strings.TrimPrefix(ref, "s3://")
	} else {
		ngref = ref
	}

	ng := &ngStore{
		ref:        ngref,
		bucket:     bucket,
		instance:   instance,
		uuid:       uuid,
		shardIndex: make(map[string]*shardT),
	}
	if err := json.Unmarshal(data, &(ng.vol)); err != nil {
		return nil, false, err
	}
	if err := ng.initialize(); err != nil {
		return nil, false, err
	}
	dvid.Infof("Loaded %q [%s] @ %q ...\n", ng.vol.StoreType, ng.vol.VolumeType, ref)
	return ng, false, nil
}

func gzipUncompress(in []byte) (out []byte, err error) {
	gzipIn := bytes.NewBuffer(in)
	var zr *gzip.Reader
	zr, err = gzip.NewReader(gzipIn)
	if err != nil {
		err = fmt.Errorf("can't uncompress gzip data: %v", err)
		return
	}
	out, err = ioutil.ReadAll(zr)
	if err != nil {
		err = fmt.Errorf("can't read gzip data: %v", err)
		return
	}
	zr.Close()
	return
}

// chunkSize = expected chunk volume, e.g., 64 x 64 x 64
// clippedSize = if on edge of image volume, this is actual size of clipped chunk
func jpegUncompress(chunkSize, clippedSize dvid.Point3d, in []byte) (out []byte, err error) {
	b := bytes.NewBuffer(in)
	imgdata, err := jpeg.Decode(b)
	if err != nil {
		return nil, err
	}
	grayscale, ok := imgdata.(*image.Gray)
	if !ok {
		err = fmt.Errorf("jpeg uncompression failed because jpeg was not image.Gray")
		return
	}
	jpegData := grayscale.Pix
	chunkVoxels := chunkSize.Prod()
	dx := int32(grayscale.Rect.Dx())
	dy := int32(grayscale.Rect.Dy())
	jpegVoxels := int64(dx * dy)
	if jpegVoxels == chunkVoxels {
		return jpegData, nil
	}
	apparentY := dy / clippedSize[2]
	if dy%clippedSize[2] != 0 {
		err = fmt.Errorf("Unexpected JPEG image size of %d x %d for clipped block size %s", dx, dy, clippedSize)
		return
	}
	clippedBytes := clippedSize.Prod()
	jpegBytes := int64(len(jpegData))
	if clippedBytes > jpegBytes {
		err = fmt.Errorf("JPEG data (%d bytes) is less than clipped volume size: %d bytes", jpegBytes, clippedBytes)
		return
	}
	inflated := make([]byte, chunkVoxels)

	var dst, src, x, y, z int32
	defer func() {
		if r := recover(); r != nil {
			dvid.Errorf("Panic in JPEG chunk uncompression at (%d,%d,%d) src %d -> dst %d\n", x, y, z, src, dst)
			dvid.Errorf("JPEG Gray image: Stride %d, Rect %v, Bytes %d\n", grayscale.Stride, grayscale.Rect, len(jpegData))
			dvid.Errorf("Apparent size of Y in JPEG: %d\n", apparentY)
			err = fmt.Errorf("Unable to uncompress JPEG and inflate to chunk size %s", chunkSize)
		}
	}()
	for z = 0; z < clippedSize[2]; z++ {
		for y = 0; y < clippedSize[1]; y++ {
			dst = z*chunkSize[1]*chunkSize[0] + y*chunkSize[0]
			src = z*apparentY*int32(grayscale.Stride) + y*dx
			for x = 0; x < clippedSize[0]; x++ {
				inflated[dst] = jpegData[src]
				src++
				dst++
			}
		}
	}
	return inflated, nil
}

func jpegCompress(chunkSize dvid.Point3d, in []byte) (out []byte, err error) {
	nx := int(chunkSize[0])
	ny := int(chunkSize[1] * chunkSize[2])
	rect := image.Rectangle{image.Point{0, 0}, image.Point{nx, ny}}
	graydata := &image.Gray{Pix: []uint8(in), Stride: nx, Rect: rect}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, graydata, &jpeg.Options{Quality: dvid.DefaultJPEGQuality}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---- NG Precomputed implementation --------

type ngShard struct {
	FormatType    string `json:"@type"` // should be "neuroglancer_uint64_sharded_v1"
	Hash          string `json:"hash"`
	MinishardBits uint8  `json:"minishard_bits"`
	PreshiftBits  uint8  `json:"preshift_bits"`
	ShardBits     uint8  `json:"shard_bits"`
	IndexEncoding string `json:"minishard_index_encoding"` // "raw" or "gzip"
	DataEncoding  string `json:"data_encoding"`            // "raw" or "gzip"
}

type ngScale struct {
	ChunkSizes []dvid.Point3d `json:"chunk_sizes"`
	Encoding   string         `json:"encoding"`
	Key        string         `json:"key"`
	Resolution dvid.Point3d   `json:"resolution"`
	Sharding   ngShard        `json:"sharding"`
	Size       dvid.Point3d   `json:"size"`

	numBits       [3]uint8 // required bits per dimension precomputed on init
	maxBits       uint8    // max of required bits across dimensions
	minishardMask uint64   // bit mask for minishard bits in hashed chunk ID
	shardMask     uint64   // bit mask for shard bits in hashed chunk ID
	shardIndexEnd uint64   // where minishard indices begin in every file
}

type ngVolume struct {
	StoreType     string    `json:"@type"`     // must be "neuroglancer_multiscale_volume"
	VolumeType    string    `json:"type"`      // "image" or "segmentation"
	DataType      string    `json:"data_type"` // "uint8", ... "float32"
	NumChannels   int       `json:"num_channels"`
	Scales        []ngScale `json:"scales"`
	MeshDir       string    `json:"mesh"`               // optional if VolumeType == segmentation
	SkelDir       string    `json:"skeletons"`          // optional if VolumeType == segmentation
	LabelPropsDir string    `json:"segment_properties"` // optional if VolumeType == segmentation
}

type ngStore struct {
	ref      string
	vol      ngVolume
	bucket   *blob.Bucket
	instance string // if set, create ephemeral multi-scale instances with this base name
	uuid     string // if set, create ephemeral instance only in repo with this uuid

	// cached shard information
	shardIndex   map[string]*shardT // cache of shard filename to shard data
	shardIndexMu sync.RWMutex
}

type shardT struct {
	sync.RWMutex
	index      []byte // fixed-size shard index
	minishards map[uint64]map[uint64]valueLoc
}

type valueLoc struct {
	pos  uint64 // byte of value start relative to start of file
	size uint64 // size of value in bytes
}

// log2 returns the power of 2 necessary to cover the given value.
func log2(value int32) uint8 {
	var exp uint8
	pow := int32(1)
	for {
		if pow >= value {
			return exp
		}
		pow *= 2
		exp++
	}
}

func (ng *ngStore) initialize() error {
	//if ng.vol.StoreType != "neuroglancer_multiscale_volume" {
	//	return fmt.Errorf("NG Store volume type %q != neuroglancer_multiscale_volume", ng.vol.StoreType)
	//}
	if ng.vol.VolumeType != "image" {
		return fmt.Errorf("NG Store volume type %q, DVID driver can only handle 'image' type", ng.vol.VolumeType)
	}
	for n, scale := range ng.vol.Scales {
		if len(scale.ChunkSizes) > 1 {
			return fmt.Errorf("Scale %d has more than one chunk size, which is unsupported: %v", n, scale.ChunkSizes)
		}
		chunkSize := scale.ChunkSizes[0]

		// compute the num bits required for each dimension and the max bits
		var maxBits uint8
		for dim := uint8(0); dim < 3; dim++ {
			chunkDim := float64(scale.Size[dim]) / float64(chunkSize[dim])
			numBits := uint8(math.Ceil(math.Log2(chunkDim)))
			if numBits > maxBits {
				maxBits = numBits
			}
			ng.vol.Scales[n].numBits[dim] = numBits
			dvid.Infof("Scale %d, dim %d with size %d requires %d bits.\n", n, dim, scale.Size[dim], numBits)
		}
		dvid.Infof("Scale %d requires maximum %d bits for a dimension.\n", n, maxBits)
		ng.vol.Scales[n].maxBits = maxBits

		switch scale.Sharding.FormatType {
		case "":
			dvid.Infof("Scale %d uses unsharded.\n", n)
		case "neuroglancer_uint64_sharded_v1":
			// compute minishard and shard masks for the hashed chunk ID
			const on uint64 = 0xFFFFFFFFFFFFFFFF
			minishardBits := scale.Sharding.MinishardBits
			shardBits := scale.Sharding.ShardBits
			minishardOff := ((on >> minishardBits) << minishardBits)
			ng.vol.Scales[n].minishardMask = ^minishardOff
			excessBits := 64 - shardBits - minishardBits
			ng.vol.Scales[n].shardMask = (minishardOff << excessBits) >> excessBits
			ng.vol.Scales[n].shardIndexEnd = (1 << uint64(minishardBits)) * 16

			dvid.Infof("Scale %d minishard mask: %0*x\n", n, 16, ng.vol.Scales[n].minishardMask)
			dvid.Infof("Scale %d     shard mask: %0*x\n", n, 16, ng.vol.Scales[n].shardMask)
		default:
			return fmt.Errorf("Scale %d has unexpected shard type: %s", n, scale.Sharding.FormatType)
		}
	}
	return nil
}

// returns nil/nil if key does not exist.
func (ng *ngStore) read(key string) (data []byte, err error) {
	// timedLog := dvid.NewTimeLog()
	ctx := context.Background()
	r, err := ng.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			dvid.Errorf("Could not find unsharded block %q\n", key)
			return nil, nil
		}
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// returns nil/nil if key does not exist.
func (ng *ngStore) rangeRead(key string, offset, size uint64) (data []byte, err error) {
	// timedLog := dvid.NewTimeLog()
	ctx := context.Background()
	r, err := ng.bucket.NewRangeReader(ctx, key, int64(offset), int64(size), nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, nil
		}
		return nil, err
	}
	defer r.Close()
	bufslice := make([]byte, 0, size)
	buf := bytes.NewBuffer(bufslice)
	if _, err := io.Copy(buf, r); err != nil {
		return nil, err
	}
	// timedLog.Infof("Range read of object %q, offset %d, size %d", key, offset, size)
	return buf.Bytes(), nil
}

// ---- dvid.Store interface implementation -----------

func (ng *ngStore) Close() {
	if err := ng.bucket.Close(); err != nil {
		dvid.Errorf("Error on trying to close ngprecomputed (%s): %v\n", ng.ref, err)
	}
}

func (ng *ngStore) String() string {
	return fmt.Sprintf("neuroglancer precomputed store [%s] @ %s", ng.vol.VolumeType, ng.ref)
}

func (ng *ngStore) Equal(config dvid.StoreConfig) bool {
	ref, _, instance, uuid, err := parseConfig(config)
	if err != nil {
		return false
	}
	return ref == ng.ref && instance == ng.instance && uuid == ng.uuid
}

// ----- storage.AutoInstanceEngine interface implementation -------

// AutoInstances returns a name if ephemeral instances are to be automatically
// created, and if so, the "n" return value specifies the number of scales.
// If name is empty, no ephemeral instances are to be created.
func (ng *ngStore) AutoInstances() (name, uuid string, n int) {
	if ng.instance != "" {
		name = ng.instance
		uuid = ng.uuid
		n = len(ng.vol.Scales)
	}
	return
}

// ----- storage.GridStoreGetter ----

// Note that for a 3D morton code in a uint64, we could only allow 21 bits for each of
// the three dimensions.

// In the following, we list each potentially used bit with a hexadecimal letter,
// so a 21-bit X coordinate would like this:
// x = ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---4 3210 fedc ba98 7654 3210
// after spacing out by 2 to allow interleaved Y and Z bits, it becomes
// x = ---4 --3- -2-- 1--0 --f- -e-- d--c --b- -a-- 9--8 --7- -6-- 5--4 --3- -2-- 1--0

// For standard morton code, we'd shift Y << 1 and Z << 2 then OR the three resulting uint64.
// But most datasets aren't symmetrical in size across dimensions.

// Using compressed 3D morton code lets us use bits asymmetrically and conserve bits where some
// dimensions are smaller and those bits would always be zero.  Compressed morton code
// drops the bits that would be zero across all entries because that dimension is limited in
// size.  Say the X has max size 42,943 which requires only 16 bits (~64K) and would only use
// up to the "f" bit in the above diagram.  The bits corresponding to the most-significant
// "4", "3", "2", "1", and "0" bits would always be zero and therefore can be removed.

// This allows us to fit more data into the single uint64, as the following example shows
// with Z having a 24 bit range.

// Start with a X coordinate that for this example has a max of 16 bits
// x = ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- fedc ba98 7654 3210
// after spacing, note MSB "f" only has room for z bit since Y has dropped out.
// x = ---- ---- ---- ---- ---f -e-- d--c --b- -a-- 9--8 --7- -6-- 5--4 --3- -2-- 1--0

// Start with a Y coordinate that for this example has a max of 14 bits
// y = ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- --dc ba98 7654 3210
// after spacing with constant 2 bits since Y has smallest range
// y = ---- ---- ---- ---- ---- ---- d--c --b- -a-- 9--8 --7- -6-- 5--4 --3- -2-- 1--0
// after shifting by 1 for future interleaving to get morton code
// y = ---- ---- ---- ---- ---- ---d --c- -b-- a--9 --8- -7-- 6--5 --4- -3-- 2--1 --0-

// Start with a Z coordinate that for this example has a max of 24 bits
// z = ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- 7654 3210 fedc ba98 7654 3210
// after spacing out Z with 24 bits max; note compression of MSB due to x and y dropout
// z = ---- ---- ---- 7654 3210 f-e- d--c --b- -a-- 9--8 --7- -6-- 5--4 --3- -2-- 1--0
// after shifting by 2 for future interleaving
// z = ---- ---- --76 5432 10f- e-d- -c-- b--a --9- -8-- 7--6 --5- -4-- 3--2 --1- -0--

// Now if you OR the final x, y, and z you see no collisions.
// x = ---- ---- ---- ---- ---f -e-- d--c --b- -a-- 9--8 --7- -6-- 5--4 --3- -2-- 1--0
// y = ---- ---- ---- ---- ---- ---d --c- -b-- a--9 --8- -7-- 6--5 --4- -3-- 2--1 --0-
// z = ---- ---- --76 5432 10f- e-d- -c-- b--a --9- -8-- 7--6 --5- -4-- 3--2 --1- -0--

// While the above was the simplest way for me to understand compressed morton codes,
// the algorithm can be implemented more simply by iteratively going bit by bit
// from LSB to MSB and keeping track of the interleaved output bit.

func (ng *ngStore) mortonCode(scale *ngScale, blockCoord dvid.ChunkPoint3d) (mortonCode uint64) {
	var coords [3]uint64
	for dim := uint8(0); dim < 3; dim++ {
		coords[dim] = uint64(blockCoord[dim])
	}

	var outBit uint8
	for curBit := uint8(0); curBit < scale.maxBits; curBit++ {
		for dim := uint8(0); dim < 3; dim++ {
			if curBit < scale.numBits[dim] {
				// set mortonCode bit position outBit to value of coord[dim] curBit position
				bitVal := coords[dim] & 0x0000000000000001
				mortonCode |= (bitVal << outBit)
				outBit++
				coords[dim] = coords[dim] >> 1
			}
		}
	}
	// dvid.Infof("Morton code for chunk %s: %x\n", blockCoord, mortonCode)
	return
}

func (ng *ngStore) calcShard(scale *ngScale, blockCoord dvid.ChunkPoint3d) (fname string, minishard, chunkID uint64, err error) {
	chunkID = ng.mortonCode(scale, blockCoord)
	hashedID := chunkID >> scale.Sharding.PreshiftBits
	switch scale.Sharding.Hash {
	case "identity":
		// no-op
	case "murmurhash3_x86_128":
		// TODO -- implement this hashing
		err = fmt.Errorf("unimplemented hash method for shard: %q", scale.Sharding.Hash)
		return
	default:
		err = fmt.Errorf("unimplemented hash method for shard: %q", scale.Sharding.Hash)
		return
	}
	minishard = hashedID & scale.minishardMask
	shard := uint32((hashedID & scale.shardMask) >> scale.Sharding.MinishardBits)
	shardPadding := uint8(1)
	if scale.Sharding.ShardBits > 4 {
		shardPadding = 1 + (scale.Sharding.ShardBits-1)/4
	}
	fname = fmt.Sprintf("%s/%0*x.shard", scale.Key, shardPadding, shard)
	return
}

func (ng *ngStore) getMinishardMap(scale *ngScale, shardFile string, minishard uint64) (minishardMap map[uint64]valueLoc, err error) {
	// get cached or load remote shard index
	ng.shardIndexMu.RLock()
	shard, found := ng.shardIndex[shardFile]
	ng.shardIndexMu.RUnlock()
	if !found {
		shard, err = ng.loadShardIndex(scale, shardFile)
		if err != nil {
			return
		}
		if shard == nil {
			return nil, nil
		}
		ng.shardIndexMu.Lock()
		ng.shardIndex[shardFile] = shard
		ng.shardIndexMu.Unlock()
	}

	// get cached or load remote minishard map
	shard.RLock()
	minishardMap, found = shard.minishards[minishard]
	shard.RUnlock()
	if !found {
		minishardMap, err = ng.loadMinishardMap(scale, shardFile, shard, minishard)
		if err != nil {
			return
		}
		shard.Lock()
		shard.minishards[minishard] = minishardMap
		shard.Unlock()
	}
	return
}

func (ng *ngStore) loadShardIndex(scale *ngScale, shardFile string) (shard *shardT, err error) {
	timedLog := dvid.NewTimeLog()

	var shardData []byte
	shardData, err = ng.rangeRead(shardFile, 0, scale.shardIndexEnd)
	if err != nil {
		return
	}
	if shardData == nil {
		timedLog.Infof("shard file %q doesn't seem to exist", shardFile)
		return nil, nil
	}
	shard = &shardT{
		index:      shardData,
		minishards: make(map[uint64]map[uint64]valueLoc),
	}
	// timedLog.Infof("loaded shard index from object %q", shardFile)
	return
}

func (ng *ngStore) loadMinishardMap(scale *ngScale, shardFile string, shard *shardT, minishard uint64) (minishardMap map[uint64]valueLoc, err error) {
	// timedLog := dvid.NewTimeLog()

	pos := minishard * 16
	begByte := binary.LittleEndian.Uint64(shard.index[pos:pos+8]) + scale.shardIndexEnd
	endByte := binary.LittleEndian.Uint64(shard.index[pos+8:pos+16]) + scale.shardIndexEnd
	if endByte == begByte {
		minishardMap = map[uint64]valueLoc{}
		return
	}

	var minishardData, rawData []byte
	rawData, err = ng.rangeRead(shardFile, begByte, endByte-begByte)
	if err != nil {
		return nil, err
	}

	switch scale.Sharding.IndexEncoding {
	case "raw":
		minishardData = rawData
	case "gzip":
		minishardData, err = gzipUncompress(rawData)
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("unknown minishard_index_encoding: %s", scale.Sharding.IndexEncoding)
		return
	}

	indexSize := len(minishardData)
	if indexSize%24 != 0 {
		err = fmt.Errorf("minishard data length is %d bytes, which is not multiple of 24", indexSize)
		return
	}
	n := uint64(indexSize) / 24
	minishardMap = make(map[uint64]valueLoc, n)

	// Note: size is not delta encoded because the stored values are unsigned and not signed,
	// instead of just changing format to be signed int64 in neuroglancer precomputed format?
	var chunkID, offset uint64
	var idPos, offsetPos, sizePos, i, sizeAcc uint64
	offsetPos = n * 8
	sizePos = n * 16
	sizeAcc = scale.shardIndexEnd
	for i = 0; i < n; i++ {
		delta := binary.LittleEndian.Uint64(minishardData[idPos : idPos+8])
		if i == 0 {
			chunkID = delta
		} else {
			chunkID += delta
		}
		delta = binary.LittleEndian.Uint64(minishardData[offsetPos : offsetPos+8])
		if i == 0 {
			offset = delta
		} else {
			offset += delta
		}
		size := binary.LittleEndian.Uint64(minishardData[sizePos : sizePos+8])

		minishardMap[chunkID] = valueLoc{
			pos:  offset + sizeAcc,
			size: size,
		}
		sizeAcc += size
		idPos += 8
		offsetPos += 8
		sizePos += 8
	}
	// timedLog.Infof("loaded minishard map with %s encoding: %d entries, %d bytes", scale.Sharding.IndexEncoding, n, indexSize)
	return
}

// ---- Functions to satisfy the storage.GridStoreGetter interface ------

// GridProperties returns properties of a GridStore.
func (ng *ngStore) GridProperties(scaleLevel int) (props storage.GridProps, err error) {
	if scaleLevel < 0 || scaleLevel >= len(ng.vol.Scales) {
		err = fmt.Errorf("Cannot get grid properties for bad scale %d (only %d scales)", scaleLevel, len(ng.vol.Scales))
		return
	}
	scaleProps := ng.vol.Scales[scaleLevel]
	props.ChunkSize = scaleProps.ChunkSizes[0]
	props.VolumeSize = scaleProps.Size
	props.Encoding = scaleProps.Encoding
	props.Resolution = scaleProps.Resolution
	return
}

// GridGet returns a chunk of data given a block coordinate.  Automatically inflates
// partial blocks on edges so that chunks meet contract with other DVID systems that
// expect a full block.
func (ng *ngStore) GridGet(scaleLevel int, blockCoord dvid.ChunkPoint3d) (val []byte, err error) {
	timedLog := dvid.NewTimeLog()
	scale := &(ng.vol.Scales[scaleLevel])
	chunkSize := scale.ChunkSizes[0]
	minPt := blockCoord.MinPoint(chunkSize).(dvid.Point3d)
	maxPt := minPt.Add(chunkSize).(dvid.Point3d)
	outsideSize := minPt.Sub(scale.Size).(dvid.Point3d)
	// dvid.Infof("Block %s with chunk size %s\n", blockCoord, chunkSize)
	// dvid.Infof("Min pt %s, max pt %s\n", minPt, maxPt)
	// dvid.Infof("Scale size: %s\n", scale.Size)
	if outsideSize[0] >= 0 || outsideSize[1] >= 0 || outsideSize[2] >= 0 {
		timedLog.Infof("Chunk %s with start voxel %s is out of bounding box %s\n",
			blockCoord, minPt, scale.Size)
	}
	clippedSize := chunkSize.Duplicate().(dvid.Point3d)
	maxPtRestricted := maxPt.Duplicate().(dvid.Point3d) // A possibly clipped version of MaxPt, for filename construction
	for i := 0; i < 3; i++ {
		if maxPt[i] <= scale.Size[i] {
			clippedSize[i] = chunkSize[i]
			maxPtRestricted[i] = maxPt[i]
		} else {
			clippedSize[i] = scale.Size[i] - minPt[i]
			maxPtRestricted[i] = scale.Size[i]
		}
	}
	if clippedSize.Prod() != chunkSize.Prod() {
		dvid.Infof("Clipped size: %s\n", clippedSize)
	}

	switch scale.Sharding.FormatType {
	case "":
		key := fmt.Sprintf("%s/%d-%d_%d-%d_%d-%d", scale.Key, minPt[0], maxPtRestricted[0], minPt[1], maxPtRestricted[1], minPt[2], maxPtRestricted[2])
		val, err = ng.read(key)

	case "neuroglancer_uint64_sharded_v1":
		var shardFile string
		var minishard, chunkID uint64
		shardFile, minishard, chunkID, err = ng.calcShard(scale, blockCoord)
		if err != nil {
			return
		}

		var minishardMap map[uint64]valueLoc
		minishardMap, err = ng.getMinishardMap(scale, shardFile, minishard)
		if err != nil {
			return
		}
		if minishardMap == nil {
			dvid.Infof("No minishard map found in shard %q for minishard %d\n", shardFile, minishard)
			return nil, nil
		}

		loc, found := minishardMap[chunkID]
		var min, max uint64
		min = math.MaxUint64
		for key := range minishardMap {
			if key < min {
				min = key
			}
			if key > max {
				max = key
			}
		}
		if !found {
			return nil, nil
		}
		val, err = ng.rangeRead(shardFile, loc.pos, loc.size)

	default:
		return nil, fmt.Errorf("Scale %d has unexpected shard type: %s", scaleLevel, scale.Sharding.FormatType)
	}

	if err != nil {
		return
	}
	chunkVoxels := chunkSize.Prod()
	clippedVoxels := clippedSize.Prod()
	if chunkVoxels == clippedVoxels {
		return
	}
	var inflated []byte
	inflated, err = jpegUncompress(chunkSize, clippedSize, val)
	if err != nil {
		dvid.Errorf("Error in jpeg uncompression for block %s\n", blockCoord)
		return
	}
	return jpegCompress(chunkSize, inflated)
}

// GridGetVolume calls the given function with the results of retrived block data in an ordered or
// unordered fashion.  Missing blocks in the subvolume are not processed.
func (ng *ngStore) GridGetVolume(scaleLevel int, minBlock, maxBlock dvid.ChunkPoint3d, ordered bool, op *storage.BlockOp, f storage.BlockFunc) error {
	if ordered {
		return fmt.Errorf("ordered retrieval not implemented at this time")
	}
	ch := make(chan dvid.ChunkPoint3d)

	// Start concurrent processing routines to read each block and then pass it to given function.
	concurrency := 10
	wg := new(sync.WaitGroup)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for blockCoord := range ch {
				val, err := ng.GridGet(scaleLevel, blockCoord)
				if err != nil {
					dvid.Errorf("unable to get block %s in GridGetVolume: %v\n", blockCoord, err)
					continue
				}
				if val == nil {
					continue
				}
				if op != nil && op.Wg != nil {
					op.Wg.Add(1)
				}
				block := &storage.Block{
					BlockOp: op,
					Coord:   blockCoord,
					Value:   val,
				}
				if err := f(block); err != nil {
					dvid.Errorf("unable to perform op on block %s: %v\n", blockCoord, err)
				}
			}
			wg.Done()
		}()
	}

	// Calculate all the block coords in ZYX for this subvolume and send down channel.
	for z := minBlock.Value(0); z <= maxBlock.Value(0); z++ {
		for y := minBlock.Value(1); y <= maxBlock.Value(1); y++ {
			for x := minBlock.Value(2); x <= maxBlock.Value(2); x++ {
				ch <- dvid.ChunkPoint3d{x, y, z}
			}
		}
	}

	close(ch)
	wg.Wait()
	return nil
}
