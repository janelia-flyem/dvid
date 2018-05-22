package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/downres"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

var (
	labelsT datastore.TypeService
	testMu  sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if labelsT == nil {
		var err error
		labelsT, err = datastore.TypeServiceByName("labelmap")
		if err != nil {
			log.Fatalf("Can't get labelmap type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

type tuple [4]int32

var labelsROI = []tuple{
	tuple{3, 3, 2, 4}, tuple{3, 4, 2, 3}, tuple{3, 5, 3, 3},
	tuple{4, 3, 2, 5}, tuple{4, 4, 3, 4}, tuple{4, 5, 2, 4},
	//tuple{5, 2, 3, 4}, tuple{5, 3, 3, 3}, tuple{5, 4, 2, 3}, tuple{5, 5, 2, 2},
}

func labelsJSON() string {
	b, err := json.Marshal(labelsROI)
	if err != nil {
		return ""
	}
	return string(b)
}

func inroi(x, y, z int32) bool {
	for _, span := range labelsROI {
		if span[0] != z {
			continue
		}
		if span[1] != y {
			continue
		}
		if span[2] > x || span[3] < x {
			continue
		}
		return true
	}
	return false
}

// A slice of bytes representing 3d label volume always with first voxel at (0,0,0)
type testVolume struct {
	data []byte
	size dvid.Point3d
}

func newTestVolume(nx, ny, nz int32) *testVolume {
	return &testVolume{
		data: make([]byte, nx*ny*nz*8),
		size: dvid.Point3d{nx, ny, nz},
	}
}

// Add a label to a subvolume.
func (v *testVolume) addSubvol(origin, size dvid.Point3d, label uint64) {
	nx := v.size[0]
	nxy := nx * v.size[1]
	spanBytes := size[0] * 8
	buf := make([]byte, spanBytes)
	for x := int32(0); x < size[0]; x++ {
		binary.LittleEndian.PutUint64(buf[x*8:x*8+8], label)
	}
	for z := origin[2]; z < origin[2]+size[2]; z++ {
		for y := origin[1]; y < origin[1]+size[1]; y++ {
			i := (z*nxy + y*nx + origin[0]) * 8
			copy(v.data[i:i+spanBytes], buf)
		}
	}
}

// add binary blocks, check test volume is sufficient size.
func (v *testVolume) addBlocks(t *testing.T, blocks []labels.BinaryBlock, label uint64) {
	for _, block := range blocks {
		maxx := block.Offset[0] + block.Size[0]
		maxy := block.Offset[1] + block.Size[1]
		maxz := block.Offset[2] + block.Size[2]
		dvid.Infof("Adding block offset %s, size %s, # voxels %d\n", block.Offset, block.Size, len(block.Voxels))

		if maxx > v.size[0] {
			t.Fatalf("block at offset %s, size %s exceeded test volume size %s\n", block.Offset, block.Size, v.size)
		}
		if maxy > v.size[1] {
			t.Fatalf("block at offset %s, size %s exceeded test volume size %s\n", block.Offset, block.Size, v.size)
		}
		if maxz > v.size[2] {
			t.Fatalf("block at offset %s, size %s exceeded test volume size %s\n", block.Offset, block.Size, v.size)
		}
		if len(block.Voxels) != int(block.Size.Prod()) {
			t.Fatalf("binary block at offset %s, size %s has bad # voxels (%d)\n", block.Offset, block.Size, len(block.Voxels))
		}
		bi := 0
		for z := block.Offset[2]; z < maxz; z++ {
			for y := block.Offset[1]; y < maxy; y++ {
				for x := block.Offset[0]; x < maxx; x++ {
					if block.Voxels[bi] {
						if x == 16 && y == 40 && z == 8 {
							dvid.Infof("voxel found and is indeed on\n")
						}
						i := (z*v.size[0]*v.size[1] + y*v.size[0] + x) * 8
						binary.LittleEndian.PutUint64(v.data[i:i+8], label)
					}
					bi++
				}
			}
		}
	}
}

// downres assumes only binary volume of some label N or label 0.
func (v *testVolume) downres(scale uint8) {
	sz := int32(1 << scale)
	nx := v.size[0] >> scale
	ny := v.size[1] >> scale
	nz := v.size[2] >> scale

	var oldpos, x, y, z int32
	for z = 0; z < v.size[2]; z++ {
		newz := z >> scale
		for y = 0; y < v.size[1]; y++ {
			newy := y >> scale
			for x = 0; x < v.size[0]; x++ {
				label := binary.LittleEndian.Uint64(v.data[oldpos*8 : oldpos*8+8])
				oldpos++
				if label != 0 || (x%sz == 0 && y%sz == 0 && z%sz == 0) {
					newx := x >> scale
					newpos := newz*nx*ny + newy*nx + newx
					binary.LittleEndian.PutUint64(v.data[newpos*8:newpos*8+8], label)
				}
			}
		}
	}
	v.size[0] = nx
	v.size[1] = ny
	v.size[2] = nz
	v.data = v.data[:nx*ny*nz*8]
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func (v *testVolume) putMutable(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func (v *testVolume) get(t *testing.T, uuid dvid.UUID, name string, supervoxels bool) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2])
	if supervoxels {
		apiStr += "?supervoxels=true"
	}
	v.data = server.TestHTTP(t, "GET", apiStr, nil)
}

func (v *testVolume) getScale(t *testing.T, uuid dvid.UUID, name string, scale uint8, supervoxels bool) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0?scale=%d", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2], scale)
	if supervoxels {
		apiStr += "&supervoxels=true"
	}
	v.data = server.TestHTTP(t, "GET", apiStr, nil)
}

func (v *testVolume) getVoxel(pt dvid.Point3d) uint64 {
	nx := v.size[0]
	nxy := nx * v.size[1]
	i := (pt[2]*nxy + pt[1]*nx + pt[0]) * 8
	return binary.LittleEndian.Uint64(v.data[i : i+8])
}

func (v *testVolume) verifyLabel(t *testing.T, expected uint64, x, y, z int32) {
	pt := dvid.Point3d{x, y, z}
	label := v.getVoxel(pt)
	if label != expected {
		_, fn, line, _ := runtime.Caller(1)
		t.Errorf("Expected label %d at %s for first downres but got %d instead [%s:%d]\n", expected, pt, label, fn, line)
	}
}

func (v *testVolume) equals(v2 *testVolume) error {
	if !v.size.Equals(v2.size) {
		return fmt.Errorf("volume sizes are not equal")
	}
	if len(v.data) != len(v2.data) {
		return fmt.Errorf("data lengths are not equal")
	}
	var i uint64
	var x, y, z int32
	for z = 0; z < v.size[2]; z++ {
		for y = 0; y < v.size[1]; y++ {
			for x = 0; x < v.size[0]; x++ {
				val1 := binary.LittleEndian.Uint64(v.data[i : i+8])
				val2 := binary.LittleEndian.Uint64(v2.data[i : i+8])
				i += 8
				if val1 != val2 {
					return fmt.Errorf("For voxel (%d,%d,%d), found value %d != %d\n", x, y, z, val2, val1)
				}
			}
		}
	}
	return nil
}

func (v *testVolume) testBlock(t *testing.T, context string, bx, by, bz int32, data []byte) {
	if len(data) < int(DefaultBlockSize*DefaultBlockSize*DefaultBlockSize*8) {
		t.Fatalf("[%s] got bad uint64 array of len %d for block (%d, %d, %d)\n", context, len(data), bx, by, bz)
	}
	p := 0
	var x, y, z, numerrors int32
	for z = 0; z < DefaultBlockSize; z++ {
		vz := bz*DefaultBlockSize + z
		for y = 0; y < DefaultBlockSize; y++ {
			vy := by*DefaultBlockSize + y
			for x = 0; x < DefaultBlockSize; x++ {
				vx := bx*DefaultBlockSize + x
				i := (vz*v.size[0]*v.size[1] + vy*v.size[0] + vx) * 8
				label := binary.LittleEndian.Uint64(v.data[i : i+8])
				got := binary.LittleEndian.Uint64(data[p : p+8])
				if label != got && numerrors < 5 {
					t.Errorf("[%s] error block (%d,%d,%d) at voxel (%d,%d,%d): expected %d, got %d\n", context, bx, by, bz, vx, vy, vz, label, got)
					numerrors++
				}
				p += 8
			}
		}
	}
}

func (v *testVolume) testGetBlocks(t *testing.T, context string, uuid dvid.UUID, name, compression string, scale uint8) {
	apiStr := fmt.Sprintf("%snode/%s/%s/blocks/%d_%d_%d/0_0_0", server.WebAPIPath, uuid, name, v.size[0], v.size[1], v.size[2])
	var qstrs []string
	if compression != "" {
		qstrs = append(qstrs, "compression="+compression)
	}
	if scale > 0 {
		qstrs = append(qstrs, fmt.Sprintf("scale=%d", scale))
	}
	if len(qstrs) > 0 {
		apiStr += "?" + strings.Join(qstrs, "&")
	}
	data := server.TestHTTP(t, "GET", apiStr, nil)
	blockBytes := DefaultBlockSize * DefaultBlockSize * DefaultBlockSize * 8

	var b int
	for {
		// Get block coord + block size
		if b+16 > len(data) {
			t.Fatalf("Only got %d bytes back from block API call, yet next coord in span would put index @ %d\n", len(data), b+16)
		}
		x := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		y := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		z := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		n := int(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4

		var uncompressed []byte
		switch compression {
		case "uncompressed":
			uncompressed = data[b : b+n]
		case "", "lz4":
			uncompressed = make([]byte, blockBytes)
			if err := lz4.Uncompress(data[b:b+n], uncompressed); err != nil {
				t.Fatalf("Unable to uncompress LZ4 data (%s), %d bytes: %v\n", apiStr, n, err)
			}
		case "gzip", "blocks":
			gzipIn := bytes.NewBuffer(data[b : b+n])
			zr, err := gzip.NewReader(gzipIn)
			if err != nil {
				t.Fatalf("can't uncompress gzip block: %v\n", err)
			}
			uncompressed, err = ioutil.ReadAll(zr)
			if err != nil {
				t.Fatalf("can't uncompress gzip block: %v\n", err)
			}
			zr.Close()

			if compression == "blocks" {
				var block labels.Block
				if err = block.UnmarshalBinary(uncompressed); err != nil {
					t.Errorf("unable to deserialize label block (%d, %d, %d): %v\n", x, y, z, err)
				}
				uint64array, size := block.MakeLabelVolume()
				if !size.Equals(dvid.Point3d{DefaultBlockSize, DefaultBlockSize, DefaultBlockSize}) {
					t.Fatalf("got bad block size from deserialized block (%d, %d, %d): %s\n", x, y, z, size)
				}
				uncompressed = uint64array
			}
		}
		v.testBlock(t, context, x, y, z, uncompressed)
		b += n
		if b == len(data) {
			break
		}
	}
}

type mergeJSON string

func (mjson mergeJSON) send(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(string(mjson)))
}

func (mjson mergeJSON) sendErr(t *testing.T, uuid dvid.UUID, name string) error {
	apiStr := fmt.Sprintf("%snode/%s/%s/merge", server.WebAPIPath, uuid, name)
	_, err := server.TestHTTPError(t, "POST", apiStr, bytes.NewBufferString(string(mjson)))
	return err
}

func checkLabels(t *testing.T, text string, expected, got []byte) {
	if len(expected) != len(got) {
		t.Errorf("%s byte mismatch: expected %d bytes, got %d bytes\n", text, len(expected), len(got))
	}
	expectLabels, err := dvid.ByteToUint64(expected)
	if err != nil {
		t.Fatal(err)
	}
	gotLabels, err := dvid.ByteToUint64(got)
	if err != nil {
		t.Fatal(err)
	}
	for i, val := range gotLabels {
		if expectLabels[i] != val {
			t.Errorf("%s label mismatch found at index %d, expected %d, got %d\n", text, i, expectLabels[i], val)
			return
		}
	}
}

type labelVol struct {
	size      dvid.Point3d
	blockSize dvid.Point3d
	offset    dvid.Point3d
	name      string
	data      []byte

	nx, ny, nz int32

	startLabel uint64
}

func (vol *labelVol) makeLabelVolume(t *testing.T, uuid dvid.UUID, startLabel uint64) {
	if vol.startLabel == startLabel && vol.data != nil {
		return
	}

	vol.startLabel = startLabel

	vol.nx = vol.size[0] * vol.blockSize[0]
	vol.ny = vol.size[1] * vol.blockSize[1]
	vol.nz = vol.size[2] * vol.blockSize[2]

	vol.data = make([]byte, vol.numBytes())
	var x, y, z, v int32
	for z = 0; z < vol.nz; z++ {
		lz := z / 3
		for y = 0; y < vol.ny; y++ {
			ly := y / 3
			for x = 0; x < vol.nx; x++ {
				label := startLabel + uint64(x>>2+ly+lz)
				binary.LittleEndian.PutUint64(vol.data[v:v+8], label)
				v += 8
			}
		}
	}
	return
}

func (vol *labelVol) numBytes() int32 {
	return vol.nx * vol.ny * vol.nz * 8
}

// Create a new label volume and post it to the test datastore.
// Each voxel in volume has sequential labels in X, Y, then Z order.
func (vol *labelVol) postLabelVolume(t *testing.T, uuid dvid.UUID, compression, roi string, startLabel uint64) {
	vol.makeLabelVolume(t, uuid, startLabel)

	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, vol.name, vol.nx, vol.ny, vol.nz, vol.offset[0], vol.offset[1], vol.offset[2])
	query := true

	var data []byte
	var err error
	switch compression {
	case "lz4":
		apiStr += "?compression=lz4"
		compressed := make([]byte, lz4.CompressBound(vol.data))
		var outSize int
		if outSize, err = lz4.Compress(vol.data, compressed); err != nil {
			t.Fatal(err)
		}
		data = compressed[:outSize]
	case "gzip":
		apiStr += "?compression=gzip"
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err = gw.Write(vol.data); err != nil {
			t.Fatal(err)
		}
		data = buf.Bytes()
		if err = gw.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		data = vol.data
		query = false
	}
	if roi != "" {
		if query {
			apiStr += "&roi=" + roi
		} else {
			apiStr += "?roi=" + roi
		}
	}
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(data))
}

func (vol *labelVol) getLabelVolume(t *testing.T, uuid dvid.UUID, compression, roi string) []byte {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, vol.name, vol.nx, vol.ny, vol.nz, vol.offset[0], vol.offset[1], vol.offset[2])
	query := true
	switch compression {
	case "lz4":
		apiStr += "?compression=lz4"
	case "gzip":
		apiStr += "?compression=gzip"
	default:
		query = false
	}
	if roi != "" {
		if query {
			apiStr += "&roi=" + roi
		} else {
			apiStr += "?roi=" + roi
		}
	}
	data := server.TestHTTP(t, "GET", apiStr, nil)
	switch compression {
	case "lz4":
		uncompressed := make([]byte, vol.numBytes())
		if err := lz4.Uncompress(data, uncompressed); err != nil {
			t.Fatalf("Unable to uncompress LZ4 data (%s), %d bytes: %v\n", apiStr, len(data), err)
		}
		data = uncompressed
	case "gzip":
		buf := bytes.NewBuffer(data)
		gr, err := gzip.NewReader(buf)
		if err != nil {
			t.Fatalf("Error on gzip new reader: %v\n", err)
		}
		uncompressed, err := ioutil.ReadAll(gr)
		if err != nil {
			t.Fatalf("Error on reading gzip: %v\n", err)
		}
		if err = gr.Close(); err != nil {
			t.Fatalf("Error on closing gzip: %v\n", err)
		}
		data = uncompressed
	default:
	}
	if len(data) != int(vol.numBytes()) {
		t.Errorf("Expected %d uncompressed bytes from 3d labelmap GET.  Got %d instead.", vol.numBytes(), len(data))
	}
	return data
}

func (vol *labelVol) testGetLabelVolume(t *testing.T, uuid dvid.UUID, compression, roi string) []byte {
	data := vol.getLabelVolume(t, uuid, compression, roi)

	// run test to make sure it's same volume as we posted.
	var x, y, z, v int32
	for z = 0; z < vol.nz; z++ {
		lz := z / 3
		for y = 0; y < vol.ny; y++ {
			ly := y / 3
			for x = 0; x < vol.nx; x++ {
				label := vol.startLabel + uint64(x>>2+ly+lz)
				got := binary.LittleEndian.Uint64(data[v : v+8])
				if label != got {
					t.Errorf("Error on 3d GET compression (%q): expected %d, got %d\n", compression, label, got)
					goto foundError
				}
				v += 8
			}
		}
	}

foundError:

	checkLabels(t, "testing label volume", vol.data, data)

	return data
}

func (vol *labelVol) testBlock(t *testing.T, context string, bx, by, bz int32, data []byte) {
	// Get offset of this block in label volume
	ox := bx*vol.blockSize[0] - vol.offset[0]
	oy := by*vol.blockSize[1] - vol.offset[1]
	oz := bz*vol.blockSize[2] - vol.offset[2]

	if int64(len(data)) < vol.blockSize.Prod()*8 {
		t.Fatalf("[%s] got bad uint64 array of len %d for block (%d, %d, %d)\n", context, len(data), bx, by, bz)
	}
	p := 0
	var x, y, z, numerrors int32
	for z = 0; z < 32; z++ {
		lz := (z + oz) / 3
		for y = 0; y < 32; y++ {
			ly := (y + oy) / 3
			for x = 0; x < 32; x++ {
				lx := x + ox
				label := vol.startLabel + uint64(lx>>2+ly+lz)
				got := binary.LittleEndian.Uint64(data[p : p+8])
				if label != got && numerrors < 5 {
					t.Errorf("[%s] error block (%d,%d,%d) at voxel (%d,%d,%d): expected %d, got %d\n", context, bx, by, bz, x, y, z, label, got)
					numerrors++
				}
				p += 8
			}
		}
	}
}

func (vol *labelVol) testBlocks(t *testing.T, context string, uuid dvid.UUID, compression string) {
	span := 5
	apiStr := fmt.Sprintf("%snode/%s/%s/blocks/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, vol.name, 160, 32, 32, vol.offset[0], vol.offset[1], vol.offset[2])
	var qstrs []string
	if compression != "" {
		qstrs = append(qstrs, "compression="+compression)
	}
	if len(qstrs) > 0 {
		apiStr += "?" + strings.Join(qstrs, "&")
	}
	data := server.TestHTTP(t, "GET", apiStr, nil)

	blockBytes := int(vol.blockSize.Prod() * 8)

	// Check if values are what we expect
	b := 0
	for i := 0; i < span; i++ {
		// Get block coord + block size
		if b+16 > len(data) {
			t.Fatalf("Only got %d bytes back from block API call, yet next coord in span would put index @ %d\n", len(data), b+16)
		}
		x := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		y := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		z := int32(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4
		n := int(binary.LittleEndian.Uint32(data[b : b+4]))
		b += 4

		// Read in the block data as assumed LZ4 and check it.
		var uncompressed []byte
		switch compression {
		case "uncompressed":
			uncompressed = data[b : b+n]
		case "", "lz4":
			uncompressed = make([]byte, blockBytes)
			if err := lz4.Uncompress(data[b:b+n], uncompressed); err != nil {
				t.Fatalf("Unable to uncompress LZ4 data (%s), %d bytes: %v\n", apiStr, n, err)
			}
		case "gzip", "blocks":
			gzipIn := bytes.NewBuffer(data[b : b+n])
			zr, err := gzip.NewReader(gzipIn)
			if err != nil {
				t.Fatalf("can't uncompress gzip block: %v\n", err)
			}
			uncompressed, err = ioutil.ReadAll(zr)
			if err != nil {
				t.Fatalf("can't uncompress gzip block: %v\n", err)
			}
			zr.Close()

			if compression == "blocks" {
				var block labels.Block
				if err = block.UnmarshalBinary(uncompressed); err != nil {
					t.Fatalf("unable to deserialize label block (%d, %d, %d): %v\n", x, y, z, err)
				}
				uint64array, size := block.MakeLabelVolume()
				if !size.Equals(vol.blockSize) {
					t.Fatalf("got bad block size from deserialized block (%d, %d, %d): %s\n", x, y, z, size)
				}
				uncompressed = uint64array
			}
		}
		vol.testBlock(t, context, x, y, z, uncompressed)
		b += n
	}
}

// the label in the test volume should just be the start label + voxel index + 1 when iterating in ZYX order.
// The passed (x,y,z) should be world coordinates, not relative to the volume offset.
func (vol *labelVol) label(x, y, z int32) uint64 {
	if x < vol.offset[0] || x >= vol.offset[0]+vol.size[0]*vol.blockSize[0] {
		return 0
	}
	if y < vol.offset[1] || y >= vol.offset[1]+vol.size[1]*vol.blockSize[1] {
		return 0
	}
	if z < vol.offset[2] || z >= vol.offset[2]+vol.size[2]*vol.blockSize[2] {
		return 0
	}
	x -= vol.offset[0]
	y -= vol.offset[1]
	z -= vol.offset[2]
	return vol.startLabel + uint64(x>>2+y/3+z/3)
}

type sliceTester struct {
	orient string
	width  int32
	height int32
	offset dvid.Point3d // offset of slice
}

func (s sliceTester) apiStr(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/%s/%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, name, s.orient, s.width, s.height, s.offset[0], s.offset[1], s.offset[2])
}

// make sure the given labels match what would be expected from the test volume.
func (s sliceTester) testLabel(t *testing.T, vol labelVol, img *dvid.Image) {
	data := img.Data()
	var x, y, z int32
	i := 0
	switch s.orient {
	case "xy":
		for y = 0; y < s.height; y++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := y + s.offset[1]
				vz := s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "xz":
		for z = 0; z < s.height; z++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "yz":
		for z = 0; z < s.height; z++ {
			for y = 0; y < s.width; y++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := s.offset[0]
				vy := y + s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	default:
		t.Fatalf("Unknown slice orientation %q\n", s.orient)
	}
}

func (vol labelVol) testSlices(t *testing.T, uuid dvid.UUID) {
	// Verify XY slice.
	sliceOffset := vol.offset
	sliceOffset[0] += 51
	sliceOffset[1] += 11
	sliceOffset[2] += 23
	slice := sliceTester{"xy", 67, 83, sliceOffset}
	apiStr := slice.apiStr(uuid, vol.name)
	xy := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err := dvid.ImageFromBytes(xy, EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on XY labels GET: %v\n", err)
	}
	if format != "png" {
		t.Errorf("Expected XY labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from XY labelmap GET.  Got %d instead.", 160*160*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)

	// Verify XZ slice returns what we expect.
	sliceOffset = vol.offset
	sliceOffset[0] += 11
	sliceOffset[1] += 4
	sliceOffset[2] += 3
	slice = sliceTester{"xz", 67, 83, sliceOffset}
	apiStr = slice.apiStr(uuid, vol.name)
	xz := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err = dvid.ImageFromBytes(xz, EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on XZ labels GET: %v\n", err)
	}
	if format != "png" {
		t.Errorf("Expected XZ labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from XZ labelmap GET.  Got %d instead.", 67*83*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)

	// Verify YZ slice returns what we expect.
	sliceOffset = vol.offset
	sliceOffset[0] += 7
	sliceOffset[1] += 33
	sliceOffset[2] += 33
	slice = sliceTester{"yz", 67, 83, sliceOffset}
	apiStr = slice.apiStr(uuid, vol.name)
	yz := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err = dvid.ImageFromBytes(yz, EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on YZ labels GET: %v\n", err)
	}
	if format != "png" {
		t.Errorf("Expected YZ labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from YZ labelmap GET.  Got %d instead.", 67*83*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)
}

type labelResp struct {
	Label uint64
}

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []uint64{23, 819229, 757, 100303, 9991}
var ydata = []uint64{66599, 201, 881067, 5488, 0}
var zdata = []uint64{1, 734, 43990122, 42, 319596}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func makeSlice(offset dvid.Point3d, size dvid.Point2d) []byte {
	numBytes := size[0] * size[1] * 8
	slice := make([]byte, numBytes, numBytes)
	i := 0
	modz := offset[2] % int32(len(zdata))
	for y := int32(0); y < size[1]; y++ {
		sy := y + offset[1]
		mody := sy % int32(len(ydata))
		sx := offset[0]
		for x := int32(0); x < size[0]; x++ {
			modx := sx % int32(len(xdata))
			binary.BigEndian.PutUint64(slice[i:i+8], xdata[modx]+ydata[mody]+zdata[modz])
			i += 8
			sx++
		}
	}
	return slice
}

// Make a 3d volume of bytes with top left corner at (ox,oy,oz) and size (nx, ny, nz)
func makeVolume(offset, size dvid.Point3d) []byte {
	sliceBytes := size[0] * size[1] * 8
	volumeBytes := sliceBytes * size[2]
	volume := make([]byte, volumeBytes, volumeBytes)
	var i int32
	size2d := dvid.Point2d{size[0], size[1]}
	startZ := offset[2]
	endZ := startZ + size[2]
	for z := startZ; z < endZ; z++ {
		offset[2] = z
		copy(volume[i:i+sliceBytes], makeSlice(offset, size2d))
		i += sliceBytes
	}
	return volume
}

// Creates a new data instance for labelmap
func newDataInstance(uuid dvid.UUID, t *testing.T, name dvid.InstanceName) *Data {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, labelsT, name, config)
	if err != nil {
		t.Fatalf("Unable to create labelmap instance %q: %v\n", name, err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast labels data service into Data\n")
	}
	return labels
}

func TestLabelarrayDirectAPI(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, versionID := initTestRepo()
	lbls := newDataInstance(uuid, t, "mylabels")
	labelsCtx := datastore.NewVersionedCtx(lbls, versionID)

	// Create a fake block-aligned label volume
	offset := dvid.Point3d{DefaultBlockSize, 0, DefaultBlockSize}
	size := dvid.Point3d{2 * DefaultBlockSize, DefaultBlockSize, 3 * DefaultBlockSize}
	subvol := dvid.NewSubvolume(offset, size)
	data := makeVolume(offset, size)

	// Store it into datastore at root
	v, err := lbls.NewVoxels(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new labels Voxels: %v\n", err)
	}
	if err = lbls.IngestVoxels(versionID, 1, v, ""); err != nil {
		t.Fatalf("Unable to put labels for %s: %v\n", labelsCtx, err)
	}
	if v.NumVoxels() != int64(len(data))/8 {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(data))/8)
	}

	// Read the stored image
	v2, err := lbls.NewVoxels(subvol, nil)
	if err != nil {
		t.Fatalf("Unable to make new labels ExtHandler: %v\n", err)
	}
	if err = lbls.GetVoxels(versionID, v2, ""); err != nil {
		t.Fatalf("Unable to get voxels for %s: %v\n", labelsCtx, err)
	}

	// Make sure the retrieved image matches the original
	if v.Stride() != v2.Stride() {
		t.Errorf("Stride in retrieved subvol incorrect\n")
	}
	if v.Interpolable() != v2.Interpolable() {
		t.Errorf("Interpolable bool in retrieved subvol incorrect\n")
	}
	if !reflect.DeepEqual(v.Size(), v2.Size()) {
		t.Errorf("Size in retrieved subvol incorrect: %s vs expected %s\n",
			v2.Size(), v.Size())
	}
	if v.NumVoxels() != v2.NumVoxels() {
		t.Errorf("# voxels in retrieved is different: %d vs expected %d\n",
			v2.NumVoxels(), v.NumVoxels())
	}
	byteData := v2.Data()

	for i := int64(0); i < v2.NumVoxels()*8; i++ {
		if byteData[i] != data[i] {
			t.Logf("Size of data: %d bytes from GET, %d bytes in PUT\n", len(data), len(data))
			t.Fatalf("GET subvol (%d) != PUT subvol (%d) @ uint64 #%d", byteData[i], data[i], i)
		}
	}
}

func TestLabelarrayRepoPersistence(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	config.Set("CountLabels", "false")
	config.Set("MaxDownresLevel", "5")
	dataservice, err := datastore.NewData(uuid, labelsT, "mylabels", config)
	if err != nil {
		t.Errorf("Unable to create labels instance: %v\n", err)
	}
	lbls, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labels data service into Data\n")
	}
	if !lbls.IndexedLabels {
		t.Errorf("expected IndexedLabels to be true for default but was false\n")
	}
	if lbls.MaxDownresLevel != 5 {
		t.Errorf("expected MaxDownresLevel to be 5, not %d\n", lbls.MaxDownresLevel)
	}
	oldData := *lbls

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, lbls); err != nil {
		t.Fatalf("Unable to save repo during labels persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "mylabels")
	if err != nil {
		t.Fatalf("Can't get labels instance from reloaded test db: %v\n", err)
	}
	lbls2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !oldData.Equals(lbls2) {
		t.Errorf("Expected %v, got %v\n", oldData, *lbls2)
	}
	if lbls2.MaxDownresLevel != 5 {
		t.Errorf("Bad MaxDownresLevel: %d\n", lbls2.MaxDownresLevel)
	}
}

func TestBadCalls(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "2")
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	reqStr := fmt.Sprintf("%snode/%s/labels/supervoxels/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/size/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/sparsevol-size/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/sparsevol/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/sparsevol-coarse/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/cleave/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, bytes.NewBufferString("[4]"))

	reqStr = fmt.Sprintf("%snode/%s/labels/split-supervoxel/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/split/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/index/100", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/indices", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/mappings", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", reqStr, nil)
}

func TestMultiscaleIngest(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "2")
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	// Create an easily interpreted label volume with a couple of labels.
	volume := newTestVolume(128, 128, 128)
	volume.addSubvol(dvid.Point3d{40, 40, 40}, dvid.Point3d{40, 40, 40}, 1)
	volume.addSubvol(dvid.Point3d{40, 40, 80}, dvid.Point3d{40, 40, 40}, 2)
	volume.addSubvol(dvid.Point3d{80, 40, 40}, dvid.Point3d{40, 40, 40}, 13)
	volume.addSubvol(dvid.Point3d{40, 80, 40}, dvid.Point3d{40, 40, 40}, 209)
	volume.addSubvol(dvid.Point3d{80, 80, 40}, dvid.Point3d{40, 40, 40}, 311)
	volume.put(t, uuid, "labels")

	// Verify initial ingest for hi-res
	if err := downres.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on update for labels: %v\n", err)
	}

	hires := newTestVolume(128, 128, 128)
	hires.get(t, uuid, "labels", false)
	hires.verifyLabel(t, 1, 45, 45, 45)
	hires.verifyLabel(t, 2, 50, 50, 100)
	hires.verifyLabel(t, 13, 100, 60, 60)
	hires.verifyLabel(t, 209, 55, 100, 55)
	hires.verifyLabel(t, 311, 81, 81, 41)

	// Verify our label index voxel counts are correct.
	for _, label := range []uint64{1, 2, 13, 209, 311} {
		reqStr := fmt.Sprintf("%snode/%s/labels/size/%d", server.WebAPIPath, uuid, label)
		r := server.TestHTTP(t, "GET", reqStr, nil)
		var jsonVal struct {
			Voxels uint64 `json:"voxels"`
		}
		if err := json.Unmarshal(r, &jsonVal); err != nil {
			t.Fatalf("unable to get size for label %d: %v", label, err)
		}
		if jsonVal.Voxels != 40*40*40 {
			t.Errorf("thought label %d would have %d voxels, got %d\n", label, 40*40*40, jsonVal.Voxels)
		}
	}
	reqStr := fmt.Sprintf("%snode/%s/labels/size/3", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "GET", reqStr, nil)

	reqStr = fmt.Sprintf("%snode/%s/labels/sizes", server.WebAPIPath, uuid)
	bodystr := "[1, 2, 3, 13, 209, 311]"
	r := server.TestHTTP(t, "GET", reqStr, bytes.NewBufferString(bodystr))
	if string(r) != "[64000,64000,0,64000,64000,64000]" {
		t.Errorf("bad batch sizes result.  got: %s\n", string(r))
	}

	// Check the first downres: 64^3
	downres1 := newTestVolume(64, 64, 64)
	downres1.getScale(t, uuid, "labels", 1, false)
	downres1.verifyLabel(t, 1, 30, 30, 30)
	downres1.verifyLabel(t, 2, 21, 21, 45)
	downres1.verifyLabel(t, 13, 45, 21, 36)
	downres1.verifyLabel(t, 209, 21, 50, 35)
	downres1.verifyLabel(t, 311, 45, 55, 35)
	expected1 := newTestVolume(64, 64, 64)
	expected1.addSubvol(dvid.Point3d{20, 20, 20}, dvid.Point3d{20, 20, 20}, 1)
	expected1.addSubvol(dvid.Point3d{20, 20, 40}, dvid.Point3d{20, 20, 20}, 2)
	expected1.addSubvol(dvid.Point3d{40, 20, 20}, dvid.Point3d{20, 20, 20}, 13)
	expected1.addSubvol(dvid.Point3d{20, 40, 20}, dvid.Point3d{20, 20, 20}, 209)
	expected1.addSubvol(dvid.Point3d{40, 40, 20}, dvid.Point3d{20, 20, 20}, 311)
	if err := downres1.equals(expected1); err != nil {
		t.Errorf("1st downres 'labels' isn't what is expected: %v\n", err)
	}

	// Check the second downres to voxel: 32^3
	expected2 := newTestVolume(32, 32, 32)
	expected2.addSubvol(dvid.Point3d{10, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	expected2.addSubvol(dvid.Point3d{10, 10, 20}, dvid.Point3d{10, 10, 10}, 2)
	expected2.addSubvol(dvid.Point3d{20, 10, 10}, dvid.Point3d{10, 10, 10}, 13)
	expected2.addSubvol(dvid.Point3d{10, 20, 10}, dvid.Point3d{10, 10, 10}, 209)
	expected2.addSubvol(dvid.Point3d{20, 20, 10}, dvid.Point3d{10, 10, 10}, 311)
	downres2 := newTestVolume(32, 32, 32)
	downres2.getScale(t, uuid, "labels", 2, false)
	if err := downres2.equals(expected2); err != nil {
		t.Errorf("2nd downres 'labels' isn't what is expected: %v\n", err)
	}

	// // Check blocks endpoint for different scales.
	volume.testGetBlocks(t, "hi-res block check", uuid, "labels", "", 0)
	volume.testGetBlocks(t, "hi-res block check", uuid, "labels", "uncompressed", 0)
	volume.testGetBlocks(t, "hi-res block check", uuid, "labels", "blocks", 0)
	volume.testGetBlocks(t, "hi-res block check", uuid, "labels", "gzip", 0)

	expected1.testGetBlocks(t, "downres #1 block check", uuid, "labels", "", 1)
	expected1.testGetBlocks(t, "downres #1 block check", uuid, "labels", "uncompressed", 1)
	expected1.testGetBlocks(t, "downres #1 block check", uuid, "labels", "blocks", 1)
	expected1.testGetBlocks(t, "downres #1 block check", uuid, "labels", "gzip", 1)

	expected2a := newTestVolume(64, 64, 64) // can only get block-aligned subvolumes
	expected2a.addSubvol(dvid.Point3d{10, 10, 10}, dvid.Point3d{10, 10, 10}, 1)
	expected2a.addSubvol(dvid.Point3d{10, 10, 20}, dvid.Point3d{10, 10, 10}, 2)
	expected2a.addSubvol(dvid.Point3d{20, 10, 10}, dvid.Point3d{10, 10, 10}, 13)
	expected2a.addSubvol(dvid.Point3d{10, 20, 10}, dvid.Point3d{10, 10, 10}, 209)
	expected2a.addSubvol(dvid.Point3d{20, 20, 10}, dvid.Point3d{10, 10, 10}, 311)
	expected2a.testGetBlocks(t, "downres #2 block check", uuid, "labels", "", 2)
	expected2a.testGetBlocks(t, "downres #2 block check", uuid, "labels", "uncompressed", 2)
	expected2a.testGetBlocks(t, "downres #2 block check", uuid, "labels", "blocks", 2)
	expected2a.testGetBlocks(t, "downres #2 block check", uuid, "labels", "gzip", 2)
}

func readGzipFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer fz.Close()

	data, err := ioutil.ReadAll(fz)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type testData struct {
	u        []uint64
	b        *labels.Block
	filename string
}

func (d testData) String() string {
	return filepath.Base(d.filename)
}

func solidTestData(label uint64) (td testData) {
	td.b = labels.MakeSolidBlock(label, dvid.Point3d{64, 64, 64})
	numVoxels := 64 * 64 * 64
	td.u = make([]uint64, numVoxels)
	td.filename = fmt.Sprintf("solid volume of label %d", label)
	for i := 0; i < numVoxels; i++ {
		td.u[i] = label
	}
	return
}

var testFiles = []string{
	"../../test_data/fib19-64x64x64-sample1.dat.gz",
	"../../test_data/fib19-64x64x64-sample2.dat.gz",
	"../../test_data/cx-64x64x64-sample1.dat.gz",
	"../../test_data/cx-64x64x64-sample2.dat.gz",
}

func loadTestData(t *testing.T, filename string) (td testData) {
	uint64array, err := readGzipFile(filename)
	if err != nil {
		t.Fatalf("unable to open test data file %q: %v\n", filename, err)
	}
	td.u, err = dvid.ByteToUint64(uint64array)
	if err != nil {
		t.Fatalf("unable to create alias []uint64 for data file %q: %v\n", filename, err)
	}
	td.b, err = labels.MakeBlock(uint64array, dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("unable to convert data from file %q into block: %v\n", filename, err)
	}
	td.filename = filename
	return
}

func writeTestInt32(t *testing.T, buf *bytes.Buffer, i int32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(i))
	n, err := buf.Write(b)
	if n != 4 || err != nil {
		t.Fatalf("couldn't write value %d (%d bytes) to buffer: %v\n", i, n, err)
	}
}

func testGetBlock(t *testing.T, uuid dvid.UUID, name string, bcoord dvid.Point3d, td testData) {
	apiStr := fmt.Sprintf("%snode/%s/%s/blocks/%d_%d_%d/%d_%d_%d?compression=blocks", server.WebAPIPath,
		uuid, name, 64, 64, 64, bcoord[0]*64, bcoord[1]*64, bcoord[2]*64)
	data := server.TestHTTP(t, "GET", apiStr, nil)

	b := 0
	if b+16 > len(data) {
		t.Fatalf("Only got %d bytes back from block API call\n", len(data))
	}
	x := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	y := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	z := int32(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	n := int(binary.LittleEndian.Uint32(data[b : b+4]))
	b += 4
	if x != bcoord[0] || y != bcoord[1] || z != bcoord[2] {
		t.Fatalf("Bad block coordinate: expected %s, got (%d,%d,%d)\n", bcoord, x, y, z)
	}

	gzipIn := bytes.NewBuffer(data[b : b+n])
	zr, err := gzip.NewReader(gzipIn)
	if err != nil {
		t.Fatalf("can't uncompress gzip block: %v\n", err)
	}
	uncompressed, err := ioutil.ReadAll(zr)
	if err != nil {
		t.Fatalf("can't uncompress gzip block: %v\n", err)
	}
	zr.Close()

	var block labels.Block
	if err = block.UnmarshalBinary(uncompressed); err != nil {
		t.Fatalf("unable to deserialize label block (%d, %d, %d): %v\n", x, y, z, err)
	}
	bytearray, _ := block.MakeLabelVolume()
	uint64array, err := dvid.ByteToUint64(bytearray)
	if err != nil {
		t.Fatalf("error converting returned block %s to uint64 array: %v\n", bcoord, err)
	}
	if len(uint64array) != len(td.u) {
		t.Fatalf("got block %s with %d labels != expected %d labels\n", bcoord, len(uint64array), len(td.u))
	}
	for i, val := range uint64array {
		if val != td.u[i] {
			t.Fatalf("error at pos %d: got label %d, expected label %d\n", i, val, td.u[i])
		}
	}
}

func TestPostBlocks(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})

	blockCoords := []dvid.Point3d{
		{1, 2, 3},
		{2, 2, 3},
		{1, 3, 4},
		{2, 3, 4},
	}
	var data [4]testData
	var buf bytes.Buffer
	for i, fname := range testFiles {
		writeTestInt32(t, &buf, blockCoords[i][0])
		writeTestInt32(t, &buf, blockCoords[i][1])
		writeTestInt32(t, &buf, blockCoords[i][2])
		data[i] = loadTestData(t, fname)
		gzipped, err := data[i].b.CompressGZIP()
		if err != nil {
			t.Fatalf("unable to gzip compress block: %v\n", err)
		}
		writeTestInt32(t, &buf, int32(len(gzipped)))
		fmt.Printf("Wrote %d gzipped block bytes for block %s\n", len(gzipped), blockCoords[i])
		n, err := buf.Write(gzipped)
		if err != nil {
			t.Fatalf("unable to write gzip block: %v\n", err)
		}
		if n != len(gzipped) {
			t.Fatalf("unable to write %d bytes to buffer, only wrote %d bytes\n", len(gzipped), n)
		}
	}

	apiStr := fmt.Sprintf("%snode/%s/labels/blocks", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, &buf)

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// test volume GET
	start := dvid.Point3d{1 * 64, 2 * 64, 3 * 64}
	end := dvid.Point3d{3*64 - 1, 4*64 - 1, 5*64 - 1}
	testExtents(t, "labels", uuid, start, end)

	vol := labelVol{
		size:      dvid.Point3d{2, 2, 2}, // in blocks
		nx:        128,
		ny:        128,
		nz:        128,
		blockSize: dvid.Point3d{64, 64, 64},
		offset:    dvid.Point3d{64, 128, 192},
		name:      "labels",
	}
	got := vol.getLabelVolume(t, uuid, "", "")
	gotLabels, err := dvid.ByteToUint64(got)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		x0 := (blockCoords[i][0] - 1) * 64
		y0 := (blockCoords[i][1] - 2) * 64
		z0 := (blockCoords[i][2] - 3) * 64

		var x, y, z int32
		for z = 0; z < 64; z++ {
			for y = 0; y < 64; y++ {
				for x = 0; x < 64; x++ {
					di := z*64*64 + y*64 + x
					gi := (z+z0)*128*128 + (y+y0)*128 + x + x0
					if data[i].u[di] != gotLabels[gi] {
						t.Fatalf("Error in block %s dvid coord (%d,%d,%d): expected %d, got %d\n", blockCoords[i], x+x0, y+y0, z+z0, data[i].u[di], gotLabels[gi])
					}
				}
			}
		}
	}

	// test GET /blocks
	for i, td := range data {
		testGetBlock(t, uuid, "labels", blockCoords[i], td)
	}
}

func testExtents(t *testing.T, name string, uuid dvid.UUID, min, max dvid.Point3d) {
	apiStr := fmt.Sprintf("%snode/%s/%s/metadata", server.WebAPIPath, uuid, name)
	r := server.TestHTTP(t, "GET", apiStr, nil)
	fmt.Printf("metadata: %s\n", string(r))
	jsonVal := make(map[string]interface{})
	if err := json.Unmarshal(r, &jsonVal); err != nil {
		t.Errorf("Unable to get metadata in JSON format.  Instead got: %v\n", jsonVal)
	}
	propData, ok := jsonVal["Properties"]
	if !ok {
		t.Fatalf("Could not parse Properties out of returned JSON: %v\n", jsonVal)
	}
	props, ok := propData.(map[string]interface{})
	if !ok {
		t.Fatalf("Could not create properties map: %v\n", propData)
	}
	pt, ok := props["MaxPoint"]
	if !ok {
		t.Fatalf("Couldn't get MaxPoint from Properties object: %v\n", props)
	}
	if pt == nil {
		t.Fatalf("Couldn't find MaxPoint in Properties object: %v\n", props)
	}
	maxPoint, ok := pt.([]interface{})
	if !ok {
		t.Fatalf("Couldn't parse MaxPoint %s: %v\n", reflect.TypeOf(pt), pt)
	}
	x, y, z := maxPoint[0].(float64), maxPoint[1].(float64), maxPoint[2].(float64)
	if x != float64(max[0]) {
		t.Errorf("Bad MaxPoint X: expected %.0f, got %d\n", x, max[0])
	}
	if y != float64(max[1]) {
		t.Errorf("Bad MaxPoint Y: expected %.0f, got %d\n", y, max[1])
	}
	if z != float64(max[2]) {
		t.Errorf("Bad MaxPoint Z: expected %.0f, got %d\n", z, max[2])
	}
}

func TestPostBlock(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})

	f, err := os.Open("../../test_data/fib19-64x64x64-sample1-block.dat.gz")
	if err != nil {
		t.Fatalf("Couldn't open compressed block test data: %v\n", err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("Couldn't read compressed block test data: %v\n", err)
	}
	var buf bytes.Buffer
	writeTestInt32(t, &buf, 0)
	writeTestInt32(t, &buf, 0)
	writeTestInt32(t, &buf, 0)
	writeTestInt32(t, &buf, int32(len(data)))
	fmt.Printf("Writing %d bytes of compressed block\n", len(data))
	n, err := buf.Write(data)
	if err != nil {
		t.Fatalf("unable to write gzip block: %v\n", err)
	}
	if n != len(data) {
		t.Fatalf("unable to write %d bytes to buffer, only wrote %d bytes\n", len(data), n)
	}

	apiStr := fmt.Sprintf("%snode/%s/labels/blocks", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, &buf)

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}
}

func TestBigPostBlock(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})

	f, err := os.Open("../../test_data/stream_1block.dat")
	if err != nil {
		t.Fatalf("Couldn't open compressed block test data: %v\n", err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("Couldn't read compressed block test data: %v\n", err)
	}

	apiStr := fmt.Sprintf("%snode/%s/labels/blocks", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(data))

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}
}

func TestBlocksWithMerge(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})

	numVoxels := 64 * 64 * 64
	blockVol0 := make([]uint64, numVoxels)
	i := 0
	for z := 0; z < 64; z++ {
		for y := 0; y < 64; y++ {
			for x := 0; x < 32; x++ {
				blockVol0[i] = 2
				i++
			}
			for x := 32; x < 64; x++ {
				blockVol0[i] = 1
				i++
			}
		}
	}
	block0, err := labels.MakeBlock(dvid.Uint64ToByte(blockVol0), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}
	block0data, err := block0.CompressGZIP()
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}
	blockVol1 := make([]uint64, numVoxels)
	i = 0
	for z := 0; z < 64; z++ {
		for y := 0; y < 64; y++ {
			for x := 0; x < 32; x++ {
				blockVol1[i] = 1
				i++
			}
			for x := 32; x < 64; x++ {
				blockVol1[i] = 2
				i++
			}
		}
	}
	block1, err := labels.MakeBlock(dvid.Uint64ToByte(blockVol1), dvid.Point3d{64, 64, 64})
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}
	block1data, err := block1.CompressGZIP()
	if err != nil {
		t.Fatalf("error making block 0: %v\n", err)
	}

	testBlockData := [2]struct {
		bcoord dvid.ChunkPoint3d
		labels []uint64
	}{
		{
			bcoord: dvid.ChunkPoint3d{2, 3, 4},
			labels: blockVol0,
		},
		{
			bcoord: dvid.ChunkPoint3d{3, 3, 4},
			labels: blockVol1,
		},
	}

	var buf bytes.Buffer
	writeTestInt32(t, &buf, 2)
	writeTestInt32(t, &buf, 3)
	writeTestInt32(t, &buf, 4)
	writeTestInt32(t, &buf, int32(len(block0data)))
	n, err := buf.Write(block0data)
	if err != nil {
		t.Fatalf("unable to write gzip block: %v\n", err)
	}
	if n != len(block0data) {
		t.Fatalf("unable to write %d bytes to buffer, only wrote %d bytes\n", len(block0data), n)
	}
	writeTestInt32(t, &buf, 3)
	writeTestInt32(t, &buf, 3)
	writeTestInt32(t, &buf, 4)
	writeTestInt32(t, &buf, int32(len(block1data)))
	n, err = buf.Write(block1data)
	if err != nil {
		t.Fatalf("unable to write gzip block: %v\n", err)
	}
	if n != len(block1data) {
		t.Fatalf("unable to write %d bytes to buffer, only wrote %d bytes\n", len(block1data), n)
	}

	apiStr := fmt.Sprintf("%snode/%s/labels/blocks", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, &buf)

	apiStr = fmt.Sprintf("%snode/%s/labels/blocks/128_64_64/128_192_256?compression=blocks", server.WebAPIPath, uuid)
	respRec := server.TestHTTPResponse(t, "GET", apiStr, nil)
	for bnum := 0; bnum < 2; bnum++ {
		gotBlock, _, bx, by, bz, err := readStreamedBlock(respRec.Body, 0)
		if err != nil {
			t.Fatalf("problem reading block %d from stream: %v\n", bnum, err)
		}
		for _, label := range gotBlock.Labels {
			if label != 1 && label != 2 {
				t.Errorf("got bad block with label %d when only 1 or 2 should have been present\n", label)
			}
		}
		bcoord := dvid.ChunkPoint3d{bx, by, bz}
		var found bool
		for i := 0; i < 2; i++ {
			if bcoord == testBlockData[i].bcoord {
				found = true
				labelBytes, _ := gotBlock.MakeLabelVolume()
				labelVol, err := dvid.ByteToUint64(labelBytes)
				if err != nil {
					t.Fatalf("problem inflating block %s labels to []uint64: %v\n", bcoord, err)
				}
				for v, val := range labelVol {
					if testBlockData[i].labels[v] != val {
						t.Fatalf("label mismatch found at index %d, expected %d, got %d\n", v, testBlockData[i].labels[v], val)
					}
				}
			}
		}
		if !found {
			t.Fatalf("got block %s but wasn't an expected block!\n", bcoord)
		}
	}

	testMerge := mergeJSON(`[1, 2]`)
	testMerge.send(t, uuid, "labels")

	respRec = server.TestHTTPResponse(t, "GET", apiStr, nil)
	for bnum := 0; bnum < 2; bnum++ {
		gotBlock, _, bx, by, bz, err := readStreamedBlock(respRec.Body, 0)
		if err != nil {
			t.Fatalf("problem reading block %d from stream: %v\n", bnum, err)
		}
		for _, label := range gotBlock.Labels {
			if label != 1 {
				t.Errorf("got bad block with label %d when only 1 should have been present\n", label)
			}
		}
		bcoord := dvid.ChunkPoint3d{bx, by, bz}
		var found bool
		for i := 0; i < 2; i++ {
			if bcoord == testBlockData[i].bcoord {
				found = true
				labelBytes, _ := gotBlock.MakeLabelVolume()
				labelVol, err := dvid.ByteToUint64(labelBytes)
				if err != nil {
					t.Fatalf("problem inflating block %s labels to []uint64: %v\n", bcoord, err)
				}
				for v, val := range labelVol {
					if testBlockData[i].labels[v] != 0 && val != 1 {
						t.Fatalf("label mismatch found at index %d, expected 1, got %d\n", v, val)
					}
				}
			}
		}
		if !found {
			t.Fatalf("got block %s but wasn't an expected block!\n", bcoord)
		}
	}

	apiStr = fmt.Sprintf("%snode/%s/labels/blocks/128_64_64/128_192_256?compression=blocks&supervoxels=true", server.WebAPIPath, uuid)
	respRec = server.TestHTTPResponse(t, "GET", apiStr, nil)
	gotBlock0, _, _, _, _, err := readStreamedBlock(respRec.Body, 0)
	gotLabels := make(labels.Set)
	for _, label := range gotBlock0.Labels {
		gotLabels[label] = struct{}{}
		if label != 1 && label != 2 {
			t.Errorf("got unexpected label in block: %d\n", label)
		}
	}
	if _, found := gotLabels[1]; !found {
		t.Errorf("expected label 1 but found none\n")
	}
	if _, found := gotLabels[2]; !found {
		t.Errorf("expected label 2 but found none\n")
	}
	gotBlock1, _, _, _, _, err := readStreamedBlock(respRec.Body, 0)
	gotLabels = make(labels.Set)
	for _, label := range gotBlock1.Labels {
		gotLabels[label] = struct{}{}
		if label != 1 && label != 2 {
			t.Errorf("got unexpected label in block: %d\n", label)
		}
	}
	if _, found := gotLabels[1]; !found {
		t.Errorf("expected label 1 but found none\n")
	}
	if _, found := gotLabels[2]; !found {
		t.Errorf("expected label 2 but found none\n")
	}
}

func testLabels(t *testing.T, labelsIndexed bool) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create a labelmap instance
	var config dvid.Config
	config.Set("BlockSize", "32,32,32")
	if !labelsIndexed {
		config.Set("IndexedLabels", "false")
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	vol := labelVol{
		startLabel: 2,
		size:       dvid.Point3d{5, 5, 5}, // in blocks
		blockSize:  dvid.Point3d{32, 32, 32},
		offset:     dvid.Point3d{32, 64, 96},
		name:       "labels",
	}
	vol.postLabelVolume(t, uuid, "", "", 0)
	vol.testGetLabelVolume(t, uuid, "", "")

	end := dvid.Point3d{32 + 160 - 1, 64 + 160 - 1, 96 + 160 - 1}
	testExtents(t, "labels", uuid, vol.offset, end)

	// Test the blocks API
	vol.testBlocks(t, "GET default blocks (lz4)", uuid, "")
	vol.testBlocks(t, "GET uncompressed blocks", uuid, "uncompressed")
	vol.testBlocks(t, "GET DVID compressed label blocks", uuid, "blocks")
	vol.testBlocks(t, "GET gzip blocks", uuid, "gzip")

	// Test the "label" endpoint.
	apiStr := fmt.Sprintf("%snode/%s/%s/label/100_64_96", server.WebAPIPath, uuid, "labels")
	jsonResp := server.TestHTTP(t, "GET", apiStr, nil)
	var r labelResp
	if err := json.Unmarshal(jsonResp, &r); err != nil {
		t.Fatalf("Unable to parse 'label' endpoint response: %s\n", jsonResp)
	}
	if r.Label != vol.label(100, 64, 96) {
		t.Fatalf("Expected label %d @ (100, 64, 96) got label %d\n", vol.label(100, 64, 96), r.Label)
	}

	apiStr = fmt.Sprintf("%snode/%s/%s/label/10000_64000_9600121", server.WebAPIPath, uuid, "labels")
	jsonResp = server.TestHTTP(t, "GET", apiStr, nil)
	if err := json.Unmarshal(jsonResp, &r); err != nil {
		t.Fatalf("Unable to parse 'label' endpoint response: %s\n", jsonResp)
	}
	if r.Label != 0 {
		t.Fatalf("Expected label 0 at random huge point, got label %d\n", r.Label)
	}

	// Test the "labels" endpoint.
	apiStr = fmt.Sprintf("%snode/%s/%s/labels", server.WebAPIPath, uuid, "labels")
	payload := `[[100,64,96],[78,93,156],[104,65,97]]`
	jsonResp = server.TestHTTP(t, "GET", apiStr, bytes.NewBufferString(payload))
	var labels [3]uint64
	if err := json.Unmarshal(jsonResp, &labels); err != nil {
		t.Fatalf("Unable to parse 'labels' endpoint response: %s\n", jsonResp)
	}
	if labels[0] != vol.label(100, 64, 96) {
		t.Fatalf("Expected label %d @ (100, 64, 96) got label %d\n", vol.label(100, 64, 96), labels[0])
	}
	if labels[1] != vol.label(78, 93, 156) {
		t.Fatalf("Expected label %d @ (78, 93, 156) got label %d\n", vol.label(78, 93, 156), labels[1])
	}
	if labels[2] != vol.label(104, 65, 97) {
		t.Fatalf("Expected label %d @ (104, 65, 97) got label %d\n", vol.label(104, 65, 97), labels[2])
	}

	// Repost the label volume 3 more times with increasing starting values.
	vol.postLabelVolume(t, uuid, "", "", 2100)
	vol.postLabelVolume(t, uuid, "", "", 8176)
	vol.postLabelVolume(t, uuid, "", "", 16623)

	vol.testSlices(t, uuid)

	// Try to post last volume concurrently 3x and then check result.
	wg := new(sync.WaitGroup)
	wg.Add(3)
	go func() {
		vol.postLabelVolume(t, uuid, "", "", 16623)
		wg.Done()
	}()
	go func() {
		vol.postLabelVolume(t, uuid, "", "", 16623)
		wg.Done()
	}()
	go func() {
		vol.postLabelVolume(t, uuid, "", "", 16623)
		wg.Done()
	}()
	wg.Wait()
	vol.testGetLabelVolume(t, uuid, "", "")

	// Try concurrent write of disjoint subvolumes.
	vol2 := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{192, 64, 96},
		name:      "labels",
	}
	vol3 := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{192, 224, 96},
		name:      "labels",
	}
	vol4 := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{32, 224, 96},
		name:      "labels",
	}

	wg.Add(3)
	go func() {
		vol2.postLabelVolume(t, uuid, "lz4", "", 4000)
		wg.Done()
	}()
	go func() {
		vol3.postLabelVolume(t, uuid, "lz4", "", 8000)
		wg.Done()
	}()
	go func() {
		vol4.postLabelVolume(t, uuid, "lz4", "", 1200)
		wg.Done()
	}()
	wg.Wait()
	vol.testGetLabelVolume(t, uuid, "", "")
	vol2.testGetLabelVolume(t, uuid, "", "")
	vol3.testGetLabelVolume(t, uuid, "", "")
	vol4.testGetLabelVolume(t, uuid, "", "")

	// Verify various GET 3d volume with compressions and no ROI.
	vol.testGetLabelVolume(t, uuid, "", "")
	vol.testGetLabelVolume(t, uuid, "lz4", "")
	vol.testGetLabelVolume(t, uuid, "gzip", "")

	// Create a new ROI instance.
	roiName := "myroi"
	server.CreateTestInstance(t, uuid, "roi", roiName, dvid.Config{})

	// Add ROI data
	apiStr = fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, roiName)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(labelsJSON()))

	// Post updated labels without ROI and make sure it returns those values.
	var labelNoROI uint64 = 20000
	vol.postLabelVolume(t, uuid, "", "", labelNoROI)
	returned := vol.testGetLabelVolume(t, uuid, "", "")
	startLabel := binary.LittleEndian.Uint64(returned[0:8])
	if startLabel != labelNoROI {
		t.Fatalf("Expected first voxel to be label %d and got %d instead\n", labelNoROI, startLabel)
	}

	// Post again but now with ROI
	var labelWithROI uint64 = 40000
	vol.postLabelVolume(t, uuid, "", roiName, labelWithROI)

	// Verify ROI masking of POST where anything outside ROI is old labels.
	returned = vol.getLabelVolume(t, uuid, "", "")

	nx := vol.size[0] * vol.blockSize[0]
	ny := vol.size[1] * vol.blockSize[1]
	nz := vol.size[2] * vol.blockSize[2]
	var x, y, z, v int32
	for z = 0; z < nz; z++ {
		voxz := z + vol.offset[2]
		blockz := voxz / vol.blockSize[2]
		for y = 0; y < ny; y++ {
			voxy := y + vol.offset[1]
			blocky := voxy / vol.blockSize[1]
			for x = 0; x < nx; x++ {
				voxx := x + vol.offset[0]
				blockx := voxx / vol.blockSize[0]
				incr := uint64(x>>2 + y/3 + z/3)
				oldlabel := labelNoROI + incr
				newlabel := labelWithROI + incr
				got := binary.LittleEndian.Uint64(returned[v : v+8])
				if inroi(blockx, blocky, blockz) {
					if got != newlabel {
						t.Fatalf("Expected %d in ROI (%d,%d,%d), got %d\n", newlabel, blockx, blocky, blockz, got)
					}
				} else {
					if got != oldlabel {
						t.Fatalf("Expected %d outside ROI (%d,%d,%d), got %d\n", oldlabel, blockx, blocky, blockz, got)
					}
				}
				v += 8
			}
		}
	}

	// Verify that a ROI-enabled GET has zeros everywhere outside ROI.
	returned = vol.getLabelVolume(t, uuid, "", roiName)

	x, y, z, v = 0, 0, 0, 0
	for z = 0; z < nz; z++ {
		voxz := z + vol.offset[2]
		blockz := voxz / vol.blockSize[2]
		for y = 0; y < ny; y++ {
			voxy := y + vol.offset[1]
			blocky := voxy / vol.blockSize[1]
			for x = 0; x < nx; x++ {
				voxx := x + vol.offset[0]
				blockx := voxx / vol.blockSize[0]
				incr := uint64(x>>2 + y/3 + z/3)
				newlabel := labelWithROI + incr
				got := binary.LittleEndian.Uint64(returned[v : v+8])
				if inroi(blockx, blocky, blockz) {
					if got != newlabel {
						t.Fatalf("Expected %d in ROI, got %d\n", newlabel, got)
					}
				} else {
					if got != 0 {
						t.Fatalf("Expected zero outside ROI, got %d\n", got)
					}
				}
				v += 8
			}
		}
	}

	// Verify non-indexed instances can't access indexed endpoints.
	if !labelsIndexed {
		methods := []string{
			"GET",
			"HEAD",
			"GET",
			"GET",
			"GET",
			"GET",
			"POST",
			"POST",
			"POST",
		}
		reqs := []string{
			"sparsevol/20",
			"sparsevol/20",
			"sparsevol-by-point/30_89_100",
			"sparsevol-coarse/20",
			"maxlabel",
			"nextlabel",
			"nextlabel",
			"merge",
			"cleave/20",
		}
		var r io.Reader
		for i, req := range reqs {
			apiStr = fmt.Sprintf("%snode/%s/labels/%s", server.WebAPIPath, uuid, req)
			if methods[i] == "POST" {
				r = bytes.NewBufferString("junkdata that should never be used anyway")
			} else {
				r = nil
			}
			server.TestBadHTTP(t, methods[i], apiStr, r)
		}
	}
}

func TestLabels(t *testing.T) {
	testLabels(t, true)
}

func TestLabelsUnindexed(t *testing.T) {
	testLabels(t, false)
}
