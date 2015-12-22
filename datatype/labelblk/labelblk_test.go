package labelblk

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	lz4 "github.com/janelia-flyem/go/golz4"
)

var (
	labelsT, rgbaT datastore.TypeService
	testMu         sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if labelsT == nil {
		var err error
		labelsT, err = datastore.TypeServiceByName("labelblk")
		if err != nil {
			log.Fatalf("Can't get labelblk type: %s\n", err)
		}
		rgbaT, err = datastore.TypeServiceByName("rgba8blk")
		if err != nil {
			log.Fatalf("Can't get rgba8blk type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
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

// Creates a new data instance for labelblk
func newDataInstance(uuid dvid.UUID, t *testing.T, name dvid.InstanceName) *Data {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, labelsT, name, config)
	if err != nil {
		t.Errorf("Unable to create labelblk instance %q: %v\n", name, err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labels data service into Data\n")
	}
	return labels
}

func TestLabelblkDirectAPI(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, versionID := initTestRepo()
	labels := newDataInstance(uuid, t, "mylabels")
	labelsCtx := datastore.NewVersionedCtx(labels, versionID)

	// Create a fake block-aligned label volume
	offset := dvid.Point3d{32, 0, 64}
	size := dvid.Point3d{96, 64, 160}
	subvol := dvid.NewSubvolume(offset, size)
	data := makeVolume(offset, size)

	// Store it into datastore at root
	v, err := labels.NewVoxels(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new labels Voxels: %v\n", err)
	}
	if err = labels.IngestVoxels(versionID, v, ""); err != nil {
		t.Errorf("Unable to put labels for %s: %v\n", labelsCtx, err)
	}
	if v.NumVoxels() != int64(len(data))/8 {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(data))/8)
	}

	// Read the stored image
	v2, err := labels.NewVoxels(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new labels ExtHandler: %v\n", err)
	}
	if err = labels.GetVoxels(versionID, v2, ""); err != nil {
		t.Errorf("Unable to get voxels for %s: %v\n", labelsCtx, err)
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
func TestLabelblkRepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	dataservice, err := datastore.NewData(uuid, labelsT, "mylabels", config)
	if err != nil {
		t.Errorf("Unable to create labels instance: %v\n", err)
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labels data service into Data\n")
	}
	oldData := *labels

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, labels); err != nil {
		t.Fatalf("Unable to save repo during labels persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUID(uuid, "mylabels")
	if err != nil {
		t.Fatalf("Can't get labels instance from reloaded test db: %v\n", err)
	}
	labels2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !oldData.Equals(labels2) {
		t.Errorf("Expected %v, got %v\n", oldData, *labels2)
	}
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

type labelVol struct {
	size      dvid.Point3d
	blockSize dvid.Point3d
	offset    dvid.Point3d
	name      string
	data      []byte

	nx, ny, nz int32

	sync.RWMutex
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
	label := startLabel
	var x, y, z, v int32
	for z = 0; z < vol.nz; z++ {
		for y = 0; y < vol.ny; y++ {
			for x = 0; x < vol.nx; x++ {
				label++
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

func (vol labelVol) getLabelVolume(t *testing.T, uuid dvid.UUID, compression, roi string) []byte {
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
		t.Errorf("Expected %d uncompressed bytes from 3d labelblk GET.  Got %d instead.", vol.numBytes(), len(data))
	}
	return data
}

func (vol labelVol) testGetLabelVolume(t *testing.T, uuid dvid.UUID, compression, roi string) []byte {
	data := vol.getLabelVolume(t, uuid, compression, roi)

	// run test to make sure it's same volume as we posted.
	label := vol.startLabel
	var x, y, z, v int32
	for z = 0; z < vol.nz; z++ {
		for y = 0; y < vol.ny; y++ {
			for x = 0; x < vol.nx; x++ {
				label++
				got := binary.LittleEndian.Uint64(data[v : v+8])
				if label != got {
					t.Fatalf("Error on 3d GET compression (%q): expected %d, got %d\n", compression, label, got)
					return nil
				}
				v += 8
			}
		}
	}

	return data
}

// the label in the test volume should just be the start label + voxel index + 1 when iterating in ZYX order.
// The passed (x,y,z) should be world coordinates, not relative to the volume offset.
func (vol labelVol) label(x, y, z int32) uint64 {
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
	nx := vol.size[0] * vol.blockSize[0]
	nxy := nx * vol.size[1] * vol.blockSize[1]
	return vol.startLabel + uint64(z*nxy) + uint64(y*nx) + uint64(x+1)
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
		t.Errorf("Expected %d bytes from XY labelblk GET.  Got %d instead.", 160*160*8, img.NumBytes())
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
		t.Errorf("Expected %d bytes from XZ labelblk GET.  Got %d instead.", 67*83*8, img.NumBytes())
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
		t.Errorf("Expected %d bytes from YZ labelblk GET.  Got %d instead.", 67*83*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)
}

type labelResp struct {
	Label uint64
}

func TestLabels(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid := dvid.UUID(server.NewTestRepo(t))
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create a labelblk instance
	server.CreateTestInstance(t, uuid, "labelblk", "labels", dvid.Config{})

	vol := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{32, 64, 96},
		name:      "labels",
	}
	vol.postLabelVolume(t, uuid, "", "", 0)

	// Test the "label" endpoint.
	apiStr := fmt.Sprintf("%snode/%s/%s/label/100_64_96", server.WebAPIPath, uuid, "labels")
	jsonResp := server.TestHTTP(t, "GET", apiStr, nil)
	var r labelResp
	if err := json.Unmarshal(jsonResp, &r); err != nil {
		t.Errorf("Unable to parse 'label' endpoint response: %s\n", jsonResp)
	}
	if r.Label != 69 {
		t.Errorf("Expected label %d @ (100, 64, 96) got label %d\n", vol.label(100, 64, 96), r.Label)
	}

	// Test the "labels" endpoint.
	apiStr = fmt.Sprintf("%snode/%s/%s/labels", server.WebAPIPath, uuid, "labels")
	payload := `[[100,64,96],[78,93,156],[104,65,97]]`
	jsonResp = server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(payload))
	var labels [3]uint64
	if err := json.Unmarshal(jsonResp, &labels); err != nil {
		t.Errorf("Unable to parse 'labels' endpoint response: %s\n", jsonResp)
	}
	if labels[0] != vol.label(100, 64, 96) {
		t.Errorf("Expected label %d @ (100, 64, 96) got label %d\n", vol.label(100, 64, 96), labels[0])
	}
	if labels[1] != vol.label(78, 93, 156) {
		t.Errorf("Expected label %d @ (78, 93, 156) got label %d\n", vol.label(78, 93, 156), labels[1])
	}
	if labels[2] != vol.label(104, 65, 97) {
		t.Errorf("Expected label %d @ (104, 65, 97) got label %d\n", vol.label(104, 65, 97), labels[2])
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
	if startLabel != labelNoROI+1 {
		t.Errorf("Expected first voxel to be label %d and got %d instead\n", labelNoROI+1, startLabel)
	}

	// TODO - Use the ROI to retrieve a 2d xy image.

	// TODO - Make sure we aren't getting labels back in non-ROI points.

	// Post again but now with ROI
	var labelWithROI uint64 = 40000
	vol.postLabelVolume(t, uuid, "", roiName, labelWithROI)

	// Verify ROI masking of POST where anything outside ROI is old labels.
	returned = vol.getLabelVolume(t, uuid, "", "")
	var newlabel uint64 = labelWithROI
	var oldlabel uint64 = labelNoROI

	nx := vol.size[0] * vol.blockSize[0]
	ny := vol.size[1] * vol.blockSize[1]
	nz := vol.size[2] * vol.blockSize[2]
	var x, y, z, v int32
	for z = 0; z < nz; z++ {
		voxz := z + vol.offset[2]
		blockz := voxz / DefaultBlockSize
		for y = 0; y < ny; y++ {
			voxy := y + vol.offset[1]
			blocky := voxy / DefaultBlockSize
			for x = 0; x < nx; x++ {
				voxx := x + vol.offset[0]
				blockx := voxx / DefaultBlockSize
				oldlabel++
				newlabel++
				got := binary.LittleEndian.Uint64(returned[v : v+8])
				if inroi(blockx, blocky, blockz) {
					if got != newlabel {
						t.Fatalf("Expected %d in ROI, got %d\n", newlabel, got)
					}
				} else {
					if got != oldlabel {
						t.Fatalf("Expected %d outside ROI, got %d\n", oldlabel, got)
					}
				}
				v += 8
			}
		}
	}

	// Verify that a ROI-enabled GET has zeros everywhere outside ROI.
	returned = vol.getLabelVolume(t, uuid, "", roiName)
	newlabel = labelWithROI

	x, y, z, v = 0, 0, 0, 0
	for z = 0; z < nz; z++ {
		voxz := z + vol.offset[2]
		blockz := voxz / DefaultBlockSize
		for y = 0; y < ny; y++ {
			voxy := y + vol.offset[1]
			blocky := voxy / DefaultBlockSize
			for x = 0; x < nx; x++ {
				voxx := x + vol.offset[0]
				blockx := voxx / DefaultBlockSize
				oldlabel++
				newlabel++
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
}
