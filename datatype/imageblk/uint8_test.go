package imageblk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image/png"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	grayscaleT,
	rgbaT,
	roiT datastore.TypeService

	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if grayscaleT == nil {
		var err error
		grayscaleT, err = datastore.TypeServiceByName("uint8blk")
		if err != nil {
			log.Fatalf("Can't get uint8blk type: %s\n", err)
		}
		rgbaT, err = datastore.TypeServiceByName("rgba8blk")
		if err != nil {
			log.Fatalf("Can't get rgba8blk type: %s\n", err)
		}
		roiT, err = datastore.TypeServiceByName("roi")
		if err != nil {
			log.Fatalf("Can't get ROI type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// Data from which to construct repeatable 3d images where adjacent voxels have different values.
var xdata = []byte{'\x01', '\x07', '\xAF', '\xFF', '\x70'}
var ydata = []byte{'\x33', '\xB2', '\x77', '\xD0', '\x4F'}
var zdata = []byte{'\x5C', '\x89', '\x40', '\x13', '\xCA'}

// Make a 2d slice of bytes with top left corner at (ox,oy,oz) and size (nx,ny)
func makeSlice(offset dvid.Point3d, size dvid.Point2d) []byte {
	slice := make([]byte, size[0]*size[1], size[0]*size[1])
	i := 0
	modz := offset[2] % int32(len(zdata))
	for y := int32(0); y < size[1]; y++ {
		sy := y + offset[1]
		mody := sy % int32(len(ydata))
		sx := offset[0]
		for x := int32(0); x < size[0]; x++ {
			modx := sx % int32(len(xdata))
			slice[i] = xdata[modx] ^ ydata[mody] ^ zdata[modz]
			i++
			sx++
		}
	}
	return slice
}

// Make a 3d volume of bytes with top left corner at (ox,oy,oz) and size (nx, ny, nz)
func makeVolume(offset, size dvid.Point3d) []byte {
	sliceBytes := size[0] * size[1]
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

func makeGrayscale(repo datastore.Repo, t *testing.T, name string) *Data {
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(grayscaleT, dvid.InstanceName(name), config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %s\n", name, err.Error())
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast uint8blk data service into Data\n")
	}
	return grayscale
}

func TestVoxelsInstanceCreation(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := dvid.UUID(server.NewTestRepo(t))

	// Create new voxels instance with optional parameters
	name := "mygrayscale"
	metadata := fmt.Sprintf(`{
		"typename": "uint8blk",
		"dataname": %q,
		"blocksize": "64,43,28",
		"VoxelSize": "13.1, 14.2, 15.3",
		"VoxelUnits": "picometers,nanometers,microns"
	}`, name)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(metadata))

	// Get metadata and make sure optional settings have been set.
	apiStr = fmt.Sprintf("%snode/%s/%s/info", server.WebAPIPath, uuid, name)
	result := server.TestHTTP(t, "GET", apiStr, nil)
	var parsed = struct {
		Base struct {
			TypeName, Name string
		}
		Extended struct {
			BlockSize  dvid.Point3d
			VoxelSize  dvid.NdFloat32
			VoxelUnits dvid.NdString
		}
	}{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Error parsing JSON response of new instance metadata: %s\n", err.Error())
	}
	if parsed.Base.Name != name {
		t.Errorf("Parsed new instance has unexpected name: %s != %s (expected)\n",
			parsed.Base.Name, name)
	}
	if parsed.Base.TypeName != "uint8blk" {
		t.Errorf("Parsed new instance has unexpected type name: %s != uint8blk (expected)\n",
			parsed.Base.TypeName)
	}
	if !parsed.Extended.BlockSize.Equals(dvid.Point3d{64, 43, 28}) {
		t.Errorf("Bad block size in new uint8blk instance: %s\n", parsed.Extended.BlockSize)
	}
	if !parsed.Extended.VoxelSize.Equals(dvid.NdFloat32{13.1, 14.2, 15.3}) {
		t.Errorf("Bad voxel size in new uint8blk instance: %s\n", parsed.Extended.VoxelSize)
	}
	if parsed.Extended.VoxelUnits[0] != "picometers" {
		t.Errorf("Got %q for X voxel units, not picometers\n", parsed.Extended.VoxelUnits[0])
	}
	if parsed.Extended.VoxelUnits[1] != "nanometers" {
		t.Errorf("Got %q for X voxel units, not picometers\n", parsed.Extended.VoxelUnits[0])
	}
	if parsed.Extended.VoxelUnits[2] != "microns" {
		t.Errorf("Got %q for X voxel units, not picometers\n", parsed.Extended.VoxelUnits[0])
	}
}

func TestForegroundROI(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()
	grayscale := makeGrayscale(repo, t, "grayscale")

	// Create a fake 500x500 8-bit grayscale image with inner 250x250 foreground and
	// outer background (value 0)
	data := make([]uint8, 500*500, 500*500)
	for y := 125; y <= 375; y++ {
		for x := 125; x <= 375; x++ {
			data[y*500+x] = 128
		}
		for x := 376; x < 500; x++ {
			data[y*500+x] = 255
		}
	}
	testImage := dvid.ImageGrayFromData(data, 500, 500)

	// PUT a slice at (127,64,64)
	var buf bytes.Buffer
	if err := png.Encode(&buf, testImage); err != nil {
		t.Fatalf("Unable to encode test image\n")
	}
	putRequest := fmt.Sprintf("%snode/%s/grayscale/raw/xy/500_500/127_64_64", server.WebAPIPath, repo.RootUUID())
	server.TestHTTP(t, "POST", putRequest, &buf)

	// PUT another slice at (1000,14,138)
	var buf2 bytes.Buffer
	if err := png.Encode(&buf2, testImage); err != nil {
		t.Fatalf("Unable to encode test image\n")
	}
	putRequest = fmt.Sprintf("%snode/%s/grayscale/raw/xy/500_500/1000_14_138", server.WebAPIPath, repo.RootUUID())
	server.TestHTTP(t, "POST", putRequest, &buf2)

	// Request a foreground ROI
	var reply datastore.Response
	cmd := dvid.Command{"node", string(repo.RootUUID()), "grayscale", "roi", "foreground", "0,255"}
	if err := grayscale.DoRPC(datastore.Request{Command: cmd}, &reply); err != nil {
		t.Fatalf("Error running foreground ROI command: %s\n", err.Error())
	}

	// Check results, making sure it's valid (200).
	var roiJSON string
	for {
		roiRequest := fmt.Sprintf("%snode/%s/foreground/roi", server.WebAPIPath, repo.RootUUID())
		req, err := http.NewRequest("GET", roiRequest, nil)
		if err != nil {
			t.Fatalf("Unsuccessful GET on foreground ROI: %s", err.Error())
		}
		w := httptest.NewRecorder()
		server.ServeSingleHTTP(w, req)
		roiJSON = string(w.Body.Bytes())
		if w.Code == http.StatusOK {
			break
		} else if w.Code != http.StatusPartialContent {
			t.Fatalf("Unknown reponse for GET foreground ROI (%d): %s\n", w.Code, roiJSON)
		}
	}

	// Check results
	// Offset for first slice: (127,64,64) with foreground starting 125 pixels in.
	//    Block index Z = 64 / 32 = 2
	//    Block index Y0 = (64 + 124) / 32 = 5
	//    Block index Y1 = (64 + 374) / 32 = 13
	//    Block index X0 = (127 + 125) / 32 = 7
	//	  Block index X1 = (127 + 374) / 32 = 15
	// Offset for first slice: (1000,14,138) with foreground starting 125 pixels in.
	//    Block index Z = 138 / 32 = 4
	//    Block index Y0 = (14 + 124) / 32 = 4
	//    Block index Y1 = (14 + 374) / 32 = 12
	//    Block index X0 = (1000 + 124) / 32 = 35
	//	  Block index X1 = (1000 + 374) / 32 = 42
	expected := "[[2,5,7,15],[2,6,7,15],[2,7,7,15],[2,8,7,15],[2,9,7,15],[2,10,7,15],[2,11,7,15],[2,12,7,15],[2,13,7,15],[4,4,35,42],[4,5,35,42],[4,6,35,42],[4,7,35,42],[4,8,35,42],[4,9,35,42],[4,10,35,42],[4,11,35,42],[4,12,35,42]]"
	if roiJSON != expected {
		t.Errorf("Expected the following foreground ROI:\n%s\nGot instead:\n%s\n", expected, roiJSON)
	}
}

func TestSubvolGrayscale8(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()
	grayscale := makeGrayscale(repo, t, "grayscale")
	grayscaleCtx := datastore.NewVersionedContext(grayscale, versionID)

	// Create a fake 100x100x100 8-bit grayscale image
	offset := dvid.Point3d{5, 35, 61}
	size := dvid.Point3d{100, 100, 100}
	subvol := dvid.NewSubvolume(offset, size)
	data := makeVolume(offset, size)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Store it into datastore at root
	v, err := grayscale.NewVoxels(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = grayscale.PutVoxels(versionID, v, nil); err != nil {
		t.Errorf("Unable to put voxels for %s: %s\n", grayscaleCtx, err.Error())
	}
	if v.NumVoxels() != int64(len(origData)) {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(origData)))
	}

	// Read the stored image
	v2, err := grayscale.NewVoxels(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = grayscale.GetVoxels(versionID, v2, nil); err != nil {
		t.Errorf("Unable to get voxels for %s: %s\n", grayscaleCtx, err.Error())
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
	data = v2.Data()
	//dvid.PrintNonZero("original value", origData)
	//dvid.PrintNonZero("returned value", data)
	for i := int64(0); i < v2.NumVoxels(); i++ {
		if data[i] != origData[i] {
			t.Logf("Size of data: %d bytes from GET, %d bytes in PUT\n", len(data), len(origData))
			t.Fatalf("GET subvol (%d) != PUT subvol (%d) @ index %d", data[i], origData[i], i)
		}
	}
}

type testDataT struct {
	repo      datastore.Repo
	versionID dvid.VersionID
	ctx       *datastore.VersionedContext
	data      *Data
	rawData   []uint8
}

func storeGrayscale(t *testing.T, name string, slice dvid.Geometry, r *ROI) testDataT {
	repo, versionID := initTestRepo()
	grayscale := makeGrayscale(repo, t, "grayscale"+name)
	grayscaleCtx := datastore.NewVersionedContext(grayscale, versionID)

	// Create a fake 100x100 8-bit grayscale image
	nx := slice.Size().Value(0)
	ny := slice.Size().Value(1)
	offset := slice.StartPoint().(dvid.Point3d)
	data := []uint8(makeSlice(offset, dvid.Point2d{nx, ny}))
	img := dvid.ImageGrayFromData(data, int(nx), int(ny))

	// Store it into datastore at root
	v, err := grayscale.NewVoxels(slice, img)
	if err != nil {
		t.Fatalf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = grayscale.PutVoxels(versionID, v, r); err != nil {
		t.Errorf("Unable to put voxels for %s: %s\n", grayscaleCtx, err.Error())
	}
	return testDataT{repo, versionID, grayscaleCtx, grayscale, data}
}

func sliceTest(t *testing.T, sliceType string, slice dvid.Geometry) {
	testData := storeGrayscale(t, sliceType, slice, nil)
	grayscaleCtx := datastore.NewVersionedContext(testData.data, testData.versionID)

	// Read the stored image
	v2, err := testData.data.NewVoxels(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}
	retrieved, err := testData.data.GetImage(testData.versionID, v2, nil)
	if err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", grayscaleCtx, err.Error())
	}

	// Make sure the retrieved image matches the original
	goImg := retrieved.Get()
	retrievedData, voxelSize, _, err := dvid.ImageData(goImg)
	if err != nil {
		t.Errorf("Unable to get data/size from retrieved image: %s\n", err.Error())
	}
	if !reflect.DeepEqual(retrievedData, testData.rawData) {
		t.Errorf("Retrieved data differs from original data\n")
	}
	if voxelSize != int32(1) {
		t.Errorf("Retrieved voxel size in bytes incorrect.  Got %d, expected 1\n", voxelSize)
	}
}

func TestXYSliceGrayscale8(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	offset := dvid.Point3d{3, 13, 24}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, "XY", slice)
}

func TestXZSliceGrayscale8(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	offset := dvid.Point3d{31, 10, 14}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, "XZ", slice)
}

func TestYZSliceGrayscale8(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	offset := dvid.Point3d{13, 40, 99}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, "YZ", slice)
}

func TestBlockAPI(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()
	grayscale := makeGrayscale(repo, t, "grayscale")
	uuid := repo.RootUUID()

	// construct random blocks of data.
	numBlockBytes := int32(grayscale.BlockSize().Prod())
	testBlocks := 1001
	var blockData []byte
	for i := 0; i < testBlocks; i++ {
		blockData = append(blockData, tests.RandomBytes(numBlockBytes)...)
	}

	// set start of span
	x := rand.Int31()
	y := rand.Int31()
	z := rand.Int31()

	// Test uncompressed roundtrip

	// send the blocks
	blockReq := fmt.Sprintf("%snode/%s/grayscale/blocks/%d_%d_%d/%d", server.WebAPIPath, uuid, x, y, z, testBlocks)
	server.TestHTTP(t, "POST", blockReq, bytes.NewBuffer(blockData))

	// read same span of blocks
	returnedData := server.TestHTTP(t, "GET", blockReq, nil)

	// make sure blocks are same
	totBytes := 4 + testBlocks*int(numBlockBytes)
	if len(returnedData) != totBytes {
		t.Errorf("Returned %d bytes, expected %d bytes", len(returnedData), totBytes)
	}
	if !reflect.DeepEqual(returnedData[4:], blockData) {
		t.Errorf("Returned block data != original block data\n")
	}

	// We shouldn't be able to get blocks at different z
	z += 1
	blockReq = fmt.Sprintf("%snode/%s/grayscale/blocks/%d_%d_%d/%d", server.WebAPIPath, uuid, x, y, z, testBlocks)
	returnedData = server.TestHTTP(t, "GET", blockReq, nil)
	if len(returnedData) != 4 {
		t.Errorf("Expected only 0 blocks header, got %d bytes instead", len(returnedData))
	}
	var numBlocks int32
	buf := bytes.NewReader(returnedData)
	if err := binary.Read(buf, binary.LittleEndian, &numBlocks); err != nil {
		t.Errorf("Error reading # blocks header: %s\n", err.Error())
	}
	if numBlocks != 0 {
		t.Errorf("Expected 0 blocks for empty area, got %d instead", numBlocks)
	}
}

// Should intersect 100x100 image at Z = 67 and Y = 108
const testROIJson = "[[2,3,10,10],[2,4,12,13]]"

func TestROIMaskGrayscale8(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	offset := dvid.Point3d{312, 87, 67}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	if err != nil {
		t.Fatalf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	testData := storeGrayscale(t, "XY", slice, nil)

	// Create ROI
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := testData.repo.NewData(roiT, "roi", config)
	if err != nil {
		t.Fatalf("Error creating new roi instance: %s\n", err.Error())
	}
	roiData, ok := dataservice.(*roi.Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// write ROI
	if err = roiData.PutJSON(testData.versionID, []byte(testROIJson)); err != nil {
		t.Fatalf("Unable to PUT test ROI: %s\n", err.Error())
	}

	// Get the buffer for the retrieved ROI-enabled grayscale slice
	roiSlice, err := testData.data.NewVoxels(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}

	// Read without ROI
	if err := testData.data.GetVoxels(testData.versionID, roiSlice, nil); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData.ctx, err.Error())
	}

	// Check points in and outside ROI are set appropriately.
	roiX := 10*32 + 1 - offset[0]
	roiY := 3*32 + 1 - offset[1]
	roiIndex := roiY*roiSlice.Stride() + roiX
	nonroiData := roiSlice.Data()
	if nonroiData[roiIndex] != testData.rawData[roiIndex] {
		t.Fatalf("Stored pixel in slice should be %d but is %d\n", testData.rawData[roiIndex], nonroiData[roiIndex])
	}
	if nonroiData[roiIndex] == 0 {
		t.Fatalf("Expected non-zero pixel in roi slice test")
	}

	// Initialize ROI iterator
	var roiObj ROI
	roiObj.Iter, err = roi.NewIterator("roi", testData.versionID, roiSlice)
	if err != nil {
		t.Fatalf("Error making ROI iterator: %s\n", err.Error())
	}

	// Read grayscale using ROI
	if err := testData.data.GetVoxels(testData.versionID, roiSlice, &roiObj); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData.ctx, err.Error())
	}

	// Make sure few points are zero and others aren't
	pixels := roiSlice.Data()
	if pixels[0] != 0 {
		t.Fatalf("Retrieved ROI-applied XY pixel != 0 and should be 0\n")
	}
	if pixels[roiIndex] == 0 {
		t.Fatalf("Retrieved ROI-applied XY pixel.  Expected non-zero pixel %d got 0\n",
			testData.rawData[roiIndex])
	}

	// Add new grayscale that uses mask for PUT.
	roiObj.Iter.Reset()
	testData2 := storeGrayscale(t, "maskedPUT", slice, &roiObj)

	// Read grayscale without ROI and make sure area outside ROI is black.
	// Get the buffer for the retrieved ROI-enabled grayscale slice
	roiSlice2, err := testData.data.NewVoxels(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}

	// Read without ROI
	if err := testData2.data.GetVoxels(testData2.versionID, roiSlice2, nil); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData2.ctx, err.Error())
	}

	// Make sure points outside ROI are black and the one inside is on.
	pixels = roiSlice2.Data()
	if pixels[0] != 0 {
		t.Fatalf("Retrieved XY pixel != 0 when PUT should be outside ROI and therefore 0\n")
	}
	if pixels[roiIndex] == 0 {
		t.Fatalf("Retrieved XY pixel == 0 when PUT was within ROI and expected non-zero pixel %d\n",
			testData2.rawData[roiIndex])
	}
}

func TestGrayscaleRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make grayscale and set various properties
	config := dvid.NewConfig()
	config.SetVersioned(true)
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	dataservice, err := repo.NewData(grayscaleT, "mygrayscale", config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance: %s\n", err.Error())
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast uint8 data service into Data\n")
	}
	oldData := *grayscale

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during grayscale persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mygrayscale")
	if err != nil {
		t.Fatalf("Can't get grayscale instance from reloaded test db: %s\n", err.Error())
	}
	grayscale2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !reflect.DeepEqual(oldData, *grayscale2) {
		t.Errorf("Expected %v, got %v\n", oldData, *grayscale2)
	}
}

/* TODO -- Improve testing of voxels handling

// Create, store, and retrieve data using HTTP API
func TestGrayscale8HTTPAPI(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	_, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	// Create a data instance
	name := "grayscaleTest"
	var config dvid.Config
	config.Set("typename", "uint8")
	config.Set("dataname", name)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	req, err := http.NewRequest("POST", apiStr, config)
	if err != nil {
		t.Fatalf("Unsuccessful POST request (%s): %s\n", apiStr, err.Error())
	}
	w := httptest.NewRecorder()
	server.WebMux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Bad server response POST to %s, status %d, for %q: %s\n", apiStr, w.Code, name,
			string(w.Body.Bytes()))
	}

	// Test badly formatted API calls return appropriate errors
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/250_250/0_0_0", server.WebAPIPath, uuid, name)
	req, err = http.NewRequest("GET", apiStr, nil)
	if err != nil {
		t.Fatalf("Unsuccessful POST request (%s): %s\n", apiStr, err.Error())
	}
	w = httptest.NewRecorder()
	server.WebMux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Bad server response POST to %s, status %d, for %q: %s\n", apiStr, w.Code, name,
			string(w.Body.Bytes()))
	}

	// Post data to the instance

	// Get a 2d XY image

	// Get a 2d XZ image

	// Get a 2d YZ image

	// Get a 3d image

	// Create a new version in repo

	// Modify a portion of the preceding grayscale slice

	// Read the first version grayscale

	// Make sure it doesn't have the new stuff

	// Read the second version grayscale

	// Make sure it has modified voxels
}
*/
