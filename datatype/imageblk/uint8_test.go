package imageblk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

var (
	grayscaleT,
	rgbaT,
	roiT datastore.TypeService

	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
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
	return datastore.NewTestRepo()
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

func makeGrayscale(uuid dvid.UUID, t *testing.T, name string) *Data {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, grayscaleT, dvid.InstanceName(name), config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %v\n", name, err)
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast uint8blk data service into Data\n")
	}
	return grayscale
}

func TestVoxelsInstanceCreation(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid := dvid.UUID(server.NewTestRepo(t))

	// Create new voxels instance with optional parameters
	name := "grayscale"
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
		t.Fatalf("Error parsing JSON response of new instance metadata: %v\n", err)
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
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()
	grayscale := makeGrayscale(uuid, t, "grayscale")

	// Create a fake 128^3 volume with inner 64^3 foreground and
	// outer background split between 0 and 255 at z = 63,64
	data := make([]uint8, 128*128*128, 128*128*128)
	for z := 64; z < 128; z++ {
		for y := 0; y < 128; y++ {
			for x := 0; x < 128; x++ {
				data[z*128*128+y*128+x] = 255
			}
		}
	}
	for z := 32; z < 96; z++ {
		for y := 32; y < 96; y++ {
			for x := 32; x < 96; x++ {
				data[z*128*128+y*128+x] = 128
			}
		}
	}

	// Put the volume so it's block-aligned
	buf := bytes.NewBuffer(data)
	putRequest := fmt.Sprintf("%snode/%s/grayscale/raw/0_1_2/128_128_128/160_64_128", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", putRequest, buf)

	// Request a foreground ROI
	var reply datastore.Response
	cmd := dvid.Command{"node", string(uuid), "grayscale", "roi", "foreground", "0,255"}
	if err := grayscale.DoRPC(datastore.Request{Command: cmd}, &reply); err != nil {
		t.Fatalf("Error running foreground ROI command: %v\n", err)
	}

	// Check results, making sure it's valid (200).
	var roiJSON string
	for {
		roiRequest := fmt.Sprintf("%snode/%s/foreground/roi", server.WebAPIPath, uuid)
		req, err := http.NewRequest("GET", roiRequest, nil)
		if err != nil {
			t.Fatalf("Unsuccessful GET on foreground ROI: %v", err)
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
	// We have block-aligned 2^3 block foreground
	//   from z = 32 -> 95, block coord 1 -> 2 + offset in z by 4 blocks = 5, 6
	//   from y = 32 -> 95, offset in y by 2 blocks = 3, 4
	//   from x = 32 -> 95, offset in x by 5 blocks = 6, 7
	expected := "[[5,3,6,7],[5,4,6,7],[6,3,6,7],[6,4,6,7]]"
	if roiJSON != expected {
		t.Errorf("Expected the following foreground ROI:\n%s\nGot instead:\n%s\n", expected, roiJSON)
	}
}

func TestDirectCalls(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, versionID := initTestRepo()
	grayscale := makeGrayscale(uuid, t, "grayscale")
	grayscaleCtx := datastore.NewVersionedCtx(grayscale, versionID)

	// Create a block-aligned 8-bit grayscale image
	offset := dvid.Point3d{512, 32, 1024}
	size := dvid.Point3d{128, 96, 64}
	subvol := dvid.NewSubvolume(offset, size)
	data := makeVolume(offset, size)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Store it into datastore at root
	v, err := grayscale.NewVoxels(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new grayscale voxels: %v\n", err)
	}
	if err = grayscale.IngestVoxels(versionID, v, ""); err != nil {
		t.Errorf("Unable to put voxels for %s: %v\n", grayscaleCtx, err)
	}
	if v.NumVoxels() != int64(len(origData)) {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(origData)))
	}

	// Read the stored image
	v2, err := grayscale.NewVoxels(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new grayscale ExtHandler: %v\n", err)
	}
	if err = grayscale.GetVoxels(versionID, v2, ""); err != nil {
		t.Errorf("Unable to get voxels for %s: %v\n", grayscaleCtx, err)
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
			t.Logf("Data returned != data stored for voxel %d\n", i)
			t.Logf("Size of data: %d bytes from GET, %d bytes in PUT\n", len(data), len(origData))
			t.Fatalf("GET subvol (%d) != PUT subvol (%d) @ index %d", data[i], origData[i], i)
		}
	}
}

func TestBlockAPI(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()
	grayscale := makeGrayscale(uuid, t, "grayscale")

	// construct random blocks of data.
	numBlockBytes := int32(grayscale.BlockSize().Prod())
	testBlocks := 1001
	var blockData []byte
	for i := 0; i < testBlocks; i++ {
		blockData = append(blockData, dvid.RandomBytes(numBlockBytes)...)
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
	totBytes := testBlocks * int(numBlockBytes)
	if len(returnedData) != totBytes {
		t.Errorf("Returned %d bytes, expected %d bytes", len(returnedData), totBytes)
	}
	if !reflect.DeepEqual(returnedData, blockData) {
		t.Errorf("Returned block data != original block data\n")
	}

	// We should get blank blocks at different z
	z += 1
	blockReq = fmt.Sprintf("%snode/%s/grayscale/blocks/%d_%d_%d/%d", server.WebAPIPath, uuid, x, y, z, testBlocks)
	returnedData = server.TestHTTP(t, "GET", blockReq, nil)
	if len(returnedData) != totBytes {
		t.Errorf("Returned %d bytes, expected %d bytes", len(returnedData), totBytes)
	}
	for i, b := range returnedData {
		if b != 0 {
			t.Fatalf("Expected 0 at returned byte %d, got %d instead.\n", i, b)
		}
	}
}

/*
func TestROIMaskGrayscale8(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	offset := dvid.Point3d{312, 87, 67}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	if err != nil {
		t.Fatalf("Problem getting new orthogonal slice: %s\n", err)
	}
	testData := storeGrayscale(t, "XY", slice, nil)

	// Create ROI
	config := dvid.NewConfig()
	dataservice, err := testData.repo.NewData(roiT, "roi", config)
	if err != nil {
		t.Fatalf("Error creating new roi instance: %s\n", err)
	}
	roiData, ok := dataservice.(*roi.Data)
	if !ok {
		t.Fatalf("Returned new data instance is not roi.Data\n")
	}

	// write ROI
	if err = roiData.PutJSON(testData.versionID, []byte(testROIJson)); err != nil {
		t.Fatalf("Unable to PUT test ROI: %s\n", err)
	}

	// Get the buffer for the retrieved ROI-enabled grayscale slice
	roiSlice, err := testData.data.NewVoxels(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err)
	}

	// Read without ROI
	if err := testData.data.GetVoxels(testData.versionID, roiSlice, nil); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData.ctx, err)
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
		t.Fatalf("Error making ROI iterator: %s\n", err)
	}

	// Read grayscale using ROI
	if err := testData.data.GetVoxels(testData.versionID, roiSlice, &roiObj); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData.ctx, err)
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
		t.Fatalf("Could not create ExtHandler: %s\n", err)
	}

	// Read without ROI
	if err := testData2.data.GetVoxels(testData2.versionID, roiSlice2, nil); err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", testData2.ctx, err)
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
*/

func TestGrayscaleRepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make grayscale and set various properties
	config := dvid.NewConfig()
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	dataservice, err := datastore.NewData(uuid, grayscaleT, "mygrayscale", config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance: %s\n", err)
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast uint8 data service into Data\n")
	}
	oldData := *grayscale

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, grayscale); err != nil {
		t.Fatalf("Unable to save repo during grayscale persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUID(uuid, "mygrayscale")
	if err != nil {
		t.Fatalf("Can't get grayscale instance from reloaded test db: %v\n", err)
	}
	grayscale2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !oldData.Equals(grayscale2) {
		t.Errorf("Expected %v, got %v\n", oldData, *grayscale2)
	}
}

// Should intersect 100x100 image at Z = 67 and Y = 108
const testROIJson = "[[2,3,10,10],[2,4,12,13]]"

/*
// Create, store, and retrieve data using HTTP API
func TestGrayscale8HTTPAPI(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	_, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err)
	}

	// Create a data instance
	name := "grayscaleTest"
	var config dvid.Config
	config.Set("typename", "uint8")
	config.Set("dataname", name)
	apiStr := fmt.Sprintf("%srepo/%s/instance", server.WebAPIPath, uuid)
	req, err := http.NewRequest("POST", apiStr, config)
	if err != nil {
		t.Fatalf("Unsuccessful POST request (%s): %s\n", apiStr, err)
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
		t.Fatalf("Unsuccessful POST request (%s): %s\n", apiStr, err)
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

	// Use an ROI mask
}
*/
