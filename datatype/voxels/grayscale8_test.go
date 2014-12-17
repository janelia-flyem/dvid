package voxels

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
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
		grayscaleT, err = datastore.TypeServiceByName("grayscale8")
		if err != nil {
			log.Fatalf("Can't get grayscale type: %s\n", err)
		}
		rgbaT, err = datastore.TypeServiceByName("rgba8")
		if err != nil {
			log.Fatalf("Can't get rgba8 type: %s\n", err)
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
	dataservice, err := repo.NewData(grayscaleT, dvid.DataString(name), config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %s\n", name, err.Error())
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast grayscale8 data service into Data\n")
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
		"typename": "grayscale8",
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
	if parsed.Base.TypeName != "grayscale8" {
		t.Errorf("Parsed new instance has unexpected type name: %s != grayscale8 (expected)\n",
			parsed.Base.TypeName)
	}
	if !parsed.Extended.BlockSize.Equals(dvid.Point3d{64, 43, 28}) {
		t.Errorf("Bad block size in new grayscale8 instance: %s\n", parsed.Extended.BlockSize)
	}
	if !parsed.Extended.VoxelSize.Equals(dvid.NdFloat32{13.1, 14.2, 15.3}) {
		t.Errorf("Bad voxel size in new grayscale8 instance: %s\n", parsed.Extended.VoxelSize)
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
	v, err := grayscale.NewExtHandler(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = PutVoxels(grayscaleCtx, grayscale, v, OpOptions{}); err != nil {
		t.Errorf("Unable to put voxels for %s: %s\n", grayscaleCtx, err.Error())
	}
	if v.NumVoxels() != int64(len(origData)) {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(origData)))
	}

	// Read the stored image
	v2, err := grayscale.NewExtHandler(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = GetVoxels(grayscaleCtx, grayscale, v2, nil); err != nil {
		t.Errorf("Unable to get voxels for %s: %s\n", grayscaleCtx, err.Error())
	}

	// Make sure the retrieved image matches the original
	if v.Stride() != v2.Stride() {
		t.Errorf("Stride in retrieved subvol incorrect\n")
	}
	if v.ByteOrder() != v2.ByteOrder() {
		t.Errorf("ByteOrder in retrieved subvol incorrect\n")
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
	ctx       storage.VersionedContext
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
	v, err := grayscale.NewExtHandler(slice, img)
	if err != nil {
		t.Fatalf("Unable to make new grayscale ExtHandler: %s\n", err.Error())
	}
	if err = PutVoxels(grayscaleCtx, grayscale, v, OpOptions{roi: r}); err != nil {
		t.Errorf("Unable to put voxels for %s: %s\n", grayscaleCtx, err.Error())
	}
	return testDataT{repo, versionID, grayscaleCtx, grayscale, data}
}

func sliceTest(t *testing.T, sliceType string, slice dvid.Geometry) {
	testData := storeGrayscale(t, sliceType, slice, nil)
	grayscaleCtx := datastore.NewVersionedContext(testData.data, testData.versionID)

	// Read the stored image
	v2, err := testData.data.NewExtHandler(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}
	retrieved, err := GetImage(grayscaleCtx, testData.data, v2, nil)
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

/*
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
	config.Set("typename", "grayscale8")
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
}
*/

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
	roiCtx := datastore.NewVersionedContext(roiData, testData.versionID)
	if err = roiData.PutJSON(roiCtx, []byte(testROIJson)); err != nil {
		t.Fatalf("Unable to PUT test ROI: %s\n", err.Error())
	}

	// Get the buffer for the retrieved ROI-enabled grayscale slice
	roiSlice, err := testData.data.NewExtHandler(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}

	// Read without ROI
	if err := GetVoxels(testData.ctx, testData.data, roiSlice, nil); err != nil {
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
	if err := GetVoxels(testData.ctx, testData.data, roiSlice, &roiObj); err != nil {
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
	roiSlice2, err := testData.data.NewExtHandler(slice, nil)
	if err != nil {
		t.Fatalf("Could not create ExtHandler: %s\n", err.Error())
	}

	// Read without ROI
	if err := GetVoxels(testData2.ctx, testData2.data, roiSlice2, nil); err != nil {
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
		t.Errorf("Can't cast grayscale8 data service into Data\n")
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
		t.Errorf("Returned new data instance 2 is not voxels.Data\n")
	}
	if !reflect.DeepEqual(oldData, *grayscale2) {
		t.Errorf("Expected %v, got %v\n", oldData, *grayscale2)
	}
}
