package imageblk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

var (
	grayscaleT,
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
		floatimgT, err = datastore.TypeServiceByName("float32blk")
		if err != nil {
			log.Fatalf("Can't get float32blk type: %s\n", err)
		}
		uint16imgT, err = datastore.TypeServiceByName("uint16blk")
		if err != nil {
			log.Fatalf("Can't get uint16blk type: %s\n", err)
		}
		// uint32imgT, err = datastore.TypeServiceByName("uint32blk")
		// if err != nil {
		// 	log.Fatalf("Can't get uint32blk type: %s\n", err)
		// }
		// uint64imgT, err = datastore.TypeServiceByName("uint64blk")
		// if err != nil {
		// 	log.Fatalf("Can't get uint64blk type: %s\n", err)
		// }
		roiT, err = datastore.TypeServiceByName("roi")
		if err != nil {
			log.Fatalf("Can't get ROI type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

// A slice of bytes representing a 3d float32 volume
type testVolume struct {
	data   []byte
	offset dvid.Point3d
	size   dvid.Point3d
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2], v.offset[0], v.offset[1], v.offset[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()

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

func TestExtentsAndRes(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	makeGrayscale(uuid, t, "grayscale")

	extents := `{
		"MinPoint": [68, 127, 210],
		"MaxPoint": [1023, 4811, 12187]
	}`
	apiStr := fmt.Sprintf("%snode/%s/grayscale/extents", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(extents))

	resolution := `[4.0, 4.0, 4.0]`
	apiStr = fmt.Sprintf("%snode/%s/grayscale/resolution", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(resolution))

	apiStr = fmt.Sprintf("%snode/%s/grayscale/info", server.WebAPIPath, uuid)
	result := server.TestHTTP(t, "GET", apiStr, nil)
	var parsed = struct {
		Base struct {
			TypeName, Name string
		}
		Extended struct {
			BlockSize  dvid.Point3d
			VoxelSize  dvid.NdFloat32
			VoxelUnits dvid.NdString
			MinPoint   dvid.Point3d
			MaxPoint   dvid.Point3d
			MinIndex   dvid.Point3d
			MaxIndex   dvid.Point3d
		}
	}{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Error parsing JSON response of new instance metadata: %v\n", err)
	}
	if !parsed.Extended.MinPoint.Equals(dvid.Point3d{68, 127, 210}) {
		t.Errorf("Bad MinPoint in new uint8blk instance: %s\n", parsed.Extended.MinPoint)
	}
	if !parsed.Extended.MaxPoint.Equals(dvid.Point3d{1023, 4811, 12187}) {
		t.Errorf("Bad MaxPoint in new uint8blk instance: %s\n", parsed.Extended.MaxPoint)
	}
	if !parsed.Extended.MinIndex.Equals(dvid.Point3d{2, 3, 6}) {
		t.Errorf("Bad MinIndex in new uint8blk instance: %s\n", parsed.Extended.MinIndex)
	}
	if !parsed.Extended.MaxIndex.Equals(dvid.Point3d{31, 150, 380}) {
		t.Errorf("Bad MaxIndex in new uint8blk instance: %s\n", parsed.Extended.MaxIndex)
	}
	if !parsed.Extended.VoxelSize.Equals(dvid.NdFloat32{4.0, 4.0, 4.0}) {
		t.Errorf("Bad voxel size in new uint8blk instance: %s\n", parsed.Extended.VoxelSize)
	}
}

func TestForegroundROI(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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

	if err := datastore.BlockOnUpdating(uuid, "foreground"); err != nil {
		t.Fatalf("Error blocking on foreground roi updating: %v\n", err)
	}

	// Check results, making sure it's valid (200).
	roiRequest := fmt.Sprintf("%snode/%s/foreground/roi", server.WebAPIPath, uuid)
	resp := server.TestHTTP(t, "GET", roiRequest, nil)

	// Check results
	// We have block-aligned 2^3 block foreground
	//   from z = 32 -> 95, block coord 1 -> 2 + offset in z by 4 blocks = 5, 6
	//   from y = 32 -> 95, offset in y by 2 blocks = 3, 4
	//   from x = 32 -> 95, offset in x by 5 blocks = 6, 7
	expected := "[[5,3,6,7],[5,4,6,7],[6,3,6,7],[6,4,6,7]]"
	if string(resp) != expected {
		t.Errorf("Expected the following foreground ROI:\n%s\nGot instead:\n%s\n", expected, string(resp))
	}
}

func TestDirectCalls(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	if err = grayscale.IngestVoxels(versionID, 1, v, ""); err != nil {
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	if err := datastore.BlockOnUpdating(uuid, "grayscale"); err != nil {
		t.Fatalf("Error blocking on POST of grayscale: %v\n", err)
	}
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

func TestGrayscaleRepoPersistence(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "mygrayscale")
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
