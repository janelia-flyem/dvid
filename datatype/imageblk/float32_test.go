package imageblk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

var (
	floatimgT datastore.TypeService
)

// A slice of bytes representing a 3d float32 volume
type testVolume struct {
	data   []byte
	offset dvid.Point3d
	size   dvid.Point3d
}

func newFloatVolume(t *testing.T, offset, size dvid.Point3d) *testVolume {
	var buf bytes.Buffer
	for i := int64(0); i < size.Prod(); i++ {
		if err := binary.Write(&buf, binary.LittleEndian, float32(i)); err != nil {
			t.Fatalf("couldn't write to float volume: %v\n", err)
		}
	}
	return &testVolume{
		data:   buf.Bytes(),
		offset: offset,
		size:   size,
	}
}

// Put label data into given data instance.
func (v *testVolume) put(t *testing.T, uuid dvid.UUID, name string) {
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d?mutate=true", server.WebAPIPath,
		uuid, name, v.size[0], v.size[1], v.size[2], v.offset[0], v.offset[1], v.offset[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(v.data))
}

func createFloatTestVolume(t *testing.T, uuid dvid.UUID, name string, offset, size dvid.Point3d) *testVolume {
	volume := newFloatVolume(t, offset, size)

	// Send data over HTTP to populate a data instance
	volume.put(t, uuid, name)
	return volume
}

func TestFloatInstanceCreation(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := datastore.NewTestRepo()

	// Create new voxels instance with optional parameters
	name := "weights"
	metadata := fmt.Sprintf(`{
		"typename": "float32blk",
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
	if parsed.Base.TypeName != "float32blk" {
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

func TestFloatDirectCalls(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, versionID := initTestRepo()

	server.CreateTestInstance(t, uuid, "float32blk", "floatimg", dvid.Config{})
	dataservice, err := datastore.GetDataByUUIDName(uuid, "floatimg")
	if err != nil {
		t.Fatal(err)
	}
	floatimg, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't convert dataservice %v into imageblk.Data\n", dataservice)
	}
	ctx := datastore.NewVersionedCtx(floatimg, versionID)

	// Create a block-aligned 8-bit grayscale image
	offset := dvid.Point3d{512, 32, 1024}
	size := dvid.Point3d{128, 96, 64}
	subvol := dvid.NewSubvolume(offset, size)
	testvol := createFloatTestVolume(t, uuid, "floatimg", offset, size)
	origData := make([]byte, len(testvol.data))
	copy(origData, testvol.data)

	// Store it into datastore at root
	v, err := floatimg.NewVoxels(subvol, testvol.data)
	if err != nil {
		t.Fatalf("Unable to make new floatimg voxels: %v\n", err)
	}
	if err = floatimg.IngestVoxels(versionID, 1, v, ""); err != nil {
		t.Errorf("Unable to put voxels for %s: %v\n", ctx, err)
	}

	// Read the stored image
	v2, err := floatimg.NewVoxels(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new grayscale ExtHandler: %v\n", err)
	}
	if err = floatimg.GetVoxels(versionID, v2, ""); err != nil {
		t.Errorf("Unable to get voxels for %s: %v\n", ctx, err)
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
	if len(v.Data()) != len(v2.Data()) {
		t.Errorf("Expected %d bytes in retrieved data, got %d bytes\n", len(v.Data()), len(v2.Data()))
	}
	received := v2.Data()
	//dvid.PrintNonZero("original value", origData)
	//dvid.PrintNonZero("returned value", data)
	for i := int64(0); i < v2.NumVoxels(); i++ {
		if received[i] != origData[i] {
			t.Logf("Data returned != data stored for voxel %d\n", i)
			t.Logf("Size of data: %d bytes from GET, %d bytes in PUT\n", len(received), len(origData))
			t.Fatalf("GET subvol (%d) != PUT subvol (%d) @ index %d", received[i], origData[i], i)
		}
	}
}

func TestFloat32RepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make grayscale and set various properties
	config := dvid.NewConfig()
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	dataservice, err := datastore.NewData(uuid, floatimgT, "floatimg", config)
	if err != nil {
		t.Errorf("Unable to create float32 instance: %s\n", err)
	}
	floatimg, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast float32 data service into Data\n")
	}
	oldData := *floatimg

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, floatimg); err != nil {
		t.Fatalf("Unable to save repo during floatimg persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "floatimg")
	if err != nil {
		t.Fatalf("Can't get floatimg instance from reloaded test db: %v\n", err)
	}
	floatimg2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !oldData.Equals(floatimg2) {
		t.Errorf("Expected %v, got %v\n", oldData, *floatimg2)
	}
}
