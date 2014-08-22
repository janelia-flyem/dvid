package voxels

import (
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	grayscaleT, rgbaT datastore.TypeService
	testMu            sync.Mutex
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

func makeGrayscale(repo datastore.Repo, t *testing.T, name dvid.DataString) *Data {
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(grayscaleT, name, config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %s\n", name, err.Error())
	}
	grayscale, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast grayscale8 data service into Data\n")
	}
	return grayscale
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
	if err = PutVoxels(grayscaleCtx, grayscale, v); err != nil {
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
	if err = GetVoxels(grayscaleCtx, grayscale, v2); err != nil {
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

func sliceTest(t *testing.T, slice dvid.Geometry) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()
	grayscale := makeGrayscale(repo, t, "grayscale2")
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
	if err = PutVoxels(grayscaleCtx, grayscale, v); err != nil {
		t.Errorf("Unable to put voxels for %s: %s\n", grayscaleCtx, err.Error())
	}

	// Read the stored image
	v2, err := grayscale.NewExtHandler(slice, nil)
	retrieved, err := GetImage(grayscaleCtx, grayscale, v2)
	if err != nil {
		t.Fatalf("Unable to get image for %s: %s\n", grayscaleCtx, err.Error())
	}

	// Make sure the retrieved image matches the original
	goImg := retrieved.Get()
	if !reflect.DeepEqual(img.Rect, goImg.Bounds()) {
		t.Errorf("Retrieved image size %s different than original %s\n",
			goImg.Bounds(), img.Rect)
	}
	retrievedData, voxelSize, _, err := dvid.ImageData(goImg)
	if err != nil {
		t.Errorf("Unable to get data/size from retrieved image: %s\n", err.Error())
	}
	if !reflect.DeepEqual(retrievedData, data) {
		t.Errorf("Retrieved data differs from original data\n")
	}
	if voxelSize != int32(1) {
		t.Errorf("Retrieved voxel size in bytes incorrect.  Got %d, expected 1\n", voxelSize)
	}
	//dvid.PrintNonZero("original value", data)
	//dvid.PrintNonZero("returned value", retrievedData)
	for i := 0; i < len(data); i++ {
		if data[i] != retrievedData[i] {
			t.Fatalf("GET slice (%d) != PUT slice (%d) @ index %d", retrievedData[i], data[i], i)
		}
	}
}

func TestXYSliceGrayscale8(t *testing.T) {
	offset := dvid.Point3d{3, 13, 24}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XY, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, slice)
}

func TestXZSliceGrayscale8(t *testing.T) {
	offset := dvid.Point3d{31, 10, 14}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.XZ, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, slice)
}

func TestYZSliceGrayscale8(t *testing.T) {
	offset := dvid.Point3d{13, 40, 99}
	size := dvid.Point2d{100, 100}
	slice, err := dvid.NewOrthogSlice(dvid.YZ, offset, size)
	if err != nil {
		t.Errorf("Problem getting new orthogonal slice: %s\n", err.Error())
	}
	sliceTest(t, slice)
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
