package labelblk

import (
	"encoding/binary"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	labelsT, rgbaT datastore.TypeService
	testMu         sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
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
	return tests.NewRepo()
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
func newDataInstance(repo datastore.Repo, t *testing.T, name dvid.InstanceName) *Data {
	config := dvid.NewConfig()
	dataservice, err := repo.NewData(labelsT, name, config)
	if err != nil {
		t.Errorf("Unable to create labelblk instance %q: %s\n", name, err.Error())
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labels data service into Data\n")
	}
	return labels
}

func TestLabelblkDirectAPI(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, versionID := initTestRepo()
	labels := newDataInstance(repo, t, "mylabels")
	labelsCtx := datastore.NewVersionedContext(labels, versionID)

	// Create a fake block-aligned label volume
	offset := dvid.Point3d{32, 0, 64}
	size := dvid.Point3d{96, 64, 160}
	subvol := dvid.NewSubvolume(offset, size)
	data := makeVolume(offset, size)

	// Store it into datastore at root
	v, err := labels.NewVoxels(subvol, data)
	if err != nil {
		t.Fatalf("Unable to make new labels Voxels: %s\n", err.Error())
	}
	if err = labels.PutVoxels(versionID, v, nil); err != nil {
		t.Errorf("Unable to put labels for %s: %s\n", labelsCtx, err.Error())
	}
	if v.NumVoxels() != int64(len(data))/8 {
		t.Errorf("# voxels (%d) after PutVoxels != # original voxels (%d)\n",
			v.NumVoxels(), int64(len(data))/8)
	}

	// Read the stored image
	v2, err := labels.NewVoxels(subvol, nil)
	if err != nil {
		t.Errorf("Unable to make new labels ExtHandler: %s\n", err.Error())
	}
	if err = labels.GetVoxels(versionID, v2, nil); err != nil {
		t.Errorf("Unable to get voxels for %s: %s\n", labelsCtx, err.Error())
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
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.Set("BlockSize", "12,13,14")
	config.Set("VoxelSize", "1.1,2.8,11")
	config.Set("VoxelUnits", "microns,millimeters,nanometers")
	dataservice, err := repo.NewData(labelsT, "mylabels", config)
	if err != nil {
		t.Errorf("Unable to create labels instance: %s\n", err.Error())
	}
	labels, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labels data service into Data\n")
	}
	oldData := *labels

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during labels persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mylabels")
	if err != nil {
		t.Fatalf("Can't get labels instance from reloaded test db: %s\n", err.Error())
	}
	labels2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imageblk.Data\n")
	}
	if !oldData.Equals(labels2) {
		t.Errorf("Expected %v, got %v\n", oldData, *labels2)
	}
}
