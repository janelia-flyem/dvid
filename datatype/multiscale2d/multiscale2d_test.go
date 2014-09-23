package multiscale2d

import (
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	mstype, grayscaleT datastore.TypeService
	testMu             sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if mstype == nil {
		var err error
		mstype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get multiscale2d type: %s\n", err)
		}
		grayscaleT, err = datastore.TypeServiceByName("grayscale8")
		if err != nil {
			log.Fatalf("Can't get grayscale type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

func makeGrayscale(repo datastore.Repo, t *testing.T, name dvid.DataString) *voxels.Data {
	config := dvid.NewConfig()
	config.SetVersioned(true)
	dataservice, err := repo.NewData(grayscaleT, name, config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %s\n", name, err.Error())
	}
	grayscale, ok := dataservice.(*voxels.Data)
	if !ok {
		t.Errorf("Can't cast grayscale8 data service into Data\n")
	}
	return grayscale
}

const testTileSpec = `
{
    "0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
    "1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
    "2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
    "3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
}
`

func TestLoadTileSpec(t *testing.T) {
	tileSpec, err := LoadTileSpec([]byte(testTileSpec))
	if err != nil {
		t.Errorf("Unable to load tile spec: %s\n", err.Error())
	}
	if len(tileSpec) != 4 {
		t.Errorf("Bad tile spec load: only %d elements != 4\n", len(tileSpec))
	}
	if tileSpec[2].Resolution.GetMax() != 40.0 {
		t.Errorf("Bad tile spec at level 2: %v\n", tileSpec[2])
	}
	if tileSpec[3].TileSize.Value(2) != 512 {
		t.Errorf("Bad tile spec at level 3: %v\n", tileSpec[3])
	}
}

func TestMultiscale2dRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make source
	makeGrayscale(repo, t, "grayscale")

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.SetVersioned(true)
	config.Set("Placeholder", "true")
	config.Set("Format", "jpg")
	config.Set("Source", "grayscale")
	dataservice, err := repo.NewData(mstype, "mymultiscale2d", config)
	if err != nil {
		t.Errorf("Unable to create multiscale2d instance: %s\n", err.Error())
	}
	msdata, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast multiscale2d data service into multiscale2d.Data\n")
	}
	oldData := *msdata

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during multiscale2d persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("mymultiscale2d")
	if err != nil {
		t.Fatalf("Can't get keyvalue instance from reloaded test db: %s\n", err.Error())
	}
	msdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not multiscale2d.Data\n")
	}
	if !reflect.DeepEqual(oldData, *msdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *msdata)
	}
}
