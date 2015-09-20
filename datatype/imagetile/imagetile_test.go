package imagetile

import (
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
)

var (
	mstype, grayscaleT datastore.TypeService
	testMu             sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if mstype == nil {
		var err error
		mstype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get imagetile type: %s\n", err)
		}
		grayscaleT, err = datastore.TypeServiceByName("uint8blk")
		if err != nil {
			log.Fatalf("Can't get grayscale type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func makeGrayscale(uuid dvid.UUID, t *testing.T, name dvid.InstanceName) *imageblk.Data {
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, grayscaleT, name, config)
	if err != nil {
		t.Errorf("Unable to create grayscale instance %q: %v\n", name, err)
	}
	grayscale, ok := dataservice.(*imageblk.Data)
	if !ok {
		t.Errorf("Can't cast data service into imageblk Data\n")
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
		t.Errorf("Unable to load tile spec: %v\n", err)
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
	datastore.OpenTest()
	defer datastore.CloseTest()

	// Make source
	uuid, _ := initTestRepo()
	makeGrayscale(uuid, t, "grayscale")

	// Make labels and set various properties
	config := dvid.NewConfig()
	config.Set("Placeholder", "true")
	config.Set("Format", "jpg")
	config.Set("Source", "grayscale")
	dataservice, err := datastore.NewData(uuid, mstype, "myimagetile", config)
	if err != nil {
		t.Errorf("Unable to create imagetile instance: %v\n", err)
	}
	msdata, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast imagetile data service into imagetile.Data\n")
	}
	oldData := *msdata

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, msdata); err != nil {
		t.Fatalf("Unable to save repo during imagetile persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUID(uuid, "myimagetile")
	if err != nil {
		t.Fatalf("Can't get keyvalue instance from reloaded test db: %v\n", err)
	}
	msdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not imagetile.Data\n")
	}

	if !reflect.DeepEqual(oldData.Properties, msdata2.Properties) {
		t.Errorf("Expected properties %v, got %v\n", oldData.Properties, msdata2.Properties)
	}
}
