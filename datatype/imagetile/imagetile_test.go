package imagetile

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
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

const testMetadata = `
{
	"MinTileCoord": [0,0,0],
	"MaxTileCoord": [5,5,4],
	"Levels": {
	    "0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
	    "1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
	    "2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
	    "3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
	}
}
`

func TestSetMetadata(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()
	server.CreateTestInstance(t, uuid, "imagetile", "tiles", dvid.Config{})

	// Store Metadata
	url := fmt.Sprintf("%snode/%s/tiles/metadata", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, bytes.NewBufferString(testMetadata))

	// Check instance really has it set.
	var metadata metadataJSON
	respStr := server.TestHTTP(t, "GET", url, nil)
	if err := json.Unmarshal(respStr, &metadata); err != nil {
		t.Fatalf("Couldn't parse JSON response to metadata request (%v):\n%s\n", err, respStr)
	}
	expectMin := dvid.Point3d{0, 0, 0}
	expectMax := dvid.Point3d{5, 5, 4}
	if !expectMin.Equals(metadata.MinTileCoord) {
		t.Errorf("Expected min tile coord %s, got %s\n", expectMin, metadata.MinTileCoord)
	}
	if !expectMax.Equals(metadata.MaxTileCoord) {
		t.Errorf("Expected max tile coord %s, got %s\n", expectMax, metadata.MaxTileCoord)
	}
	tileSpec, err := parseTileSpec(metadata.Levels)
	if err != nil {
		t.Errorf("Error parsing returned tile level spec:\n%v\n", metadata.Levels)
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

func TestTileKey(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()
	server.CreateTestInstance(t, uuid, "imagetile", "tiles", dvid.Config{})

	keyURL := fmt.Sprintf("%snode/%s/tiles/tilekey/xy/0/1_2_3", server.WebAPIPath, uuid)
	respStr := server.TestHTTP(t, "GET", keyURL, nil)
	keyResp := struct {
		Key string `json:"key"`
	}{}
	if err := json.Unmarshal(respStr, &keyResp); err != nil {
		t.Fatalf("Couldn't parse JSON response to tilekey request (%v):\n%s\n", err, keyResp)
	}
	kb := make([]byte, hex.DecodedLen(len(keyResp.Key)))
	_, err := hex.Decode(kb, []byte(keyResp.Key))
	if err != nil {
		t.Fatalf("Couldn't parse return hex key: %s", keyResp.Key)
	}

	// Decipher TKey portion to make sure it's correct.
	key := storage.Key(kb)
	var ctx storage.DataContext
	tk, err := ctx.TKeyFromKey(key)
	if err != nil {
		t.Fatalf("Couldn't get TKey from returned key (%v): %x", err, kb)
	}
	tile, plane, scale, err := DecodeTKey(tk)
	if err != nil {
		t.Fatalf("Bad decode of TKey (%v): %x", err, tk)
	}
	expectTile := dvid.ChunkPoint3d{1, 2, 3}
	if tile != expectTile {
		t.Errorf("Expected tile %v, got %v\n", expectTile, tile)
	}
	if !plane.Equals(dvid.XY) {
		t.Errorf("Expected plane to be XY, got %v\n", plane)
	}
	if scale != 0 {
		t.Errorf("Expected scale to be 0, got %d\n", scale)
	}
}
