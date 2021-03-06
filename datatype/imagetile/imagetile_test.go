package imagetile

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	mstype, grayscaleT, roitype datastore.TypeService
	testMu                      sync.Mutex
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
		roitype, err = datastore.TypeServiceByName(roi.TypeName)
		if err != nil {
			log.Fatalf("Can't get ROI type: %v\n", err)
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "myimagetile")
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

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
	tk, err := storage.TKeyFromKey(key)
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

const testMetadata2 = `
{
	"MinTileCoord": [0,0,0],
	"MaxTileCoord": [30,30,30],
	"Levels": {
	    "0": {  "Resolution": [10.0, 10.0, 10.0], "TileSize": [512, 512, 512] },
	    "1": {  "Resolution": [20.0, 20.0, 20.0], "TileSize": [512, 512, 512] },
	    "2": {  "Resolution": [40.0, 40.0, 40.0], "TileSize": [512, 512, 512] },
	    "3": {  "Resolution": [80.0, 80.0, 80.0], "TileSize": [512, 512, 512] }
	}
}
`

var testSpans = []dvid.Span{
	dvid.Span{100, 101, 200, 210}, dvid.Span{100, 102, 200, 210}, dvid.Span{100, 103, 201, 212},
	dvid.Span{101, 101, 201, 213}, dvid.Span{101, 102, 202, 215}, dvid.Span{101, 103, 202, 216},
	dvid.Span{102, 101, 200, 210}, dvid.Span{102, 103, 201, 216}, dvid.Span{102, 104, 203, 217},
	dvid.Span{103, 101, 200, 210}, dvid.Span{103, 103, 200, 210}, dvid.Span{103, 105, 201, 212},
}

func getSpansJSON(spans []dvid.Span) io.Reader {
	jsonBytes, err := json.Marshal(spans)
	if err != nil {
		log.Fatalf("Can't encode spans into JSON: %v\n", err)
	}
	return bytes.NewReader(jsonBytes)
}

func TestTileCheck(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Make source
	uuid, _ := initTestRepo()
	makeGrayscale(uuid, t, "grayscale")

	// Make imagetile and set various properties
	config := dvid.NewConfig()
	config.Set("Placeholder", "true")
	config.Set("Format", "jpg")
	config.Set("Source", "grayscale")
	tileservice, err := datastore.NewData(uuid, mstype, "myimagetile", config)
	if err != nil {
		t.Errorf("Unable to create imagetile instance: %v\n", err)
	}
	msdata, ok := tileservice.(*Data)
	if !ok {
		t.Fatalf("Can't cast imagetile data service into imagetile.Data\n")
	}

	// Store Metadata
	url := fmt.Sprintf("%snode/%s/myimagetile/metadata", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", url, bytes.NewBufferString(testMetadata2))

	// Create the ROI
	_, err = datastore.NewData(uuid, roitype, "myroi", dvid.NewConfig())
	if err != nil {
		t.Errorf("Error creating new roi instance: %v\n", err)
	}

	// PUT an ROI
	roiRequest := fmt.Sprintf("%snode/%s/myroi/roi", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", roiRequest, getSpansJSON(testSpans))

	// Create fake filter
	spec := fmt.Sprintf("roi:myroi,%s", uuid)
	f, err := msdata.NewFilter(storage.FilterSpec(spec))
	if err != nil {
		t.Errorf("Couldn't make filter: %v\n", err)
	}
	if f == nil {
		t.Fatalf("Couldn't detect myroi data instance\n")
	}

	// Check various key values for proper spatial checks.
	var tx, ty int32
	tx = (205 * 32) / 512
	ty = (101 * 32) / 512
	tile := dvid.ChunkPoint3d{tx, ty, 101 * 32}
	scale := Scaling(0)
	tk, err := NewTKey(tile, dvid.XY, scale)
	if err != nil {
		t.Errorf("bad tkey: %v\n", err)
	}
	tkv := &storage.TKeyValue{K: tk}
	skip, err := f.Check(tkv)
	if err != nil {
		t.Errorf("Error on Check of key %q: %v\n", tkv.K, err)
	}
	if skip {
		t.Errorf("Expected false skip, got %v for tile %s\n", skip, tile)
	}

	tile = dvid.ChunkPoint3d{tx, ty, 106 * 32}
	tk, err = NewTKey(tile, dvid.XY, scale)
	if err != nil {
		t.Errorf("bad tkey: %v\n", err)
	}
	tkv = &storage.TKeyValue{K: tk}
	skip, err = f.Check(tkv)
	if err != nil {
		t.Errorf("Error on Check of key %q: %v\n", tkv.K, err)
	}
	if !skip {
		t.Errorf("Expected true skip, got %v for tile %s\n", skip, tile)
	}

	tx = (205 * 32) / 512
	ty = (121 * 32) / 512
	tile = dvid.ChunkPoint3d{tx, ty, 101 * 32}
	tk, err = NewTKey(tile, dvid.XY, scale)
	if err != nil {
		t.Errorf("bad tkey: %v\n", err)
	}
	tkv = &storage.TKeyValue{K: tk}
	skip, err = f.Check(tkv)
	if err != nil {
		t.Errorf("Error on Check of key %q: %v\n", tkv.K, err)
	}
	if !skip {
		t.Errorf("Expected true skip, got %v for tile %s\n", skip, tile)
	}

	tx = (225 * 32) / 512
	ty = (101 * 32) / 512
	tile = dvid.ChunkPoint3d{tx, ty, 101 * 32}
	tk, err = NewTKey(tile, dvid.XY, scale)
	if err != nil {
		t.Errorf("bad tkey: %v\n", err)
	}
	tkv = &storage.TKeyValue{K: tk}
	skip, err = f.Check(tkv)
	if err != nil {
		t.Errorf("Error on Check of key %q: %v\n", tkv.K, err)
	}
	if !skip {
		t.Errorf("Expected true skip, got %v for tile %s\n", skip, tile)
	}

}
