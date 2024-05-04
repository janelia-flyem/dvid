//go:build kvautobus
// +build kvautobus

package kvautobus

import (
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/BurntSushi/toml"
)

const testConfig = `
[store]
    [store.raid6]
    engine = "badger"
    path = "/data/dbs/badgerdb"
 
    [store.ssd]
    engine = "badger"
    path = "/datassd/dbs/badgerdb"
 
    [store.kvautobus]
    engine = "kvautobus"
    path = "http://tem-dvid.int.janelia.org:9000"
    timeout = 30   # allow max 30 seconds per request to above HTTP service
                   # use 0 for no timeout.
    collection = "99ef22cd85f143f58a623bd22aad0ef7"
    owner = "flyEM"

    [store.kvautobus2]
    engine = "kvautobus"
    path = "http://tem-dvid.int.janelia.org:9000"
    timeout = 30
    collection = "389a22cd85f143f511923bd22aac776b"
    owner = "otherTeam"
`

type tomlConfig struct {
	Store map[storage.Alias]storeConfig
}

type storeConfig map[string]interface{}

func TestParseConfig(t *testing.T) {
	var tc tomlConfig
	if _, err := toml.Decode(testConfig, &tc); err != nil {
		t.Fatalf("Could not decode TOML config: %v\n", err)
	}
	sc, ok := tc.Store["kvautobus"]
	if !ok {
		t.Fatalf("Couldn't find kvautobus config in test\n")
	}
	var config dvid.Config
	config.SetAll(sc)
	kvconfig := dvid.StoreConfig{
		Config: config,
		Engine: "kvautobus",
	}
	path, timeout, owner, collection, err := parseConfig(kvconfig)
	if err != nil {
		t.Errorf("Error parsing kvautobus config: %v\n", err)
	}

	if path != "http://tem-dvid.int.janelia.org:9000" {
		t.Errorf("Bad parsing of kvautobus config.  Path = %s, not http://tem-dvid.int.janelia.org:9000", path)
	}

	if timeout != time.Duration(30)*time.Second {
		t.Errorf("Expected parsing of kvautobus config: timeout = 30 * time.Second, got %d\n", timeout)
	}

	if owner != "flyEM" {
		t.Errorf("expected owner for kvautobus to be %q, got %q\n", "flyEM", owner)
	}

	if collection != dvid.UUID("99ef22cd85f143f58a623bd22aad0ef7") {
		t.Errorf("expected collection for kvautobus to be 99ef22cd85f143f58a623bd22aad0ef7, got %s\n", collection)
	}
}
