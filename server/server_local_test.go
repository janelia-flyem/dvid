package server

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func loadConfigFile(t *testing.T, filename string) string {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("couldn't open TOML file %q: %v\n", filename, err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("couldn't read TOML file %q: %v\n", filename, err)
	}
	return string(data)
}

func TestStartWebhook(t *testing.T) {
	tomlCfg, _, err := LoadConfig("../scripts/distro-files/config-full.toml")
	if err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}

	var sent []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		sent, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("couldn't read all of POSTed body: %v\n", err)
		}
	}))
	defer ts.Close()
	tomlCfg.Server.StartWebhook = ts.URL

	if err := tc.Server.Initialize(); err != nil {
		t.Fatalf("couldn't initialize server: %v\n", err)
	}
	if string(sent) != `{"HTTP Address":"localhost:8000","Host":"mygreatserver.test.com","Note":"You can put anything you want in here and have it available via /api/server/note.\nMultiple lines!\n","RPC Address":"localhost:8001"}` {
		t.Fatalf("Expected server info to be sent to webhook, but received this instead:\n%s\n", string(sent))
	}

	// check if there's no recipient for webhook.
	tomlCfg.Server.StartWebhook = "http://mybadurl:2718"
	if err := tc.Server.Initialize(); err == nil {
		t.Fatalf("expected error in supplying bad webhook, but got no error!\n")
	}
}

func TestParseConfig(t *testing.T) {
	tomlCfg, backendCfg, err := LoadConfig("../scripts/distro-files/config-full.toml")
	if err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}

	sz := CacheSize("labelarray")
	if sz != 10*dvid.Mega {
		t.Errorf("Expected labelarray cache to be set to 10 (MB), got %d bytes instead\n", sz)
	}

	instanceCfg := tomlCfg.Server.DatastoreInstanceConfig()
	if instanceCfg.Gen != "sequential" || instanceCfg.Start != 100 {
		t.Errorf("Bad instance id retrieval of configuration: %v\n", instanceCfg)
	}

	logCfg := tomlCfg.Logging
	if logCfg.Logfile != "/demo/logs/dvid.log" || logCfg.MaxSize != 500 || logCfg.MaxAge != 30 {
		t.Errorf("Bad logging configuration retrieval: %v\n", logCfg)
	}
	if backendCfg.DefaultKVDB != "raid6" || backendCfg.DefaultLog != "mutationlog" || backendCfg.KVStore["grayscale:99ef22cd85f143f58a623bd22aad0ef7"] != "kvautobus" {
		t.Errorf("Bad backend configuration retrieval: %v\n", backendCfg)
	}

	kafkaCfg := tomlCfg.Kafka
	if len(kafkaCfg.Servers) != 2 || kafkaCfg.Servers[0] != "http://foo.bar.com:1234" || kafkaCfg.Servers[1] != "http://foo2.bar.com:1234" {
		t.Errorf("Bad Kafka config: %v\n", kafkaCfg)
	}
}

func TestTOMLConfigAbsolutePath(t *testing.T) {
	// Initialize the filepath settings
	var c tomlConfig
	c.Server.WebClient = "dvid-distro/dvid-console"
	c.Logging.Logfile = "./foobar.log"

	c.Store = make(map[storage.Alias]storeConfig)

	c.Store["foo"] = make(storeConfig)
	c.Store["foo"]["engine"] = "basholeveldb"
	c.Store["foo"]["path"] = "foo-storage-db"

	c.Store["bar"] = make(storeConfig)
	c.Store["bar"]["engine"] = "basholeveldb"
	c.Store["bar"]["path"] = "/tmp/bar-storage-db" // Already absolute, should stay unchanged.

	// Convert relative paths to absolute
	c.ConvertPathsToAbsolute("/tmp/dvid-configs/myconfig.toml")

	// Checks
	if c.Server.WebClient != "/tmp/dvid-configs/dvid-distro/dvid-console" {
		t.Errorf("WebClient not correctly converted to absolute path: %s", c.Server.WebClient)
	}

	if c.Logging.Logfile != "/tmp/dvid-configs/foobar.log" {
		t.Errorf("Logfile not correctly converted to absolute path: %s", c.Logging.Logfile)
	}

	foo, _ := c.Store["foo"]
	path, _ := foo["path"]
	if path.(string) != "/tmp/dvid-configs/foo-storage-db" {
		t.Errorf("[store.foo].path not correctly converted to absolute path: %s", path)
	}

	engine, _ := foo["engine"]
	if engine.(string) != "basholeveldb" {
		t.Errorf("[store.foo].engine should not have been touched: %s", path)
	}

	bar, _ := c.Store["bar"]
	path, _ = bar["path"]
	if path.(string) != "/tmp/bar-storage-db" {
		t.Errorf("[store.bar].path was already absolute and should have been left unchanged: %s", path)
	}
}
