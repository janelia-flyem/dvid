package server

import (
	"encoding/json"
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
	if err := LoadConfig("../scripts/distro-files/config-full.toml"); err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}

	// test handler for standard start webhook
	var sent []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		sent, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("couldn't read all of POSTed body: %v\n", err)
		}
	}))
	defer ts.Close()
	tc.Server.StartWebhook = ts.URL

	// test handler for Janelia-specific start webhook
	var data string
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error

		err = r.ParseForm()
		if err != nil {
			t.Fatalf("couldn't parse posted form from StartJaneliaConfig: %v\n", err)
		}

		data = r.PostFormValue("config")
		if len(data) == 0 {
			t.Fatalf("no data sent to StartJaneliaConfig: %v\n", err)
		}
	}))
	defer ts2.Close()
	tc.Server.StartJaneliaConfig = ts2.URL

	// check server startup
	tc.Kafka = storage.KafkaConfig{}
	if err := Initialize(); err != nil {
		t.Fatalf("couldn't initialize server: %v\n", err)
	}

	// check standard webhook
	if string(sent) != `{"HTTP Address":"localhost:8000","Host":"mygreatserver.test.com","Note":"You can put anything you want in here and have it available via /api/server/note.\nMultiple lines!\n","RPC Address":"localhost:8001"}` {
		t.Fatalf("Expected server info to be sent to webhook, but received this instead:\n%s\n", string(sent))
	}

	// check Janelia webhook
	if data != `{"HTTP Address":"localhost:8000","Host":"mygreatserver.test.com","Note":"You can put anything you want in here and have it available via /api/server/note.\nMultiple lines!\n","RPC Address":"localhost:8001"}` {
		t.Fatalf("Expected server info to be sent to Janelia webhook, but received this instead:\n%s\n", data)
	}
}

func TestServerInfo(t *testing.T) {
	if err := LoadConfig("../scripts/distro-files/config-full.toml"); err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}
	jsonStr, err := AboutJSON()
	if err != nil {
		t.Fatalf("bad AboutJSON: %v\n", err)
	}
	var jsonVal struct {
		Host         string
		KafkaServers string `json:"Kafka Servers"`
		TopicPrefix  string `json:"Kafka Topic Prefix"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
		t.Fatalf("unable to unmarshal JSON (%s): %v\n", jsonStr, err)
	}
	if jsonVal.Host != "mygreatserver.test.com:8000" {
		t.Errorf("expected %q, got %q for host\n", "mygreatserver.test.com:8000", jsonVal.Host)
	}
	if jsonVal.KafkaServers != "foo.bar.com:1234,foo2.bar.com:1234" {
		t.Errorf("expected %q for kafka servers, got JSON:\n%s\n", "foo.bar.com:1234,foo2.bar.com:1234", jsonStr)
	}
	if jsonVal.TopicPrefix != "postsFromServer1" {
		t.Errorf("unexpected kafka topic prefix, got JSON:\n%s\n", jsonStr)
	}
}

func TestParseConfig(t *testing.T) {
	if err := LoadConfig("../scripts/distro-files/config-full.toml"); err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}

	sz := CacheSize("labelarray")
	if sz != 10*dvid.Mega {
		t.Errorf("Expected labelarray cache to be set to 10 (MB), got %d bytes instead\n", sz)
	}

	datacfg := DatastoreConfig()
	if datacfg.InstanceGen != "sequential" || datacfg.InstanceStart != 100 {
		t.Errorf("Bad instance id retrieval of configuration: %v\n", datacfg)
	}
	if datacfg.MutationStart != 1000100000 {
		t.Errorf("Bad mutation id start retrieval in configuration: %v\n", datacfg)
	}

	logCfg := tc.Logging
	if logCfg.Logfile != "/demo/logs/dvid.log" || logCfg.MaxSize != 500 || logCfg.MaxAge != 30 {
		t.Errorf("Bad logging configuration retrieval: %v\n", logCfg)
	}
	backend, err := GetBackend()
	if err != nil {
		t.Errorf("couldn't get backend: %v\n", err)
	}
	if backend.DefaultKVDB != "raid6" || backend.DefaultLog != "mutationlog" || backend.KVStore["grayscale:99ef22cd85f143f58a623bd22aad0ef7"] != "kvautobus" {
		t.Errorf("Bad backend configuration retrieval: %v\n", backend)
	}

	mutCfg := tc.Mutations
	if mutCfg.Blobstore != "raid6" {
		t.Errorf("got unexpected value for mutations.blobstore: %s\n", mutCfg.Blobstore)
	}
	if mutCfg.Logstore != "logstore:mutationlog" {
		t.Errorf("got unexpected value for mutations.Logstore: %s\n", mutCfg.Logstore)
	}

	kafkaCfg := tc.Kafka
	if len(kafkaCfg.Servers) != 2 || kafkaCfg.Servers[0] != "foo.bar.com:1234" || kafkaCfg.Servers[1] != "foo2.bar.com:1234" {
		t.Errorf("Bad Kafka config: %v\n", kafkaCfg)
	}

	if len(tc.Mirror) != 2 {
		t.Errorf("Bad mirror config: %v\n", tc.Mirror)
	}
	spec := dvid.DataSpecifier(`"bc95398cb3ae40fcab2529c7bca1ad0d:99ef22cd85f143f58a623bd22aad0ef7"`)
	mirrorCfg, found := tc.Mirror[spec]
	if !found {
		t.Fatalf("did not find expected mirror configuration for %s: %v\n", spec, tc.Mirror)
	}
	if len(mirrorCfg.Servers) != 2 || mirrorCfg.Servers[0] != "http://mirror3.janelia.org:7000" || mirrorCfg.Servers[1] != "http://mirror4.janelia.org:7000" {
		t.Fatalf("bad parsed mirror servers: %v\n", mirrorCfg)
	}
	spec = dvid.DataSpecifier(`all`)
	mirrorCfg, found = tc.Mirror[spec]
	if !found {
		t.Fatalf("did not find expected mirror configuration for %s: %v\n", spec, tc.Mirror)
	}
	if len(mirrorCfg.Servers) != 2 || mirrorCfg.Servers[0] != "http://mirror1.janelia.org:7000" || mirrorCfg.Servers[1] != "http://mirror2.janelia.org:7000" {
		t.Fatalf("bad parsed mirror servers: %v\n", mirrorCfg)
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
	c.convertPathsToAbsolute("/tmp/dvid-configs/myconfig.toml")

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
