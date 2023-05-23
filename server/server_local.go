//go:build !clustered && !gcloud
// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
)

// TestConfig specifies configuration for testing servers.
type TestConfig struct {
	KVStoresMap  storage.DataMap
	LogStoresMap storage.DataMap
	CacheSize    map[string]int // MB for caches
}

// OpenTest initializes the server for testing, setting up caching, datastore, etc.
// Later configurations will override earlier ones.
func OpenTest(configs ...TestConfig) error {
	var dataMap datastore.DataStorageMap
	var dataMapped bool
	tc = tomlConfig{}
	if len(configs) > 0 {
		for _, c := range configs {
			if len(c.KVStoresMap) != 0 {
				dataMap.KVStores = c.KVStoresMap
				dataMapped = true
			}
			if len(c.LogStoresMap) != 0 {
				dataMap.LogStores = c.LogStoresMap
				dataMapped = true
			}
			if len(c.CacheSize) != 0 {
				for id, size := range c.CacheSize {
					if tc.Cache == nil {
						tc.Cache = make(map[string]sizeConfig)
					}
					tc.Cache[id] = sizeConfig{Size: size}
				}
			}
		}
	}
	dvid.Infof("OpenTest with %v: cache setting %v\n", configs, tc.Cache)
	if dataMapped {
		datastore.OpenTest(dataMap)
	} else {
		datastore.OpenTest()
	}
	return nil
}

// CloseTest shuts down server for testing.
func CloseTest() {
	datastore.CloseTest()
}

// StartAuxServices sets up the server (runs webhook, kafka goroutine, etc) and
// presumes that LoadConfig() or another method was already used to configure
// the server.
func StartAuxServices() error {
	if err := writePidFile(); err != nil {
		return err
	}

	tc.Logging.SetLogger()

	corsDomains = make(map[string]struct{})
	for _, domain := range tc.Server.CorsDomains {
		corsDomains[domain] = struct{}{}
	}

	switch tc.Server.RWMode {
	case "readonly":
		readonly = true
	case "fullwrite":
		fullwrite = true
	}

	// don't let server start if it can't create critical directories like
	// mutation log files.
	if tc.Mutations.Jsonstore != "" {
		if _, err := os.Stat(tc.Mutations.Jsonstore); os.IsNotExist(err) {
			if err := os.MkdirAll(tc.Mutations.Jsonstore, 0744); err != nil {
				return err
			}
		}
	}

	if err := tc.Kafka.Initialize(WebServer()); err != nil {
		return err
	}

	if err := authorizations.initialize(); err != nil {
		return err
	}

	sc := tc.Server
	if sc.StartWebhook == "" && sc.StartJaneliaConfig == "" {
		return nil
	}

	data := map[string]string{
		"Host":         sc.Host,
		"Note":         sc.Note,
		"HTTP Address": sc.HTTPAddress,
		"RPC Address":  sc.RPCAddress,
	}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if sc.StartWebhook != "" {
		req, err := http.NewRequest("POST", sc.StartWebhook, bytes.NewBuffer(jsonBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("called webhook specified in TOML (%q) and received bad status code: %d", sc.StartWebhook, resp.StatusCode)
		}
	}

	if sc.StartJaneliaConfig != "" {
		// Janelia specific startup webhook; this format matches what's expected
		//	by our local config server
		// new: format like config server wants
		resp, err := http.PostForm(sc.StartJaneliaConfig, url.Values{"config": {string(jsonBytes)}})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("called webhook specified in TOML (%q) and received bad status code: %d", sc.StartWebhook, resp.StatusCode)
		}
	}
	return nil
}

func Stores() (map[storage.Alias]dvid.StoreConfig, error) {
	stores := make(map[storage.Alias]dvid.StoreConfig, len(tc.Store))
	for alias, sc := range tc.Store {
		e, ok := sc["engine"]
		if !ok {
			return nil, fmt.Errorf("store configurations must have %q set to valid driver", "engine")
		}
		engine, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("engine set for store %q must be a string", alias)
		}
		var config dvid.Config
		config.SetAll(sc)
		stores[alias] = dvid.StoreConfig{
			Config: config,
			Engine: engine,
		}
	}
	return stores, nil
}

// localConfig holds ports, host name, and other properties of this dvid server.
type localConfig struct {
	Host            string
	HTTPAddress     string
	RPCAddress      string
	WebClient       string
	WebRedirectPath string
	WebDefaultFile  string
	PidFile         string
	Note            string
	CorsDomains     []string
	RWMode          string // optional setting can be empty, "readonly" or "fullwrite"

	AllowTiming        bool   // If true, returns * for Timing-Allow-Origin in response headers.
	StartWebhook       string // http address that should be called when server is started up.
	StartJaneliaConfig string // like StartWebhook, but with Janelia-specific behavior

	NoLabelmapSplit bool // If true (default false), prevents labelmap /split endpoint.

	IIDGen   string `toml:"instance_id_gen"`
	IIDStart uint32 `toml:"instance_id_start"`

	MutIDStart uint64 `toml:"min_mutation_id_start"`

	InteractiveOpsBeforeBlock int // # of interactive ops in 2 min period before batch processing is blocked.  Zero value = no blocking.
	ShutdownDelay             int // seconds to delay after receiving shutdown request to let HTTP requests drain.
}

// DatastoreConfig returns data instance configuration necessary to
// handle id generation.
func DatastoreConfig() datastore.Config {
	return datastore.Config{
		InstanceGen:   tc.Server.IIDGen,
		InstanceStart: dvid.InstanceID(tc.Server.IIDStart),
		MutationStart: tc.Server.MutIDStart,
		ReadOnly:      readonly,
	}
}

type pathConfig struct {
	Path string
}

type sizeConfig struct {
	Size int
}

type storeConfig map[string]interface{}

type backendConfig struct {
	Store storage.Alias
	Log   storage.Alias
}

type mirrorConfig struct {
	Servers []string
}

// GetBackend returns a backend from current configuration.
func GetBackend() (backend *storage.Backend, err error) {
	// Get all defined stores.
	backend = new(storage.Backend)
	backend.Groupcache = tc.Groupcache
	if backend.Stores, err = Stores(); err != nil {
		return
	}

	// Get default store if there's only one store defined.
	if len(backend.Stores) == 1 {
		for k := range backend.Stores {
			backend.DefaultKVDB = storage.Alias(strings.Trim(string(k), "\""))
		}
	}

	// Create the backend mapping.
	backend.KVAssign = make(storage.DataMap)
	backend.LogAssign = make(storage.DataMap)
	for k, v := range tc.Backend {
		// lookup store config
		_, found := backend.Stores[v.Store]
		if !found {
			err = fmt.Errorf("Backend for %q specifies unknown store %q", k, v.Store)
			return
		}
		spec := dvid.DataSpecifier(strings.Trim(string(k), "\""))
		backend.KVAssign[spec] = v.Store
		dvid.Infof("backend.KVStore[%s] = %s\n", spec, v.Store)
		if v.Log != "" {
			backend.LogAssign[spec] = v.Log
		}
	}
	defaultStore, found := backend.KVAssign["default"]
	if found {
		backend.DefaultKVDB = defaultStore
	} else {
		if backend.DefaultKVDB == "" {
			err = fmt.Errorf("if no default backend specified, must have exactly one store defined in config file")
			return
		}
	}

	defaultLog, found := backend.LogAssign["default"]
	if found {
		backend.DefaultLog = defaultLog
	}

	defaultMetadataName, found := backend.KVAssign["metadata"]
	if found {
		backend.Metadata = defaultMetadataName
	} else {
		if backend.DefaultKVDB == "" {
			err = fmt.Errorf("can't set metadata if no default backend specified, must have exactly one store defined in config file")
			return
		}
		backend.Metadata = backend.DefaultKVDB
	}
	return
}

// Serve starts HTTP and RPC servers.
func Serve() {
	// Use defaults if not set via TOML config file.
	if tc.Server.Host == "" {
		tc.Server.Host = DefaultHost
	}
	if tc.Server.HTTPAddress == "" {
		tc.Server.HTTPAddress = DefaultWebAddress
	}
	if tc.Server.RPCAddress == "" {
		tc.Server.RPCAddress = DefaultRPCAddress
	}

	dvid.TimeInfof("DVID code version: %s\n", gitVersion)
	dvid.TimeInfof("Serving HTTP on %s (host alias %q)\n", tc.Server.HTTPAddress, tc.Server.Host)
	dvid.TimeInfof("Serving command-line use via RPC %s\n", tc.Server.RPCAddress)
	dvid.TimeInfof("Using web client files from %s\n", tc.Server.WebClient)
	dvid.TimeInfof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Launch the web server
	go serveHTTP()

	// Launch the rpc server
	go func() {
		if err := rpc.StartServer(tc.Server.RPCAddress); err != nil {
			dvid.Criticalf("Could not start RPC server: %v\n", err)
		}
	}()

	<-shutdownCh
}
