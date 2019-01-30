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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/toml"
)

const (
	// DefaultWebAddress is the default URL of the DVID web server
	DefaultWebAddress = "localhost:8000"

	// DefaultRPCAddress is the default RPC address for command-line use of a remote DVID server
	DefaultRPCAddress = "localhost:8001"

	// ErrorLogFilename is the name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"
)

var (
	// DefaultHost is the default most understandable alias for this server.
	DefaultHost = "localhost"

	tc tomlConfig
)

func init() {
	// Set default Host name for understandability from user perspective.
	// Assumes Linux or Mac.  From stackoverflow suggestion.
	cmd := exec.Command("/bin/hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		dvid.Errorf("Unable to get default Host name via /bin/hostname: %v\n", err)
		dvid.Errorf("Using 'localhost' as default Host name.\n")
		return
	}
	DefaultHost = out.String()
	DefaultHost = DefaultHost[:len(DefaultHost)-1] // removes EOL
	tc.Server.ShutdownDelay = 5
}

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

// Initialize sets up the server (runs webhook, kafka goroutine, etc) and
// presumes that LoadConfig() or another method was already used to configure
// the server.
func Initialize() error {
	tc.Logging.SetLogger()

	if err := tc.Kafka.Initialize(WebServer()); err != nil {
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
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("called webhook specified in TOML (%q) and received bad status code: %d", sc.StartWebhook, resp.StatusCode)
		}
	}
	return nil
}

type tomlConfig struct {
	Server     localConfig
	Email      dvid.EmailConfig
	Logging    dvid.LogConfig
	Mutations  MutationsConfig
	Kafka      storage.KafkaConfig
	Store      map[storage.Alias]storeConfig
	Backend    map[dvid.DataSpecifier]backendConfig
	Cache      map[string]sizeConfig
	Groupcache storage.GroupcacheConfig
	Mirror     map[dvid.DataSpecifier]mirrorConfig
}

// Some settings in the TOML can be given as relative paths.
// This function converts them in-place to absolute paths,
// assuming the given paths were relative to the TOML file's own directory.
func (c *tomlConfig) convertPathsToAbsolute(configPath string) error {
	var err error

	configDir := filepath.Dir(configPath)

	// [server].webClient
	c.Server.WebClient, err = dvid.ConvertToAbsolute(c.Server.WebClient, configDir)
	if err != nil {
		return fmt.Errorf("Error converting webClient setting to absolute path")
	}

	// [logging].logfile
	c.Logging.Logfile, err = dvid.ConvertToAbsolute(c.Logging.Logfile, configDir)
	if err != nil {
		return fmt.Errorf("Error converting logfile setting to absolute path")
	}

	// [store.foobar].path
	for alias, sc := range c.Store {
		p, ok := sc["path"]
		if !ok {
			continue
		}
		path, ok := p.(string)
		if !ok {
			return fmt.Errorf("Don't understand path setting for store %q", alias)
		}
		absPath, err := dvid.ConvertToAbsolute(path, configDir)
		if err != nil {
			return fmt.Errorf("Error converting store.%s.path to absolute path: %q", alias, path)
		}
		sc["path"] = absPath
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

// Host returns the most understandable host alias + any port.
func Host() string {
	parts := strings.Split(tc.Server.HTTPAddress, ":")
	host := tc.Server.Host
	if len(parts) > 1 {
		host = host + ":" + parts[len(parts)-1]
	}
	return host
}

func Note() string {
	return tc.Server.Note
}

func HTTPAddress() string {
	return tc.Server.HTTPAddress
}

func RPCAddress() string {
	return tc.Server.RPCAddress
}

func WebClient() string {
	return tc.Server.WebClient
}

func WebRedirectPath() string {
	return tc.Server.WebRedirectPath
}

func WebDefaultFile() string {
	return tc.Server.WebDefaultFile
}

func AllowTiming() bool {
	return tc.Server.AllowTiming
}

func KafkaServers() []string {
	if len(tc.Kafka.Servers) != 0 {
		return tc.Kafka.Servers
	}
	return nil
}

func KafkaActivityTopic() string {
	return tc.Kafka.TopicActivity
}

func KafkaPrefixTopic() string {
	return tc.Kafka.TopicPrefix
}

func MutationLogSpec() MutationsConfig {
	return tc.Mutations
}

func repoMirrors(dataUUID, versionUUID dvid.UUID) []string {
	if len(tc.Mirror) == 0 {
		return nil
	}
	dataspec := dvid.DataSpecifier(`"` + string(dataUUID) + `:` + string(versionUUID) + `"`)
	mirror, found := tc.Mirror[dataspec]
	if found {
		return mirror.Servers
	}
	return nil
}

func instanceMirrors(dataUUID, versionUUID dvid.UUID) []string {
	if len(tc.Mirror) == 0 {
		return nil
	}
	mirror, found := tc.Mirror[dvid.DataSpecifier("all")]
	if found {
		return mirror.Servers
	}
	dataspec := dvid.DataSpecifier(`"` + string(dataUUID) + `:` + string(versionUUID) + `"`)
	mirror, found = tc.Mirror[dataspec]
	if found {
		return mirror.Servers
	}
	return nil
}

// CacheSize returns the number oF bytes reserved for the given identifier.
// If unset, will return 0.
func CacheSize(id string) int {
	if tc.Cache == nil {
		return 0
	}
	setting, found := tc.Cache[id]
	if !found {
		return 0
	}
	return setting.Size * dvid.Mega
}

// WebServer returns the configured host name from TOML file or, if not specified,
// the retrieved hostname plus the port of the web server.
func WebServer() string {
	host := DefaultHost
	if tc.Server.Host != "" {
		host = tc.Server.Host
	} else if tc.Server.HTTPAddress != "" {
		host += ":" + tc.Server.HTTPAddress
	}
	return host
}

// localConfig holds ports, host name, and other properties of this dvid server.
type localConfig struct {
	Host            string
	HTTPAddress     string
	RPCAddress      string
	WebClient       string
	WebRedirectPath string
	WebDefaultFile  string
	Note            string

	AllowTiming        bool   // If true, returns * for Timing-Allow-Origin in response headers.
	StartWebhook       string // http address that should be called when server is started up.
	StartJaneliaConfig string // like StartWebhook, but with Janelia-specific behavior

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
	}
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

// LoadConfig loads DVID server configuration from a TOML file.
func LoadConfig(filename string) error {
	if filename == "" {
		return fmt.Errorf("No server TOML configuration file provided")
	}
	if _, err := toml.DecodeFile(filename, &tc); err != nil {
		return fmt.Errorf("could not decode TOML config: %v", err)
	}
	dvid.Infof("tomlConfig: %v\n", tc)
	var err error
	err = tc.convertPathsToAbsolute(filename)
	if err != nil {
		return fmt.Errorf("could not convert relative paths to absolute paths in TOML config: %v", err)
	}

	if tc.Email.IsAvailable() {
		dvid.SetEmailServer(tc.Email)
	}
	return nil
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
	backend.KVStore = make(storage.DataMap)
	backend.LogStore = make(storage.DataMap)
	for k, v := range tc.Backend {
		// lookup store config
		_, found := backend.Stores[v.Store]
		if !found {
			err = fmt.Errorf("Backend for %q specifies unknown store %q", k, v.Store)
			return
		}
		spec := dvid.DataSpecifier(strings.Trim(string(k), "\""))
		backend.KVStore[spec] = v.Store
		dvid.Infof("backend.KVStore[%s] = %s\n", spec, v.Store)
		if v.Log != "" {
			backend.LogStore[spec] = v.Log
		}
	}
	defaultStore, found := backend.KVStore["default"]
	if found {
		backend.DefaultKVDB = defaultStore
	} else {
		if backend.DefaultKVDB == "" {
			err = fmt.Errorf("if no default backend specified, must have exactly one store defined in config file")
			return
		}
	}

	defaultLog, found := backend.LogStore["default"]
	if found {
		backend.DefaultLog = defaultLog
	}

	defaultMetadataName, found := backend.KVStore["metadata"]
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

// KafkaAvailable returns true if kafka servers are initialized.
func KafkaAvailable() bool {
	if len(tc.Kafka.Servers) != 0 {
		return true
	}
	return false
}
