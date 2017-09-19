// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"bytes"
	"fmt"
	"path/filepath"
	"net/smtp"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"text/template"

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
}

type tomlConfig struct {
	Server     serverConfig
	Email      emailConfig
	Logging    dvid.LogConfig
	Store      map[storage.Alias]storeConfig
	Backend    map[dvid.DataSpecifier]backendConfig
	Groupcache storage.GroupcacheConfig
}

// Some settings in the TOML can be given as relative paths.
// This function converts them in-place to absolute paths,
// assuming the given paths were relative to the TOML file's own directory.
func (c *tomlConfig) ConvertPathsToAbsolute(configPath string) error {
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

func (c tomlConfig) Stores() (map[storage.Alias]dvid.StoreConfig, error) {
	stores := make(map[storage.Alias]dvid.StoreConfig, len(c.Store))
	for alias, sc := range c.Store {
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
func (c *tomlConfig) Host() string {
	parts := strings.Split(c.Server.HTTPAddress, ":")
	host := c.Server.Host
	if len(parts) > 1 {
		host = host + ":" + parts[len(parts)-1]
	}
	return host
}

func (c *tomlConfig) HTTPAddress() string {
	return c.Server.HTTPAddress
}

func (c *tomlConfig) RPCAddress() string {
	return c.Server.RPCAddress
}

func (c *tomlConfig) WebClient() string {
	return c.Server.WebClient
}

func (c *tomlConfig) AllowTiming() bool {
	return c.Server.AllowTiming
}

type serverConfig struct {
	Host        string
	HTTPAddress string
	RPCAddress  string
	WebClient   string
	AllowTiming bool
	Note        string

	IIDGen   string `toml:"instance_id_gen"`
	IIDStart uint32 `toml:"instance_id_start"`
}

type storeConfig map[string]interface{}

type backendConfig struct {
	Store storage.Alias
	Log   storage.Alias
}

type emailConfig struct {
	Notify   []string
	Username string
	Password string
	Server   string
	Port     int
}

func (e emailConfig) Host() string {
	return fmt.Sprintf("%s:%d", e.Server, e.Port)
}

// LoadConfig loads DVID server configuration from a TOML file.
func LoadConfig(filename string) (*datastore.InstanceConfig, *dvid.LogConfig, *storage.Backend, error) {
	if filename == "" {
		return nil, nil, nil, fmt.Errorf("No server TOML configuration file provided")
	}
	if _, err := toml.DecodeFile(filename, &tc); err != nil {
		return nil, nil, nil, fmt.Errorf("Could not decode TOML config: %v\n", err)
	}
	var err error
	err = tc.ConvertPathsToAbsolute(filename)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not convert relative paths to absolute paths in TOML config: %v\n", err)
	}

	// Get all defined stores.
	backend := new(storage.Backend)
	backend.Groupcache = tc.Groupcache
	backend.Stores, err = tc.Stores()
	if err != nil {
		return nil, nil, nil, err
	}

	// Get default store if there's only one store defined.
	if len(backend.Stores) == 1 {
		for k := range backend.Stores {
			backend.DefaultKVDB = storage.Alias(strings.Trim(string(k), "\""))
		}
	}

	// Create the backend mapping.
	backend.KVStore = make(map[dvid.DataSpecifier]storage.Alias)
	backend.LogStore = make(map[dvid.DataSpecifier]storage.Alias)
	for k, v := range tc.Backend {
		// lookup store config
		_, found := backend.Stores[v.Store]
		if !found {
			return nil, nil, nil, fmt.Errorf("Backend for %q specifies unknown store %q", k, v.Store)
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
			return nil, nil, nil, fmt.Errorf("if no default backend specified, must have exactly one store defined in config file")
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
			return nil, nil, nil, fmt.Errorf("can't set metadata if no default backend specified, must have exactly one store defined in config file")
		}
		backend.Metadata = backend.DefaultKVDB
	}

	// The server config could be local, cluster, gcloud-specific config.  Here it is local.
	config = &tc
	ic := datastore.InstanceConfig{
		Gen:   tc.Server.IIDGen,
		Start: dvid.InstanceID(tc.Server.IIDStart),
	}
	return &ic, &(tc.Logging), backend, nil
}

type emailData struct {
	From    string
	To      string
	Subject string
	Body    string
	Host    string
}

// Go template
const emailTemplate = `From: {{.From}}
To: {{.To}}
Subject: {{.Subject}}

{{.Body}}

Sincerely,

DVID at {{.Host}}
`

// SendNotification sends e-mail to the given recipients or the default emails loaded
// during configuration.
func SendNotification(message string, recipients []string) error {
	e := tc.Email
	var auth smtp.Auth
	if e.Password != "" {
		auth = smtp.PlainAuth("", e.Username, e.Password, e.Server)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}

	for _, recipient := range e.Notify {
		context := &emailData{
			From:    e.Username,
			To:      recipient,
			Subject: "DVID panic report",
			Body:    message,
			Host:    hostname,
		}

		t := template.New("emailTemplate")
		if t, err = t.Parse(emailTemplate); err != nil {
			return fmt.Errorf("error trying to parse mail template: %v", err)
		}

		// Apply the values we have initialized in our struct context to the template.
		var doc bytes.Buffer
		if err = t.Execute(&doc, context); err != nil {
			return fmt.Errorf("error trying to execute mail template: %v", err)
		}

		// Send notification
		err = smtp.SendMail(e.Host(), auth, e.Username, []string{recipient}, doc.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
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

	dvid.Infof("------------------\n")
	dvid.Infof("DVID code version: %s\n", gitVersion)
	dvid.Infof("Serving HTTP on %s (host alias %q)\n", tc.Server.HTTPAddress, tc.Server.Host)
	dvid.Infof("Serving command-line use via RPC %s\n", tc.Server.RPCAddress)
	dvid.Infof("Using web client files from %s\n", tc.Server.WebClient)
	dvid.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

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
