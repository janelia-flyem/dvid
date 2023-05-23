package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
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

	// the parsed TOML configuration data
	tc tomlConfig

	// the TOML config file location
	tcLocation string

	// the TOML config raw contents
	tcContent string

	corsDomains map[string]struct{}
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

type tomlConfig struct {
	Server     localConfig
	Auth       authConfig
	Email      dvid.EmailConfig
	Logging    dvid.LogConfig
	Mutations  MutationsConfig
	Kafka      storage.KafkaConfig
	Store      map[storage.Alias]storeConfig
	Backend    map[dvid.DataSpecifier]backendConfig
	Mutcache   map[dvid.InstanceName]pathConfig
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

	// [server].pidFile
	if c.Server.PidFile != "" {
		c.Server.PidFile, err = dvid.ConvertToAbsolute(c.Server.PidFile, configDir)
		if err != nil {
			return fmt.Errorf("Error converting pidFile to absolute path")
		}
	}

	// [server].webClient
	if c.Server.WebClient != "" {
		c.Server.WebClient, err = dvid.ConvertToAbsolute(c.Server.WebClient, configDir)
		if err != nil {
			return fmt.Errorf("Error converting webClient setting to absolute path")
		}
	}

	// [logging].logfile
	if c.Logging.Logfile != "" {
		c.Logging.Logfile, err = dvid.ConvertToAbsolute(c.Logging.Logfile, configDir)
		if err != nil {
			return fmt.Errorf("Error converting logfile setting to absolute path")
		}
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

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

// DVID console directory.
func ConsoleDir() string {
	if tc.Server.WebClient == "" {
		return filepath.Join(currentDir(), "console")
	}
	return tc.Server.WebClient
}

// LoadConfig loads DVID server configuration from a TOML file.
func LoadConfig(console, ports, filename string) error {
	if filename == "" {
		return fmt.Errorf("no server TOML configuration file provided")
	}
	if _, err := toml.DecodeFile(filename, &tc); err != nil {
		return fmt.Errorf("could not decode TOML config: %v", err)
	}
	tcLocation = filename

	fp, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fp.Close()
	if byteContents, err := io.ReadAll(fp); err != nil {
		return err
	} else {
		tcContent = string(byteContents)
	}

	// cli overrides -- won't set if empty string
	SetConsole(console)
	SetPorts(ports)

	dvid.Infof("tomlConfig: %v\n", tc)
	if err := tc.convertPathsToAbsolute(filename); err != nil {
		return fmt.Errorf("could not convert relative paths to absolute paths in TOML config: %v", err)
	}

	if tc.Email.IsAvailable() {
		dvid.SetEmailServer(tc.Email)
	}
	return nil
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

func ConfigLocation() string {
	return tcLocation
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

func NoLabelmapSplit() bool {
	return tc.Server.NoLabelmapSplit
}

func KafkaServers() []string {
	if len(tc.Kafka.Servers) != 0 {
		return tc.Kafka.Servers
	}
	return nil
}

func KafkaActivityTopic() string {
	return storage.KafkaActivityTopic()
}

func KafkaPrefixTopic() string {
	return tc.Kafka.TopicPrefix
}

// KafkaAvailable returns true if kafka servers are initialized.
func KafkaAvailable() bool {
	if len(tc.Kafka.Servers) != 0 {
		return true
	}
	return false
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

// MutcachePath returns path to where mutation cache should be placed for
// the given labelmap instance. If nothing specified, empty string returned.
func MutcachePath(name dvid.InstanceName) string {
	if tc.Mutcache == nil {
		return ""
	}
	setting, found := tc.Mutcache[name]
	if !found {
		return ""
	}
	return setting.Path
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
