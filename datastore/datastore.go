package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/janelia-flyem/dvid/keyvalue"
)

const (
	Version = "0.1"

	Kilo = 1 << 10
	Mega = 1 << 20
	Giga = 1 << 30
	Tera = 1 << 40

	// ConfigFilename is name of JSON file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

// Config holds the essential configuration data for a datastore instance.
type Config struct {
	// Volume extents
	VolumeX int
	VolumeY int
	VolumeZ int

	// Relative resolution of voxels in volume
	VoxelResX float32
	VoxelResY float32
	VoxelResZ float32

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits string

	// Block size
	BlockX int
	BlockY int
	BlockZ int

	// The UUID of the current HEAD node
	Head UUID

	// Data types supported
	Datatypes []Datatype
}

// ReadJson reads in a Config from a JSON file with errors leading to
// termination of program.
func ReadJsonConfig(filename string) (config *Config) {
	var file *os.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: Failed to open JSON config file: %s [%s]\n",
			filename, err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	config = new(Config)
	err = dec.Decode(&config)
	if err == io.EOF {
		log.Fatalf("Error: No data in JSON config file: %s [%s]\n", filename, err)
	} else if err != nil {
		log.Fatalf("Error: Reading JSON config file (%s): %s\n", filename, err)
	}
	return
}

// WriteJson writes a Config to a JSON file.
func (config *Config) WriteJsonConfig(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Error: Failed to create JSON config file: %s [%s]\n",
			filename, err)
	}
	m, err := json.Marshal(config)
	if err != nil {
		log.Fatalf("Error in writing JSON config file: %s [%s]\n",
			filename, err)
	}
	var buf bytes.Buffer
	json.Indent(&buf, m, "", "    ")
	buf.WriteTo(file)
	file.Close()
}

// VerifyCompiledTypes will return an error if any required data type in the datastore 
// configuration was not compiled into DVID executable.
func (config *Config) VerifyCompiledTypes() error {
	if SupportedTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}
	var errMsg string
	for _, datatype := range config.Datatypes {
		_, found := SupportedTypes[datatype.Url]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.Name, datatype.Url)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

// SupportedTypeChart returns a chart (names/urls) of data types supported by this datastore
func (config *Config) SupportedTypeChart() string {
	var text string = "\nData types supported by this DVID datastore:\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range config.Datatypes {
		writeLine(datatype.Name, datatype.Url)
	}
	text += "\nUse the 'dvid <data type name> help' command for type-specific help.\n"
	return text
}

// IsSupportedType returns true if given data type name is supported by this datastore
func (config *Config) IsSupportedType(name string) bool {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			return true
		}
	}
	return false
}

func (config *Config) GetSupportedTypeUrl(name string) (url UrlString, err error) {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			url = datatype.Url
			return
		}
	}
	err = fmt.Errorf("data type '%s' not supported by opened datastore.", name)
	return
}

// Init creates a key-value datastore using default parameters.  Datastore 
// configuration is stored in the datastore and in a human-readable JSON file
// in the datastore directory.
func Init(directory string, config *Config, create bool) (uuid UUID) {
	err := config.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err)
	}
	db.Close()
	filename := filepath.Join(directory, ConfigFilename)
	config.WriteJsonConfig(filename)

	// Initialize the versioning system
	uuid = NewUUID() // This will be the HEAD

	// Put config data and current head data into datastore.
	return
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	// The datastore configuration for this open DVID datastore,
	// including the supported data types
	Config

	// The underlying leveldb
	keyvalue.KeyValueDB
}

// Open opens a DVID datastore at the given directory and returns
// a Service that allows operations on that datastore.
func Open(directory string) (service *Service, err error) {
	// Read this datastore's configuration
	config := ReadJsonConfig(filepath.Join(directory, ConfigFilename))

	// Open the datastore
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	create := false
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		err = fmt.Errorf("Error opening datastore (%s): %s\n", directory, err)
	} else {
		service = &Service{*config, db}
	}
	return
}

// Close closes a DVID datastore.
func (service *Service) Close() {
	service.Close()
}

// GetUuidFromString returns a UUID given its string representation.  Partial
// matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (service *Service) GetUuidFromString(s string) (uuid UUID, err error) {
	// TODO -- Implement this once we have DAG.
	uuid, err = UUIDfromString(s)
	return
}

func (service *Service) GetHeadUuid() (uuid UUID, err error) {
	// TODO -- Implement this once we have DAG.
	uuid = NewUUID()
	return
}
