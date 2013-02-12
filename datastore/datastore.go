package datastore

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/janelia-flyem/dvid/datatype"
	"github.com/janelia-flyem/dvid/keyvalue"
)

const Kilo = 1 << 10
const Mega = 1 << 20
const Giga = 1 << 30
const Tera = 1 << 40

// ConfigFilename is name of JSON file with datastore configuration data
// just for human inspection.
const ConfigFilename = "dvid-config.json"

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

	// Data types supported
	Datatypes []datatype.Format
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
func (config *Config) WriteJson(filename string) {
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

// InitDatastore opens and optionally creates a key-value datastore
// using default parameters.  Also datastore configuration is stored both
// in the datastore as well as in a human-readable JSON file in the
// datastore directory.
func InitDatastore(directory string, config *Config, create bool) {
	config.checkSupported()
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err)
	}
	db.Close()
	filename := filepath.Join(directory, ConfigFilename)
	config.WriteJson(filename)
}

func (config *Config) checkSupported() {
	if datatype.Supported == nil {
		log.Fatalln("Error: DVID was not compiled with any data type support!")
	}
	for _, format := range config.Datatypes {
		_, found := datatype.Supported[format.Url]
		if !found {
			log.Fatalf("Error: DVID was not compiled with support for data type %s (%s)\n",
				format.Name, format.Url)
		}
	}
}
