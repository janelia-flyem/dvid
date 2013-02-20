package datastore

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

// configData holds the essential configuration data for a datastore instance and therefore
// is private to package datastore.
type configData struct {
	// Volume extents
	VolumeMaxX int
	VolumeMaxY int
	VolumeMaxZ int

	// Relative resolution of voxels in volume
	VoxelResX float32
	VoxelResY float32
	VoxelResZ float32

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits string

	// Block size
	BlockMaxX int
	BlockMaxY int
	BlockMaxZ int

	// Data types supported
	Datatypes []Datatype
}

// Init creates a key-value datastore using default parameters.  Datastore 
// configuration is stored in the datastore and in a human-readable JSON file
// in the datastore directory.
func Init(directory string, configFile string, create bool) (uuid UUID) {
	var config *configData
	if configFile == "" {
		config = promptConfig()
	} else {
		config = readJsonConfig(configFile)
	}
	err := config.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err.Error())
	}

	// Store the configuration in a human-readable JSON
	filename := filepath.Join(directory, ConfigFilename)
	config.writeJsonConfig(filename)

	// Put config data
	err = config.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing configuration to datastore: %s\n", err.Error())
	}

	// Put cached global variables
	uuid = NewUUID()
	globals := cachedData{
		Head:  uuid,
		Uuids: map[string]int{string(uuid): 1},
	}
	err = globals.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing global cache to datastore: %s\n", err.Error())
	}

	db.Close()
	return
}

// VerifyCompiledTypes will return an error if any required data type in the datastore 
// configuration was not compiled into DVID executable.
func (config *configData) VerifyCompiledTypes() error {
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
func (config *configData) SupportedTypeChart() string {
	var text string = "\nData types supported by this DVID datastore:\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range config.Datatypes {
		writeLine(datatype.Name, datatype.Url)
	}
	text += "\nUse the '<data type name> help' command for type-specific help.\n"
	return text
}

// IsSupportedType returns true if given data type name is supported by this datastore
func (config *configData) IsSupportedType(name string) bool {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			return true
		}
	}
	return false
}

func (config *configData) GetSupportedTypeUrl(name string) (url UrlString, err error) {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			url = datatype.Url
			return
		}
	}
	err = fmt.Errorf("data type '%s' not supported by opened datastore.", name)
	return
}

// promptConfig prompts the user to enter configuration data.
func promptConfig() (config *configData) {

	prompt := func(message, defaultValue string) string {
		fmt.Print(message + " [" + defaultValue + "]: ")
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			return defaultValue
		}
		return line
	}

	config = new(configData)

	// Volume extents
	config.VolumeMaxX, _ = strconv.Atoi(prompt("# voxels in X", "250"))
	config.VolumeMaxY, _ = strconv.Atoi(prompt("# voxels in Y", "250"))
	config.VolumeMaxZ, _ = strconv.Atoi(prompt("# voxels in Z", "250"))

	// Relative resolution of voxels in volume
	var res float64
	res, _ = strconv.ParseFloat(prompt("Voxel resolution in X", "1.0"), 32)
	config.VoxelResX = float32(res)
	res, _ = strconv.ParseFloat(prompt("Voxel resolution in Y", "1.0"), 32)
	config.VoxelResY = float32(res)
	res, _ = strconv.ParseFloat(prompt("Voxel resolution in Z", "1.0"), 32)
	config.VoxelResZ = float32(res)

	// Units of resolution, e.g., "nanometers"
	config.VoxelResUnits = prompt("Units of resolution", "nanometers")

	// Block size
	config.BlockMaxX, _ = strconv.Atoi(prompt("Size of blocks in X", "16"))
	config.BlockMaxY, _ = strconv.Atoi(prompt("Size of blocks in Y", "16"))
	config.BlockMaxZ, _ = strconv.Atoi(prompt("Size of blocks in Z", "16"))

	// Data types supported
	config.Datatypes = DefaultDataTypes
	return
}

// readJsonConfig reads in a Config from a JSON file with errors leading to
// termination of program.
func readJsonConfig(filename string) (config *configData) {
	var file *os.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: Failed to open JSON config file: %s [%s]\n",
			filename, err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	config = new(configData)
	err = dec.Decode(&config)
	if err == io.EOF {
		log.Fatalf("Error: No data in JSON config file: %s [%s]\n", filename, err)
	} else if err != nil {
		log.Fatalf("Error: Reading JSON config file (%s): %s\n", filename, err)
	}
	return
}

// writeJsonConfig writes a Config to a JSON file.
func (config *configData) writeJsonConfig(filename string) {
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

// kvdb is a private wrapper around the underlying key/value datastore and provides
// a number of functions for DVID-specific types.
type kvdb struct {
	keyvalue.KeyValueDB
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	// The datastore configuration for this open DVID datastore,
	// including the supported data types
	configData

	// The globals cache
	cachedData

	// The underlying leveldb which is private since we want to create a
	// simplified interface and deal with DVID-specific keys.
	kvdb
}

// Open opens a DVID datastore at the given directory and returns
// a Service that allows operations on that datastore.
func Open(directory string) (service *Service, err error) {
	// Open the datastore
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	create := false
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		err = fmt.Errorf("Error opening datastore (%s): %s\n", directory, err)
		return
	}

	// Read this datastore's configuration
	config := new(configData)
	err = config.get(kvdb{db})
	if err != nil {
		return
	}
	fmt.Println("Retrieved configuration:\n", *config)

	// Read this datastore's cached globals
	cache := new(cachedData)
	err = cache.get(kvdb{db})
	if err != nil {
		return
	}
	fmt.Println("Retrieved globals cache:\n", *cache)

	service = &Service{*config, *cache, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (service *Service) Close() {
	service.Close()
}

// GetUuidNumFromString returns a UUID index given its string representation.  
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (service *Service) GetUuidFromString(s string) (uuidNum int, err error) {
	// Valid hex decode requires pairs of characters
	if len(s)%2 == 0 {
		var uuid UUID
		uuid, err = UUIDfromString(s)
		if err != nil {
			return
		}
		var found bool
		uuidNum, found = service.Uuids[string(uuid)]
		if found {
			return
		}
	}

	// Use the hex string version to try to find a partial match
	var lastMatch int
	numMatches := 0
	for key, uuidNum := range service.Uuids {
		hexString := UUID(key).String()
		fmt.Printf("Checking %s against %s\n", s, hexString)
		if strings.HasPrefix(hexString, s) {
			numMatches++
			lastMatch = uuidNum
		}
	}
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", s)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", s)
	} else {
		uuidNum = lastMatch
	}
	return
}

// GetHeadUuid returns the UUID of the HEAD node, i.e., the version that is
// considered the most current.
func (service *Service) GetHeadUuid() UUID {
	return service.Head
}
