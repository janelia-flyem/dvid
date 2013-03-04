package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
)

const (
	Version = "0.2"

	// ConfigFilename is name of JSON file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

// configData holds the essential configuration data for a datastore instance and therefore
// is private to package datastore.
type configData struct {
	// Volume extents
	VolumeMax dvid.VoxelCoord

	// Relative resolution of voxels in volume
	VoxelRes dvid.VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.VoxelResolutionUnits

	// Block size
	BlockMax dvid.VoxelCoord

	// Spatial indexing scheme
	Indexing SpatialIndexScheme

	// Data types supported
	Datatypes []Datatype
}

// Versions returns a chart of version identifiers for data types and and DVID's datastore
func Versions() string {
	var text string
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Leveldb", keyvalue.Version)
	for _, datatype := range CompiledTypes {
		writeLine(datatype.BaseDatatype().Name, datatype.BaseDatatype().Version)
	}
	return text
}

// Init creates a key-value datastore using default arguments.  Datastore 
// configuration is stored in the datastore and in a human-readable JSON file
// in the datastore directory.
func Init(directory string, configFile string, create bool) (uuid UUID) {
	// Initialize the configuration
	var config *configData
	if configFile == "" {
		config = promptConfig()
	} else {
		config = readJsonConfig(configFile)
	}
	config.Datatypes = CompiledTypesList()

	fmt.Println("\nInitializing datastore at", directory)
	fmt.Printf("Volume size: %d x %d x %d\n",
		config.VolumeMax[0], config.VolumeMax[1], config.VolumeMax[2])
	fmt.Printf("Voxel resolution: %4.1f x %4.1f x %4.1f %s\n",
		config.VoxelRes[0], config.VoxelRes[1], config.VoxelRes[2], config.VoxelResUnits)
	fmt.Printf("Block size: %d x %d x %d voxels\n",
		config.BlockMax[0], config.BlockMax[1], config.BlockMax[2])
	fmt.Println(config.SupportedTypeChart())

	// Verify the data types
	err := config.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create the leveldb
	dbOptions := keyvalue.NewKeyValueOptions()
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
		Uuids: map[string]int16{string(uuid): 1},
	}
	err = globals.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing global cache to datastore: %s\n", err.Error())
	}

	db.Close()
	return
}

// BlockNumVoxels returns the # of voxels within a block.  Block size should
// be immutable during the course of a datastore's lifetime.
func (config *configData) BlockNumVoxels() int {
	x := int(config.BlockMax[0] * config.BlockMax[1] * config.BlockMax[2])
	return x
}

// VerifyCompiledTypes will return an error if any required data type in the datastore 
// configuration was not compiled into DVID executable.  Check is done by more exact
// URL and not the data type name.
func (config *configData) VerifyCompiledTypes() error {
	if CompiledTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}
	var errMsg string
	for _, datatype := range config.Datatypes {
		_, found := CompiledTypes[datatype.BaseDatatype().Url]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.BaseDatatype().Name, datatype.BaseDatatype().Url)
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
	writeLine := func(name, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-8s %s\n", name, version, url)
	}
	writeLine("Name", "Version", "Url")
	for _, datatype := range config.Datatypes {
		writeLine(datatype.BaseDatatype().Name, datatype.BaseDatatype().Version,
			datatype.BaseDatatype().Url)
	}
	return text
}

// Version returns a chart of version identifiers for data types and and DVID's datastore
func (config *configData) Versions() string {
	var text string
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Leveldb", keyvalue.Version)
	for _, datatype := range config.Datatypes {
		writeLine(datatype.BaseDatatype().Name, datatype.BaseDatatype().Version)
	}
	return text
}

// IsSupportedType returns true if given data type name is supported by this datastore
// IsSupportedType returns true if given data type name is supported by this datastore
func (config *configData) IsSupportedType(name string) bool {
	for _, datatype := range config.Datatypes {
		if name == datatype.BaseDatatype().Name {
			return true
		}
	}
	return false
}

func (config *configData) GetSupportedTypeUrl(name string) (url UrlString, err error) {
	for _, datatype := range config.Datatypes {
		if name == datatype.BaseDatatype().Name {
			url = datatype.BaseDatatype().Url
			return
		}
	}
	err = fmt.Errorf("data type '%s' not supported by opened datastore.", name)
	return
}

// promptConfig prompts the user to enter configuration data.
func promptConfig() (config *configData) {
	config = new(configData)
	pVolumeMax := &(config.VolumeMax)
	pVolumeMax.Prompt("# voxels", "250")
	pVoxelRes := &(config.VoxelRes)
	pVoxelRes.Prompt("Voxel resolution", "10.0")
	config.VoxelResUnits = dvid.VoxelResolutionUnits(dvid.Prompt("Units of resolution", "nanometer"))
	pBlockMax := &(config.BlockMax)
	pBlockMax.Prompt("Size of blocks", "16")

	config.Indexing = SIndexZXY
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
	config.Indexing = SIndexZXY
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

// VersionService encapsulates both an open DVID datastore and the version that
// should be operated upon.  It is the main unit of service for operations.
type VersionService struct {
	Service

	// The datastore-specific UUID index for the version we are operating upon
	uuidNum int16
}

// UuidNum is the datastore-specific UUID index.  We assume we'll have no more
// than 32,000 image versions.
func (vs *VersionService) UuidNum() int16 {
	return vs.uuidNum
}

// NewVersionService packages a Service and a UUID representing a version
func NewVersionService(service *Service, uuidNum int16) *VersionService {
	return &VersionService{
		*service,
		uuidNum,
	}
}

func (vs *VersionService) UuidBytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, vs.uuidNum)
	if err != nil {
		log.Fatalf("ERROR encoding binary uuid %d: %s", vs.uuidNum, err.Error())
	}
	return buf.Bytes()
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	// The datastore configuration for this open DVID datastore,
	// including the supported data types
	configData

	// The globals cache
	cachedData

	// The underlying leveldb which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	kvdb
}

// Open opens a DVID datastore at the given directory and returns
// a Service that allows operations on that datastore.
func Open(directory string) (service *Service, err error) {
	// Open the datastore
	dbOptions := keyvalue.NewKeyValueOptions()
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

	// Read this datastore's cached globals
	cache := new(cachedData)
	err = cache.get(kvdb{db})
	if err != nil {
		return
	}

	service = &Service{*config, *cache, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (service *Service) Close() {
	service.Close()
}

// GetUUIDFromString returns a UUID index given its string representation.  
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (service *Service) GetUUIDFromString(s string) (uuidNum int16, err error) {
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
	var lastMatch int16
	numMatches := 0
	for key, uuidNum := range service.Uuids {
		hexString := UUID(key).String()
		dvid.Fmt(dvid.Debug, "Checking %s against %s\n", s, hexString)
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
