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
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
)

const (
	Version = "0.3"

	// ConfigFilename is name of JSON file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

// initConfig holds the essential configuration data AT INIT TIME for a datastore instance. 
type initConfig struct {
	// Volume extents
	VolumeMax dvid.VoxelCoord

	// Relative resolution of voxels in volume
	VoxelRes dvid.VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.VoxelResolutionUnits
}

// DataTypeIndex is a unique id for a type within a DVID instance.
type DataTypeIndex uint16

func (index DataTypeIndex) ToKey() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, index)
	if err != nil {
		log.Fatalf("Unable to convert DataTypeIndex (%d) to []byte!\n", index)
	}
	return buf.Bytes()
}

type DatastoreType struct {
	TypeService

	// A unique id for the type within this DVID datastore instance. Used to construct keys.
	DataIndexBytes []byte
}

// runtimeConfig holds the essential configuration data AT RUN TIME for a datastore instance. 
type runtimeConfig struct {
	// Data supported.  This is a map of a user-defined name like "grayscale8" with
	// the supporting data type "voxel8"
	DataNames map[string]DatastoreType
}

// Shutdown handles graceful cleanup of datastore before exiting DVID.
func Shutdown() {
	log.Println("TODO -- Datastore would cleanup cache here...\n")
}

// Versions returns a chart of version identifiers for data types and and DVID's datastore
// fixed at compile-time for this DVID executable
func Versions() string {
	var text string = "\nCompile-time version information for this DVID executable:\n\n"
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Leveldb", keyvalue.Version)
	for _, datatype := range CompiledTypes {
		writeLine(datatype.Name(), datatype.Version())
	}
	return text
}

// Init creates a key-value datastore using default arguments.  Datastore 
// configuration is stored in the datastore and in a human-readable JSON file
// in the datastore directory.
func Init(directory string, configFile string, create bool) (uuid UUID) {
	// Initialize the configuration
	var config *initConfig
	if configFile == "" {
		config = promptConfig()
	} else {
		config = readJsonConfig(configFile)
	}

	fmt.Println("\nInitializing datastore at", directory)
	fmt.Printf("Volume size: %d x %d x %d\n",
		config.VolumeMax[0], config.VolumeMax[1], config.VolumeMax[2])
	fmt.Printf("Voxel resolution: %4.1f x %4.1f x %4.1f %s\n",
		config.VoxelRes[0], config.VoxelRes[1], config.VoxelRes[2], config.VoxelResUnits)

	// Create the leveldb
	dbOptions := keyvalue.NewKeyValueOptions()
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err.Error())
	}

	// Store the configuration in a human-readable JSON
	filename := filepath.Join(directory, ConfigFilename)
	config.writeJsonConfig(filename)

	// Put initial config data
	err = config.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing configuration to datastore: %s\n", err.Error())
	}

	// Put empty runtime configuration
	rconfig := new(runtimeConfig)
	err = rconfig.put(kvdb{db})
	if err != nil {
		err = fmt.Errorf("Error writing blank runtime configuration: %s", err.Error())
		return
	}

	// Put cached global variables
	uuid = NewUUID()
	globals := uuidData{
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

// VerifyCompiledTypes will return an error if any required data type in the datastore 
// configuration was not compiled into DVID executable.  Check is done by more exact
// URL and not the data type name.
func (config *runtimeConfig) VerifyCompiledTypes() error {
	if CompiledTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}
	var errMsg string
	for name, datatype := range config.DataNames {
		_, found := CompiledTypes[datatype.Url()]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.Name(), datatype.Url())
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

// DataChart returns a chart of data set names and their types for this runtime configuration. 
func (config *runtimeConfig) DataChart() string {
	var text string
	if config.DataNames == nil {
		return "No data types has been added to this datastore.\n"
	}
	writeLine := func(name, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-15s  %s\n", name, version, url)
	}
	writeLine("Name", "Type Name", "Url")
	for name, datatype := range config.DataNames {
		writeLine(name, datatype.Name()+" ("+datatype.Version()+")", datatype.Url())
	}
	return text
}

// Version returns a chart of version identifiers for compile-time DVID datastore
// and the runtime data types.
func (config *runtimeConfig) Versions() string {
	var text string
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Leveldb", keyvalue.Version)
	for _, datatype := range config.DataNames {
		writeLine(datatype.Name(), datatype.Version())
	}
	return text
}

// promptConfig prompts the user to enter configuration data.
func promptConfig() (config *initConfig) {
	config = new(initConfig)
	pVolumeMax := &(config.VolumeMax)
	pVolumeMax.Prompt("# voxels", "250")
	pVoxelRes := &(config.VoxelRes)
	pVoxelRes.Prompt("Voxel resolution", "10.0")
	config.VoxelResUnits = dvid.VoxelResolutionUnits(dvid.Prompt("Units of resolution", "nanometer"))
	return
}

// readJsonConfig reads in a Config from a JSON file with errors leading to
// termination of program.
func readJsonConfig(filename string) (config *initConfig) {
	var file *os.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: Failed to open JSON config file: %s [%s]\n",
			filename, err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	config = new(initConfig)
	err = dec.Decode(&config)
	if err == io.EOF {
		log.Fatalf("Error: No data in JSON config file: %s [%s]\n", filename, err)
	} else if err != nil {
		log.Fatalf("Error: Reading JSON config file (%s): %s\n", filename, err)
	}
	return
}

// writeJsonConfig writes a Config to a JSON file.
func (config *initConfig) writeJsonConfig(filename string) {
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
	initConfig

	runtimeConfig

	// Holds map of all UUIDs in datastore
	uuidData

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
		err = fmt.Errorf("Error opening datastore (%s): %s", directory, err.Error())
		return
	}

	// Read this datastore's configuration
	iconfig := new(initConfig)
	err = iconfig.get(kvdb{db})
	if err != nil {
		err = fmt.Errorf("Error reading initial configuration: %s", err.Error())
		return
	}
	rconfig := new(runtimeConfig)
	err = rconfig.get(kvdb{db})
	if err != nil {
		err = fmt.Errorf("Error reading runtime configuration: %s", err.Error())
		return
	}

	// Read this datastore's UUIDs
	uuids := new(uuidData)
	err = uuids.get(kvdb{db})
	if err != nil {
		return
	}

	service = &Service{*iconfig, *rconfig, *uuids, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (service *Service) Close() {
	service.Close()
}

// SupportedDataChart returns a chart (names/urls) of data referenced by this datastore
func (service *Service) SupportedDataChart() string {
	header := "\nData types currently referenced within this DVID datastore:\n\n"
	return header + service.runtimeConfig.DataChart()
}

// TypeService returns a type-specific service given an identifier that should be
// specific to a DVID instance
func (service *Service) TypeService(name string) (typeService TypeService, err error) {
	for dataname, datatype := range service.DataNames {
		if name == dataname {
			typeService = service.DataNames[dataname]
			return
		}
	}
	err = fmt.Errorf("Data type '%s' has not been added yet in opened datastore.", name)
	return
}

// NewDataSet registers a data set name of a given data type with this datastore.  
func (service *Service) NewDataSet(dataSetName, dataTypeName string) error {
	// Check if this is a valid data type
	typeService, err := service.TypeService(dataTypeName)
	if err != nil {
		return err
	}
	// Check if we already have this registered
	foundTypeService, found := service.DataNames[dataSetName]
	if found {
		if typeService.Url() != foundTypeService.Url() {
			return fmt.Errorf("Data set name '%s' already has type '%s' not '%s'",
				dataSetName, foundTypeService.Url(), typeService.Url())
		}
		return nil
	}
	// Add it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	index := DataTypeIndex(len(service.DataNames) + 1)
	if index > 65500 {
		return fmt.Errorf("Only 65500 distinct data sets allowed per DVID instance.")
	}
	service.DataNames[dataSetName] = DatastoreType{typeService, index.ToKey()}
	err = service.runtimeConfig.put(service.kvdb)
	lock.Unlock()
	return err
}

// DataIndexBytes returns bytes that uniquely identify (within the current datastore)
// the given data type name.   Returns nil if type name was not found. 
func (service *Service) DataIndexBytes(name string) []byte {
	dtype, found := service.DataNames[name]
	if found {
		return dtype.DataIndexBytes
	}
	return nil
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

// VersionService encapsulates both an open DVID datastore and the version that
// should be operated upon.  It is the main unit of service for operations.
type VersionService struct {
	Service

	// The datastore-specific UUID index for the version we are operating upon
	uuidNum int16

	// Channels for the block handlers
	channels BlockChannels
}

// NewVersionService packages a Service and a UUID representing a version
func NewVersionService(service *Service, uuidNum int16) *VersionService {
	return &VersionService{
		*service,
		uuidNum,
		make(BlockChannels),
	}
}

// UuidNum is the datastore-specific UUID index.  We assume we'll have no more
// than 32,000 image versions.
func (vs *VersionService) UuidNum() int16 {
	return vs.uuidNum
}

// UuidBytes returns a sequence of bytes encoding the UUID for this version service. 
func (vs *VersionService) UuidBytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, vs.uuidNum)
	if err != nil {
		log.Fatalf("ERROR encoding binary uuid %d: %s", vs.uuidNum, err.Error())
	}
	return buf.Bytes()
}

func (vs *VersionService) DatatypeKey() []byte {
	// Determine the index of this datatype for this particular datastore.
	var datatypeIndex int8 = -1
	for i, d := range vs.Datatypes {
		if d.Url == data.TypeUrl() {
			datatypeIndex = int8(i)
			break
		}
	}
	if datatypeIndex < 0 {
		return fmt.Errorf("Could not match datatype (%s) to supported data types!",
			datatype.Url)
	}
	datatypeKey := byte(datatypeIndex)
}

// Request adds a datastore version service to a DVID request. 
type Request struct {
	svc *VersionService
	dvid.Request
}

// VersionService returns the version service associated with this datastore request. 
func (r *Request) VersionService() *VersionService {
	return r.svc
}
