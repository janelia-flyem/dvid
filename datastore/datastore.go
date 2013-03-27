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
	Version = "0.4"

	// ConfigFilename is name of JSON file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

// initConfig holds configuration data set at init time for a datastore instance. 
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

type datastoreType struct {
	TypeService

	// A unique id for the type within this DVID datastore instance. Used to construct keys.
	dataIndexBytes []byte
}

// runtimeConfig holds editable configuration data for a datastore instance. 
type runtimeConfig struct {
	// Data supported.  This is a map of a user-defined name like "grayscale8" with
	// the supporting data type "voxel8"
	dataNames map[string]datastoreType
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
		writeLine(datatype.TypeName(), datatype.TypeVersion())
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
	for _, datatype := range config.dataNames {
		_, found := CompiledTypes[datatype.TypeUrl()]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.TypeName(), datatype.TypeUrl())
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
	if len(config.dataNames) == 0 {
		return "  No data sets have been added to this datastore.\n  Use 'dvid dataset ...'"
	}
	writeLine := func(name, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	writeLine("Name", "Type Name", "Url")
	for name, datatype := range config.dataNames {
		writeLine(name, datatype.TypeName()+" ("+datatype.TypeVersion()+")", datatype.TypeUrl())
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
	for _, datatype := range config.dataNames {
		writeLine(datatype.TypeName(), datatype.TypeVersion())
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

type OpenErrorType int

const (
	ErrorOpening OpenErrorType = iota
	ErrorInitConfig
	ErrorRuntimeConfig
	ErrorUUIDs
)

type OpenError struct {
	error
	ErrorType OpenErrorType
}

// Open opens a DVID datastore at the given directory and returns
// a Service that allows operations on that datastore.
func Open(directory string) (s *Service, openErr *OpenError) {
	// Open the datastore
	dbOptions := keyvalue.NewKeyValueOptions()
	create := false
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening datastore (%s): %s", directory, err.Error()),
			ErrorOpening,
		}
		return
	}

	// Read this datastore's configuration
	iconfig := new(initConfig)
	err = iconfig.get(kvdb{db})
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading initial configuration: %s", err.Error()),
			ErrorInitConfig,
		}
		return
	}
	rconfig := new(runtimeConfig)
	err = rconfig.get(kvdb{db})
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading runtime configuration: %s", err.Error()),
			ErrorRuntimeConfig,
		}
		return
	}

	// Read this datastore's UUIDs
	uuids := new(uuidData)
	err = uuids.get(kvdb{db})
	if err != nil {
		openErr = &OpenError{
			err,
			ErrorUUIDs,
		}
		return
	}

	s = &Service{*iconfig, *rconfig, *uuids, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (s *Service) Close() {
	s.Close()
}

// KeyValueDB returns the key value database used by the DVID datastore.
func (s *Service) KeyValueDB() keyvalue.KeyValueDB {
	return s.kvdb
}

// SupportedDataChart returns a chart (names/urls) of data referenced by this datastore
func (s *Service) SupportedDataChart() string {
	text := CompiledTypeChart()
	text += "Data types currently referenced within this DVID datastore:\n\n"
	text += s.runtimeConfig.DataChart()
	return text
}

// LogInfo returns information on versions and 

// DataSetService returns a type-specific service given a dataset name that should be
// specific to a DVID instance.
func (s *Service) DataSetService(dataSetName string) (typeService TypeService, err error) {
	for name, _ := range s.dataNames {
		if dataSetName == name {
			typeService = s.dataNames[name]
			return
		}
	}
	err = fmt.Errorf("Data set '%s' was not in opened datastore.", dataSetName)
	return
}

// TypeService returns a type-specific service given a type name that should be
// specific to a DVID instance.
func (s *Service) TypeService(typeName string) (typeService TypeService, err error) {
	for _, dtype := range CompiledTypes {
		if typeName == dtype.TypeName() {
			typeService = dtype
			return
		}
	}
	err = fmt.Errorf("Data type '%s' is not supported in current DVID executable", typeName)
	return
}

// NewDataSet registers a data set name of a given data type with this datastore.  
func (s *Service) NewDataSet(dataSetName, typeName string) error {
	// Check if this is a valid data type
	typeService, err := s.TypeService(typeName)
	if err != nil {
		return err
	}
	// Check if we already have this registered
	foundTypeService, found := s.dataNames[dataSetName]
	if found {
		if typeService.TypeUrl() != foundTypeService.TypeUrl() {
			return fmt.Errorf("Data set name '%s' already has type '%s' not '%s'",
				dataSetName, foundTypeService.TypeUrl(), typeService.TypeUrl())
		}
		return nil
	}
	// Add it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	index := DataTypeIndex(len(s.dataNames) + 1)
	if index > 65500 {
		return fmt.Errorf("Only 65500 distinct data sets allowed per DVID instance.")
	}
	if s.dataNames == nil {
		s.dataNames = make(map[string]datastoreType)
	}
	s.dataNames[dataSetName] = datastoreType{typeService, index.ToKey()}
	err = s.runtimeConfig.put(s.kvdb)
	lock.Unlock()
	return err
}

// DataIndexBytes returns bytes that uniquely identify (within the current datastore)
// the given data type name.   Returns nil if type name was not found. 
func (s *Service) DataIndexBytes(name string) []byte {
	dtype, found := s.dataNames[name]
	if found {
		return dtype.dataIndexBytes
	}
	return nil
}

// LogInfo returns provenance information on the version.
func (s *Service) LogInfo() string {
	text := "Versions:\n"
	for uuid, index := range s.Uuids {
		text += fmt.Sprintf("%s  (%d)\n", UUID(uuid), index)
	}
	return text
}

// GetUUIDFromString returns a UUID index given its string representation.  
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (s *Service) GetUUIDFromString(str string) (uuidNum int16, err error) {
	// Valid hex decode requires pairs of characters
	if len(str)%2 == 0 {
		var uuid UUID
		uuid, err = UUIDfromString(str)
		if err != nil {
			return
		}
		var found bool
		uuidNum, found = s.Uuids[string(uuid)]
		if found {
			return
		}
	}

	// Use the hex string version to try to find a partial match
	var lastMatch int16
	numMatches := 0
	for key, uuidNum := range s.Uuids {
		hexString := UUID(key).String()
		dvid.Fmt(dvid.Debug, "Checking %s against %s\n", str, hexString)
		if strings.HasPrefix(hexString, str) {
			numMatches++
			lastMatch = uuidNum
		}
	}
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", str)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", str)
	} else {
		uuidNum = lastMatch
	}
	return
}

// GetHeadUuid returns the UUID of the HEAD node, i.e., the version that is
// considered the most current.
func (s *Service) GetHeadUuid() UUID {
	return s.Head
}

// VersionService encapsulates both an open DVID datastore and the version that
// should be operated upon.  It is the main unit of service for operations.
type VersionService struct {
	*Service

	// The datastore-specific UUID index for the version we are operating upon
	uuidNum int16

	// Channels for the block handlers
	channels BlockChannels
}

// NewVersionService returns a VersionService given a UUID string that has enough
// characters to uniquely identify a version in the datastore.
func NewVersionService(s *Service, uuidStr string) (vs *VersionService, err error) {
	uuidNum, err := s.GetUUIDFromString(uuidStr)
	if err != nil {
		return
	}
	vs = &VersionService{s, uuidNum, make(BlockChannels)}
	return
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
