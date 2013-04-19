package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
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

// initConfig defines the extent and resolution of the volume served by a DVID
// datastore instance at init time.   Modifying these values after init is
// a bad idea since spatial indexing for all keys might have to be modified
// if the underlying volume extents and resolution are changed.
type initConfig struct {
	dvid.Volume
}

// DataSetString is a string that is the name of a DVID data set. 
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data set name.
type DataSetString string

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
	// Data supported.  This is a map of a user-defined name like "fib_data" with
	// the supporting data type "grayscale8"
	dataNames map[DataSetString]datastoreType
}

// Shutdown handles graceful cleanup of datastore before exiting DVID.
func Shutdown() {
	dvid.Fmt(dvid.Debug, "TODO -- Datastore would cleanup cache here...\n")
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

	// Put initial version DAG
	dag := NewVersionDAG()
	err = dag.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing initial version DAG to datastore: %s\n", err.Error())
	}

	db.Close()
	uuid = dag.Head
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
	writeLine := func(name DataSetString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	writeLine("Name", "Type Name", "Url")
	for name, datatype := range config.dataNames {
		writeLine(name, datatype.TypeName()+" ("+datatype.TypeVersion()+")", datatype.TypeUrl())
	}
	return text
}

// About returns a chart of the code versions of compile-time DVID datastore
// and the runtime data types.
func (config *runtimeConfig) About() string {
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
func readJsonConfig(filename string) *initConfig {
	vol, err := dvid.ReadVolumeJSON(filename)
	if err != nil {
		log.Fatalf(err.Error())
	}
	return &initConfig{*vol}
}

// writeJsonConfig writes a Config to a JSON file.
func (config *initConfig) writeJsonConfig(filename string) {
	err := config.WriteJSON(filename)
	if err != nil {
		log.Fatalf(err.Error())
	}
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

	// Holds all version data including map of UUIDs to nodes
	VersionDAG

	// The underlying leveldb which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	kvdb
}

type OpenErrorType int

const (
	ErrorOpening OpenErrorType = iota
	ErrorInitConfig
	ErrorRuntimeConfig
	ErrorVersionDAG
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

	// Read this datastore's Version DAG
	dag := new(VersionDAG)
	err = dag.get(kvdb{db})
	if err != nil {
		openErr = &OpenError{
			err,
			ErrorVersionDAG,
		}
		return
	}

	s = &Service{*iconfig, *rconfig, *dag, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (s *Service) Close() {
	s.kvdb.Close()
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
func (s *Service) DataSetService(name DataSetString) (typeService TypeService, err error) {
	for key, _ := range s.dataNames {
		if name == key {
			typeService = s.dataNames[key]
			return
		}
	}
	err = fmt.Errorf("Data set '%s' was not in opened datastore.", name)
	return
}

// DataSets returns a map from data set name to type-specific services.
func (s *Service) DataSets() map[DataSetString]datastoreType {
	return s.dataNames
}

// TypeService returns a type-specific service given a type name.
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
func (s *Service) NewDataSet(name DataSetString, typeName string) error {
	// Check if this is a valid data type
	typeService, err := s.TypeService(typeName)
	if err != nil {
		return err
	}
	// Check if we already have this registered
	foundTypeService, found := s.dataNames[name]
	if found {
		if typeService.TypeUrl() != foundTypeService.TypeUrl() {
			return fmt.Errorf("Data set name '%s' already has type '%s' not '%s'",
				name, foundTypeService.TypeUrl(), typeService.TypeUrl())
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
		s.dataNames = make(map[DataSetString]datastoreType)
	}
	s.dataNames[name] = datastoreType{typeService, index.ToKey()}
	err = s.runtimeConfig.put(s.kvdb)
	ReserveBlockHandlers(name, s.dataNames[name])
	lock.Unlock()
	return err
}

// DeleteDataSet deletes a data set from the datastore and is considered
// a DANGEROUS operation.  Only to be used during debugging and development.
// TODO -- Deleted dataset data should also be expunged. 
func (s *Service) DeleteDataSet(name DataSetString) error {
	// Find the data set
	_, found := s.dataNames[name]
	if !found {
		return fmt.Errorf("Data set '%s' not present in DVID datastore.", name)
	}
	// Delete it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	delete(s.dataNames, name)
	// Remove load stat cache
	delete(loadLastSec, name)
	err := s.runtimeConfig.put(s.kvdb)
	// Should halt ReserveBlockHandlers(name, s.dataNames[name])
	lock.Unlock()
	return err
}

// DataIndexBytes returns bytes that uniquely identify (within the current datastore)
// the given data set name.   Returns nil if data set name was not found. 
func (s *Service) DataIndexBytes(name DataSetString) (index []byte, err error) {
	dtype, found := s.dataNames[name]
	if found {
		index = dtype.dataIndexBytes
		return
	}
	err = fmt.Errorf("Dataset name '%s' not registered for this datastore!", name)
	return
}

// GetHeadUuid returns the UUID of the HEAD node, i.e., the version that is
// considered the most current.
func (s *Service) GetHeadUuid() UUID {
	return s.Head
}

// TypeInfo contains data type information reformatted for easy consumption by clients.
type TypeInfo struct {
	Name            string
	Url             string
	Version         string
	BlockSize       dvid.Point3d
	SpatialIndexing string
	Help            string
}

// ConfigJSON returns configuration data in JSON format.
func (s *Service) ConfigJSON() (jsonStr string, err error) {
	datasets := make(map[DataSetString]TypeInfo)
	for name, dtype := range s.dataNames {
		datasets[name] = TypeInfo{
			Name:            dtype.TypeName(),
			Url:             string(dtype.TypeUrl()),
			Version:         dtype.TypeVersion(),
			BlockSize:       dtype.BlockSize(),
			SpatialIndexing: dtype.SpatialIndexing().String(),
			Help:            dtype.Help(),
		}
	}
	data := struct {
		Volume   dvid.Volume
		DataSets map[DataSetString]TypeInfo
	}{
		s.initConfig.Volume,
		datasets,
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// VersionsJSON returns the version DAG data in JSON format.
func (s *Service) VersionsJSON() (jsonStr string, err error) {
	m, err := json.Marshal(s.VersionDAG)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// VersionService encapsulates both an open DVID datastore and the version that
// should be operated upon.  It is the main unit of service for operations.
type VersionService struct {
	*Service

	// The datastore-specific UUID index for the version we are operating upon
	versionId VersionId

	// Channels for the block handlers
	channels BlockChannels
}

// NewVersionService returns a VersionService given a UUID string that has enough
// characters to uniquely identify a version in the datastore.
func NewVersionService(s *Service, uuidStr string) (vs *VersionService, err error) {
	id, err := s.VersionIdFromString(uuidStr)
	if err != nil {
		return
	}
	vs = &VersionService{s, id, make(BlockChannels)}
	return
}

// VersionId is the datastore-specific version index.  We assume we'll have no more
// than 32,000 image versions.
func (vs *VersionService) VersionId() VersionId {
	return vs.versionId
}

// UuidBytes returns a sequence of bytes encoding the UUID for this version service. 
func (vs *VersionService) UuidBytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, vs.versionId)
	if err != nil {
		log.Fatalf("ERROR encoding binary uuid %d: %s", vs.versionId, err.Error())
	}
	return buf.Bytes()
}
