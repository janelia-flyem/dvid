package datastore

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.6"
)

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
	writeLine("Storage driver", storage.Version)
	for _, datatype := range CompiledTypes {
		writeLine(datatype.DatatypeName(), datatype.DatatypeVersion())
	}
	return text
}

// Init creates a key-value datastore using default arguments.  Datastore 
// configuration is stored as key/values in the datastore and also in a 
// human-readable config file in the datastore directory.
func Init(directory string, configFile string, create bool) (uuid UUID) {
	// Initialize the configuration
	var ic *initConfig
	var rc *runtimeConfig
	if configFile == "" {
		ic = initByPrompt()
	} else {
		ic, rc, err = initByJson(configFile)
	}

	fmt.Println("\nInitializing datastore at", directory)
	fmt.Printf("Volume size: %d x %d x %d\n",
		config.VolumeMax[0], config.VolumeMax[1], config.VolumeMax[2])
	fmt.Printf("Voxel resolution: %4.1f x %4.1f x %4.1f %s\n",
		config.VoxelRes[0], config.VoxelRes[1], config.VoxelRes[2], config.VoxelResUnits)

	// Initialize the backend database
	dbOptions := storage.Options{}
	db, err := storage.NewDataHandler(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err.Error())
	}

	// Store the configuration in a human-readable JSON
	filename := filepath.Join(directory, ConfigFilename)
	config.writeJsonConfig(filename)

	// Put initial config data
	err = config.put(db)
	if err != nil {
		log.Fatalf("Error writing configuration to datastore: %s\n", err.Error())
	}

	// Put empty runtime configuration
	rconfig := new(runtimeConfig)
	err = rconfig.put(db)
	if err != nil {
		err = fmt.Errorf("Error writing blank runtime configuration: %s", err.Error())
		return
	}

	// Put initial version DAG
	dag := NewVersionDAG()
	err = dag.put(db)
	if err != nil {
		log.Fatalf("Error writing initial version DAG to datastore: %s\n", err.Error())
	}

	db.Close()
	uuid = dag.Head
	return
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	initConfig
	runtimeConfig

	// Holds all version data including map of UUIDs to nodes
	VersionDAG

	// The backend storage which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	db storage.DataHandler
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
	dbOptions := storage.Options{}
	create := false
	db, err := storage.NewDataHandler(directory, create, dbOptions)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening datastore (%s): %s", directory, err.Error()),
			ErrorOpening,
		}
		return
	}

	// Read this datastore's configuration
	iconfig := new(initConfig)
	err = iconfig.get(db)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading initial configuration: %s", err.Error()),
			ErrorInitConfig,
		}
		return
	}
	rconfig := new(runtimeConfig)
	err = rconfig.get(db)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading runtime configuration: %s", err.Error()),
			ErrorRuntimeConfig,
		}
		return
	}

	// Read this datastore's Version DAG
	dag := new(VersionDAG)
	err = dag.get(db)
	if err != nil {
		openErr = &OpenError{
			err,
			ErrorVersionDAG,
		}
		return
	}

	s = &Service{*iconfig, *rconfig, *dag, db}
	return
}

// Close closes a DVID datastore.
func (s *Service) Close() {
	s.db.Close()
}

// DataHandler returns the backend data handler used by this DVID instance.
func (s *Service) DataHandler() storage.DataHandler {
	return s.db
}

// SupportedDataChart returns a chart (names/urls) of data referenced by this datastore
func (s *Service) SupportedDataChart() string {
	text := CompiledTypeChart()
	text += "Data types currently referenced within this DVID datastore:\n\n"
	text += s.runtimeConfig.DataChart()
	return text
}

// LogInfo returns information on versions and 

// DatasetService returns a type-specific service given a dataset name that should be
// specific to a DVID instance.
func (s *Service) DatasetService(name DatasetString) (typeService TypeService, err error) {
	for key, _ := range s.datasets {
		if name == key {
			typeService = s.datasets[key]
			return
		}
	}
	err = fmt.Errorf("Data set '%s' was not in opened datastore.", name)
	return
}

// Datasets returns a map from data set name to type-specific services.
func (s *Service) Datasets() map[DatasetString]Dataset {
	return s.datasets
}

// TypeService returns a type-specific service given a type name.
func (s *Service) TypeService(typeName string) (typeService TypeService, err error) {
	for _, dtype := range CompiledTypes {
		if typeName == dtype.DatatypeName() {
			typeService = dtype
			return
		}
	}
	err = fmt.Errorf("Data type '%s' is not supported in current DVID executable", typeName)
	return
}

// NewDataset registers a data set name of a given data type with this datastore.
// TODO -- Will have to pass in initialization parameters like block size,
// block resolution, units, etc.  Then, within the data type, for each dataset
// we create a blocksize that's tailored for this DVID instance's volume, after
// taking into account units, resolution, etc. 
func (s *Service) NewDataset(name DatasetString, typeName string) error {
	// Check if this is a valid data type
	typeService, err := s.TypeService(typeName)
	if err != nil {
		return err
	}
	// Check if we already have this registered
	foundTypeService, found := s.datasets[name]
	if found {
		if typeService.DatatypeUrl() != foundTypeService.DatatypeUrl() {
			return fmt.Errorf("Data set name '%s' already has type '%s' not '%s'",
				name, foundTypeService.DatatypeUrl(), typeService.DatatypeUrl())
		}
		return nil
	}
	// Add it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	id := DatasetID(len(s.datasets) + 1)
	if id > 65500 {
		return fmt.Errorf("Only 65500 distinct data sets allowed per DVID instance.")
	}
	if s.datasets == nil {
		s.datasets = make(map[DatasetString]Dataset)
	}
	s.datasets[name] = typeService.NewDataset(name, s, nil, id.Bytes())
	err = s.runtimeConfig.put(s.db)
	lock.Unlock()
	return err
}

// DeleteDataset deletes a data set from the datastore and is considered
// a DANGEROUS operation.  Only to be used during debugging and development.
// TODO -- Deleted dataset data should also be expunged. 
func (s *Service) DeleteDataset(name DatasetString) error {
	// Find the data set
	_, found := s.datasets[name]
	if !found {
		return fmt.Errorf("Data set '%s' not present in DVID datastore.", name)
	}
	// Delete it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	s.datasets[name].Shutdown()
	delete(s.datasets, name)
	// Remove load stat cache
	delete(loadLastSec, name)
	err := s.runtimeConfig.put(s.db)
	lock.Unlock()
	return err
}

// DataIndexBytes returns bytes that uniquely identify (within the current datastore)
// the given data set name.   Returns nil if data set name was not found. 
func (s *Service) DatasetBytes(name DatasetString) (b []byte, err error) {
	dset, found := s.datasets[name]
	if found {
		b = dset.datasetKey
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

// VersionBytes returns a slice of bytes representing the VersionID corresponding
// to the supplied UUID string.  The UUID string can be incomplete as long as it
// contains a unique prefix of a valid UUID.
func (s *Service) VersionBytes(uuidStr string) (b []byte, err error) {
	id, err := s.VersionIDFromString(uuidStr)
	if err != nil {
		return
	}
	b = id.Bytes()
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
