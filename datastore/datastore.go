package datastore

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.6"
)

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
func Init(directory string, create bool) (uuid UUID) {
	fmt.Println("\nInitializing datastore at", directory)

	// Initialize the backend database
	dbOptions := storage.Options{}
	db, err := storage.NewDataHandler(directory, create, &dbOptions)
	if err != nil {
		log.Fatalf("Error initializing datastore (%s): %s\n", directory, err.Error())
	}

	// Put empty runtime configuration
	rconfig := new(runtimeConfig)
	err = rconfig.Put(db)
	if err != nil {
		log.Fatalf("Error writing blank runtime configuration: %s", err.Error())
		return
	}

	// Put initial version DAG
	dag := NewVersionDAG()
	err = dag.Put(db)
	if err != nil {
		log.Fatalf("Error writing initial version DAG to datastore: %s\n", err.Error())
	}

	db.Close()
	uuid = dag.Head
	return
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
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
	ErrorRuntimeConfig
	ErrorVersionDAG
)

type OpenError struct {
	error
	ErrorType OpenErrorType
}

// Open opens a DVID datastore at the given path (directory, url, etc) and returns
// a Service that allows operations on that datastore.
func Open(path string) (s *Service, openErr *OpenError) {
	// Open the datastore
	dbOptions := storage.Options{}
	create := false
	db, err := storage.NewDataHandler(path, create, &dbOptions)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening datastore (%s): %s", path, err.Error()),
			ErrorOpening,
		}
		return
	}

	// Read this datastore's configuration
	rconfig := new(runtimeConfig)
	err = rconfig.Get(db)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading runtime configuration: %s", err.Error()),
			ErrorRuntimeConfig,
		}
		return
	}

	// Verify that the runtime configuration can be supported by this DVID's
	// compiled-in data types.
	dvid.Fmt(dvid.Debug, "Verifying datastore's supported types were compiled into DVID...\n")
	err = rconfig.VerifyCompiledTypes()
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Datasets are not fully supported by this DVID: %s", err.Error()),
			ErrorRuntimeConfig,
		}
		return
	}

	// Read this datastore's Version DAG
	dag := new(VersionDAG)
	err = dag.Get(db)
	if err != nil {
		openErr = &OpenError{
			err,
			ErrorVersionDAG,
		}
		return
	}
	fmt.Printf("\nDatastoreService successfully opened: %s\n", path)
	s = &Service{*rconfig, *dag, db}
	return
}

// Shutdown closes a DVID datastore.
func (s *Service) Shutdown() {
	// Close all dataset handlers
	for _, dataset := range s.Datasets {
		stopChunkHandlers(dataset)
	}

	// Close the backend database
	s.db.Close()
}

// KeyValueDB returns a a key-value database interface.
func (s *Service) KeyValueDB() storage.KeyValueDB {
	return s.db
}

// IteratorMaker returns an interface that can create a new iterator.
func (s *Service) IteratorMaker() (db storage.IteratorMaker, err error) {
	if s.db.ProvidesIterator() {
		var ok bool
		db, ok = s.db.(storage.IteratorMaker)
		if !ok {
			err = fmt.Errorf("DVID backend says it supports iterators but does not!")
		}
	} else {
		err = fmt.Errorf("DVID backend database does not provide iterator support")
	}
	return
}

// Batcher returns an interface that can create a new batch write.
func (s *Service) Batcher() (db storage.Batcher, err error) {
	if s.db.IsBatcher() {
		var ok bool
		db, ok = s.db.(storage.Batcher)
		if !ok {
			err = fmt.Errorf("DVID backend says it supports batch write but does not!")
		}
	} else {
		err = fmt.Errorf("DVID backend database does not support batch write")
	}
	return
}

// SupportedDataChart returns a chart (names/urls) of data referenced by this datastore
func (s *Service) SupportedDataChart() string {
	text := CompiledTypeChart()
	text += "Datasets currently referenced within this DVID datastore:\n\n"
	text += s.runtimeConfig.DataChart()
	return text
}

// LogInfo returns information on versions and

// DataService returns a type-specific service given a data name that should be
// specific to a DVID server.
func (s *Service) DataService(name DataString) (dataService DataService, err error) {
	for key, _ := range s.Data {
		if name == key {
			dataService = s.Data[key]
			return
		}
	}
	err = fmt.Errorf("Data '%s' was not in opened datastore.", name)
	return
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

// NewData registers a data set name of a given data type with this datastore.
// TODO -- Will have to pass in initialization parameters like block size,
// block resolution, units, etc.  Then, within the data type, for each dataset
// we create a blocksize that's tailored for this DVID instance's volume, after
// taking into account units, resolution, etc.
func (s *Service) NewData(name DataString, typeName string, config dvid.Config) error {
	// Check if this is a valid data type
	typeService, err := s.TypeService(typeName)
	if err != nil {
		return err
	}
	// Check if we already have this registered
	foundTypeService, found := s.Data[name]
	if found {
		if typeService.DatatypeUrl() != foundTypeService.DatatypeUrl() {
			return fmt.Errorf("Data set name '%s' already has type '%s' not '%s'",
				name, foundTypeService.DatatypeUrl(), typeService.DatatypeUrl())
		}
		return nil
	}
	// Add it and store updated runtimeConfig to datastore.
	s.NewLocalID++
	if s.NewLocalID >= 0xFFFE {
		return fmt.Errorf("Too many distinct data sets are in this DVID instance: %d",
			s.NewLocalID)
	}
	var lock sync.Mutex
	lock.Lock()
	if s.Data == nil {
		s.Data = make(map[DataString]DataService)
	}
	s.Data[name] = typeService.NewData(DataID{name, s.NewLocalID}, config)
	err = s.runtimeConfig.Put(s.db)
	lock.Unlock()
	return err
}

// DeleteData deletes data from the datastore and is considered
// a DANGEROUS operation.  Only to be used during debugging and development.
// TODO -- Deleted dataset data should also be expunged.
func (s *Service) DeleteData(name DataString) error {
	// Find the data set
	data, found := s.Data[name]
	if !found {
		return fmt.Errorf("Data '%s' not present in DVID datastore.", name)
	}
	// Delete it and store updated runtimeConfig to datastore.
	var lock sync.Mutex
	lock.Lock()
	stopChunkHandlers(data)
	delete(s.Data, name)
	err := s.runtimeConfig.Put(s.db)
	lock.Unlock()
	return err
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
