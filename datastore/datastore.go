/*
	This file provides the highest-level view of the datastore via a Service.
*/

package datastore

import (
	"encoding/json"
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.7"
)

// Versions returns a chart of version identifiers for data types and and DVID's datastore
// fixed at compile-time for this DVID executable
func Versions() string {
	var text string = "\nCompile-time version information for this DVID executable:\n\n"
	writeLine := func(name dvid.TypeString, version string) {
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
func Init(directory string, create bool, config dvid.Config) error {
	fmt.Println("\nInitializing datastore at", directory)

	// Initialize the backend database
	engine, err := storage.NewStore(directory, create, config)
	if err != nil {
		return fmt.Errorf("Error initializing datastore (%s): %s\n", directory, err.Error())
	}
	defer engine.Close()

	kvDB, ok := engine.(storage.KeyValueDB)
	if !ok {
		return fmt.Errorf("Datastore at %s does not support key-value database ops.", directory)
	}
	// Initialize the graph backend database
	gengine, err := storage.NewGraphStore(directory, create, config, kvDB)
	if err != nil {
		return fmt.Errorf("Error initializing graph datastore (%s): %s\n", directory, err.Error())
	}
	defer gengine.Close()

	// Put empty Datasets
	db, ok := engine.(storage.KeyValueSetter)
	if !ok {
		return fmt.Errorf("Datastore at %s does not support setting of key-value pairs!", directory)
	}
	datasets := new(Datasets)
	err = datasets.Put(db)
	return err
}

// Service couples an open DVID storage engine and DVID datasets.
type Service struct {
	*Datasets

	// The backend storage which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	engine      storage.Engine
	kvDB        storage.KeyValueDB
	kvSetter    storage.KeyValueSetter
	kvGetter    storage.KeyValueGetter
	graphengine storage.Engine
	gDB         storage.GraphDB
	gSetter     storage.GraphSetter
	gGetter     storage.GraphGetter
}

type OpenErrorType int

const (
	ErrorOpening OpenErrorType = iota
	ErrorDatasets
	ErrorDatatypeUnavailable
)

type OpenError struct {
	error
	ErrorType OpenErrorType
}

// Open opens a DVID datastore at the given path (directory, url, etc) and returns
// a Service that allows operations on that datastore.
func Open(path string) (s *Service, openErr *OpenError) {
	// Open the datastore
	create := false
	engine, err := storage.NewStore(path, create, dvid.Config{})
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening datastore (%s): %s", path, err.Error()),
			ErrorOpening,
		}
		return
	}

	// Get interfaces this engine supports.
	kvGetter, ok := engine.(storage.KeyValueGetter)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore cannot get key-value pairs."),
			ErrorOpening,
		}
		return
	}
	kvSetter, ok := engine.(storage.KeyValueSetter)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore cannot set key-value pairs."),
			ErrorOpening,
		}
		return
	}
	kvDB, ok := engine.(storage.KeyValueDB)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore does not support key-value database ops."),
			ErrorOpening,
		}
		return
	}

	// Open the graph datastore (nothing happens if the graph key value store is used)
	gengine, err := storage.NewGraphStore(path, create, dvid.Config{}, kvDB)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening graph datastore (%s): %s", path, err.Error()),
			ErrorOpening,
		}
		return
	}

	gSetter, ok := gengine.(storage.GraphSetter)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore cannot set graph objects."),
			ErrorOpening,
		}
		return
	}
	gGetter, ok := gengine.(storage.GraphGetter)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore cannot get graph objects."),
			ErrorOpening,
		}
		return
	}
	gDB, ok := gengine.(storage.GraphDB)
	if !ok {
		openErr = &OpenError{
			fmt.Errorf("Opened datastore does not support graph database ops."),
			ErrorOpening,
		}
		return
	}

	// Read this datastore's configuration
	datasets := new(Datasets)
	err = datasets.Load(kvGetter)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading datasets: %s", err.Error()),
			ErrorDatasets,
		}
		return
	}

	// Verify that the runtime configuration can be supported by this DVID's
	// compiled-in data types.
	dvid.Fmt(dvid.Debug, "Verifying datastore's supported types were compiled into DVID...\n")
	err = datasets.VerifyCompiledTypes()
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Data are not fully supported by this DVID server: %s", err.Error()),
			ErrorDatatypeUnavailable,
		}
		return
	}

	fmt.Printf("\nDatastoreService successfully opened: %s\n", path)
	s = &Service{datasets, engine, kvDB, kvSetter, kvGetter, gengine, gDB, gSetter, gGetter}
	return
}

// StorageEngine returns a a key-value database interface.
func (s *Service) StorageEngine() storage.Engine {
	return s.engine
}

// KeyValueDB returns a a key-value database interface.
func (s *Service) KeyValueDB() (storage.KeyValueDB, error) {
	return s.kvDB, nil
}

// KeyValueGetter returns a a key-value getter interface.
func (s *Service) KeyValueGetter() (storage.KeyValueGetter, error) {
	return s.kvGetter, nil
}

// KeyValueSetter returns a a key-value setter interface.
func (s *Service) KeyValueSetter() (storage.KeyValueSetter, error) {
	return s.kvSetter, nil
}

// GraphStorageEngine returns a graph database interface.
func (s *Service) GraphStorageEngine() storage.Engine {
	return s.graphengine
}

// GraphDB returns a graph database interface.
func (s *Service) GraphDB() (storage.GraphDB, error) {
	return s.gDB, nil
}

// GraphGetter returns a GraphDB getter interface.
func (s *Service) GraphGetter() (storage.GraphGetter, error) {
	return s.gGetter, nil
}

// GraphSetter returns a GraphDB setter interface.
func (s *Service) GraphSetter() (storage.GraphSetter, error) {
	return s.gSetter, nil
}

// Batcher returns an interface that can create a new batch write.
func (s *Service) Batcher() (db storage.Batcher, err error) {
	var ok bool
	db, ok = s.kvSetter.(storage.Batcher)
	if !ok {
		err = fmt.Errorf("DVID key-value store does not support batch write")
	}
	return
}

// Shutdown closes a DVID datastore.
func (s *Service) Shutdown() {
	s.engine.Close()
}

// DatasetsListJSON returns JSON of a list of datasets.
func (s *Service) DatasetsListJSON() (stringJSON string, err error) {
	if s.Datasets == nil {
		stringJSON = "{}"
		return
	}
	var bytesJSON []byte
	bytesJSON, err = s.Datasets.MarshalJSON()
	if err != nil {
		return
	}
	return string(bytesJSON), nil
}

// DatasetsAllJSON returns JSON of a list of datasets.
func (s *Service) DatasetsAllJSON() (stringJSON string, err error) {
	if s.Datasets == nil {
		stringJSON = "{}"
		return
	}
	var bytesJSON []byte
	bytesJSON, err = s.Datasets.AllJSON()
	if err != nil {
		return
	}
	return string(bytesJSON), nil
}

// DatasetJSON returns JSON for a particular dataset referenced by a uuid.
func (s *Service) DatasetJSON(root dvid.UUID) (stringJSON string, err error) {
	if s.Datasets == nil {
		stringJSON = "{}"
		return
	}
	dataset, err := s.Datasets.DatasetFromUUID(root)
	if err != nil {
		return "{}", err
	}
	stringJSON, err = dataset.JSONString()
	return
}

// NOTE: Alterations of Datasets should invoke persistence to the key-value database.
// All interaction with datasets at the datastore.Service level should be using
// opaque UUID or the shortened datasetID.

// NewDataset creates a new dataset.
func (s *Service) NewDataset() (root dvid.UUID, datasetID dvid.DatasetLocalID, err error) {
	if s.Datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, err = s.Datasets.newDataset()
	if err != nil {
		return
	}
	err = s.Datasets.Put(s.kvSetter) // Need to persist change to list of Dataset
	if err != nil {
		return
	}
	err = dataset.Put(s.kvSetter)
	root = dataset.Root
	datasetID = dataset.DatasetID
	return
}

// NewVersions creates a new version (child node) off of a LOCKED parent node.
// Will return an error if the parent node has not been locked.
func (s *Service) NewVersion(parent dvid.UUID) (u dvid.UUID, err error) {
	if s.Datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, u, err = s.Datasets.newChild(parent)
	if err != nil {
		return
	}
	err = dataset.Put(s.kvSetter)
	return
}

// NewData adds data of given name and type to a dataset specified by a UUID.
func (s *Service) NewData(u dvid.UUID, typename dvid.TypeString, dataname dvid.DataString, config dvid.Config) error {
	if s.Datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	dataset, err := s.Datasets.DatasetFromUUID(u)
	if err != nil {
		return err
	}
	err = dataset.newData(dataname, typename, config)
	if err != nil {
		return err
	}
	return dataset.Put(s.kvSetter)
}

// ModifyData modifies data of given name in dataset specified by a UUID.
func (s *Service) ModifyData(u dvid.UUID, dataname dvid.DataString, config dvid.Config) error {
	if s.Datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	dataset, err := s.Datasets.DatasetFromUUID(u)
	if err != nil {
		return err
	}
	err = dataset.modifyData(dataname, config)
	if err != nil {
		return err
	}
	return dataset.Put(s.kvSetter)
}

// Locks the node with the given UUID.
func (s *Service) Lock(u dvid.UUID) error {
	if s.Datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	dataset, err := s.Datasets.DatasetFromUUID(u)
	if err != nil {
		return err
	}
	err = dataset.Lock(u)
	if err != nil {
		return err
	}
	return dataset.Put(s.kvSetter)
}

// SaveDataset forces this service to persist the dataset with given UUID.
// It is useful when modifying datasets internally.
func (s *Service) SaveDataset(u dvid.UUID) error {
	if s.Datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	dataset, err := s.Datasets.DatasetFromUUID(u)
	if err != nil {
		return err
	}
	return dataset.Put(s.kvSetter)
}

// LocalIDFromUUID when supplied a UUID string, returns smaller sized local IDs that identify a
// dataset and a version.
func (s *Service) LocalIDFromUUID(u dvid.UUID) (dID dvid.DatasetLocalID, vID dvid.VersionLocalID, err error) {
	if s.Datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, err = s.Datasets.DatasetFromUUID(u)
	if err != nil {
		return
	}
	dID = dataset.DatasetID
	var found bool
	vID, found = dataset.VersionMap[u]
	if !found {
		err = fmt.Errorf("UUID (%s) not found in dataset", u)
	}
	return
}

// NodeIDFromString when supplied a UUID string, returns the matched UUID as well as
// more compact local IDs that identify the dataset and a version.  Partial matches
// are allowed, similar to DatasetFromString.
func (s *Service) NodeIDFromString(str string) (u dvid.UUID, dID dvid.DatasetLocalID,
	vID dvid.VersionLocalID, err error) {

	if s.Datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, u, err = s.Datasets.DatasetFromString(str)
	if err != nil {
		return
	}
	dID = dataset.DatasetID
	vID = dataset.VersionMap[u]
	return
}

// SupportedDataChart returns a chart (names/urls) of data referenced by this datastore
func (s *Service) SupportedDataChart() string {
	text := CompiledTypeChart()
	text += "Data currently referenced within this DVID datastore:\n\n"
	text += s.DataChart()
	return text
}

// About returns a chart of the code versions of compile-time DVID datastore
// and the runtime data types.
func (s *Service) About() string {
	var text string
	writeLine := func(name dvid.TypeString, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Storage backend", storage.Version)
	if s.Datasets != nil {
		for _, dtype := range s.Datasets.Datatypes() {
			writeLine(dtype.DatatypeName(), dtype.DatatypeVersion())
		}
	}
	return text
}

// TypesJSON returns the components and versions of DVID software available
// in this DVID server.
func (s *Service) TypesJSON() (jsonStr string, err error) {
	data := make(map[dvid.TypeString]string)
	for _, datatype := range CompiledTypes {
		data[datatype.DatatypeName()] = string(datatype.DatatypeUrl())
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// CurrentTypesJSON returns the components and versions of DVID software associated
// with the current datasets in the service.
func (s *Service) CurrentTypesJSON() (jsonStr string, err error) {
	data := make(map[dvid.TypeString]string)
	if s.Datasets != nil {
		for _, dtype := range s.Datasets.Datatypes() {
			data[dtype.DatatypeName()] = dtype.DatatypeVersion()
		}
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// DataChart returns a text chart of data names and their types for this DVID server.
func (s *Service) DataChart() string {
	var text string
	if s.Datasets == nil || len(s.Datasets.list) == 0 {
		return "  No datasets have been added to this datastore.\n"
	}
	writeLine := func(name dvid.DataString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	for num, dset := range s.Datasets.list {
		text += fmt.Sprintf("\nDataset %d (UUID = %s):\n\n", num+1, dset.Root)
		writeLine("Name", "Type Name", "Url")
		for name, data := range dset.DataMap {
			writeLine(name, string(data.DatatypeName())+" ("+data.DatatypeVersion()+")",
				data.DatatypeUrl())
		}
	}
	return text
}
