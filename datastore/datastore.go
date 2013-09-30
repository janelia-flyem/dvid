/*
	This file provides the highest-level view of the datastore via a Service.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	"time"

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
func Init(directory string, create bool) error {
	fmt.Println("\nInitializing datastore at", directory)

	// Initialize the backend database
	dbOptions := storage.Options{}
	db, err := storage.NewStore(directory, create, &dbOptions)
	defer db.Close()
	if err != nil {
		return fmt.Errorf("Error initializing datastore (%s): %s\n", directory, err.Error())
	}

	// Put empty Datasets
	datasets := new(Datasets)
	err = datasets.Put(db)
	return err
}

// Service couples an open DVID storage engine and DVID datasets.  If more than one
// storage engine is used by a DVID server, e.g., polyglot persistence where graphs
// are managed by a graph database and key-value by a key-value database, this would
// be the level at which the storage engines are integrated.
type Service struct {
	datasets *Datasets

	// The backend storage which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	db storage.DataHandler

	// Channel for marking the datasets as dirty.
	DirtyDatasets chan bool
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
	dbOptions := storage.Options{}
	create := false
	db, err := storage.NewStore(path, create, &dbOptions)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error opening datastore (%s): %s", path, err.Error()),
			ErrorOpening,
		}
		return
	}

	// Read this datastore's configuration
	datasets := new(Datasets)
	err = datasets.Get(db)
	if err != nil {
		openErr = &OpenError{
			fmt.Errorf("Error reading datasets information: %s", err.Error()),
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
	s = &Service{datasets, db, nil}
	s.DirtyDatasets = make(chan bool, 10000)

	// Start a goroutine that periodically persists datasets.
	go s.periodicallySave()

	return
}

// Shutdown closes a DVID datastore.
func (s *Service) Shutdown() {
	if s.DirtyDatasets != nil {
		s.DirtyDatasets <- false // Signal dataset persistence goroutine to stop.
	}
	s.datasets.Put(s.db)
	s.db.Close()
}

func drain(c chan bool) (quit bool) {
	for {
		select {
		case noquit := <-c:
			if !noquit {
				return true
			}
			continue
		default:
			return false
		}
	}
}

// Saves the datasets at most once per second.
// TODO -- Separate datasets into their own key/value pair once we start
// managing larger # of datasets.
func (s *Service) periodicallySave() {
	saveTick := time.Tick(1 * time.Second)
	for {
		<-saveTick
		if len(s.DirtyDatasets) > 0 {
			quit := drain(s.DirtyDatasets)
			if quit {
				break
			}
			dvid.Fmt(dvid.Debug, "Saving datasets after change...\n")
			s.datasets.Put(s.db)
		}
	}
}

// DatasetsJSON returns a JSON-encoded string of exportable datasets information.
func (s *Service) DatasetsJSON() (stringJSON string, err error) {
	if s.datasets == nil {
		stringJSON = "{}"
		return
	}
	return s.datasets.StringJSON()
}

// NOTE: Alterations of Datasets should invoke persistence to the key-value database.
// All interaction with datasets at the datastore.Service level should be using
// opaque UUID or the shortened datasetID.

// NewDataset creates a new dataset.
func (s *Service) NewDataset() (root UUID, datasetID dvid.LocalID32, err error) {
	if s.datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, err = s.datasets.newDataset()
	if err != nil {
		return
	}
	root = dataset.Root
	datasetID = dataset.DatasetID
	s.DirtyDatasets <- true
	return
}

// NewVersions creates a new version (child node) off of a LOCKED parent node.
// Will return an error if the parent node has not been locked.
func (s *Service) NewVersion(parent UUID) (u UUID, err error) {
	if s.datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	u, err = s.datasets.newChild(parent)
	if err != nil {
		return
	}
	s.DirtyDatasets <- true
	return
}

// NewData adds data of given name and type to a dataset specified by a UUID.
func (s *Service) NewData(u UUID, typename, dataname string, versioned bool) error {
	if s.datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	config := dvid.Config{"versioned": versioned}
	err := s.datasets.newData(u, DataString(dataname), typename, config)
	if err != nil {
		return err
	}
	s.DirtyDatasets <- true
	return nil
}

// Locks the node with the given UUID.
func (s *Service) Lock(u UUID) error {
	if s.datasets == nil {
		return fmt.Errorf("Datastore service has no datasets available")
	}
	dataset, err := s.datasets.DatasetFromUUID(u)
	if err != nil {
		return err
	}
	return dataset.Lock(u)
}

// LocalIDFromUUID when supplied a UUID string, returns smaller sized local IDs that identify a
// dataset and a version.
func (s *Service) LocalIDFromUUID(u UUID) (datasetID dvid.LocalID32, versionID dvid.LocalID, err error) {
	if s.datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	var dataset *Dataset
	dataset, err = s.datasets.DatasetFromUUID(u)
	if err != nil {
		return
	}
	datasetID = dataset.DatasetID
	var found bool
	versionID, found = dataset.VersionMap[u]
	if !found {
		err = fmt.Errorf("UUID (%s) not found in dataset", u)
	}
	return
}

// NodeIDFromString when supplied a UUID string, returns the matched UUID as well as
// more compact local IDs that identify the dataset and a version.  Partial matches
// are allowed, similar to DatasetFromString.
func (s *Service) NodeIDFromString(str string) (globalID UUID, datasetID dvid.LocalID32,
	versionID dvid.LocalID, err error) {

	if s.datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	dataset, u, err := s.datasets.DatasetFromString(str)
	if err != nil {
		return
	}
	globalID = u
	datasetID = dataset.DatasetID
	versionID = dataset.VersionMap[u]
	return
}

// DataService returns a service for data of a given name and version
func (s *Service) DataService(u UUID, name DataString) (dataservice DataService, err error) {
	if s.datasets == nil {
		err = fmt.Errorf("Datastore service has no datasets available")
		return
	}
	return s.datasets.DataService(u, name)
}

// KeyValueDB returns a a key-value database interface.
func (s *Service) KeyValueDB() storage.KeyValueDB {
	return s.db
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
	text += "Data currently referenced within this DVID datastore:\n\n"
	text += s.DataChart()
	return text
}

// About returns a chart of the code versions of compile-time DVID datastore
// and the runtime data types.
func (s *Service) About() string {
	var text string
	writeLine := func(name, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Storage backend", storage.Version)
	if s.datasets != nil {
		for _, dtype := range s.datasets.Datatypes() {
			writeLine(dtype.DatatypeName(), dtype.DatatypeVersion())
		}
	}
	return text
}

// AboutJSON returns the components and versions of DVID software.
func (s *Service) AboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"DVID datastore":   Version,
		"Storage backend":  storage.Version,
		"Cores":            fmt.Sprintf("%d", dvid.NumCPU),
		"Maximum handlers": fmt.Sprintf("%d", dvid.MaxHandlers),
	}
	if s.datasets != nil {
		for _, dtype := range s.datasets.Datatypes() {
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
	if s.datasets == nil || len(s.datasets.Datasets) == 0 {
		return "  No datasets have been added to this datastore.\n"
	}
	writeLine := func(name DataString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	for num, dset := range s.datasets.Datasets {
		text += fmt.Sprintf("\nDataset %d (UUID = %s):\n\n", num+1, dset.Root)
		writeLine("Name", "Type Name", "Url")
		for name, data := range dset.DataMap {
			writeLine(name, data.DatatypeName()+" ("+data.DatatypeVersion()+")",
				data.DatatypeUrl())
		}
	}
	return text
}
