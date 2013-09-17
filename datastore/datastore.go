/*
	This file provides the highest-level view of the datastore via a Service.
*/

package datastore

import (
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

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	*Datasets

	// The backend storage which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	db storage.DataHandler
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
	s = &Service{datasets, db}
	return
}

// Shutdown closes a DVID datastore.
func (s *Service) Shutdown() {
	s.StopChunkHandlers()

	// Close the backend database
	s.db.Close()
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
