/*
	This file handles configuration info for a DVID datastore and its serialization
	to files as well as the keys to be used to store values in the key/value store.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	_ "log"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// ConfigFilename is name of a file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

var (
	// KeyConfig is the key for a DVID configuration
	KeyConfig = storage.Key{
		Dataset: storage.KeyDatasetGlobal,
		Version: storage.KeyVersionGlobal,
		Index:   []byte{0x01},
	}

	// KeyVersionDAG is the key for a Version DAG.
	KeyVersionDAG = storage.Key{
		Dataset: storage.KeyDatasetGlobal,
		Version: storage.KeyVersionGlobal,
		Index:   []byte{0x02},
	}
)

// runtimeConfig holds editable configuration data for a datastore instance.
type runtimeConfig struct {
	// Data supported.  This is a map of a user-defined name like "fib_data" with
	// the supporting data type "grayscale8"
	Datasets map[DatasetString]DatasetService

	// Always incremented counter that provides local dataset ID so we can use
	// smaller # of bytes (dvid.LocalID size) instead of full identifier.
	NewLocalID dvid.LocalID
}

// Get retrieves a configuration from a KeyValueDB.
func (config *runtimeConfig) Get(db storage.KeyValueDB) (err error) {
	// Get data
	var data []byte
	data, err = db.Get(KeyConfig)
	if err != nil {
		return
	}

	// Deserialize into object
	err = config.Deserialize(dvid.Serialization(data))
	return
}

// Put stores a configuration into a KeyValueDB.
func (config *runtimeConfig) Put(db storage.KeyValueDB) (err error) {
	// Get serialization
	var serialization dvid.Serialization
	serialization, err = config.Serialize()

	// Put data
	return db.Put(KeyConfig, []byte(serialization))
}

// Serialize returns a serialization of configuration with Snappy compression and
// CRC32 checksum.
func (config *runtimeConfig) Serialize() (s dvid.Serialization, err error) {
	return dvid.Serialize(config, dvid.Snappy, dvid.CRC32)
}

// Deserialize converts a serialization to a runtime configuration and checks to
// make sure the data types are available.
// TODO -- Handle versions of data types.
func (config *runtimeConfig) Deserialize(s dvid.Serialization) (err error) {
	var obj interface{}
	obj, err = dvid.Deserialize(s)
	if err != nil {
		return
	}
	var ok bool
	if config, ok = obj.(*runtimeConfig); !ok {
		err = fmt.Errorf("Deserialize() can't make a runtimeConfig!")
	} else {
		err = config.VerifyCompiledTypes()
	}
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
	for name, datatype := range config.Datasets {
		_, found := CompiledTypes[datatype.DatatypeUrl()]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for %s, data type %s [%s]\n",
				name, datatype.DatatypeName(), datatype.DatatypeUrl())
		}
	}
	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}
	return nil
}

// DataChart returns a chart of data set names and their types for this runtime configuration.
func (config *runtimeConfig) DataChart() string {
	var text string
	if len(config.Datasets) == 0 {
		return "  No data sets have been added to this datastore.\n  Use 'dvid dataset ...'"
	}
	writeLine := func(name DatasetString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	writeLine("Name", "Type Name", "Url")
	for name, dtype := range config.Datasets {
		writeLine(name, dtype.DatatypeName()+" ("+dtype.DatatypeVersion()+")", dtype.DatatypeUrl())
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
	writeLine("Leveldb", storage.Version)
	for _, dtype := range config.Datasets {
		writeLine(dtype.DatatypeName(), dtype.DatatypeVersion())
	}
	return text
}

// AboutJSON returns the components and versions of DVID software.
func (config *runtimeConfig) AboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"DVID datastore":  Version,
		"Backend storage": storage.Version,
	}
	for _, datatype := range config.Datasets {
		data[datatype.DatatypeName()] = datatype.DatatypeVersion()
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

// TypeInfo contains data type information reformatted for easy consumption by clients.
type TypeInfo struct {
	Name    string
	Url     string
	Version string
	Help    string
}

// ConfigJSON returns configuration data in JSON format.
func (config *runtimeConfig) ConfigJSON() (jsonStr string, err error) {
	datasets := make(map[DatasetString]TypeInfo)
	for name, dtype := range config.Datasets {
		datasets[name] = TypeInfo{
			Name:    dtype.DatatypeName(),
			Url:     string(dtype.DatatypeUrl()),
			Version: dtype.DatatypeVersion(),
			Help:    dtype.Help(),
		}
	}
	data := struct {
		Datasets map[DatasetString]TypeInfo
	}{
		datasets,
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}
