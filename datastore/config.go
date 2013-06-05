/*
	This file handles configuration info for a DVID datastore and its serialization
	to files as well as the keys to be used to store values in the key/value store.
*/

package datastore

import (
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
	datasets map[DatasetString]DatasetService

	// Always incremented counter that provides local dataset ID so we can use
	// smaller # of bytes (dvid.LocalID size) instead of full identifier.
	newDatasetID dvid.LocalID
}

// rconfig is a local struct used for explicit export of runtime configurations
// that uses more global identification of data types via URL.
type rconfig struct {
	TypeUrl   UrlString
	DatasetID dvid.LocalID
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
	var buf bytes.Buffer

	c := make(map[DatasetString]rconfig)
	for key, value := range config.datasets {
		c[key] = rconfig{value.TypeService.DatatypeUrl(), value.DatasetLocalID()}
	}
	return dvid.Serialize(c, dvid.Snappy, dvid.CRC32)
}

// Deserialize converts a serialization to a runtime configuration.
func (config *runtimeConfig) Deserialize(s dvid.Serialization) (err error) {
	var c map[DatasetString]rconfig
	c, err = dvid.Deserialize(s)
	if err != nil {
		return
	}
	for key, value := range c {
		dtype, found := CompiledTypes[value.TypeUrl]
		if !found {
			return fmt.Errorf("Data set in datastore no longer present in DVID executable: %s",
				value.TypeUrl)
		}
		config.datasets[key] = datastoreType{dtype, value.DataIndexBytes}
	}
	return
}

// MarshalJSON fulfills the Marshaler interface for JSON output.
func (rc *runtimeConfig) MarshalJSON() (b []byte, err error) {
	if rc == nil {
		b = []byte("{}")
	} else {
		b, err = json.Marshal(rc.datasets)
	}
	return
}

// UnmarshallJSON fulfills the Unmarshaler interface for JSON input.
func (rc *runtimeConfig) UnmarshalJSON(jsonText []byte) error {
	if rc == nil {
		return fmt.Errorf("Error, can't unmarshall to a nil runtimeConfig!")
	}
	err := json.Unmarshal(jsonText, rc.datasets)
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
	for _, datatype := range config.datasets {
		_, found := CompiledTypes[datatype.DatatypeUrl()]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.DatatypeName(), datatype.DatatypeUrl())
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
	if len(config.datasets) == 0 {
		return "  No data sets have been added to this datastore.\n  Use 'dvid dataset ...'"
	}
	writeLine := func(name DatasetString, version string, url UrlString) {
		text += fmt.Sprintf("%-15s  %-25s  %s\n", name, version, url)
	}
	writeLine("Name", "Type Name", "Url")
	for name, datatype := range config.datasets {
		writeLine(name, datatype.DatatypeName()+" ("+datatype.DatatypeVersion()+")", datatype.DatatypeUrl())
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
	for _, datatype := range config.datasets {
		writeLine(datatype.DatatypeName(), datatype.DatatypeVersion())
	}
	return text
}

// AboutJSON returns the components and versions of DVID software.
func (config *runtimeConfig) AboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"DVID datastore":  Version,
		"Backend storage": storage.Version,
	}
	for _, datatype := range config.datasets {
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
func (s *Service) ConfigJSON() (jsonStr string, err error) {
	datasets := make(map[DatasetString]TypeInfo)
	for name, dtype := range s.datasets {
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
