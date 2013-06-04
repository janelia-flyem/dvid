/*
	This file handles configuration info for a DVID datastore and its serialization
	to files as well as the backend key/value datastore itself.
*/

package datastore

import (
	"fmt"
	"log"

	"github.com/janelia-flyem/dvid/dvid"
	_ "github.com/janelia-flyem/dvid/storage"
)

const (
	// ConfigFilename is name of a file with datastore configuration data
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

// DatasetString is a string that is the name of a DVID data set. 
// This gets its own type for documentation and also provide static error checks
// to prevent conflation of type name from data set name.
type DatasetString string

// DatasetID is a unique id for a dataset within a DVID instance.
type DatasetID dvid.LocalID

// runtimeConfig holds editable configuration data for a datastore instance. 
type runtimeConfig struct {
	// Data supported.  This is a map of a user-defined name like "fib_data" with
	// the supporting data type "grayscale8"
	datasets map[DatasetString]Dataset
}

// rconfig is a local struct used for explicit export of runtime configurations
type rconfig struct {
	TypeUrl        UrlString
	DataIndexBytes []byte
}

func (config *runtimeConfig) Serialize() (s Serialization, err error) {
	c := make(map[DatasetString]rconfig)
	for key, value := range config.datasets {
		c[key] = rconfig{value.TypeService.DatatypeUrl(), value.dataIndexBytes}
	}
	return db.putValue(keyvalue.Key{keyFamilyGlobal, keyRuntimeConfig}, c)
}

func (config *runtimeConfig) get(db kvdb) error {
	config.datasets = map[DatasetString]datastoreType{}
	c := make(map[DatasetString]rconfig)
	err := db.getValue(keyvalue.Key{keyFamilyGlobal, keyRuntimeConfig}, &c)
	if err != nil {
		return err
	}
	for key, value := range c {
		dtype, found := CompiledTypes[value.TypeUrl]
		if !found {
			return fmt.Errorf("Data set in datastore no longer present in DVID executable: %s",
				value.TypeUrl)
		}
		config.datasets[key] = datastoreType{dtype, value.DataIndexBytes}
	}
	return nil
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

// initByPrompt prompts the user to enter configuration data.
func initByPrompt() (config *initConfig) {
	config = new(initConfig)
	pVolumeMax := &(config.VolumeMax)
	pVolumeMax.Prompt("# voxels", "250")
	pVoxelRes := &(config.VoxelRes)
	pVoxelRes.Prompt("Voxel resolution", "10.0")
	config.VoxelResUnits = dvid.VoxelResolutionUnits(dvid.Prompt("Units of resolution", "nanometer"))
	return
}

// initByJson reads in a Config from a JSON file with errors leading to
// termination of program.
func initByJson(filename string) (ic *initConfig, rc *runtimeConfig, err error) {
	json, err := dvid.ReadJSON(filename)
	if err != nil {
		return
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

// TypeInfo contains data type information reformatted for easy consumption by clients.
type TypeInfo struct {
	Name        string
	Url         string
	Version     string
	BlockSize   dvid.Point3d
	IndexScheme string
	Help        string
}

// ConfigJSON returns configuration data in JSON format.
func (s *Service) ConfigJSON() (jsonStr string, err error) {
	datasets := make(map[DatasetString]TypeInfo)
	for name, dtype := range s.datasets {
		datasets[name] = TypeInfo{
			Name:        dtype.DatatypeName(),
			Url:         string(dtype.DatatypeUrl()),
			Version:     dtype.DatatypeVersion(),
			BlockSize:   dtype.BlockSize(),
			IndexScheme: dtype.IndexScheme().String(),
			Help:        dtype.Help(),
		}
	}
	data := struct {
		Volume   dvid.Volume
		Datasets map[DatasetString]TypeInfo
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
