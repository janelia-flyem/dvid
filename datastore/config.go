/*
	This file handles configuration info for a DVID datastore and its serialization
	to files as well as the backend key/value datastore itself.
*/

package datastore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
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

// MarshalJSON fulfills the Marshaler interface for JSON output.
func (rc *runtimeConfig) MarshalJSON() (b []byte, err error) {
	if rc == nil {
		b = []byte("{}")
	} else {
		b, err = json.Marshal(rc.dataNames)
	}
	return
}

// UnmarshallJSON fulfills the Unmarshaler interface for JSON input.
func (rc *runtimeConfig) UnmarshalJSON(jsonText []byte) error {
	if rc == nil {
		return fmt.Errorf("Error, can't unmarshall to a nil runtimeConfig!")
	}
	err := json.Unmarshal(jsonText, rc.dataNames)
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
	datasets := make(map[DataSetString]TypeInfo)
	for name, dtype := range s.dataNames {
		datasets[name] = TypeInfo{
			Name:        dtype.TypeName(),
			Url:         string(dtype.TypeUrl()),
			Version:     dtype.TypeVersion(),
			BlockSize:   dtype.BlockSize(),
			IndexScheme: dtype.IndexScheme().String(),
			Help:        dtype.Help(),
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
