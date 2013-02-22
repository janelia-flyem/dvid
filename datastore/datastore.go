package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/keyvalue"
)

const (
	Version = "0.1"

	// ConfigFilename is name of JSON file with datastore configuration data
	// just for human inspection.
	ConfigFilename = "dvid-config.json"
)

// configData holds the essential configuration data for a datastore instance and therefore
// is private to package datastore.
type configData struct {
	// Volume extents
	VolumeMax dvid.VoxelCoord

	// Relative resolution of voxels in volume
	VoxelRes dvid.VoxelResolution

	// Units of resolution, e.g., "nanometers"
	VoxelResUnits dvid.VoxelResolutionUnits

	// Block size
	BlockMax dvid.VoxelCoord

	// Spatial indexing scheme
	Indexing SpatialIndexScheme

	// Data types supported
	Datatypes []Datatype
}

// Init creates a key-value datastore using default parameters.  Datastore 
// configuration is stored in the datastore and in a human-readable JSON file
// in the datastore directory.
func Init(directory string, configFile string, create bool) (uuid UUID) {
	var config *configData
	if configFile == "" {
		config = promptConfig()
	} else {
		config = readJsonConfig(configFile)
	}
	err := config.VerifyCompiledTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * dvid.Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		log.Fatalf("Error opening datastore (%s): %s\n", directory, err.Error())
	}

	// Store the configuration in a human-readable JSON
	filename := filepath.Join(directory, ConfigFilename)
	config.writeJsonConfig(filename)

	// Put config data
	err = config.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing configuration to datastore: %s\n", err.Error())
	}

	// Put cached global variables
	uuid = NewUUID()
	globals := cachedData{
		Head:  uuid,
		Uuids: map[string]int{string(uuid): 1},
	}
	err = globals.put(kvdb{db})
	if err != nil {
		log.Fatalf("Error writing global cache to datastore: %s\n", err.Error())
	}

	db.Close()
	return
}

// BlockNumVoxels returns the # of voxels within a block.  Block size should
// be immutable during the course of a datastore's lifetime.
func (config *configData) BlockNumVoxels() int {
	return int(config.BlockMax[0] * config.BlockMax[1] * config.BlockMax[2])
}

// VerifyCompiledTypes will return an error if any required data type in the datastore 
// configuration was not compiled into DVID executable.
func (config *configData) VerifyCompiledTypes() error {
	if SupportedTypes == nil {
		return fmt.Errorf("DVID was not compiled with any data type support!")
	}
	var errMsg string
	for _, datatype := range config.Datatypes {
		_, found := SupportedTypes[datatype.Url]
		if !found {
			errMsg += fmt.Sprintf("DVID was not compiled with support for data type %s [%s]\n",
				datatype.Name, datatype.Url)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

// SupportedTypeChart returns a chart (names/urls) of data types supported by this datastore
func (config *configData) SupportedTypeChart() string {
	var text string = "\nData types supported by this DVID datastore:\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range config.Datatypes {
		writeLine(datatype.Name, datatype.Url)
	}
	text += "\nUse the '<data type name> help' command for type-specific help.\n"
	return text
}

// IsSupportedType returns true if given data type name is supported by this datastore
func (config *configData) IsSupportedType(name string) bool {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			return true
		}
	}
	return false
}

func (config *configData) GetSupportedTypeUrl(name string) (url UrlString, err error) {
	for _, datatype := range config.Datatypes {
		if name == datatype.Name {
			url = datatype.Url
			return
		}
	}
	err = fmt.Errorf("data type '%s' not supported by opened datastore.", name)
	return
}

// promptConfig prompts the user to enter configuration data.
func promptConfig() (config *configData) {
	config = new(configData)
	pVolumeMax := &(config.VolumeMax)
	pVolumeMax.Prompt("# voxels", "250")
	pVoxelRes := &(config.VoxelRes)
	pVoxelRes.Prompt("Voxel resolution", "10.0")
	config.VoxelResUnits = dvid.VoxelResolutionUnits(dvid.Prompt("Units of resolution", "nanometer"))
	pBlockMax := &(config.BlockMax)
	pBlockMax.Prompt("Size of blocks", "16")

	config.Indexing = SIndexZXY
	config.Datatypes = DefaultDataTypes
	return
}

// readJsonConfig reads in a Config from a JSON file with errors leading to
// termination of program.
func readJsonConfig(filename string) (config *configData) {
	var file *os.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: Failed to open JSON config file: %s [%s]\n",
			filename, err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	config = new(configData)
	err = dec.Decode(&config)
	config.Indexing = SIndexZXY
	if err == io.EOF {
		log.Fatalf("Error: No data in JSON config file: %s [%s]\n", filename, err)
	} else if err != nil {
		log.Fatalf("Error: Reading JSON config file (%s): %s\n", filename, err)
	}
	return
}

// writeJsonConfig writes a Config to a JSON file.
func (config *configData) writeJsonConfig(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Error: Failed to create JSON config file: %s [%s]\n",
			filename, err)
	}
	m, err := json.Marshal(config)
	if err != nil {
		log.Fatalf("Error in writing JSON config file: %s [%s]\n",
			filename, err)
	}
	var buf bytes.Buffer
	json.Indent(&buf, m, "", "    ")
	buf.WriteTo(file)
	file.Close()
}

// kvdb is a private wrapper around the underlying key/value datastore and provides
// a number of functions for DVID-specific types.
type kvdb struct {
	keyvalue.KeyValueDB
}

// VersionService encapsulates both an open DVID datastore, the version that
// should be operated upon, and a block-level cache to improve conversion to
// the datastore's block values.  It is the main unit of service for operations.
type VersionService struct {
	Service

	// The datastore-specific UUID index for the version we are operating upon
	uuidNum int

	// Caching for blocks
	blocks blockCache
}

func MakeVersionService(service *Service, uuidNum int) *VersionService {
	return &VersionService{
		*service,
		uuidNum,
		nil,
	}
}

func (vs *VersionService) InitializeBlockCache(capacity ...int) {
	if vs.blocks == nil {
		switch len(capacity) {
		case 0:
			vs.blocks = make(blockCache)
			log.Println("datastore: Block cache map created with default size.")
		case 1:
			vs.blocks = make(blockCache, capacity[0])
			log.Printf("datastore: Block cache map created with %d entries\n", capacity[0])
		default:
			log.Printf("datastore: Error!  InitializeBlockCache(%s) called incorrectly!",
				capacity)
			vs.blocks = make(blockCache)
			log.Println("datastore: Block cache map created with default size.")
		}
	}
}

// PrepareBlock makes sure the cache has a block at a given spatial index and
// is ready for operations on its data.
func (vs *VersionService) InitializeBlock(si SpatialIndex, bytesPerVoxel int) {
	// Make sure the block cache exists.
	vs.InitializeBlockCache()

	// Make sure the block for given spatial index exists.
	block, found := vs.blocks[si]
	if !found {
		block.offset = vs.OffsetToBlock(si)
		numBytes := vs.BlockNumVoxels() * bytesPerVoxel
		block.data = make([]byte, numBytes, numBytes)
		block.dirty = make([]bool, vs.BlockNumVoxels(), vs.BlockNumVoxels())
		vs.blocks[si] = block
		log.Printf("InitializeBlock(%s).  Blocks in cache: %d\n", si, len(vs.blocks))
	}
}

// Write all subvolume data that overlaps the block with the given spatial index.
func (vs *VersionService) WriteBlock(si SpatialIndex, subvol *dvid.Subvolume) {
	// Get min and max voxel coordinates of this block
	minBlockVoxel := vs.OffsetToBlock(si)
	maxBlockVoxel := minBlockVoxel.AddSize(vs.BlockMax)

	// Get min and max voxel coordinates of the entire subvolume
	minSubvolVoxel := subvol.Offset
	maxSubvolVoxel := minSubvolVoxel.AddSize(subvol.Size)

	// Bound the start and end voxel coordinates of the subvolume by the block limits.
	start := minSubvolVoxel.BoundMin(minBlockVoxel)
	end := maxSubvolVoxel.BoundMax(maxBlockVoxel)

	// Get the block corresponding to the spatial index.
	block := vs.blocks[si]

	// Traverse the data from start to end voxel coordinates and write to the block.
	// TODO -- Optimize the inner loop and see if we actually get faster :)  Currently,
	// the code tries to make it very clear what transformations are happening.
	for z := start[2]; z <= end[2]; z++ {
		for y := start[1]; y <= end[1]; y++ {
			for x := start[0]; x <= end[0]; x++ {
				coord := dvid.VoxelCoord{x, y, z}
				i := subvol.VoxelCoordToDataIndex(coord)
				b := vs.VoxelCoordToBlockIndex(coord)
				bI := b * subvol.BytesPerVoxel // index into block.data
				if !block.dirty[b] {
					block.numDirty++
				}
				block.dirty[b] = true
				for n := 0; n < subvol.BytesPerVoxel; n++ {
					block.data[bI+n] = subvol.Data[i+n]
				}
			}
		}
	}
	vs.blocks[si] = block
}

// Service encapsulates an open DVID datastore, available for operations.
type Service struct {
	// The datastore configuration for this open DVID datastore,
	// including the supported data types
	configData

	// The globals cache
	cachedData

	// The underlying leveldb which is private since we want to create an object
	// interface (e.g., cache object or UUID map) and hide DVID-specific keys.
	kvdb
}

// Open opens a DVID datastore at the given directory and returns
// a Service that allows operations on that datastore.
func Open(directory string) (service *Service, err error) {
	// Open the datastore
	dbOptions := keyvalue.GetKeyValueOptions()
	dbOptions.SetLRUCacheSize(100 * dvid.Mega)
	dbOptions.SetBloomFilterBitsPerKey(10)
	create := false
	db, err := keyvalue.OpenLeveldb(directory, create, dbOptions)
	if err != nil {
		err = fmt.Errorf("Error opening datastore (%s): %s\n", directory, err)
		return
	}

	// Read this datastore's configuration
	config := new(configData)
	err = config.get(kvdb{db})
	if err != nil {
		return
	}

	// Read this datastore's cached globals
	cache := new(cachedData)
	err = cache.get(kvdb{db})
	if err != nil {
		return
	}

	service = &Service{*config, *cache, kvdb{db}}
	return
}

// Close closes a DVID datastore.
func (service *Service) Close() {
	service.Close()
}

// GetUuidNumFromString returns a UUID index given its string representation.  
// Partial matches are accepted as long as they are unique for a datastore.  So if
// a datastore has nodes with UUID strings 3FA22..., 7CD11..., and 836EE..., 
// we can still find a match even if given the minimum 3 letters.  (We don't
// allow UUID strings of less than 3 letters just to prevent mistakes.)
func (service *Service) GetUuidFromString(s string) (uuidNum int, err error) {
	// Valid hex decode requires pairs of characters
	if len(s)%2 == 0 {
		var uuid UUID
		uuid, err = UUIDfromString(s)
		if err != nil {
			return
		}
		var found bool
		uuidNum, found = service.Uuids[string(uuid)]
		if found {
			return
		}
	}

	// Use the hex string version to try to find a partial match
	var lastMatch int
	numMatches := 0
	for key, uuidNum := range service.Uuids {
		hexString := UUID(key).String()
		fmt.Printf("Checking %s against %s\n", s, hexString)
		if strings.HasPrefix(hexString, s) {
			numMatches++
			lastMatch = uuidNum
		}
	}
	if numMatches > 1 {
		err = fmt.Errorf("More than one UUID matches %s!", s)
	} else if numMatches == 0 {
		err = fmt.Errorf("Could not find UUID with partial match to %s!", s)
	} else {
		uuidNum = lastMatch
	}
	return
}

// GetHeadUuid returns the UUID of the HEAD node, i.e., the version that is
// considered the most current.
func (service *Service) GetHeadUuid() UUID {
	return service.Head
}
