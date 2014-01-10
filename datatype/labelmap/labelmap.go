/*
	Package labelmap implements DVID support for label->label mapping including
	spatial index tracking.
*/
package labelmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labels64"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/labelmap"
)

const HelpMessage = `
API for 'labelmap' datatype (github.com/janelia-flyem/dvid/datatype/labelmap)
=============================================================================

Command-line:

$ dvid dataset <UUID> new labelmap <data name> <settings...>

	Adds newly named labelmap data to dataset with specified UUID.

	Example:

	$ dvid dataset 3f8c new labelmap sp2body Labels=mylabels

    Arguments:

    UUID             Hexidecimal string with enough characters to uniquely identify a version node.
    data name        Name of data to create, e.g., "sp2body"
    settings         Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Labels           Name of labels64 data for which this is a label mapping. (required)
    Versioned        "true" or "false" (default)

$ dvid node <UUID> <data name> load raveler <superpixel-to-segment filename> <segment-to-body filename>

    Loads a superpixel-to-body mapping using two Raveler-formatted text files.

    Example: 

    $ dvid node 3f8c sp2body load raveler superpixel_to_segment_map.txt segment_to_body_map.txt

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
	
	
    ------------------

HTTP API (Level 2 REST):

Note that browsers support HTTP PUT and DELETE via javascript but only GET/POST are
included in HTML specs.  For ease of use in constructing clients, HTTP POST is used
to create or modify resources in an idempotent fashion.

GET  /api/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  /api/node/<UUID>/<data name>/info
POST /api/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET /api/node/3f8c/stuff/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.

GET /api/node/<UUID>/<data name>/sparsevol/<mapped label>

	Returns a sparse volume with voxels of the given forward label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Payload descriptor:
	               Bit 0 (LSB) - 8-bit grayscale
	               Bit 1 - 16-bit grayscale
	               Bit 2 - 16-bit normal
	               ...
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Voxels [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
			  ...
	        int32   Length of run
	        bytes   Optional payload dependent on first byte descriptor

GET /api/node/<UUID>/<data name>/sizerange/<min size>/<max size>

    Returns JSON list of labels that have # voxels that fall within the given range
    of sizes.
	
TODO:

GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts mapped label data.

    Example: 

    GET /api/node/3f8c/superpixels/0_1/512_256/0_0_100

    Returns an XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in PNG format.
    The example offset assumes the "superpixels" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream".

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png"
                  nD: uses default "octet-stream".

`

func init() {
	labelmap := NewDatatype()
	labelmap.DatatypeID = &datastore.DatatypeID{
		Name:    "labelmap",
		Url:     RepoUrl,
		Version: Version,
	}
	datastore.RegisterDatatype(labelmap)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
}

var (
	emptyValue          = []byte{}
	zeroSuperpixelBytes = make([]byte, 8, 8)
)

const MaxLabel = 0xFFFFFFFFFFFFFFFF

// Sparse Volume binary encoding payload descriptors.
const (
	PayloadBinary      byte = 0x00
	PayloadGrayscale8       = 0x01
	PayloadGrayscale16      = 0x02
	PayloadNormal16         = 0x04
)

type KeyType byte

// Label indexing is handled through a variety of key spaces that optimize
// throughput for access patterns required by our API.  For dcumentation purposes,
// consider the following key components:
//   a: original label
//   b: mapped label
//   s: spatial index (coordinate of a block)
//   v: # of voxels for a label
const (
	// KeyInverseMap have keys of form 'b+a'
	KeyInverseMap KeyType = iota

	// KeyForwardMap have keys of form 'a+b'
	// For superpixel->body maps, this key would be superpixel+body.
	KeyForwardMap

	// KeySpatialMap have keys of form 's+a+b'
	// They are useful for composing label maps for a spatial index.
	KeySpatialMap

	// KeyLabelSpatialMap have keys of form 'b+s' and have a sparse volume
	// encoding for its value. They are useful for returning all blocks
	// intersected by a label.
	KeyLabelSpatialMap

	// KeyLabelSizes have keys of form 'v+b'.
	// They allow rapid size range queries.
	KeyLabelSizes
)

func (t KeyType) String() string {
	switch t {
	case KeyInverseMap:
		return "Inverse Label Map"
	case KeyForwardMap:
		return "Forward Label Map"
	case KeySpatialMap:
		return "Spatial Index to Labels Map"
	case KeyLabelSpatialMap:
		return "Forward Label to Spatial Index Map"
	case KeyLabelSizes:
		return "Forward Label sorted by volume"
	default:
		return "Unknown Key Type"
	}
}

func getRelatedLabels(uuid dvid.UUID, name dvid.DataString) (*labels64.Data, error) {
	service := server.DatastoreService()
	source, err := service.DataService(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*labels64.Data)
	if !ok {
		return nil, fmt.Errorf("Can only use labelmap with labels64 data: %s", name)
	}
	return data, nil
}

// Datatype embeds the datastore's Datatype to create a unique type for labelmap functions.
type Datatype struct {
	datastore.Datatype
}

// NewDatatype returns a pointer to a new labelmap Datatype with default values set.
func NewDatatype() (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to new labelmap data with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, c dvid.Config) (datastore.DataService, error) {
	// Make sure we have valid labels64 data for mapping
	name, found, err := c.GetString("Labels")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make labelmap without valid 'Labels' setting.")
	}
	labelsName := dvid.DataString(name)

	basedata, err := datastore.NewDataService(id, dtype, c)
	if err != nil {
		return nil, err
	}
	return &Data{Data: basedata, Labels: labelsName}, nil
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data

	// Labels64 data that we will be mapping.
	Labels dvid.DataString

	// ZeroLocked is true if the zero label is locked and always mapped to zero.
	ZeroLocked bool

	// Ready is true if inverse map, forward map, and spatial queries are ready.
	Ready bool
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// --- DataService interface ---

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	case "load":
		if len(request.Command) < 6 {
			return fmt.Errorf("Poorly formatted load command.  See command-line help.")
		}
		switch request.Command[4] {
		case "raveler":
			return d.LoadRavelerMaps(request, reply)
		default:
			return fmt.Errorf("Cannot load unknown input file types '%s'", request.Command[3])
		}
	default:
		return d.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this data.
func (d *Data) DoHTTP(uuid dvid.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil

	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil

	case "sparsevol":
		// GET /api/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			err := fmt.Errorf("ERROR: DVID requires label ID to follow 'sparsevol' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		data, err := d.GetSparseVol(uuid, label)
		if err != nil {
			return err
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			return err
		}
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: sparsevol on label %d (%s)",
			r.Method, label, r.URL)

	case "sizerange":
		// GET /api/node/<UUID>/<data name>/sizerange/<min size>/<max size>
		if len(parts) < 6 {
			err := fmt.Errorf("ERROR: DVID requires min & max sizes to follow 'sizerange' command")
			server.BadRequest(w, r, err.Error())
			return err
		}
		minSize, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		maxSize, err := strconv.ParseUint(parts[5], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		jsonStr, err := d.GetSizeRange(uuid, minSize, maxSize)
		if err != nil {
			return err
		}
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, jsonStr)
		dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: get labels with volume > %d and < %d (%s)",
			r.Method, minSize, maxSize, r.URL)

	default:
	}

	return nil
}

func loadSegBodyMap(filename string) (map[uint64]uint64, error) {
	startTime := time.Now()
	dvid.Log(dvid.Normal, "Loading segment->body map: %s\n", filename)

	segmentToBodyMap := make(map[uint64]uint64, 100000)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Could not open segment->body map: %s", filename)
	}
	defer file.Close()
	linenum := 0
	lineReader := bufio.NewReader(file)
	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		storage.FileBytesRead <- len(line)
		var segment, body uint64
		if _, err := fmt.Sscanf(line, "%d %d", &segment, &body); err != nil {
			return nil, fmt.Errorf("Error loading segment->body map, line %d in %s", linenum, filename)
		}
		segmentToBodyMap[segment] = body
		linenum++
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Loaded Raveler segment->body file: %s", filename)
	return segmentToBodyMap, nil
}

func (d *Data) getHooks(uuid dvid.UUID) (storage.Engine, dvid.VersionLocalID, *labels64.Data, error) {
	service := server.DatastoreService()
	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		err = fmt.Errorf("Error in getting version ID from UUID '%s': %s\n", uuid, err.Error())
		return nil, 0, nil, err
	}

	db := server.StorageEngine()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return nil, versionID, nil, err
	}

	labels, err := getRelatedLabels(uuid, d.Labels)
	if err != nil {
		dvid.Log(dvid.Normal, "Error in getting related labels ('%s'): %s\n", d.Labels, err.Error())
		return nil, versionID, nil, err
	}
	return db, versionID, labels, nil
}

// NewForwardMapKey returns a datastore.DataKey that encodes a "label + mapping", where
// the label and mapping are both uint64.
func (d *Data) NewForwardMapKey(vID dvid.VersionLocalID, label []byte, mapping uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], label)
	binary.BigEndian.PutUint64(index[9:17], mapping)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewRavelerForwardMapKey returns a datastore.DataKey that encodes a "label + mapping", where
// the label is a uint64 with top 4 bytes encoding Z and least-significant 4 bytes encoding
// the superpixel ID.  Also, the zero label is reserved.
func (d *Data) NewRavelerForwardMapKey(vID dvid.VersionLocalID, z, spid uint32, body uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyForwardMap)
	copy(index[1:9], labels64.RavelerSuperpixelBytes(z, spid))
	binary.BigEndian.PutUint64(index[9:17], body)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewSpatialMapKey returns a datastore.DataKey that encodes a "spatial index + label + mapping".
func (d *Data) NewSpatialMapKey(vID dvid.VersionLocalID, block dvid.IndexZYX, label []byte,
	mapping uint64) *datastore.DataKey {

	index := make([]byte, 1+dvid.IndexZYXSize+8+8) // s + a + b
	index[0] = byte(KeySpatialMap)
	i := 1 + dvid.IndexZYXSize
	copy(index[1:i], block.Bytes())
	if label != nil {
		copy(index[i:i+8], label)
	}
	binary.BigEndian.PutUint64(index[i+8:i+16], mapping)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSpatialMapKey returns a datastore.DataKey that encodes a "label + spatial index", where
// the spatial index references a block that contains a voxel with the given label.
func (d *Data) NewLabelSpatialMapKey(vID dvid.VersionLocalID, label uint64, block dvid.IndexZYX) *datastore.DataKey {
	index := make([]byte, 1+8+dvid.IndexZYXSize)
	index[0] = byte(KeyLabelSpatialMap)
	binary.BigEndian.PutUint64(index[1:9], label)
	copy(index[9:9+dvid.IndexZYXSize], block.Bytes())
	return d.DataKey(vID, dvid.IndexBytes(index))
}

// NewLabelSizesKey returns a datastore.DataKey that encodes a "size + mapped label".
func (d *Data) NewLabelSizesKey(vID dvid.VersionLocalID, size, label uint64) *datastore.DataKey {
	index := make([]byte, 17)
	index[0] = byte(KeyLabelSizes)
	binary.BigEndian.PutUint64(index[1:9], size)
	binary.BigEndian.PutUint64(index[9:17], label)
	return d.DataKey(vID, dvid.IndexBytes(index))
}

type sparseOp struct {
	versionID dvid.VersionLocalID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// Adds retrieved RLE runs to an encoding.
func (d *Data) processLabelRuns(chunk *storage.Chunk) {
	op := chunk.Op.(*sparseOp)
	op.numBlocks++
	op.encoding = append(op.encoding, chunk.V...)
	op.numRuns += uint32(len(chunk.V) / 16)
	chunk.Wg.Done()
}

// Encodes RLE as bytes.
func encodeRuns(starts []dvid.Point3d, lengths []int32) ([]byte, error) {
	if starts == nil {
		return nil, fmt.Errorf("Cannot encode run with nil slice of start points")
	}
	if lengths == nil {
		return nil, fmt.Errorf("Cannot encode run with nil slice of lengths")
	}
	if len(starts) != len(lengths) {
		return nil, fmt.Errorf("Cannot encode runs with %d start points and %d lengths",
			len(starts), len(lengths))
	}
	buf := new(bytes.Buffer)
	for i, start := range starts {
		binary.Write(buf, binary.LittleEndian, start[0])
		binary.Write(buf, binary.LittleEndian, start[1])
		binary.Write(buf, binary.LittleEndian, start[2])
		binary.Write(buf, binary.LittleEndian, lengths[i])
	}
	return buf.Bytes(), nil
}

// Get the total number of voxels and runs in an encoded RLE.
func statsRuns(encoding []byte) (numVoxels, numRuns int32, err error) {
	if len(encoding)%16 != 0 {
		err = fmt.Errorf("RLE encoding doesn't have correct # bytes: %d", len(encoding))
		return
	}
	coord := make([]byte, 12)
	buf := bytes.NewBuffer(encoding)
	var n int
	var length int32
	for {
		n, err = buf.Read(coord)
		if n == 0 || err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		numRuns++
		err = binary.Read(buf, binary.LittleEndian, &length)
		if err != nil {
			return
		}
		numVoxels += length
	}
	return
}

// Runs asynchronously and assumes that sparse volumes per spatial indices are ordered
// by mapped label, i.e., we will get all data for body N before body N+1.  Exits when
// receives a nil in channel.
func (d *Data) computeSizes(sizeCh chan *storage.Chunk, db storage.Engine,
	versionID dvid.VersionLocalID, wg *sync.WaitGroup) {

	const BATCH_SIZE = 10000
	batcher, ok := db.(storage.Batcher)
	if !ok {
		dvid.Log(dvid.Normal, "Storage engine does not support Batch PUT.  Aborting\n")
		return
	}
	batch := batcher.NewBatch()

	defer func() {
		wg.Done()
		batch.Close()
	}()

	// Sequentially process all the sparse volume data for each label
	var curLabel, curSize uint64
	putsInBatch := 0
	notFirst := true
	for {
		chunk := <-sizeCh
		if chunk == nil {
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			batch.Put(key, emptyValue)
			if err := batch.Commit(); err != nil {
				dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
					d.DataName(), err.Error())
				return
			}
		}

		// Get label associated with this sparse volume.
		dataKey := chunk.K.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		label := binary.LittleEndian.Uint64(indexBytes[1:9])

		// Compute the size
		numVoxels, _, err := statsRuns(chunk.V)
		if err != nil {
			dvid.Log(dvid.Normal, "Error on computing label sizes: %s\n", err.Error())
			return
		}

		// If we are a new label, store size
		if notFirst && label != curLabel {
			key := d.NewLabelSizesKey(versionID, curSize, curLabel)
			curSize = 0
			batch.Put(key, emptyValue)
			putsInBatch++
			if putsInBatch%BATCH_SIZE == 0 {
				if err := batch.Commit(); err != nil {
					dvid.Log(dvid.Normal, "Error on batch PUT of label sizes for %s: %s\n",
						d.DataName(), err.Error())
					return
				}
			}
		}
		curLabel = label
		curSize += uint64(numVoxels)
		notFirst = true
	}
}

// GetSizeRange returns a JSON list of mapped labels that have volumes within the given range.
func (d *Data) GetSizeRange(uuid dvid.UUID, minSize, maxSize uint64) (string, error) {
	db, versionID, _, err := d.getHooks(uuid)
	if err != nil {
		return "{}", err
	}

	// Get the start/end keys for the size range.
	firstKey := d.NewLabelSizesKey(versionID, minSize, 0)
	lastKey := d.NewLabelSizesKey(versionID, maxSize, MaxLabel)

	// Grab all keys for this range in one sequential read.
	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return "{}", err
	}

	// Convert them to a JSON compatible structure.
	labels := make([]uint64, len(keys))
	for i, key := range keys {
		dataKey := key.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		labels[i] = binary.LittleEndian.Uint64(indexBytes[9:17])
	}
	m, err := json.Marshal(labels)
	if err != nil {
		return "{}", nil
	}
	return string(m), nil
}

// GetSparseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
//    byte     Payload descriptor:
//               Bit 0 (LSB) - 8-bit grayscale
//               Bit 1 - 16-bit grayscale
//               Bit 2 - 16-bit normal
//               ...
//    uint8    Number of dimensions
//    uint8    Dimension of run (typically 0 = X)
//    byte     Reserved (to be used later)
//    uint32    # Voxels
//    uint32    # Spans
//    Repeating unit of:
//        int32   Coordinate of run start (dimension 0)
//        int32   Coordinate of run start (dimension 1)
//        int32   Coordinate of run start (dimension 2)
//		  ...
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func (d *Data) GetSparseVol(uuid dvid.UUID, label uint64) ([]byte, error) {
	db, versionID, labelsVol, err := d.getHooks(uuid)
	if err != nil {
		return nil, err
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(PayloadBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))
	binary.Write(buf, binary.LittleEndian, byte(0))
	buf.WriteByte(byte(0))
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	// Get the start/end keys for this body's KeyLabelSpatialMap (b + s) keys.
	firstKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MinIndexZYX)
	lastKey := d.NewLabelSpatialMapKey(versionID, label, dvid.MaxIndexZYX)

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	wg := new(sync.WaitGroup)
	op := &sparseOp{versionID: versionID, encoding: buf.Bytes()}
	err = db.ProcessRange(firstKey, lastKey, &storage.ChunkOp{op, wg}, d.processLabelRuns)
	if err != nil {
		return nil, err
	}
	wg.Wait()

	binary.LittleEndian.PutUint32(op.encoding[8:12], op.numRuns)

	dvid.Log(dvid.Debug, "For data '%s' label %d: found %d blocks, %d runs\n",
		labelsVol.DataName(), label, op.numBlocks, op.numRuns)
	return op.encoding, nil
}

// LoadRavelerMaps loads maps from Raveler-formatted superpixel->segment and
// segment->body maps.  Ignores any mappings that are in slices outside
// associated labels64 volume.
func (d *Data) LoadRavelerMaps(request datastore.Request, reply *datastore.Response) error {
	startTime := time.Now()

	// Parse the request
	var uuidStr, dataName, cmdStr, fileTypeStr, spsegStr, segbodyStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &fileTypeStr, &spsegStr, &segbodyStr)

	// Get the version
	uuid, err := server.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Use of Raveler maps causes zero labels to be reserved.
	d.ZeroLocked = true
	d.Ready = false
	service := server.DatastoreService()
	if err := service.SaveDataset(uuid); err != nil {
		return err
	}

	// Get the extents of associated labels.
	labels, err := getRelatedLabels(uuid, d.Labels)
	if err != nil {
		return err
	}
	minLabelZ := uint32(labels.Extents().MinPoint.Value(2))
	maxLabelZ := uint32(labels.Extents().MaxPoint.Value(2))

	// Get the seg->body map
	seg2body, err := loadSegBodyMap(segbodyStr)
	if err != nil {
		return err
	}

	// Prepare for datastore access
	versionID, err := server.VersionLocalID(uuid)
	if err != nil {
		return err
	}
	db := server.StorageEngine()

	var slice, superpixel32 uint32
	var segment, body uint64
	forwardIndex := make([]byte, 17)
	forwardIndex[0] = byte(KeyForwardMap)
	inverseIndex := make([]byte, 17)
	inverseIndex[0] = byte(KeyInverseMap)

	// Get the sp->seg map, persisting each computed sp->body.
	dvid.Log(dvid.Normal, "Processing superpixel->segment map (Z %d-%d): %s\n",
		minLabelZ, maxLabelZ, spsegStr)
	file, err := os.Open(spsegStr)
	if err != nil {
		return fmt.Errorf("Could not open superpixel->segment map: %s", spsegStr)
	}
	defer file.Close()
	lineReader := bufio.NewReader(file)
	linenum := 0

	for {
		line, err := lineReader.ReadString('\n')
		if err != nil {
			break
		}
		if line[0] == ' ' || line[0] == '#' {
			continue
		}
		storage.FileBytesRead <- len(line)
		if _, err := fmt.Sscanf(line, "%d %d %d", &slice, &superpixel32, &segment); err != nil {
			return fmt.Errorf("Error loading superpixel->segment map, line %d in %s", linenum, spsegStr)
		}
		if slice < minLabelZ || slice > maxLabelZ {
			continue
		}
		if superpixel32 == 0 {
			continue
		}
		if superpixel32 > 0x0000000000FFFFFF {
			return fmt.Errorf("Error in line %d: superpixel id exceeds 24-bit value!", linenum)
		}
		superpixelBytes := labels64.RavelerSuperpixelBytes(slice, superpixel32)
		var found bool
		body, found = seg2body[segment]
		if !found {
			return fmt.Errorf("Segment (%d) in %s not found in %s", segment, spsegStr, segbodyStr)
		}

		// PUT the forward label pair without compression.
		copy(forwardIndex[1:9], superpixelBytes)
		binary.BigEndian.PutUint64(forwardIndex[9:17], body)
		key := d.DataKey(versionID, dvid.IndexBytes(forwardIndex))
		err = db.Put(key, emptyValue)
		if err != nil {
			return fmt.Errorf("ERROR on PUT of forward label mapping (%x -> %d): %s\n",
				superpixelBytes, body, err.Error())
		}

		// PUT the inverse label pair without compression.
		binary.BigEndian.PutUint64(inverseIndex[1:9], body)
		copy(inverseIndex[9:17], superpixelBytes)
		key = d.DataKey(versionID, dvid.IndexBytes(inverseIndex))
		err = db.Put(key, emptyValue)
		if err != nil {
			return fmt.Errorf("ERROR on PUT of inverse label mapping (%d -> %x): %s\n",
				body, superpixelBytes, err.Error())
		}

		linenum++
		if linenum%1000000 == 0 {
			fmt.Printf("Added %d forward and inverse mappings\n", linenum)
		}
	}
	dvid.Log(dvid.Normal, "Added %d forward and inverse mappings\n", linenum)
	dvid.ElapsedTime(dvid.Normal, startTime, "Processed Raveler superpixel->body files")

	// Spawn goroutine to do spatial processing on associated label volume.
	go d.ProcessSpatially(uuid)

	return nil
}

// GetLabelMapping returns the mapping for a label.
func (d *Data) GetLabelMapping(versionID dvid.VersionLocalID, label []byte) (uint64, error) {
	firstKey := d.NewForwardMapKey(versionID, label, 0)
	lastKey := d.NewForwardMapKey(versionID, label, MaxLabel)

	db := server.StorageEngine()
	if db == nil {
		return 0, fmt.Errorf("Did not find a working key-value datastore to get image!")
	}
	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return 0, err
	}
	numKeys := len(keys)
	switch {
	case numKeys == 0:
		return 0, fmt.Errorf("Label %d is not mapped to any other label.", label)
	case numKeys > 1:
		var mapped string
		for i := 0; i < len(keys); i++ {
			mapped += fmt.Sprintf("%d ", keys[i])
		}
		return 0, fmt.Errorf("Label %d is mapped to more than one label: %s", label, mapped)
	}

	b := keys[0].Bytes()
	indexBytes := b[datastore.DataKeyIndexOffset:]
	mapping := binary.BigEndian.Uint64(indexBytes[9:17])

	return mapping, nil
}

// GetBlockMapping returns the label -> mappedLabel map for a given block.
func (d *Data) GetBlockMapping(vID dvid.VersionLocalID, block dvid.IndexZYX) (map[string]uint64, error) {
	db := server.StorageEngine()
	if db == nil {
		return nil, fmt.Errorf("Did not find a working key-value datastore to get image!")
	}

	firstKey := d.NewSpatialMapKey(vID, block, nil, 0)
	maxLabel := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	lastKey := d.NewSpatialMapKey(vID, block, maxLabel, MaxLabel)

	keys, err := db.KeysInRange(firstKey, lastKey)
	if err != nil {
		return nil, err
	}
	numKeys := len(keys)
	mapping := make(map[string]uint64, numKeys)
	offset := 1 + dvid.IndexZYXSize
	for _, key := range keys {
		dataKey := key.(*datastore.DataKey)
		indexBytes := dataKey.Index.Bytes()
		label := indexBytes[offset : offset+8]
		mappedLabel := binary.BigEndian.Uint64(indexBytes[offset+8 : offset+16])
		mapping[string(label)] = mappedLabel
	}
	return mapping, nil
}

// GetBlockLayerMapping gets the label mapping for a Z layer of blocks and stores the result
// in the passed Operation.
func (d *Data) GetBlockLayerMapping(blockZ uint32, op *blockOp) (
	minChunkPt, maxChunkPt dvid.ChunkPoint3d, err error) {

	// Convert blockZ to actual voxel space Z range.
	minChunkPt = dvid.ChunkPoint3d{dvid.MinChunkPoint3d[0], dvid.MinChunkPoint3d[1], blockZ}
	maxChunkPt = dvid.ChunkPoint3d{dvid.MaxChunkPoint3d[0], dvid.MaxChunkPoint3d[1], blockZ}
	minVoxelPt := minChunkPt.MinVoxelPoint(op.labels.BlockSize())
	maxVoxelPt := minChunkPt.MaxVoxelPoint(op.labels.BlockSize())

	// Get first and last keys that span that voxel space Z range.
	minZ := uint32(minVoxelPt.Value(2))
	maxZ := uint32(maxVoxelPt.Value(2))
	firstKey := d.NewRavelerForwardMapKey(op.versionID, minZ, 1, 0)
	lastKey := d.NewRavelerForwardMapKey(op.versionID, maxZ, 0xFFFFFFFF, MaxLabel)

	// Get all forward mappings from the key-value store.
	op.mapping = nil

	db := server.StorageEngine()
	if db == nil {
		err = fmt.Errorf("Did not find a working key-value datastore to get image!")
		return
	}
	var keys []storage.Key
	keys, err = db.KeysInRange(firstKey, lastKey)
	if err != nil {
		err = fmt.Errorf("Could not find mapping with slice between %d and %d: %s",
			minZ, maxZ, err.Error())
		return
	}

	// Cache this layer of blocks' mappings.
	numKeys := len(keys)
	if numKeys != 0 {
		op.mapping = make(map[string]uint64, numKeys)
		for _, key := range keys {
			keyBytes := key.Bytes()
			indexBytes := keyBytes[datastore.DataKeyIndexOffset:]
			label := string(indexBytes[1:9])
			mappedLabel := binary.BigEndian.Uint64(indexBytes[9:17])
			op.mapping[label] = mappedLabel
		}
	}

	//dvid.Log(dvid.Debug, "Loaded %d mappings that cover Z: %d to %d\n", numKeys,
	//	minVoxelPt.Value(2), maxVoxelPt.Value(2))
	return
}

type blockOp struct {
	labels    *labels64.Data
	versionID dvid.VersionLocalID
	mapping   map[string]uint64
}

// Iterate through all blocks in the associated label volume, computing the spatial indices
// for bodies and the mappings for each spatial index.
func (d *Data) ProcessSpatially(uuid dvid.UUID) {
	dvid.Log(dvid.Normal, "Adding spatial information from label volume %s for mapping %s...\n",
		d.Labels, d.DataName())

	db, versionID, labels, err := d.getHooks(uuid)
	if err != nil {
		dvid.Log(dvid.Normal, "Error in %s.ProcessSpatially(): %s\n", d.DataName(), err.Error())
		return
	}

	// Iterate through all labels chunks incrementally in Z, loading and then using the maps
	// for all blocks in that layer.
	startTime := time.Now()
	wg := new(sync.WaitGroup)
	op := &blockOp{labels, versionID, nil}

	dataID := labels.DataID()
	extents := labels.Extents()
	minIndexZ := extents.MinIndex.(dvid.IndexZYX)[2]
	maxIndexZ := extents.MaxIndex.(dvid.IndexZYX)[2]
	for z := minIndexZ; z <= maxIndexZ; z++ {
		t := time.Now()

		// Get the label->label map for this Z
		var minChunkPt, maxChunkPt dvid.ChunkPoint3d
		minChunkPt, maxChunkPt, err := d.GetBlockLayerMapping(z, op)
		if err != nil {
			dvid.Log(dvid.Normal, "Error getting label mapping for block Z %d: %s\n", z, err.Error())
			return
		}

		// Process the labels chunks for this Z
		minIndex := dvid.IndexZYX(minChunkPt)
		maxIndex := dvid.IndexZYX(maxChunkPt)
		if op.mapping != nil {
			startKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, minIndex}
			endKey := &datastore.DataKey{dataID.DsetID, dataID.ID, versionID, maxIndex}
			chunkOp := &storage.ChunkOp{op, wg}
			err = db.ProcessRange(startKey, endKey, chunkOp, d.ProcessChunk)
			wg.Wait()
		}

		dvid.ElapsedTime(dvid.Debug, t, "Processed all %s blocks for layer %d/%d",
			d.Labels, z-minIndexZ+1, maxIndexZ-minIndexZ+1)
	}
	dvid.ElapsedTime(dvid.Debug, startTime, "Processed spatial information from %s for mapping %s",
		d.Labels, d.DataName())

	// Iterate through all mapped labels and determine the size in voxels.
	startTime = time.Now()
	startKey := d.NewLabelSpatialMapKey(versionID, 0, dvid.MinIndexZYX)
	endKey := d.NewLabelSpatialMapKey(versionID, MaxLabel, dvid.MaxIndexZYX)
	sizeCh := make(chan *storage.Chunk, 10000)
	wg.Add(1)
	go d.computeSizes(sizeCh, db, versionID, wg)
	err = db.ProcessRange(startKey, endKey, &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		sizeCh <- chunk
	})
	if err != nil {
		dvid.Log(dvid.Normal, "Error indexing sizes for %s: %s\n", d.DataName(), err.Error())
		return
	}
	sizeCh <- nil
	wg.Wait()
	dvid.ElapsedTime(dvid.Debug, startTime,
		"Created size index for mapping '%s' applied to labels '%s'",
		d.DataName(), d.Labels)

	// Wait for results then set Updating.
	d.Ready = true
	if err := server.DatastoreService().SaveDataset(uuid); err != nil {
		dvid.Log(dvid.Normal, "Could not save READY state to data '%s', uuid %s: %s",
			d.DataName(), uuid, err.Error())
	}
}

// ProcessChunk processes a chunk of data as part of a mapped operation.
// Only some multiple of the # of CPU cores can be used for chunk handling before
// it waits for chunk processing to abate via the buffered server.HandlerToken channel.
func (d *Data) ProcessChunk(chunk *storage.Chunk) {
	<-server.HandlerToken
	go d.processChunk(chunk)
}

func (d *Data) processChunk(chunk *storage.Chunk) {
	defer func() {
		// After processing a chunk, return the token.
		server.HandlerToken <- 1
	}()

	op := chunk.Op.(*blockOp)
	db := server.StorageEngine()
	if db == nil {
		dvid.Log(dvid.Normal, "Did not find a working key-value datastore to get image!")
		return
	}
	batcher, ok := db.(storage.Batcher)
	batch := batcher.NewBatch()

	// Get the spatial index associated with this chunk.
	dataKey := chunk.K.(*datastore.DataKey)
	zyx := dataKey.Index.(*dvid.IndexZYX)
	zyxBytes := zyx.Bytes()

	// Initialize the label buffer.  For voxels, this data needs to be uncompressed and deserialized.
	blockData, _, err := dvid.DeserializeData(chunk.V, true)
	if err != nil {
		dvid.Log(dvid.Normal, "Unable to deserialize block in '%s': %s\n",
			d.DataID.DataName(), err.Error())
		return
	}

	// Construct block-level mapping keys that allow quick range queries pertinent to access patterns.
	// We work with the spatial index (s), original label (a), and mapped label (b).
	offsetSAB := 1 + dvid.IndexZYXSize
	sabIndex := make([]byte, offsetSAB+8+8) // s + a + b
	sabIndex[0] = byte(KeySpatialMap)
	copy(sabIndex[1:offsetSAB], zyxBytes)

	// Iterate through this block of labels.
	blockBytes := len(blockData)
	if blockBytes%8 != 0 {
		dvid.Log(dvid.Normal, "Retrieved, deserialized block is wrong size: %d bytes\n", blockBytes)
		return
	}
	written := make(map[string]bool, blockBytes/10)
	runStarts := make(map[uint64]([]dvid.Point3d), 10)
	runLengths := make(map[uint64]([]int32), 10)

	firstPt := zyx.FirstPoint(op.labels.BlockSize()).(dvid.Point3d)
	lastPt := zyx.LastPoint(op.labels.BlockSize()).(dvid.Point3d)
	var curPt dvid.Point3d
	var b, curLabel uint64
	var z, y, x, curRun int32
	start := 0
	for z = firstPt.Value(2); z <= lastPt.Value(2); z++ {
		for y = firstPt.Value(1); y <= lastPt.Value(1); y++ {
			for x = firstPt.Value(0); x <= lastPt.Value(0); x++ {
				a := blockData[start : start+8]

				// Get the label to which the current label is mapped.
				zeroToken := d.ZeroLocked && bytes.Compare(a, zeroSuperpixelBytes) == 0
				if zeroToken {
					b = 0
				} else {
					b, ok = op.mapping[string(a)]
					if !ok {
						zBeg := zyx.FirstPoint(op.labels.BlockSize()).Value(2)
						zEnd := zyx.LastPoint(op.labels.BlockSize()).Value(2)
						slice := binary.BigEndian.Uint32(a[0:4])
						dvid.Log(dvid.Normal, "No mapping found for %x (slice %d) in block with Z %d to %d\n",
							a, slice, zBeg, zEnd)
						b = 0
					}
				}

				// Track run length
				if curRun != 0 && b == curLabel {
					curRun++
				} else {
					// Save old run
					if curRun > 0 {
						runLengths[curLabel] = append(runLengths[curLabel], curRun)
					}
					// Create new run start
					curPt = dvid.Point3d{x, y, z}
					if runStarts[b] == nil {
						runStarts[b] = []dvid.Point3d{curPt}
						runLengths[b] = []int32{}
					} else {
						runStarts[b] = append(runStarts[b], curPt)
					}
					curRun = 1
					curLabel = b
				}

				// Store a KeySpatialMap key (index = s + a + b)
				copy(sabIndex[offsetSAB:offsetSAB+8], a)
				binary.BigEndian.PutUint64(sabIndex[offsetSAB+8:offsetSAB+16], b)
				_, found := written[string(sabIndex)]
				if !found {
					key := d.DataKey(op.versionID, dvid.IndexBytes(sabIndex))
					batch.Put(key, emptyValue)
					written[string(sabIndex)] = true
				}

				start += 8
			}
			// Force break of any runs when we finish x scan.
			if curRun > 0 {
				runLengths[curLabel] = append(runLengths[curLabel], curRun)
				curRun = 0
			}
		}
	}
	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeySpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
		return
	}
	batch.Clear()

	// Store the KeyLabelSpatialMap keys (index = b + s) with slice of runs for value.
	bsIndex := make([]byte, 1+8+dvid.IndexZYXSize)
	bsIndex[0] = byte(KeyLabelSpatialMap)
	copy(bsIndex[9:9+dvid.IndexZYXSize], zyxBytes)
	for b, coords := range runStarts {
		binary.BigEndian.PutUint64(bsIndex[1:9], b)
		key := d.DataKey(op.versionID, dvid.IndexBytes(bsIndex))
		runsBytes, err := encodeRuns(coords, runLengths[b])
		if err != nil {
			dvid.Log(dvid.Normal, "Error encoding KeyLabelSpatialMap keys for mapped label %d: %s\n",
				b, err.Error())
			return
		}
		batch.Put(key, runsBytes)
	}
	if err := batch.Commit(); err != nil {
		dvid.Log(dvid.Normal, "Error on batch PUT of KeyLabelSpatialMap on %s: %s\n",
			dataKey.Index, err.Error())
		return
	}
	batch.Close()

	// Notify the requestor that this chunk is done.
	if chunk.Wg != nil {
		chunk.Wg.Done()
	}
}
