/*
	Package labelvol supports label-specific sparse volumes.  It can be synced
	with labelblk and is a different view of 64-bit label data.
*/
package labelvol

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelvol"
	TypeName = "labelvol"
)

const HelpMessage = `
API for label sparse volume data type (github.com/janelia-flyem/dvid/datatype/labelvol)
=======================================================================================

Note: Denormalizations like sparse volumes are *not* performed for the "0" label, which is
considered a special label useful for designating background.  This allows users to define
sparse labeled structures in a large volume without requiring processing of entire volume.


Command-line:

$ dvid repo <UUID> new labelvol <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new labelvol sparsevols

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "sparsevols"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    BlockSize      Size in pixels  (default: %s)
    VoxelSize      Resolution of voxels (default: 8.0, 8.0, 8.0)
    VoxelUnits     Resolution units (default: "nanometers")
	
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/bodies/info

    Returns JSON with configuration settings that include location in DVID space and
    min/max block indices.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelvol data.


GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns a sparse volume with voxels of the given label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Payload descriptor:
	               Bit 0 (LSB) - 8-bit grayscale
	               Bit 1 - 16-bit grayscale
	               Bit 2 - 16-bit normal
	               If set to all 0, there is no payload and it's a binary sparse volume.
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Voxels [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
	        int32   Length of run
	        bytes   Optional payload dependent on first byte descriptor
			  ...

    Query-string Options:

    minx    Spans must be equal to or larger than this minimum x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum z voxel coordinate.
    exact   "true" if all RLEs should respect voxel bounds.
            "false" if RLEs can extend a bit outside voxel bounds within border blocks.   


GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>

	Returns a sparse volume with voxels that pass through a given voxel.
	The encoding is described in the "sparsevol" request above.
	
    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of mapping data.
    coord     	  Coordinate of voxel with underscore as separator, e.g., 10_20_30


GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>

	Returns a sparse volume with blocks of the given label in encoded RLE format.
	The encoding has the following format where integers are little endian and the order
	of data is exactly as specified below:

	    byte     Set to 0
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Blocks [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Block coordinate of run start (dimension 0)
	        int32   Block coordinate of run start (dimension 1)
	        int32   Block coordinate of run start (dimension 2)
			  ...
	        int32   Length of run

	Note that the above format is the RLE encoding of sparsevol, where voxel coordinates
	have been replaced by block coordinates.


GET <api URL>/node/<UUID>/<data name>/maxlabel

	Returns the maximum label for the version of data in JSON form:

		{ "maxlabel": <label #> }


POST <api URL>/node/<UUID>/<data name>/merge

	Merges labels.  Requires JSON in request body using the following format:

	[toLabel1, fromLabel1, fromLabel2, fromLabel3, ...]

	The first element of the JSON array specifies the label to be used as the merge result.


POST <api URL>/node/<UUID>/<data name>/split/<label>

	Splits a portion of a label's voxels into a new label.  Returns the following JSON:

		{ "label": <new label> }

	This request requires a binary sparse volume in the POSTed body with the following 
	encoded RLE format, which is compatible with the format returned by a GET on the 
	"sparsevol" endpoint described above:

		All integers are in little-endian format.

	    byte     Payload descriptor:
	               Set to 0 to indicate it's a binary sparse volume.
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

`

var (
	dtype *Type

	DefaultBlockSize int32   = labelblk.DefaultBlockSize
	DefaultRes       float32 = labelblk.DefaultRes
	DefaultUnits             = labelblk.DefaultUnits
)

func init() {
	dtype = new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.Register(dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

// NewData returns a pointer to labelvol data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	// Initialize the Data for this data type
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	props := new(Properties)
	props.setDefault()
	if err := props.setByConfig(c); err != nil {
		return nil, err
	}
	data := &Data{
		Data:       basedata,
		Properties: *props,
	}
	data.Properties.MaxLabel = make(map[dvid.VersionID]uint64)
	return data, nil
}

// --- Labelvol Datatype -----

type Type struct {
	datastore.Type
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

// Properties are additional properties for data beyond those in standard datastore.Data.
type Properties struct {
	Resolution dvid.Resolution

	// Block size for this repo
	BlockSize dvid.Point3d

	ml_mu sync.RWMutex // For atomic access of MaxLabel

	// The maximum label id found in this dataset.
	MaxLabel map[dvid.VersionID]uint64
}

func (p Properties) MarshalJSON() ([]byte, error) {
	maxLabels := make(map[string]uint64)
	for v, max := range p.MaxLabel {
		uuid, err := datastore.UUIDFromVersion(v)
		if err != nil {
			return nil, err
		}
		maxLabels[string(uuid)] = max
	}
	return json.Marshal(struct {
		dvid.Resolution
		BlockSize dvid.Point3d
		MaxLabel  map[string]uint64
	}{
		p.Resolution,
		p.BlockSize,
		maxLabels,
	})
}

func (p *Properties) setDefault() {
	for d := 0; d < 3; d++ {
		p.BlockSize[d] = DefaultBlockSize
	}
	p.Resolution.VoxelSize = make(dvid.NdFloat32, 3)
	for d := 0; d < 3; d++ {
		p.Resolution.VoxelSize[d] = DefaultRes
	}
	p.Resolution.VoxelUnits = make(dvid.NdString, 3)
	for d := 0; d < 3; d++ {
		p.Resolution.VoxelUnits[d] = DefaultUnits
	}
}

func (p *Properties) setByConfig(config dvid.Config) error {
	s, found, err := config.GetString("BlockSize")
	if err != nil {
		return err
	}
	if found {
		p.BlockSize, err = dvid.StringToPoint3d(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelSize")
	if err != nil {
		return err
	}
	if found {
		dvid.Infof("Changing resolution of voxels to %s\n", s)
		p.Resolution.VoxelSize, err = dvid.StringToNdFloat32(s, ",")
		if err != nil {
			return err
		}
	}
	s, found, err = config.GetString("VoxelUnits")
	if err != nil {
		return err
	}
	if found {
		p.Resolution.VoxelUnits, err = dvid.StringToNdString(s, ",")
		if err != nil {
			return err
		}
	}
	return nil
}

// Data instance of labelvol, label sparse volumes.
type Data struct {
	*datastore.Data
	Properties
}

func (d *Data) GetSyncedLabelblk(v dvid.VersionID) (*labelblk.Data, error) {
	// Go through all synced names, and checking if there's a valid source.
	for _, name := range d.SyncedNames() {
		source, err := labelblk.GetByVersion(v, name)
		if err == nil {
			return source, nil
		}
	}
	return nil, fmt.Errorf("no labelblk data is syncing with %d", d.DataName())
}

// GetByUUID returns a pointer to labelvol data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelvol datatype!", name)
	}
	return data, nil
}

// --- datastore.InstanceMutator interface -----

// LoadMutable loads mutable properties of label volumes like the maximum labels
// for each version.
func (d *Data) LoadMutable() error {
	ctx := storage.NewDataContext(d, 0)
	minKey, err := ctx.MinVersionKey(NewMaxLabelIndex())
	if err != nil {
		return err
	}
	maxKey, err := ctx.MaxVersionKey(NewMaxLabelIndex())
	if err != nil {
		return err
	}
	store, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}
	var maxes int
	d.MaxLabel = make(map[dvid.VersionID]uint64)
	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		v, err := ctx.VersionFromKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Can't decode key when loading mutable data for %s", d.DataName())
		}
		if len(chunk.V) != 8 {
			return fmt.Errorf("Got bad value.  Expected 64-bit label, got %v", chunk.V)
		}
		label := binary.LittleEndian.Uint64(chunk.V)
		d.MaxLabel[v] = label
		maxes++
		return nil
	}
	err = store.ProcessRange(nil, minKey, maxKey, &storage.ChunkOp{}, f)
	if err != nil {
		return err
	}
	dvid.Infof("Loaded %d maximum label values for labelvol %q\n", maxes, d.DataName())
	return nil
}

// --- datastore.DataService interface ---------

func (d *Data) Help() string {
	return HelpMessage
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Errorf("labelvol.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	return nil
}

type Bounds struct {
	VoxelBounds *dvid.Bounds
	BlockBounds *dvid.Bounds
	Exact       bool // All RLEs must respect the voxel bounds.  If false, just screen on blocks.
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	repo, versions, err := datastore.FromContext(ctx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n",
			d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && action == "put" {
		config, err := server.DecodeJSON(r)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := repo.Save(); err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		fmt.Fprintf(w, "Changed '%s' based on received configuration:\n%s\n", d.DataName(), config)
		return
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "Incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, dtype.Help())

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "sparsevol":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		queryValues := r.URL.Query()
		var b Bounds
		b.VoxelBounds, err = dvid.BoundsFromQueryString(r)
		if err != nil {
			server.BadRequest(w, r, "Error parsing bounds from query string: %s\n", err.Error())
			return
		}
		b.BlockBounds = b.VoxelBounds.Divide(d.BlockSize)
		b.Exact = queryValues.Get("exact") == "true"
		data, err := GetSparseVol(storeCtx, label, b)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: sparsevol on label %d (%s)", r.Method, label, r.URL)

	case "sparsevol-by-point":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
			return
		}
		coord, err := dvid.StringToPoint(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		source, err := d.GetSyncedLabelblk(versionID)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		label, err := source.GetLabelAtPoint(versionID, coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := GetSparseVol(storeCtx, label, Bounds{})
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, coord, r.URL)

	case "sparsevol-coarse":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol-coarse' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := GetSparseCoarseVol(storeCtx, label)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		timedLog.Infof("HTTP %s: sparsevol-coarse on label %d (%s)", r.Method, label, r.URL)

	case "split":
		// POST <api URL>/node/<UUID>/<data name>/split/<label>
		if action != "post" {
			server.BadRequest(w, r, "Split requests must be POST actions.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
			return
		}
		fromLabel, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		toLabel, err := d.SplitLabels(versionID, fromLabel, r.Body)
		if err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on split: %s", err.Error()))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %d}", "Label", toLabel)
		timedLog.Infof("HTTP split request (%s)", r.URL)

	case "merge":
		// POST <api URL>/node/<UUID>/<data name>/merge
		if action != "post" {
			server.BadRequest(w, r, "Merge requests must be POST actions.")
			return
		}
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, "Bad POSTed data for merge.  Should be JSON.")
			return
		}
		var tuple labels.MergeTuple
		if err := json.Unmarshal(data, &tuple); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Bad merge op JSON: %s", err.Error()))
			return
		}
		mergeOp, err := tuple.Op()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		if err := d.MergeLabels(versionID, mergeOp); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on merge: %s", err.Error()))
			return
		}
		timedLog.Infof("HTTP merge request (%s)", r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call %q for labelvol data %q.  See API help.",
			parts[3], d.DataName())
	}
}

// MaxLabel returns the maximum recorded label for the given version.
func (d *Data) GetMaxLabel(v dvid.VersionID) (uint64, error) {
	d.ml_mu.RLock()
	defer d.ml_mu.RUnlock()

	max, found := d.MaxLabel[v]
	if !found {
		return 0, fmt.Errorf("no max label found for version %d", v)
	}
	return max, nil
}

func (d *Data) casMaxLabel(batch storage.Batch, v dvid.VersionID, label uint64) {
	d.ml_mu.Lock()
	defer d.ml_mu.Unlock()
	save := false
	maxLabel, found := d.MaxLabel[v]
	if !found {
		// Get parent's max or start with 1.
		parent, found, err := datastore.GetParentByVersion(v)
		if !found || err != nil {
			maxLabel = 1
		} else {
			maxLabel, found = d.MaxLabel[parent]
			if found {
				maxLabel++
			} else {
				maxLabel = 1
			}
		}
		save = true
	}
	if maxLabel < label {
		maxLabel = label
		save = true
	}
	if save {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, label)
		batch.Put(NewMaxLabelIndex(), buf)
		d.MaxLabel[v] = maxLabel
	}
	if err := batch.Commit(); err != nil {
		dvid.Errorf("batch put: %s\n", err.Error())
		return
	}
}

// NewLabel returns a new label for the given version.
func (d *Data) NewLabel(v dvid.VersionID) (uint64, error) {
	d.ml_mu.Lock()
	defer d.ml_mu.Unlock()

	var label uint64

	max, found := d.MaxLabel[v]
	if found {
		label = max + 1
	} else {
		// Get max from parent
		parent, found, err := datastore.GetParentByVersion(v)
		if err != nil {
			return 0, err
		}
		if found {
			maxP, found := d.MaxLabel[parent]
			if found {
				label = maxP + 1
			} else {
				label = 1
			}
		} else {
			label = 1
		}
	}
	d.MaxLabel[v] = label
	store, err := storage.SmallDataStore()
	if err != nil {
		return 0, fmt.Errorf("can't initializing small data store: %s\n", err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, label)
	ctx := datastore.NewVersionedContext(d, v)
	store.Put(ctx, NewMaxLabelIndex(), buf)
	return label, nil
}

// Returns RLEs for a given label where the key of the returned map is the block index
// in string format.
func (d *Data) GetLabelRLEs(v dvid.VersionID, label uint64) (dvid.BlockRLEs, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}

	// Get the start/end indices for this body's KeyLabelSpatialMap (b + s) keys.
	begIndex := NewIndex(label, dvid.MinIndexZYX.Bytes())
	endIndex := NewIndex(label, dvid.MaxIndexZYX.Bytes())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	labelRLEs := dvid.BlockRLEs{}
	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		// Get the block index where the fromLabel is present
		_, blockBytes, err := DecodeKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Can't recover block index with chunk key %v: %s\n", chunk.K, err.Error())
		}
		blockStr := dvid.IZYXString(blockBytes)

		var blockRLEs dvid.RLEs
		if err := blockRLEs.UnmarshalBinary(chunk.V); err != nil {
			return fmt.Errorf("Unable to unmarshal RLE for label in block %v", chunk.K)
		}
		labelRLEs[blockStr] = blockRLEs
		return nil
	}
	ctx := datastore.NewVersionedContext(d, v)
	err = store.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, f)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Found %d blocks with label %d\n", len(labelRLEs), label)
	return labelRLEs, nil
}

type sparseOp struct {
	versionID dvid.VersionID
	encoding  []byte
	numBlocks uint32
	numRuns   uint32
	//numVoxels int32
}

// Alter serialized RLEs by the bounds.
func boundRLEs(b []byte, bounds *dvid.Bounds) ([]byte, error) {
	var oldRLEs dvid.RLEs
	err := oldRLEs.UnmarshalBinary(b)
	if err != nil {
		return nil, err
	}
	newRLEs := oldRLEs.FitToBounds(bounds)
	return newRLEs.MarshalBinary()
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
//        int32   Length of run
//        bytes   Optional payload dependent on first byte descriptor
//
func GetSparseVol(ctx storage.Context, label uint64, bounds Bounds) ([]byte, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # voxels
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # spans

	// Get the start/end indices for this body's KeyLabelSpatialMap (b + s) keys.
	minZYX := dvid.MinIndexZYX
	maxZYX := dvid.MaxIndexZYX
	blockBounds := bounds.BlockBounds
	if blockBounds == nil {
		blockBounds = new(dvid.Bounds)
	}
	if minZ, ok := blockBounds.MinZ(); ok {
		minZYX[2] = minZ
	}
	if maxZ, ok := blockBounds.MaxZ(); ok {
		maxZYX[2] = maxZ
	}
	begIndex := NewIndex(label, minZYX.Bytes())
	endIndex := NewIndex(label, maxZYX.Bytes())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	// TODO -- Make processing asynchronous so can overlap with range disk read now that
	//   there could be more processing due to bounding calcs.
	var numRuns, numBlocks uint32
	encoding := buf.Bytes()

	var f storage.ChunkProcessor = func(chunk *storage.Chunk) error {
		// Make sure this block is within the optinonal bounding.
		if blockBounds.BoundedX() || blockBounds.BoundedY() {
			_, blockBytes, err := DecodeKey(chunk.K)
			if err != nil {
				return fmt.Errorf("Error decoding sparse volume key (%v): %s\n", chunk.K, err.Error())
			}
			var indexZYX dvid.IndexZYX
			if err := indexZYX.IndexFromBytes(blockBytes); err != nil {
				return fmt.Errorf("Error decoding block coordinate (%v) for sparse volume: %s\n",
					blockBytes, err.Error())
			}
			blockX, blockY, _ := indexZYX.Unpack()
			if blockBounds.OutsideX(blockX) || blockBounds.OutsideY(blockY) {
				return nil
			}
		}

		// Adjust RLEs within block if we are bounded.
		var rles []byte
		var err error
		if bounds.Exact && bounds.VoxelBounds.IsSet() {
			rles, err = boundRLEs(chunk.V, bounds.VoxelBounds)
			if err != nil {
				return fmt.Errorf("Error in adjusting RLEs to bounds: %s\n", err.Error())
			}
		} else {
			rles = chunk.V
		}

		numRuns += uint32(len(rles) / 16)
		numBlocks++
		if int64(len(encoding))+int64(len(rles)) > server.MaxDataRequest {
			return fmt.Errorf("Sparse volume read aborted because length exceeds %d bytes", server.MaxDataRequest)
		}
		encoding = append(encoding, rles...)
		return nil
	}

	if err := store.ProcessRange(ctx, begIndex, endIndex, &storage.ChunkOp{}, f); err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(encoding[8:12], numRuns)

	dvid.Debugf("[%s] label %d: found %d blocks, %d runs\n", ctx, label, numBlocks, numRuns)
	return encoding, nil
}

// PutSparseVol stores an encoded sparse volume that stays within a given forward label.
// This function handles modification/deletion of all denormalized data touched by this
// sparse label volume.
func PutSparseVol(ctx storage.Context, label uint64, data []byte) error {
	/*
		bigdata, err := storage.BigDataStore()
		if err != nil {
			return fmt.Errorf("Cannot get datastore that handles big data: %s\n", err.Error())
		}

		if data[0] != dvid.EncodingBinary {
			return fmt.Errorf("Received corrupt sparse volume -- first byte not %d", dvid.EncodingBinary)
		}
		if data[1] != 3 {
			return fmt.Errorf("Can't process sparse volume with # of dimensions = %d", data[1])
		}
		if data[2] != 0 {
			return fmt.Errorf("Can't process sparse volumes with runs encoded in dimension %d", data[2])
		}
		// numVoxels := binary.LittleEndian.Uint32(data[4:8])  [not used right now]
		numSpans := binary.LittleEndian.Uint32(data[8:12])

		//
	*/
	return nil
}

// GetSparseCoarseVol returns an encoded sparse volume given a label.  The encoding has the
// following format where integers are little endian:
// 		byte     Set to 0
// 		uint8    Number of dimensions
// 		uint8    Dimension of run (typically 0 = X)
// 		byte     Reserved (to be used later)
// 		uint32    # Blocks [TODO.  0 for now]
// 		uint32    # Spans
// 		Repeating unit of:
//     		int32   Block coordinate of run start (dimension 0)
//     		int32   Block coordinate of run start (dimension 1)
//     		int32   Block coordinate of run start (dimension 2)
//     		int32   Length of run
//
func GetSparseCoarseVol(ctx storage.Context, label uint64) ([]byte, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}

	// Create the sparse volume header
	buf := new(bytes.Buffer)
	buf.WriteByte(dvid.EncodingBinary)
	binary.Write(buf, binary.LittleEndian, uint8(3))  // # of dimensions
	binary.Write(buf, binary.LittleEndian, byte(0))   // dimension of run (X = 0)
	buf.WriteByte(byte(0))                            // reserved for later
	binary.Write(buf, binary.LittleEndian, uint32(0)) // Placeholder for # blocks
	encoding := buf.Bytes()

	// Get the start/end indices for this body's KeyLabelSpatialMap (b + s) keys.
	begIndex := NewIndex(label, dvid.MinIndexZYX.Bytes())
	endIndex := NewIndex(label, dvid.MaxIndexZYX.Bytes())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	var numBlocks uint32
	var span *dvid.Span
	var spans dvid.Spans
	keys, err := store.KeysInRange(ctx, begIndex, endIndex)
	if err != nil {
		return nil, fmt.Errorf("Cannot get keys for coarse sparse volume: %s", err.Error())
	}
	for _, key := range keys {
		numBlocks++
		_, blockBytes, err := DecodeKey(key)
		if err != nil {
			return nil, fmt.Errorf("Error retrieving RLE runs for label %d: %s", label, err.Error())
		}
		var indexZYX dvid.IndexZYX
		if err := indexZYX.IndexFromBytes(blockBytes); err != nil {
			return nil, fmt.Errorf("Error decoding block coordinate (%v) for coarse sparse volume: %s",
				blockBytes, err.Error())
		}
		x, y, z := indexZYX.Unpack()
		if span == nil {
			span = &dvid.Span{z, y, x, x}
		} else if !span.Extends(x, y, z) {
			spans = append(spans, *span)
			span = &dvid.Span{z, y, x, x}
		}
	}
	if err != nil {
		return nil, err
	}
	if span != nil {
		spans = append(spans, *span)
	}
	spansBytes, err := spans.MarshalBinary()
	if err != nil {
		return nil, err
	}
	encoding = append(encoding, spansBytes...)
	dvid.Debugf("[%s] coarse subvol for label %d: found %d blocks\n", ctx, label, numBlocks)
	return encoding, nil
}
