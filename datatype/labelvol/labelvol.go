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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

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


GET  <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns or stores a sparse volume with voxels of the given label in encoded RLE format.

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

    GET Query-string Options:

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

	GET returns the maximum label for the version of data in JSON form:

		{ "maxlabel": <label #> }


GET <api URL>/node/<UUID>/<data name>/nextlabel
POST <api URL>/node/<UUID>/<data name>/nextlabel

	GET returns the next label for the version of data in JSON form:

		{ "nextlabel": <label #> }

	POST allows the client to request some # of labels that will be reserved.
	This is used if the client wants to introduce new labels.

	The request:

		{ "needed": <# of labels> }

	Response:

		{ "start": <starting label #>, "end": <ending label #> }


POST <api URL>/node/<UUID>/<data name>/merge

	Merges labels.  Requires JSON in request body using the following format:

	[toLabel1, fromLabel1, fromLabel2, fromLabel3, ...]

	The first element of the JSON array specifies the label to be used as the merge result.
	Note that it's computationally more efficient to group a number of merges into the
	same toLabel as a single merge request instead of multiple merge requests.


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
	v, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	data.Properties.MaxLabel[v] = 0
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

	ml_mu sync.RWMutex // For atomic access of MaxLabel and MaxRepoLabel

	// The maximum label id found in each version of this instance.
	MaxLabel map[dvid.VersionID]uint64

	// The maximum label for this instance in the entire repo.  This allows us to do
	// conflict-free merges without any relabeling.
	MaxRepoLabel uint64
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

// --- datastore.VersionInitializer interface ----

// InitVersion initializes max label tracking for a new version if it has a parent
func (d *Data) InitVersion(uuid dvid.UUID, v dvid.VersionID) error {
	// Get the parent max label
	parents, err := datastore.GetParentsByVersion(v)
	if err != nil {
		return err
	}
	switch len(parents) {
	case 0:
		return fmt.Errorf("InitVersion(%s, %d) called on node with no parents, which shouldn't be possible for branch", uuid, v)
	case 1:
		maxLabel, ok := d.MaxLabel[parents[0]]
		if !ok {
			return fmt.Errorf("parent of uuid %s had no max label", uuid)
		}
		d.MaxLabel[v] = maxLabel

		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, maxLabel)
		ctx := datastore.NewVersionedCtx(d, v)
		store, err := storage.SmallDataStore()
		if err != nil {
			return fmt.Errorf("data type labelvol had error initializing store: %s\n", err.Error())
		}
		store.Put(ctx, maxLabelTKey, buf)
		return nil
	default:
		return fmt.Errorf("InitVersion(%s, %d) called on node with more than one parent: %v", uuid, v, parents)
	}
}

// --- datastore.InstanceMutator interface -----

// LoadMutable loads mutable properties of label volumes like the maximum labels
// for each version.  Note that we load these max labels from key-value pairs
// rather than data instance properties persistence, because in the case of a crash,
// the actually stored repo data structure may be out-of-date compared to the guaranteed
// up-to-date key-value pairs for max labels.
func (d *Data) LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (bool, error) {
	ctx := storage.NewDataContext(d, 0)
	store, err := storage.SmallDataStore()
	if err != nil {
		return false, fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	ch := make(chan *storage.KeyValue)

	// Start appropriate migration function if any.
	var saveRequired bool

	switch storedVersion {
	case 0:
		// Need to update all max labels and set repo-level max label.
		saveRequired = true
		go d.migrateMaxLabels(root, wg, ch)
	default:
		// Load in each version max label without migration.
		go d.loadMaxLabels(wg, ch)

		// Load in the repo-wide max label.
		data, err := store.Get(ctx, maxRepoLabelTKey)
		if err != nil {
			return false, err
		}
		d.MaxRepoLabel = binary.LittleEndian.Uint64(data)
	}

	// Send the max label data per version
	minKey, err := ctx.MinVersionKey(maxLabelTKey)
	if err != nil {
		return false, err
	}
	maxKey, err := ctx.MaxVersionKey(maxLabelTKey)
	if err != nil {
		return false, err
	}
	keysOnly := false
	if err = store.SendRange(minKey, maxKey, keysOnly, ch); err != nil {
		return false, err
	}
	wg.Wait()

	dvid.Infof("Loaded max label values for labelvol %q with repo-wide max %d\n", d.DataName(), d.MaxRepoLabel)
	return saveRequired, nil
}

func (d *Data) migrateMaxLabels(root dvid.VersionID, wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	store, err := storage.SmallDataStore()
	if err != nil {
		dvid.Errorf("Can't initializing small data store: %s\n", err.Error())
	}

	var maxRepoLabel uint64
	d.MaxLabel = make(map[dvid.VersionID]uint64)
	for {
		kv := <-ch
		if kv == nil {
			break
		}
		v, err := ctx.VersionFromKey(kv.K)
		if err != nil {
			dvid.Errorf("Can't decode key when loading mutable data for %s", d.DataName())
			continue
		}
		if len(kv.V) != 8 {
			dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
			continue
		}
		label := binary.LittleEndian.Uint64(kv.V)
		d.MaxLabel[v] = label
		if label > maxRepoLabel {
			maxRepoLabel = label
		}
	}

	// Adjust the MaxLabel data to make sure we correct for any case of child max < parent max.
	d.adjustMaxLabels(store, root)

	// Set the repo-wide max label.
	d.MaxRepoLabel = maxRepoLabel

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, maxRepoLabel)
	store.Put(ctx, maxRepoLabelTKey, buf)

	wg.Done()
	return
}

func (d *Data) adjustMaxLabels(store storage.SmallDataStorer, root dvid.VersionID) error {
	buf := make([]byte, 8)

	parentMax, ok := d.MaxLabel[root]
	if !ok {
		return fmt.Errorf("can't adjust version id %d since none exists in metadata", root)
	}
	childIDs, err := datastore.GetChildrenByVersion(root)
	if err != nil {
		return err
	}
	for _, childID := range childIDs {
		var save bool
		childMax, ok := d.MaxLabel[childID]
		if !ok {
			// set to parent max
			d.MaxLabel[childID] = parentMax
			save = true
		} else if childMax < parentMax {
			d.MaxLabel[childID] = parentMax + childMax + 1
			save = true
		}

		// save the key-value
		if save {
			binary.LittleEndian.PutUint64(buf, d.MaxLabel[childID])
			ctx := datastore.NewVersionedCtx(d, childID)
			store.Put(ctx, maxLabelTKey, buf)
		}

		// recurse for depth-first
		if err := d.adjustMaxLabels(store, childID); err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) loadMaxLabels(wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	d.MaxLabel = make(map[dvid.VersionID]uint64)
	for {
		kv := <-ch
		if kv == nil {
			wg.Done()
			return
		}
		v, err := ctx.VersionFromKey(kv.K)
		if err != nil {
			dvid.Errorf("Can't decode key when loading mutable data for %s", d.DataName())
			continue
		}
		if len(kv.V) != 8 {
			dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
			continue
		}
		label := binary.LittleEndian.Uint64(kv.V)
		d.MaxLabel[v] = label
	}
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
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()
	versionID := ctx.VersionID()

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
		if err := datastore.SaveDataByUUID(uuid, d); err != nil {
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
		// POST <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		switch action {
		case "get":
			queryValues := r.URL.Query()
			var b Bounds
			b.VoxelBounds, err = dvid.BoundsFromQueryString(r)
			if err != nil {
				server.BadRequest(w, r, "Error parsing bounds from query string: %s\n", err.Error())
				return
			}
			b.BlockBounds = b.VoxelBounds.Divide(d.BlockSize)
			b.Exact = queryValues.Get("exact") == "true"
			data, err := GetSparseVol(ctx, label, b)
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
		case "post":
			server.BadRequest(w, r, "POST of sparsevol not currently implemented\n")
			return
			// if err := d.PutSparseVol(versionID, label, r.Body); err != nil {
			// 	server.BadRequest(w, r, err.Error())
			// 	return
			// }
		default:
			server.BadRequest(w, r, "Unable to handle HTTP action %s on sparsevol endpoint", action)
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
		label, err := source.GetLabelAtPoint(ctx.VersionID(), coord)
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		data, err := GetSparseVol(ctx, label, Bounds{})
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
		data, err := GetSparseCoarseVol(ctx, label)
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

	case "maxlabel":
		// GET <api URL>/node/<UUID>/<data name>/maxlabel
		w.Header().Set("Content-Type", "application/json")
		switch action {
		case "get":
			maxlabel, ok := d.MaxLabel[versionID]
			if !ok {
				server.BadRequest(w, r, "No maximum label found for %s version %d\n", d.DataName(), versionID)
				return
			}
			fmt.Fprintf(w, "{%q: %d}", "maxlabel", maxlabel)
		default:
			server.BadRequest(w, r, "Unknown action %q requested: %s\n", action, r.URL)
			return
		}
		timedLog.Infof("HTTP maxlabel request (%s)", r.URL)

	case "nextlabel":
		// GET <api URL>/node/<UUID>/<data name>/nextlabel
		// POST <api URL>/node/<UUID>/<data name>/nextlabel
		w.Header().Set("Content-Type", "application/json")
		switch action {
		case "get":
			fmt.Fprintf(w, "{%q: %d}", "nextlabel", d.MaxRepoLabel+1)
		case "post":
			server.BadRequest(w, r, "POST on maxlabel is not supported yet.\n")
			return
		default:
			server.BadRequest(w, r, "Unknown action %q requested: %s\n", action, r.URL)
			return
		}
		timedLog.Infof("HTTP maxlabel request (%s)", r.URL)

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
		toLabel, err := d.SplitLabels(ctx.VersionID(), fromLabel, r.Body)
		if err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on split: %s", err.Error()))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %d}", "label", toLabel)
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
		if err := d.MergeLabels(ctx.VersionID(), mergeOp); err != nil {
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

// Given a stored label, make sure our max label tracking is updated.
func (d *Data) casMaxLabel(batch storage.Batch, v dvid.VersionID, label uint64) {
	d.ml_mu.Lock()
	defer d.ml_mu.Unlock()

	save := false
	maxLabel, found := d.MaxLabel[v]
	if !found {
		dvid.Errorf("Bad max label of version %d -- none found!\n", v)
		maxLabel = 0
	}
	if maxLabel < label {
		maxLabel = label
		save = true
	}
	if save {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, maxLabel)
		batch.Put(maxLabelTKey, buf)
		d.MaxLabel[v] = maxLabel

		if d.MaxRepoLabel < maxLabel {
			d.MaxRepoLabel = maxLabel
			ctx := storage.NewDataContext(d, 0)
			store, err := storage.SmallDataStore()
			if err != nil {
				dvid.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
			} else {
				store.Put(ctx, maxRepoLabelTKey, buf)
			}
		}
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

	// Make sure we aren't trying to increment a label on a locked node.
	locked, err := datastore.LockedVersion(v)
	if err != nil {
		return 0, err
	}
	if locked {
		return 0, fmt.Errorf("can't ask for new label in a locked version id %d", v)
	}

	// Increment and store.
	d.MaxRepoLabel++
	d.MaxLabel[v] = d.MaxRepoLabel

	store, err := storage.SmallDataStore()
	if err != nil {
		return 0, fmt.Errorf("can't initializing small data store: %s\n", err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.MaxRepoLabel)
	ctx := datastore.NewVersionedCtx(d, v)
	store.Put(ctx, maxLabelTKey, buf)

	ctx2 := storage.NewDataContext(d, 0)
	store.Put(ctx2, maxRepoLabelTKey, buf)

	return d.MaxRepoLabel, nil
}

// Returns RLEs for a given label where the key of the returned map is the block index
// in string format.
func (d *Data) GetLabelRLEs(v dvid.VersionID, label uint64) (dvid.BlockRLEs, error) {
	store, err := storage.SmallDataStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}

	// Get the start/end indices for this body's KeyLabelSpatialMap (b + s) keys.
	begIndex := NewTKey(label, dvid.MinIndexZYX.ToIZYXString())
	endIndex := NewTKey(label, dvid.MaxIndexZYX.ToIZYXString())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	labelRLEs := dvid.BlockRLEs{}
	var f storage.ChunkFunc = func(chunk *storage.Chunk) error {
		// Get the block index where the fromLabel is present
		_, blockStr, err := DecodeTKey(chunk.K)
		if err != nil {
			return fmt.Errorf("Can't recover block index with chunk key %v: %s\n", chunk.K, err.Error())
		}

		var blockRLEs dvid.RLEs
		if err := blockRLEs.UnmarshalBinary(chunk.V); err != nil {
			return fmt.Errorf("Unable to unmarshal RLE for label in block %v", chunk.K)
		}
		labelRLEs[blockStr] = blockRLEs
		return nil
	}
	ctx := datastore.NewVersionedCtx(d, v)
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
	begTKey := NewTKey(label, minZYX.ToIZYXString())
	endTKey := NewTKey(label, maxZYX.ToIZYXString())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	// TODO -- Make processing asynchronous so can overlap with range disk read now that
	//   there could be more processing due to bounding calcs.
	var numRuns, numBlocks uint32
	encoding := buf.Bytes()

	var f storage.ChunkFunc = func(chunk *storage.Chunk) error {
		// Make sure this block is within the optinonal bounding.
		if blockBounds.BoundedX() || blockBounds.BoundedY() {
			_, blockStr, err := DecodeTKey(chunk.K)
			if err != nil {
				return fmt.Errorf("Error decoding sparse volume key (%v): %s\n", chunk.K, err.Error())
			}
			indexZYX, err := blockStr.IndexZYX()
			if err != nil {
				return fmt.Errorf("Error decoding block coordinate (%v) for sparse volume: %s\n",
					blockStr, err.Error())
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

	if err := store.ProcessRange(ctx, begTKey, endTKey, &storage.ChunkOp{}, f); err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(encoding[8:12], numRuns)

	dvid.Debugf("[%s] label %d: found %d blocks, %d runs\n", ctx, label, numBlocks, numRuns)
	return encoding, nil
}

// PutSparseVol stores an encoded sparse volume that stays within a given forward label.
// Note that this encoded sparse volume is added to any existing sparse volume for
// a body rather than replace it.  To carve out a part of an existing sparse volume,
// use the SplitLabels().
//
// This function handles modification/deletion of all denormalized data touched by this
// sparse label volume.
//
// EVENTS
//
//
func (d *Data) PutSparseVol(v dvid.VersionID, label uint64, r io.Reader) error {
	store, err := storage.SmallDataStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %s\n", err.Error())
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	// Mark the label as dirty until done.
	iv := dvid.InstanceVersion{d.DataName(), v}
	dirtyLabels.Incr(iv, label)
	defer dirtyLabels.Decr(iv, label)

	// TODO -- Signal that we are starting a label modification.

	// Read the sparse volume from reader.
	header := make([]byte, 8)
	if _, err = io.ReadFull(r, header); err != nil {
		return err
	}
	if header[0] != dvid.EncodingBinary {
		return fmt.Errorf("sparse vol for split has unknown encoding format: %v", header[0])
	}
	var numSpans uint32
	if err = binary.Read(r, binary.LittleEndian, &numSpans); err != nil {
		return err
	}
	var mods dvid.RLEs
	if err = mods.UnmarshalBinaryReader(r, numSpans); err != nil {
		return err
	}

	// Partition the mods spans into blocks.
	var modmap dvid.BlockRLEs
	modmap, err = mods.Partition(d.BlockSize)
	if err != nil {
		return err
	}

	// Publish sparsevol mod event
	evt := datastore.SyncEvent{d.DataName(), labels.SparsevolModEvent}
	msg := datastore.SyncMessage{v, labels.DeltaSparsevol{label, modmap}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	// Get a sorted list of blocks that cover mods.
	modblks := modmap.SortedKeys()

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	var voxelsAdded int64

	for _, modblk := range modblks {

		// Get original block
		tk := NewTKey(label, modblk)
		val, err := store.Get(ctx, tk)
		if err != nil {
			return err
		}

		// If there's no original block, just write the mod block.
		if val == nil || len(val) == 0 {
			numVoxels, _ := modmap[modblk].Stats()
			voxelsAdded += int64(numVoxels)
			rleBytes, err := modmap[modblk].MarshalBinary()
			if err != nil {
				return fmt.Errorf("can't serialize modified RLEs for %d: %s\n", label, err.Error())
			}
			batch.Put(tk, rleBytes)
			continue
		}

		// if there's an original, integrate and write merged RLE.
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", modblk.Print())
		}
		voxelsAdded += rles.Add(modmap[modblk])
		rleBytes, err := rles.MarshalBinary()
		if err != nil {
			return fmt.Errorf("can't serialize modified RLEs of %d: %s\n", label, err.Error())
		}
		batch.Put(tk, rleBytes)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Batch commit during mod of %s label %d: %s\n", d.DataName(), label, err.Error())
	}

	// Publish change in label sizes.
	delta := labels.DeltaModSize{
		Label:      label,
		SizeChange: voxelsAdded,
	}
	evt = datastore.SyncEvent{d.DataName(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	// TODO -- Publish label mod end

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
	begTKey := NewTKey(label, dvid.MinIndexZYX.ToIZYXString())
	endTKey := NewTKey(label, dvid.MaxIndexZYX.ToIZYXString())

	// Process all the b+s keys and their values, which contain RLE runs for that label.
	var numBlocks uint32
	var span *dvid.Span
	var spans dvid.Spans
	keys, err := store.KeysInRange(ctx, begTKey, endTKey)
	if err != nil {
		return nil, fmt.Errorf("Cannot get keys for coarse sparse volume: %s", err.Error())
	}
	for _, tk := range keys {
		numBlocks++
		_, blockStr, err := DecodeTKey(tk)
		if err != nil {
			return nil, fmt.Errorf("Error retrieving RLE runs for label %d: %s", label, err.Error())
		}
		indexZYX, err := blockStr.IndexZYX()
		if err != nil {
			return nil, fmt.Errorf("Error decoding block coordinate (%v) for sparse volume: %s\n",
				blockStr, err.Error())
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
