/*
	Package labelvol supports label-specific sparse volumes.  It can be synced
	with labelblk and is a different view of 64-bit label data.
*/
package labelvol

import (
	"bytes"
	"compress/gzip"
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

	lz4 "github.com/janelia-flyem/go/golz4"
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

    Sync           Name of labelblk data to which this labelvol data should be synced.
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

	Returns a sparse volume with voxels of the given label in encoded RLE format.  The returned
	data can be optionally compressed using the "compression" option below.

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
    exact   "false" if RLEs can extend a bit outside voxel bounds within border blocks.
            This will give slightly faster responses. 

    compression   Allows retrieval of data in "lz4" and "gzip"
                  compressed format.


HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>?<options>

	Returns:
		200 (OK) if a sparse volume of the given label exists within any optional bounds.
		204 (No Content) if there is no sparse volume for the given label within any optional bounds.

	Note that for speed, the optional bounds are always expanded to the block-aligned containing
	subvolume, i.e., it's as if exact=false for the corresponding GET.

    GET Query-string Options:

    minx    Spans must be equal to or larger than this minimum x voxel coordinate.
    maxx    Spans must be equal to or smaller than this maximum x voxel coordinate.
    miny    Spans must be equal to or larger than this minimum y voxel coordinate.
    maxy    Spans must be equal to or smaller than this maximum y voxel coordinate.
    minz    Spans must be equal to or larger than this minimum z voxel coordinate.
    maxz    Spans must be equal to or smaller than this maximum z voxel coordinate.


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


POST <api URL>/node/<UUID>/<data name>/split/<label>[?splitlabel=X]

	Splits a portion of a label's voxels into a new label or, if "splitlabel" is specified
	as an optional query string, the given split label.  Returns the following JSON:

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

	NOTE 1: The POSTed split sparse volume must be a subset of the given label's voxels.  You cannot
	give an arbitrary sparse volume that may span multiple labels.

	NOTE 2: If a split label is specified, it is the client's responsibility to make sure the given
	label will not create conflict with labels in other versions.  It should primarily be used in
	chain operations like "split-coarse" followed by "split" using voxels, where the new label
	created by the split coarse is used as the split label for the smaller, higher-res "split".

POST <api URL>/node/<UUID>/<data name>/split-coarse/<label>[?splitlabel=X]

	Splits a portion of a label's blocks into a new label or, if "splitlabel" is specified
	as an optional query string, the given split label.  Returns the following JSON:

		{ "label": <new label> }

	This request requires a binary sparse volume in the POSTed body with the following 
	encoded RLE format, which is similar to the "split" request format but uses block
	instead of voxel coordinates:

		All integers are in little-endian format.

	    byte     Payload descriptor:
	               Set to 0 to indicate it's a binary sparse volume.
	    uint8    Number of dimensions
	    uint8    Dimension of run (typically 0 = X)
	    byte     Reserved (to be used later)
	    uint32    # Blocks [TODO.  0 for now]
	    uint32    # Spans
	    Repeating unit of:
	        int32   Coordinate of run start (dimension 0)
	        int32   Coordinate of run start (dimension 1)
	        int32   Coordinate of run start (dimension 2)
			  ...
	        int32   Length of run

	The Notes for "split" endpoint above are applicable to this "split-coarse" endpoint.
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

	ml_mu sync.RWMutex // For atomic access of MaxLabel and MaxRepoLabel

	// The maximum label id found in each version of this instance.
	// Can be unset if no new label was added at that version, in which case
	// you must traverse DAG to find max label of parent.
	MaxLabel map[dvid.VersionID]uint64

	// The maximum label for this instance in the entire repo.  This allows us to do
	// conflict-free merges without any relabeling.  Note that relabeling (rebasing)
	// is required if we move data between repos, e.g., when pushing remote nodes,
	// since we have no control over which labels were created remotely and there
	// could be conflicts between the local and remote repos.  When mutations only
	// occur within a single repo, however, this atomic label allows us to prevent
	// conflict across all versions within this repo.
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

	// Keep track of sync operations that could be updating the data.
	// TODO: Think about making this per label since sync status is pessimistic, assuming
	// all labels are being updated.
	datastore.Updater
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
// for each version.  Note that we load these max labels from key-value pairs
// rather than data instance properties persistence, because in the case of a crash,
// the actually stored repo data structure may be out-of-date compared to the guaranteed
// up-to-date key-value pairs for max labels.
func (d *Data) LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (bool, error) {
	ctx := storage.NewDataContext(d, 0)
	store, err := storage.MutableStore()
	if err != nil {
		return false, fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
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
		dvid.Infof("Migrating old version of labelvol %q to new version\n", d.DataName())
		go d.migrateMaxLabels(root, wg, ch)
	default:
		// Load in each version max label without migration.
		go d.loadMaxLabels(wg, ch)
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
	if err = store.RawRangeQuery(minKey, maxKey, keysOnly, ch); err != nil {
		return false, err
	}
	wg.Wait()

	dvid.Infof("Loaded max label values for labelvol %q with repo-wide max %d\n", d.DataName(), d.MaxRepoLabel)
	return saveRequired, nil
}

func (d *Data) migrateMaxLabels(root dvid.VersionID, wg *sync.WaitGroup, ch chan *storage.KeyValue) {
	ctx := storage.NewDataContext(d, 0)
	store, err := storage.MutableStore()
	if err != nil {
		dvid.Errorf("Can't initializing small data store: %v\n", err)
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

func (d *Data) adjustMaxLabels(store storage.MutableStorer, root dvid.VersionID) error {
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
	var repoMax uint64
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
		if label > repoMax {
			repoMax = label
		}
	}

	// Load in the repo-wide max label.
	store, err := storage.MutableStore()
	if err != nil {
		dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	data, err := store.Get(ctx, maxRepoLabelTKey)
	if err != nil {
		dvid.Errorf("Error getting repo-wide max label: %v\n", err)
		return
	}
	if data == nil || len(data) != 8 {
		dvid.Errorf("Could not load repo-wide max label for instance %q.  Only got %d bytes, not 64-bit label.\n", d.DataName(), len(data))
		dvid.Errorf("Using max label across versions: %d\n", repoMax)
		d.MaxRepoLabel = repoMax
	} else {
		d.MaxRepoLabel = binary.LittleEndian.Uint64(data)
		if d.MaxRepoLabel < repoMax {
			dvid.Errorf("Saved repo-wide max for instance %q was %d, changed to largest version max %d\n", d.DataName(), d.MaxRepoLabel, repoMax)
			d.MaxRepoLabel = repoMax
		}
	}
	wg.Done()
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
			server.BadRequest(w, r, err)
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := datastore.SaveDataByUUID(uuid, d); err != nil {
			server.BadRequest(w, r, err)
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
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "sparsevol":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		// POST <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		// HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if label == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
			return
		}
		queryStrings := r.URL.Query()
		var b Bounds
		b.VoxelBounds, err = dvid.BoundsFromQueryString(r)
		if err != nil {
			server.BadRequest(w, r, "Error parsing bounds from query string: %v\n", err)
			return
		}
		b.BlockBounds = b.VoxelBounds.Divide(d.BlockSize)
		b.Exact = true
		if queryStrings.Get("exact") == "false" {
			b.Exact = false
		}

		compression := queryStrings.Get("compression")

		switch action {
		case "get":
			data, err := GetSparseVol(ctx, label, b)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/octet-stream")
			switch compression {
			case "":
				_, err = w.Write(data)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
			case "lz4":
				compressed := make([]byte, lz4.CompressBound(data))
				var n, outSize int
				if outSize, err = lz4.Compress(data, compressed); err != nil {
					server.BadRequest(w, r, err)
					return
				}
				compressed = compressed[:outSize]
				if n, err = w.Write(compressed); err != nil {
					server.BadRequest(w, r, err)
					return
				}
				if n != outSize {
					errmsg := fmt.Sprintf("Only able to write %d of %d lz4 compressed bytes\n", n, outSize)
					dvid.Errorf(errmsg)
					server.BadRequest(w, r, errmsg)
					return
				}
			case "gzip":
				gw := gzip.NewWriter(w)
				if _, err = gw.Write(data); err != nil {
					server.BadRequest(w, r, err)
					return
				}
				if err = gw.Close(); err != nil {
					server.BadRequest(w, r, err)
					return
				}
			default:
				server.BadRequest(w, r, "unknown compression type %q", compression)
				return
			}

		case "head":
			w.Header().Set("Content-type", "text/html")
			found, err := FoundSparseVol(ctx, label, b)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if found {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return

		case "post":
			server.BadRequest(w, r, "POST of sparsevol not currently implemented\n")
			return
			// if err := d.PutSparseVol(versionID, label, r.Body); err != nil {
			// 	server.BadRequest(w, r, err)
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
			server.BadRequest(w, r, err)
			return
		}
		source, err := d.GetSyncedLabelblk(versionID)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		label, err := source.GetLabelAtPoint(ctx.VersionID(), coord)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if label == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
			return
		}
		data, err := GetSparseVol(ctx, label, Bounds{})
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, coord, r.URL)

	case "sparsevol-coarse":
		// GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires label ID to follow 'sparsevol-coarse' command")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if label == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
			return
		}
		data, err := GetSparseCoarseVol(ctx, label)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		_, err = w.Write(data)
		if err != nil {
			server.BadRequest(w, r, err)
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
		// POST <api URL>/node/<UUID>/<data name>/split/<label>[?splitlabel=X]
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
			server.BadRequest(w, r, err)
			return
		}
		if fromLabel == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
			return
		}
		var splitLabel uint64
		queryStrings := r.URL.Query()
		splitStr := queryStrings.Get("splitlabel")
		if splitStr != "" {
			splitLabel, err = strconv.ParseUint(splitStr, 10, 64)
			if err != nil {
				server.BadRequest(w, r, "Bad parameter for 'splitlabel' query string (%q).  Must be uint64.\n", splitStr)
			}
		}
		toLabel, err := d.SplitLabels(ctx.VersionID(), fromLabel, splitLabel, r.Body)
		if err != nil {
			server.BadRequest(w, r, fmt.Sprintf("split: %v", err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %d}", "label", toLabel)
		timedLog.Infof("HTTP split request (%s)", r.URL)

	case "split-coarse":
		// POST <api URL>/node/<UUID>/<data name>/split-coarse/<label>[?splitlabel=X]
		if action != "post" {
			server.BadRequest(w, r, "Split-coarse requests must be POST actions.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
			return
		}
		fromLabel, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if fromLabel == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
			return
		}
		var splitLabel uint64
		queryStrings := r.URL.Query()
		splitStr := queryStrings.Get("splitlabel")
		if splitStr != "" {
			splitLabel, err = strconv.ParseUint(splitStr, 10, 64)
			if err != nil {
				server.BadRequest(w, r, "Bad parameter for 'splitlabel' query string (%q).  Must be uint64.\n", splitStr)
			}
		}
		toLabel, err := d.SplitCoarseLabels(ctx.VersionID(), fromLabel, splitLabel, r.Body)
		if err != nil {
			server.BadRequest(w, r, fmt.Sprintf("split-coarse: %v", err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %d}", "label", toLabel)
		timedLog.Infof("HTTP split-coarse request (%s)", r.URL)

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
			server.BadRequest(w, r, fmt.Sprintf("Bad merge op JSON: %v", err))
			return
		}
		mergeOp, err := tuple.Op()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.MergeLabels(ctx.VersionID(), mergeOp); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on merge: %v", err))
			return
		}
		timedLog.Infof("HTTP merge request (%s)", r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call %q for labelvol data %q.  See API help.",
			parts[3], d.DataName())
	}
}

// Given a stored label, make sure our max label tracking is updated.
func (d *Data) casMaxLabel(batch storage.Batch, v dvid.VersionID, label uint64) {
	d.ml_mu.Lock()
	defer d.ml_mu.Unlock()

	save := false
	maxLabel, found := d.MaxLabel[v]
	if !found {
		dvid.Infof("Bad max label of version %d -- none found!\n", v)
		maxLabel = 1
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
			store, err := storage.MutableStore()
			if err != nil {
				dvid.Errorf("Data type labelvol had error initializing store: %v\n", err)
			} else {
				store.Put(ctx, maxRepoLabelTKey, buf)
			}
		}
	}
	if err := batch.Commit(); err != nil {
		dvid.Errorf("batch put: %v\n", err)
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

	store, err := storage.MutableStore()
	if err != nil {
		return 0, fmt.Errorf("can't initializing small data store: %v\n", err)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.MaxRepoLabel)
	ctx := datastore.NewVersionedCtx(d, v)
	if err := store.Put(ctx, maxLabelTKey, buf); err != nil {
		return 0, err
	}

	ctx2 := storage.NewDataContext(d, 0)
	if err := store.Put(ctx2, maxRepoLabelTKey, buf); err != nil {
		return 0, err
	}

	return d.MaxRepoLabel, nil
}

// Returns RLEs for a given label where the key of the returned map is the block index
// in string format.
func (d *Data) GetLabelRLEs(v dvid.VersionID, label uint64) (dvid.BlockRLEs, error) {
	store, err := storage.MutableStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
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
			return fmt.Errorf("Can't recover block index with chunk key %v: %v\n", chunk.K, err)
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
	dvid.Infof("Found %d blocks with label %d\n", len(labelRLEs), label)
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

// FoundSparseVol returns true if a sparse volume is found for the given label
// within the given bounds.
func FoundSparseVol(ctx storage.Context, label uint64, bounds Bounds) (bool, error) {
	store, err := storage.MutableStore()
	if err != nil {
		return false, fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
	}

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

	// Scan through all keys for this range to see if we have any hits.
	var found bool
	keyCh := make(storage.KeyChan, 100)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			key := <-keyCh
			if key == nil {
				return
			}

			// Make sure this block is within the optional bounding.
			tk, err := ctx.TKeyFromKey(key)
			if err != nil {
				dvid.Errorf("Unable to get TKey from Key (%v): %v\n", key, err)
				return
			}
			if blockBounds.BoundedX() || blockBounds.BoundedY() {
				_, blockStr, err := DecodeTKey(tk)
				if err != nil {
					dvid.Errorf("Error decoding sparse volume key (%v): %v\n", key, err)
					return
				}
				indexZYX, err := blockStr.IndexZYX()
				if err != nil {
					dvid.Errorf("Error decoding block coordinate (%v) for sparse volume: %v\n", blockStr, err)
					return
				}
				blockX, blockY, _ := indexZYX.Unpack()
				if blockBounds.OutsideX(blockX) || blockBounds.OutsideY(blockY) {
					continue
				}
			}

			// We have a valid key
			found = true
			return
		}
	}()

	if err := store.SendKeysInRange(ctx, begTKey, endTKey, keyCh); err != nil {
		return false, err
	}
	wg.Wait()

	return found, nil
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
	store, err := storage.MutableStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
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
				return fmt.Errorf("Error decoding sparse volume key (%v): %v\n", chunk.K, err)
			}
			indexZYX, err := blockStr.IndexZYX()
			if err != nil {
				return fmt.Errorf("Error decoding block coordinate (%v) for sparse volume: %v\n", blockStr, err)
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
				return fmt.Errorf("Error in adjusting RLEs to bounds: %v\n", err)
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
	store, err := storage.MutableStore()
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
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
				return fmt.Errorf("can't serialize modified RLEs for %d: %v\n", label, err)
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
			return fmt.Errorf("can't serialize modified RLEs of %d: %v\n", label, err)
		}
		batch.Put(tk, rleBytes)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Batch commit during mod of %s label %d: %v\n", d.DataName(), label, err)
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
	store, err := storage.MutableStore()
	if err != nil {
		return nil, fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
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
		return nil, fmt.Errorf("Cannot get keys for coarse sparse volume: %v", err)
	}
	for _, tk := range keys {
		numBlocks++
		_, blockStr, err := DecodeTKey(tk)
		if err != nil {
			return nil, fmt.Errorf("Error retrieving RLE runs for label %d: %v", label, err)
		}
		indexZYX, err := blockStr.IndexZYX()
		if err != nil {
			return nil, fmt.Errorf("Error decoding block coordinate (%v) for sparse volume: %v\n", blockStr, err)
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
