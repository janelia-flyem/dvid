/*
	Package roi implements DVID support for Region-Of-Interest operations.
*/
package roi

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/roi"
	TypeName = "roi"
)

const HelpMessage = `
API for 'roi' datatype (github.com/janelia-flyem/dvid/datatype/roi)
===================================================================

Command-line:

$ dvid repo <UUID> new roi <data name> <settings...>

	Adds newly named roi data to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new roi medulla

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "medulla"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)

    Versioned      "true" or "false" (default)
    BlockSize      Size in pixels  (default: %s)
	
    ------------------

HTTP API (Level 2 REST):

Note that browsers support HTTP PUT and DELETE via javascript but only GET/POST are
included in HTML specs.  For ease of use in constructing clients, HTTP POST is used
to create or modify resources in an idempotent fashion.

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET <api URL>/node/3f8c/stuff/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of roi data.


GET  <api URL>/node/<UUID>/<data name>/roi
POST <api URL>/node/<UUID>/<data name>/roi
DEL  <api URL>/node/<UUID>/<data name>/roi  (TO DO)

    Performs operations on an ROI depending on the HTTP verb.

    Example: 

    GET <api URL>/node/3f8c/medulla/roi

    Returns the data associated with the "medulla" ROI at version 3f8c.

    The "Content-type" of the HTTP response (and usually the request) are
    "application/json" for arbitrary binary data.  Returns a list of 4-tuples:

  	"[[0, 0, 0, 1], [0, 2, 3, 5], [0, 2, 8, 9], [1, 2, 3, 4]]"

	Each element is expressed as [z, y, x0, x1], which represents blocks with the block indices
	(x0, y, z) to (x1, y, z)

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of ROI data to save/modify or get.

POST <api URL>/node/<UUID>/<data name>/ptquery

	Determines with a list of 3d points in JSON format sent by POST is within the ROI.
	Returns a list of true/false answers for each point in the same sequence as the POSTed list.

    The "Content-type" of the HTTP response (and usually the request) are
    "application/json" for arbitrary binary data.  Returns a list of 4-tuples:

  	Sent: "[[0, 100, 910], [0, 121, 900]]"

  	Returned: "[false, true]"

`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
}

// Type embeds the datastore's Type to create a unique type for keyvalue functions.
type Type struct {
	datastore.Type
}

// NewType returns a pointer to a new keyvalue Type with default values set.
func NewType() *Type {
	dtype := new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}
	return dtype
}

// --- TypeService interface ---

// NewData returns a pointer to new ROI data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	s, found, err := c.GetString("BlockSize")
	if err != nil {
		return nil, err
	}
	var blockSize dvid.Point3d
	if found {
		pt, err := dvid.StringToPoint(s, ",")
		if err != nil {
			return nil, err
		}
		if pt.NumDims() != 3 {
			return nil, fmt.Errorf("BlockSize must be 3d, not %dd", pt.NumDims())
		}
		blockSize, _ = pt.(dvid.Point3d)
	} else {
		blockSize = dvid.Point3d{voxels.DefaultBlockSize, voxels.DefaultBlockSize, voxels.DefaultBlockSize}
	}
	return &Data{basedata, blockSize}, nil
}

func (dtype *Type) Help() string {
	return fmt.Sprintf(HelpMessage, voxels.DefaultBlockSize)
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*datastore.Data
	BlockSize dvid.Point3d
}

var (
	minIndexRLE = indexRLE{dvid.MinIndexZYX, 0}
	maxIndexRLE = indexRLE{dvid.MaxIndexZYX, 0xFFFFFFFF}
)

// indexRLE is the key for block indices included in an ROI.
// Because we use dvid.IndexZYX for index byte slices, we know
// the key ordering will be Z, then Y, then X0 (and then X1).
type indexRLE struct {
	start dvid.IndexZYX
	span  uint32 // the span along X
}

func (i *indexRLE) Bytes() []byte {
	buf := new(bytes.Buffer)
	_, err := buf.Write(i.start.Bytes())
	if err != nil {
		dvid.Errorf("Error in roi.go, indexRLE.Bytes(): %s\n", err.Error())
	}
	binary.Write(buf, binary.BigEndian, i.span)
	return buf.Bytes()
}

func (i *indexRLE) IndexFromBytes(b []byte) error {
	if len(b) != 16 {
		return fmt.Errorf("Illegal byte length (%d) for ROI RLE Index", len(b))
	}
	if err := i.start.IndexFromBytes(b[0:12]); err != nil {
		return err
	}
	i.span = binary.BigEndian.Uint32(b[12:])
	return nil
}

// Tuples are (Z, Y, X0, X1)
type tuple [4]int32

func (t tuple) less(block dvid.ChunkPoint3d) bool {
	if t[0] > block[2] {
		return false
	}
	if t[1] > block[1] {
		return false
	}
	if t[3] > block[0] {
		return false
	}
	return true
}

func (t tuple) includes(block dvid.ChunkPoint3d) bool {
	if t[0] != block[2] {
		return false
	}
	if t[1] != block[1] {
		return false
	}
	if t[2] > block[0] || t[3] < block[0] {
		return false
	}
	return true
}

// Returns all (z, y, x0, x1) tuples in sorted order: z, then y, then x0.
func getROI(ctx storage.Context) ([]tuple, error) {
	db, err := storage.SmallDataStore()
	if err != nil {
		return nil, err
	}
	spans := []tuple{}
	err = db.ProcessRange(ctx, minIndexRLE.Bytes(), maxIndexRLE.Bytes(), &storage.ChunkOp{}, func(chunk *storage.Chunk) {
		indexBytes, err := ctx.IndexFromKey(chunk.K)
		if err != nil {
			dvid.Errorf("Unable to recover roi RLE from chunk key %v: %s\n", chunk.K, err.Error())
			return
		}
		index := new(indexRLE)
		if err = index.IndexFromBytes(indexBytes); err != nil {
			dvid.Errorf("Unable to get indexRLE out of []byte encoding: %s\n", err.Error())
		}
		z := index.start.Value(2)
		y := index.start.Value(1)
		x0 := index.start.Value(0)
		x1 := x0 + int32(index.span) - 1
		spans = append(spans, tuple{z, y, x0, x1})
	})
	return spans, nil
}

// Get returns a JSON-encoded byte slice of the ROI in the form of 4-tuples,
// where each tuple is [z, y, xstart, xend]
func Get(ctx storage.Context) ([]byte, error) {
	spans, err := getROI(ctx)
	if err != nil {
		return nil, err
	}
	jsonBytes, err := json.Marshal(spans)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}

// Put saves JSON-encoded data representing an ROI into the datastore.
func Put(ctx storage.Context, jsonBytes []byte) error {
	db, err := storage.SmallDataStore()
	if err != nil {
		return err
	}
	spans := []tuple{}
	err = json.Unmarshal(jsonBytes, &spans)
	if err != nil {
		return fmt.Errorf("Error trying to parse POSTed JSON: %s", err.Error())
	}
	// Delete the old key/values
	// TODO ... should just reuse DEL

	// Make sure our small data store can do batching.
	batcher, ok := db.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Unable to store ROI: small data store can't do batching!")
	}

	// Put the new key/values
	const BATCH_SIZE = 10000
	batch := batcher.NewBatch(ctx)
	for i, span := range spans {
		index := indexRLE{
			start: dvid.IndexZYX{span[2], span[1], span[0]},
			span:  uint32(span[3] - span[2] + 1),
		}
		batch.Put(index.Bytes(), dvid.EmptyValue())
		if (i+1)%BATCH_SIZE == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("Error on batch PUT at span %d: %s\n", i, err.Error())
			}
			batch = batcher.NewBatch(ctx)
		}
	}
	if len(spans)%BATCH_SIZE != 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("Error on last batch PUT: %s\n", err.Error())
		}
	}
	return nil
}

// Returns the current span index and whether given point is included in span.
func (d *Data) seekSpan(pt dvid.Point3d, spans []tuple, curSpanI int) (int, bool) {
	numSpans := len(spans)
	if curSpanI >= numSpans {
		return curSpanI, false
	}

	// Determine current block index of point.
	chunkPt, _ := pt.Chunk(d.BlockSize).(dvid.ChunkPoint3d)

	// Keep going through spans until we are equal to or past the chunk point.
	for {
		curSpan := spans[curSpanI]
		if curSpan.less(chunkPt) {
			curSpanI++
		} else {
			if curSpan.includes(chunkPt) {
				return curSpanI, true
			} else {
				return curSpanI, false
			}
		}
		if curSpanI >= numSpans {
			return curSpanI, false
		}
	}
}

// PointQuery checks if a JSON-encoded list of voxel points are within an ROI.
// It returns a JSON list of bools, each corresponding to the original list of points.
func (d *Data) PointQuery(ctx storage.Context, jsonBytes []byte) ([]byte, error) {
	// Convert given set of JSON-encoded points to a sorted list of points.
	var pts dvid.ListPoint3d
	if err := json.Unmarshal(jsonBytes, &pts); err != nil {
		return nil, err
	}
	sort.Sort(dvid.ByZYX(pts))

	// Get the ROI.  The spans are ordered in z, y, then x0.
	spans, err := getROI(ctx)
	if err != nil {
		return nil, err
	}

	// Iterate through each query point, using the ordering to make the search more efficient.
	inclusions := make([]bool, len(pts))
	var included bool
	curSpan := 0
	for i := 0; i < len(pts); i++ {
		curSpan, included = d.seekSpan(pts[i], spans, curSpan)
		inclusions[i] = included
	}

	// Convert to JSON
	inclusionsJSON, err := json.Marshal(inclusions)
	if err != nil {
		return nil, err
	}
	return inclusionsJSON, nil
}

// --- DataService interface ---

func (d *Data) Help() string {
	return fmt.Sprintf(HelpMessage, voxels.DefaultBlockSize)
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("Unknown command.  Data '%s' [%s] does not support '%s' command.",
		d.DataName(), d.TypeName(), request.TypeCommand())
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(requestCtx context.Context, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	// Get repo and version ID of this request
	_, versions, err := datastore.FromContext(requestCtx)
	if err != nil {
		server.BadRequest(w, r, "Error: %q ServeHTTP has invalid context: %s\n", d.DataName, err.Error())
		return
	}

	// Construct storage.Context using a particular version of this Data
	var versionID dvid.VersionID
	if len(versions) > 0 {
		versionID = versions[0]
	}
	storeCtx := datastore.NewVersionedContext(d, versionID)

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API specification")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return
	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return
	default:
	}

	// Get the key and process request
	var comment string
	command := parts[3]
	method := strings.ToLower(r.Method)
	switch command {
	case "roi":
		switch method {
		case "get":
			jsonBytes, err := Get(storeCtx)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
			comment = fmt.Sprintf("HTTP GET ROI '%s': %d bytes\n", d.DataName(), len(jsonBytes))
		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			err = Put(storeCtx, data)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			comment = fmt.Sprintf("HTTP POST ROI '%s': %d bytes\n", d.DataName(), len(data))
		}
	case "ptquery":
		switch method {
		case "get":
			server.BadRequest(w, r, "ptquery requires POST with list of points")
			return
		case "post":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			jsonBytes, err := d.PointQuery(storeCtx, data)
			if err != nil {
				server.BadRequest(w, r, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, string(jsonBytes))
			comment = fmt.Sprintf("HTTP POST ptquery '%s'\n", d.DataName())
		}
	case "partition":
		fmt.Fprintf(w, "Partitioning not available yet...\n")
	default:
		w.Header().Set("Content-Type", "text/plain")
		server.BadRequest(w, r, "Can only handle GET or POST HTTP verbs")
		return
	}

	timedLog.Infof(comment)
}
