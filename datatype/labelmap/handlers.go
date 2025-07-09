// Handler functions for HTTP requests.

package labelmap

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	pb "google.golang.org/protobuf/proto"
)

func (d *Data) handleLabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/label/<coord>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires coord to follow 'label' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	coord, err := dvid.StringToPoint3d(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	labels, err := d.GetLabelPoints(ctx.VersionID(), []dvid.Point3d{coord}, scale, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-type", "application/json")
	jsonStr := fmt.Sprintf(`{"Label": %d}`, labels[0])
	fmt.Fprint(w, jsonStr)

	timedLog.Infof("HTTP GET label at %s (%s)", parts[4], r.URL)
}

func (d *Data) handleLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/labels
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch labels query must be a GET request")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad GET request body for batch query: %v", err)
		return
	}
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	hash := queryStrings.Get("hash")
	if err := checkContentHash(hash, data); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	var coords []dvid.Point3d
	if err := json.Unmarshal(data, &coords); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad labels request JSON: %v", err))
		return
	}
	labels, err := d.GetLabelPoints(ctx.VersionID(), coords, scale, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, label := range labels {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", label)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch label-at-point query of %d points (%s)", len(coords), r.URL)
}

func (d *Data) handleMapping(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/mapping
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch mapping query must be a GET request")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad GET request body for batch query: %v", err)
		return
	}
	queryStrings := r.URL.Query()
	hash := queryStrings.Get("hash")
	if err := checkContentHash(hash, data); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	var supervoxels []uint64
	if err := json.Unmarshal(data, &supervoxels); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad mapping request JSON: %v", err))
		return
	}
	svmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, "couldn't get mapping for data %q, version %d: %v", d.DataName(), ctx.VersionID(), err)
		return
	}
	labels, found, err := svmap.MappedLabels(ctx.VersionID(), supervoxels)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if queryStrings.Get("nolookup") != "true" {
		labels, err = d.verifyMappings(ctx, supervoxels, labels, found)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, label := range labels {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", label)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch mapping query of %d labels (%s)", len(labels), r.URL)
}

func (d *Data) handleSupervoxelSplits(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/supervoxel-splits
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "The /supervoxel-splits endpoint is GET only")
		return
	}
	svmap, err := getMapping(d, ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, "couldn't get mapping for data %q, version %d: %v", d.DataName(), ctx.VersionID(), err)
		return
	}
	splitsJSON, err := svmap.SupervoxelSplitsJSON(ctx.VersionID())
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, splitsJSON)

	timedLog.Infof("HTTP GET supervoxel splits query (%s)", r.URL)
}

func (d *Data) handleBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>[?compression=...]
	// POST <api URL>/node/<UUID>/<data name>/blocks[?compression=...]
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}

	compression := queryStrings.Get("compression")
	downscale := queryStrings.Get("downres") == "true"
	supervoxels := queryStrings.Get("supervoxels") == "true"

	if strings.ToLower(r.Method) == "get" {
		if len(parts) < 6 {
			server.BadRequest(w, r, "must specify size and offset with GET /blocks endpoint")
			return
		}
		sizeStr, offsetStr := parts[4], parts[5]
		subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if subvol.StartPoint().NumDims() != 3 || subvol.Size().NumDims() != 3 {
			server.BadRequest(w, r, "must specify 3D subvolumes", subvol.StartPoint(), subvol.EndPoint())
			return
		}

		// Make sure subvolume gets align with blocks
		if !dvid.BlockAligned(subvol, d.BlockSize()) {
			server.BadRequest(w, r, "cannot use labels via 'blocks' endpoint in non-block aligned geometry %s -> %s", subvol.StartPoint(), subvol.EndPoint())
			return
		}

		if err := d.sendBlocksVolume(ctx, w, supervoxels, scale, subvol, compression); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP GET blocks at size %s, offset %s (%s)", parts[4], parts[5], r.URL)
	} else {
		var indexing bool
		if queryStrings.Get("noindexing") != "true" {
			indexing = true
		}
		if err := d.storeBlocks(ctx, r.Body, scale, downscale, compression, indexing); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP POST blocks, indexing = %t, downscale = %t (%s)", indexing, downscale, r.URL)
	}
}

func (d *Data) handleIngest(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// POST <api URL>/node/<UUID>/<data name>/ingest-supervoxels[?scale=...]
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	if err := d.ingestBlocks(ctx, r.Body, scale); err != nil {
		server.BadRequest(w, r, err)
	}
	timedLog.Infof("HTTP POST ingest-supervoxels %q, scale = %d", d.DataName(), scale)
}

func (d *Data) handleProximity(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/proximity/<label 1>,<label 2>
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	label1, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	label2, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label1 == 0 || label2 == 0 {
		server.BadRequest(w, r, "there is no index for protected label 0")
		return
	}
	if label1 == label2 {
		server.BadRequest(w, r, "the two supplied labels were identical")
		return
	}

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, fmt.Errorf("only GET action allowed on /proximity endpoint"))
		return
	}

	idx1, err := getCachedLabelIndex(d, ctx.VersionID(), label1)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	idx2, err := getCachedLabelIndex(d, ctx.VersionID(), label2)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if idx1 == nil || idx2 == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	jsonBytes, err := d.getProximity(ctx, idx1, idx2)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))

	timedLog.Infof("HTTP %s proximity for labels %d, %d (%s)", r.Method, label1, label2, r.URL)
}

func (d *Data) handleIndexVersion(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET  <api URL>/node/<UUID>/<data name>/index-version/<label>
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "there is no index for protected label 0")
		return
	}

	switch strings.ToLower(r.Method) {
	case "get":
		versionID, err := getLabelIndexVersion(ctx, label)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		// Translate from VersionID to UUID.
		uuid, err := datastore.UUIDFromVersion(versionID)
		if err != nil {
			server.BadRequest(w, r, fmt.Errorf("could not convert version ID %d to UUID: %v", versionID, err))
			return
		}
		// Write the UUID as a string to response.
		w.Header().Set("Content-type", "text/plain")
		if _, err := w.Write([]byte(uuid)); err != nil {
			server.BadRequest(w, r, fmt.Errorf("error writing UUID %s to response: %v", uuid, err))
			return
		}

	default:
		server.BadRequest(w, r, "only GET action allowed for /index-version endpoint")
		return
	}

	timedLog.Infof("HTTP %s index-version for label %d (%s)", r.Method, label, r.URL)
}

func (d *Data) handleIndex(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET  <api URL>/node/<UUID>/<data name>/index/<label>
	// POST <api URL>/node/<UUID>/<data name>/index/<label>
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}
	metadata := (queryStrings.Get("metadata-only") == "true")
	mutidStr := queryStrings.Get("mutid")
	var mutID uint64
	var err error
	if mutidStr != "" {
		mutID, err = strconv.ParseUint(mutidStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "mutid query string cannot be parsed: %v", err)
			return
		}
	}

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "there is no index for protected label 0")
		return
	}

	switch strings.ToLower(r.Method) {
	case "post":
		if r.Body == nil {
			server.BadRequest(w, r, fmt.Errorf("no data POSTed"))
			return
		}
		serialization, err := io.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		idx := new(labels.Index)
		if err := pb.Unmarshal(serialization, idx); err != nil {
			server.BadRequest(w, r, err)
		}
		if idx.Label != label {
			server.BadRequest(w, r, "serialized Index was for label %d yet was POSTed to label %d", idx.Label, label)
			return
		}
		if len(idx.Blocks) == 0 {
			if err := deleteLabelIndex(ctx, label); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP POST index for label %d (%s) -- empty index so deleted index", label, r.URL)
			return
		}
		if err := putCachedLabelIndex(d, ctx.VersionID(), idx); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "get":
		var idx *labels.Index
		if mutidStr != "" {
			idx, err = d.getMutcache(ctx.VersionID(), mutID, label)
		} else {
			idx, err = getCachedLabelIndex(d, ctx.VersionID(), label)
		}
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if idx == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if metadata {
			w.Header().Set("Content-type", "application/json")
			fmt.Fprintf(w, `{"num_voxels":%d,"last_mutid":%d,"last_mod_time":"%s","last_mod_user":"%s","last_mod_app":"%s"}`,
				idx.NumVoxels(), idx.LastMutid, idx.LastModTime, idx.LastModUser, idx.LastModApp)
		} else {
			serialization, err := pb.Marshal(idx)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/octet-stream")
			n, err := w.Write(serialization)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if n != len(serialization) {
				server.BadRequest(w, r, "unable to write all %d bytes of serialized label %d index: only %d bytes written", len(serialization), label, n)
				return
			}
		}

	default:
		server.BadRequest(w, r, "only POST or GET action allowed for /index endpoint")
		return
	}

	timedLog.Infof("HTTP %s index for label %d (%s)", r.Method, label, r.URL)
}

func (d *Data) handleExistingLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/existing-labels

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "only GET action allowed for /existing-labels endpoint")
		return
	}

	if err := d.writeExistingIndices(ctx, w, nil); err != nil {
		dvid.Errorf("unable to write existing labels: %v", err)
	}
}

func (d *Data) handleListLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/listlabels
	const MaxReturnedLabels = 10000000
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	var err error
	var start, number uint64
	var sizes bool
	startStr := queryStrings.Get("start")
	numberStr := queryStrings.Get("number")
	if queryStrings.Get("sizes") == "true" {
		sizes = true
	}
	if startStr != "" {
		start, err = strconv.ParseUint(startStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	}
	if numberStr != "" {
		number, err = strconv.ParseUint(numberStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	} else {
		number = MaxReturnedLabels
	}
	if number > MaxReturnedLabels {
		number = MaxReturnedLabels
	}

	method := strings.ToLower(r.Method)
	switch method {
	case "get":
	default:
		server.BadRequest(w, r, "only GET actions allowed for /listlabels endpoint")
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")
	numLabels, err := d.listLabels(ctx, start, number, sizes, w)
	if err != nil {
		// Because streaming HTTP writes precludes ability to signal error, we cap with four 0 uint64.
		for i := 0; i < 4; i++ {
			binary.Write(w, binary.LittleEndian, uint64(0))
		}
		dvid.Errorf("Error in trying to transmit label list stream: %v\n", err)
	}

	timedLog.Infof("HTTP %s listlabels for %d labels (%s)", method, numLabels, r.URL)
}

func (d *Data) handleIndices(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/indices
	// POST <api URL>/node/<UUID>/<data name>/indices
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	method := strings.ToLower(r.Method)
	switch method {
	case "post":
	case "get":
	default:
		server.BadRequest(w, r, "only GET and POST actions allowed for /indices endpoint")
		return
	}
	if r.Body == nil {
		server.BadRequest(w, r, fmt.Errorf("expected data to be sent for /indices request"))
		return
	}
	dataIn, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, err)
	}
	if method == "post" {
		numAdded, numDeleted, err := putProtoLabelIndices(ctx, dataIn)
		if err != nil {
			server.BadRequest(w, r, "error putting protobuf label indices: %v", err)
			return
		}
		timedLog.Infof("HTTP %s indices - %d added, %d deleted (%s)", method, numAdded, numDeleted, r.URL)
	} else { // GET
		numLabels, dataOut, err := getProtoLabelIndices(ctx, dataIn)
		if err != nil {
			server.BadRequest(w, r, "error getting protobuf label indices: %v", err)
			return
		}
		requestSize := int64(len(dataOut))
		if requestSize > server.MaxDataRequest {
			server.BadRequest(w, r, "requested payload (%d bytes) exceeds this DVID server's set limit (%d)",
				requestSize, server.MaxDataRequest)
			return
		}
		w.Header().Set("Content-type", "application/octet-stream")
		n, err := w.Write(dataOut)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if n != len(dataOut) {
			server.BadRequest(w, r, "unable to write all %d bytes of serialized label indices: only %d bytes written", len(dataOut), n)
			return
		}
		timedLog.Infof("HTTP %s indices for %d labels (%s)", method, numLabels, r.URL)
	}
}

func (d *Data) handleIndicesCompressed(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/indices-compressed
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	if r.Method != http.MethodGet {
		server.BadRequest(w, r, "only GET action allowed for /indices endpoint")
		return
	}
	if r.Body == nil {
		server.BadRequest(w, r, fmt.Errorf("expected data to be sent for /indices request"))
		return
	}
	dataIn, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, err)
	}
	var numLabels int
	var labelList []uint64
	if err := json.Unmarshal(dataIn, &labelList); err != nil {
		server.BadRequest(w, r, "expected JSON label list for GET /indices: %v", err)
		return
	}
	numLabels = len(labelList)
	if numLabels > 50000 {
		server.BadRequest(w, r, "only 50,000 label indices can be returned at a time, %d given", len(labelList))
		return
	}
	store, err := datastore.GetKeyValueDB(d)
	if err != nil {
		server.BadRequest(w, r, "can't get store for data %q", d.DataName())
		return
	}

	defer timedLog.Infof("HTTP %s compressed indices for %d labels (%s)", r.Method, numLabels, r.URL)

	headerBytes := make([]byte, 16)
	zeroBytes := make([]byte, 16)
	binary.LittleEndian.PutUint64(zeroBytes[0:8], 0)
	binary.LittleEndian.PutUint64(zeroBytes[8:16], 0)
	w.Header().Set("Content-type", "application/octet-stream")
	for i, label := range labelList {
		data, err := getLabelIndexCompressed(store, ctx, label)
		if err != nil {
			w.Write(zeroBytes)
			dvid.Errorf("Error reading streaming compressed label index at pos %d, label %d: %v\n", i, label, err)
			return
		}
		length := uint64(len(data))
		binary.LittleEndian.PutUint64(headerBytes[0:8], length)
		binary.LittleEndian.PutUint64(headerBytes[8:16], label)

		if _, err = w.Write(headerBytes); err != nil {
			w.Write(zeroBytes)
			dvid.Errorf("Error writing header of streaming compressed label index at pos %d, label %d: %v\n", i, label, err)
			return
		}

		if length > 0 {
			if _, err = w.Write(data); err != nil {
				dvid.Errorf("Error writing data of streaming compressed label index at pos %d, label %d: %v\n", i, label, err)
				return
			}
		}
	}
}

func (d *Data) handleMappings(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// POST <api URL>/node/<UUID>/<data name>/mappings
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	format := queryStrings.Get("format")

	switch strings.ToLower(r.Method) {
	case "post":
		if r.Body == nil {
			server.BadRequest(w, r, fmt.Errorf("no data POSTed"))
			return
		}
		serialization, err := io.ReadAll(r.Body)
		if err != nil {
			server.BadRequest(w, r, err)
		}
		var mappings proto.MappingOps
		if err := pb.Unmarshal(serialization, &mappings); err != nil {
			server.BadRequest(w, r, err)
		}
		if err := d.ingestMappings(ctx, &mappings); err != nil {
			server.BadRequest(w, r, err)
		}
		timedLog.Infof("HTTP POST %d mappings (%s)", len(mappings.Mappings), r.URL)

	case "get":
		if err := d.writeMappings(w, ctx.VersionID(), (format == "binary")); err != nil {
			server.BadRequest(w, r, "unable to write mappings: %v", err)
		}
		timedLog.Infof("HTTP GET mappings (%s)", r.URL)

	default:
		server.BadRequest(w, r, "only POST action allowed for /mappings endpoint")
	}
}

func (d *Data) handleMutations(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/mutations
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}

	switch strings.ToLower(r.Method) {
	case "get":
		if err := server.StreamMutationsForVersion(w, ctx.VersionUUID(), d.DataUUID()); err != nil {
			server.BadRequest(w, r, "unable to write mutations to client: %v", err)
		}
		timedLog.Infof("HTTP GET mutations (%s)", r.URL)

	default:
		server.BadRequest(w, r, "only POST action allowed for /mappings endpoint")
	}
}

func (d *Data) handleMutationsRange(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/mutations-range/<beg>/<end>
	// TODO: If feedback requests it, add querystring ?rangefmt=<format>
	//       The range format of parameters <beg> and <end> is specified by the
	//		 value of the "rangefmt" query string:
	// 		 	default:  If no query string is given, the range is in the form of version UUIDs.
	// 			"mutids":  The range is in the form of mutation IDs (uint64).
	// 			"timestamps":  The range is in the form of RFC 3339 timestamps.

	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
		if server.ThrottledHTTP(w) {
			return
		}
		defer server.ThrottledOpDone()
	}
	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "only GET action allowed for /mutations-range endpoint")
		return
	}

	rangefmt := queryStrings.Get("rangefmt")
	switch rangefmt {
	default:
		begUUID, _, err := datastore.MatchingUUID(parts[4])
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		endUUID, _, err := datastore.MatchingUUID(parts[5])
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		uuidSeq, err := datastore.GetVersionSequence(begUUID, endUUID)
		if err != nil {
			server.BadRequest(w, r, err)
		}
		if err := server.StreamMutationsForSequence(w, d.DataUUID(), uuidSeq); err != nil {
			server.BadRequest(w, r, "unable to write mutations to client: %v", err)
		}
		timedLog.Infof("HTTP GET mutations-range (%s)", r.URL)
	}
}

func (d *Data) handleHistory(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/history/<label>/<from UUID>/<to UUID>
	if len(parts) < 7 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID, 'from' UUID, and 'to' UUID to follow 'history' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "only GET action allowed for /history endpoint")
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
	fromUUID, _, err := datastore.MatchingUUID(parts[5])
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	toUUID, _, err := datastore.MatchingUUID(parts[6])
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	if err := d.GetLabelMutationHistory(w, fromUUID, toUUID, label); err != nil {
		server.BadRequest(w, r, "unable to get mutation history: %v", err)
	}
	timedLog.Infof("HTTP GET history (%s)", r.URL)
}

func (d *Data) handlePseudocolor(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 7 {
		server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
		return
	}
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}

	shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	switch plane.ShapeDimensions() {
	case 2:
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if strings.ToLower(r.Method) != "get" {
			server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
			return
		}
		lbl, err := d.NewLabels(slice, nil)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		img, err := d.GetImage(ctx.VersionID(), lbl, false, scale, roiname)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}

		// Convert to pseudocolor
		pseudoColor, err := colorImage(img)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}

		//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
		var formatStr string
		if len(parts) >= 8 {
			formatStr = parts[7]
		}
		err = dvid.WriteImageHttp(w, pseudoColor, formatStr)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	default:
		server.BadRequest(w, r, "DVID currently supports only 2d pseudocolor image requests")
		return
	}
	timedLog.Infof("HTTP GET pseudocolor with shape %s, size %s, offset %s", parts[4], parts[5], parts[6])
}

func (d *Data) handleDataRequest(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 7 {
		server.BadRequest(w, r, "'%s' must be followed by shape/size/offset", parts[3])
		return
	}
	timedLog := dvid.NewTimeLog()

	var isotropic = (parts[3] == "isotropic")
	shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	queryStrings := r.URL.Query()
	roiname := dvid.InstanceName(queryStrings.Get("roi"))
	supervoxels := queryStrings.Get("supervoxels") == "true"

	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}

	switch plane.ShapeDimensions() {
	case 2:
		slice, err := dvid.NewSliceFromStrings(planeStr, offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if strings.ToLower(r.Method) != "get" {
			server.BadRequest(w, r, "DVID does not permit 2d mutations, only 3d block-aligned stores")
			return
		}
		rawSlice, err := dvid.Isotropy2D(d.Properties.VoxelSize, slice, isotropic)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		lbl, err := d.NewLabels(rawSlice, nil)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		img, err := d.GetImage(ctx.VersionID(), lbl, supervoxels, scale, roiname)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if isotropic {
			dstW := int(slice.Size().Value(0))
			dstH := int(slice.Size().Value(1))
			img, err = img.ScaleImage(dstW, dstH)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
		var formatStr string
		if len(parts) >= 8 {
			formatStr = parts[7]
		}
		//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
		err = dvid.WriteImageHttp(w, img.Get(), formatStr)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
	case 3:
		if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
			if server.ThrottledHTTP(w) {
				return
			}
			defer server.ThrottledOpDone()
		}
		compression := queryStrings.Get("compression")
		subvol, err := dvid.NewSubvolumeFromStrings(offsetStr, sizeStr, "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if strings.ToLower(r.Method) == "get" {
			done, err := d.writeBlockToHTTP(ctx, w, subvol, compression, supervoxels, scale, roiname)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if !done {
				// Couldn't stream blocks, so need to allocate buffer and send all at once.
				lbl, err := d.NewLabels(subvol, nil)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				data, err := d.GetVolume(ctx.VersionID(), lbl, supervoxels, scale, roiname)
				if err != nil {
					server.BadRequest(w, r, err)
					return
				}
				if err := writeCompressedToHTTP(compression, data, subvol, w); err != nil {
					server.BadRequest(w, r, err)
					return
				}
			}
		} else {
			if isotropic {
				server.BadRequest(w, r, "can only POST 'raw' not 'isotropic' images")
				return
			}
			estsize := subvol.NumVoxels() * 8
			data, err := uncompressReaderData(compression, r.Body, estsize)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			mutate := queryStrings.Get("mutate") == "true"
			if err = d.PutLabels(ctx.VersionID(), subvol, data, roiname, mutate); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		}
	default:
		server.BadRequest(w, r, "DVID currently supports shapes of only 2 and 3 dimensions")
		return
	}
	timedLog.Infof("HTTP %s %s with shape %s, size %s, offset %s, scale %d", r.Method, parts[3], parts[4], parts[5], parts[6], scale)
}

func (d *Data) getSparsevolOptions(r *http.Request) (b dvid.Bounds, compression string, err error) {
	queryStrings := r.URL.Query()
	compression = queryStrings.Get("compression")

	if b.Voxel, err = dvid.OptionalBoundsFromQueryString(r); err != nil {
		err = fmt.Errorf("error parsing bounds from query string: %v", err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		err = fmt.Errorf("Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	b.Block = b.Voxel.Divide(blockSize)
	b.Exact = true
	if queryStrings.Get("exact") == "false" {
		b.Exact = false
	}
	return
}

func (d *Data) handleSupervoxels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/supervoxels/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'supervoxels' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}

	supervoxels, err := d.GetSupervoxels(ctx.VersionID(), label)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if len(supervoxels) == 0 {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, "[")
		i := 0
		for supervoxel := range supervoxels {
			fmt.Fprintf(w, "%d", supervoxel)
			i++
			if i < len(supervoxels) {
				fmt.Fprintf(w, ",")
			}
		}
		fmt.Fprintf(w, "]")
	}

	timedLog.Infof("HTTP GET supervoxels for label %d (%s)", label, r.URL)
}

func (d *Data) handleSupervoxelSizes(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/supervoxel-sizes/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'supervoxels' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}

	idx, err := GetLabelIndex(d, ctx.VersionID(), label, false)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	counts := idx.GetSupervoxelCounts()
	var supervoxels_json, sizes_json string
	for sv, count := range counts {
		supervoxels_json += strconv.FormatUint(sv, 10) + ","
		sizes_json += strconv.FormatUint(count, 10) + ","
	}
	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "{\n  ")
	fmt.Fprintf(w, `"supervoxels": [%s],`, supervoxels_json[:len(supervoxels_json)-1])
	fmt.Fprintf(w, "\n  ")
	fmt.Fprintf(w, `"sizes": [%s]`, sizes_json[:len(sizes_json)-1])
	fmt.Fprintf(w, "\n}")

	timedLog.Infof("HTTP GET supervoxel-sizes for label %d (%s)", label, r.URL)
}

func (d *Data) handleLabelmod(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/lastmod/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'lastmod' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}

	idx, err := GetLabelIndex(d, ctx.VersionID(), label, false)
	if err != nil {
		server.BadRequest(w, r, "unable to get label %d index: %v", label, err)
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, `{`)
	fmt.Fprintf(w, `"mutation id": %d, `, idx.LastMutid)
	fmt.Fprintf(w, `"last mod user": %q, `, idx.LastModUser)
	fmt.Fprintf(w, `"last mod app": %q, `, idx.LastModApp)
	fmt.Fprintf(w, `"last mod time": %q `, idx.LastModTime)
	fmt.Fprintf(w, `}`)

	timedLog.Infof("HTTP GET lastmod for label %d (%s)", label, r.URL)
}

func (d *Data) handleSize(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/size/<label>[?supervoxels=true]
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label to follow 'size' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be queried as body.\n")
		return
	}
	queryStrings := r.URL.Query()
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	size, err := GetLabelSize(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "unable to get label %d size: %v", label, err)
		return
	}
	if size == 0 {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.Header().Set("Content-type", "application/json")
		fmt.Fprintf(w, `{"voxels": %d}`, size)
	}

	timedLog.Infof("HTTP GET size for label %d, supervoxels=%t (%s)", label, isSupervoxel, r.URL)
}

func (d *Data) handleSizes(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// GET <api URL>/node/<UUID>/<data name>/sizes
	timedLog := dvid.NewTimeLog()

	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "Batch sizes query must be a GET request")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad GET request body for batch sizes query: %v", err)
		return
	}
	queryStrings := r.URL.Query()
	hash := queryStrings.Get("hash")
	if err := checkContentHash(hash, data); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	var labelList []uint64
	if err := json.Unmarshal(data, &labelList); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad mapping request JSON: %v", err))
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	sizes, err := GetLabelSizes(d, ctx.VersionID(), labelList, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "unable to get label sizes: %v", err)
		return
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "[")
	sep := false
	for _, size := range sizes {
		if sep {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "%d", size)
		sep = true
	}
	fmt.Fprintf(w, "]")

	timedLog.Infof("HTTP GET batch sizes query of %d labels (%s)", len(sizes), r.URL)
}

func (d *Data) handleSparsevolSize(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-size/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol-size' command")
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
	if strings.ToLower(r.Method) != "get" {
		server.BadRequest(w, r, "DVID does not support %s on /sparsevol-size endpoint", r.Method)
		return
	}

	queryStrings := r.URL.Query()
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	idx, err := GetLabelIndex(d, ctx.VersionID(), label, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, "problem getting label set idx on label %: %v", label, err)
		return
	}
	if idx == nil || len(idx.Blocks) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if isSupervoxel {
		idx, err = idx.LimitToSupervoxel(label)
		if err != nil {
			server.BadRequest(w, r, "error limiting label %d index to supervoxel %d: %v\n", idx.Label, label, err)
			return
		}
		if idx == nil || len(idx.Blocks) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}

	w.Header().Set("Content-type", "application/json")
	fmt.Fprintf(w, "{")
	fmt.Fprintf(w, `"voxels": %d, `, idx.NumVoxels())
	fmt.Fprintf(w, `"numblocks": %d, `, len(idx.Blocks))
	minBlock, maxBlock, err := idx.GetBlockIndices().GetBounds()
	if err != nil {
		server.BadRequest(w, r, "problem getting bounds on blocks of label %d: %v", label, err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		server.BadRequest(w, r, "Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	minx := minBlock[0] * blockSize[0]
	miny := minBlock[1] * blockSize[1]
	minz := minBlock[2] * blockSize[2]
	maxx := (maxBlock[0]+1)*blockSize[0] - 1
	maxy := (maxBlock[1]+1)*blockSize[1] - 1
	maxz := (maxBlock[2]+1)*blockSize[2] - 1
	fmt.Fprintf(w, `"minvoxel": [%d, %d, %d], `, minx, miny, minz)
	fmt.Fprintf(w, `"maxvoxel": [%d, %d, %d]`, maxx, maxy, maxz)
	fmt.Fprintf(w, "}")
}

func (d *Data) handleSparsevol(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// POST <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	// HEAD <api URL>/node/<UUID>/<data name>/sparsevol/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'sparsevol' command")
		return
	}
	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	b, compression, err := d.getSparsevolOptions(r)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	timedLog := dvid.NewTimeLog()
	switch strings.ToLower(r.Method) {
	case "get":
		labelBlockMeta, exists, err := d.constrainLabelIndex(ctx, label, scale, b, isSupervoxel)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if !exists {
			dvid.Infof("GET sparsevol on label %d was not found.\n", label)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-type", "application/octet-stream")

		switch svformatFromQueryString(r) {
		case FormatLegacyRLE:
			err = d.writeLegacyRLE(ctx, labelBlockMeta, compression, w)
		case FormatBinaryBlocks:
			err = d.writeBinaryBlocks(ctx, labelBlockMeta, compression, w)
		case FormatStreamingRLE:
			err = d.writeStreamingRLE(ctx, labelBlockMeta, compression, w)
		}
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "head":
		w.Header().Set("Content-type", "text/html")
		found, err := d.FoundSparseVol(ctx, label, b, isSupervoxel)
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
	default:
		server.BadRequest(w, r, "Unable to handle HTTP action %s on sparsevol endpoint", r.Method)
		return
	}

	timedLog.Infof("HTTP %s: sparsevol on label %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolByPoint(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-by-point/<coord>
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires coord to follow 'sparsevol-by-point' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	queryStrings := r.URL.Query()
	scale, err := getScale(queryStrings)
	if err != nil {
		server.BadRequest(w, r, "bad scale specified: %v", err)
		return
	}
	isSupervoxel := queryStrings.Get("supervoxels") == "true"

	coord, err := dvid.StringToPoint(parts[4], "_")
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	label, err := d.GetLabelAtScaledPoint(ctx.VersionID(), coord, scale, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	b, compression, err := d.getSparsevolOptions(r)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	w.Header().Set("Content-type", "application/octet-stream")

	labelBlockMeta, exists, err := d.constrainLabelIndex(ctx, label, scale, b, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if !exists {
		dvid.Infof("GET sparsevol on label %d was not found.\n", label)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch svformatFromQueryString(r) {
	case FormatLegacyRLE:
		err = d.writeLegacyRLE(ctx, labelBlockMeta, compression, w)
	case FormatBinaryBlocks:
		err = d.writeBinaryBlocks(ctx, labelBlockMeta, compression, w)
	case FormatStreamingRLE:
		err = d.writeStreamingRLE(ctx, labelBlockMeta, compression, w)
	}
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}

	timedLog.Infof("HTTP %s: sparsevol-by-point at %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevol-coarse/<label>
	if len(parts) < 5 {
		server.BadRequest(w, r, "DVID requires label ID to follow 'sparsevol-coarse' command")
		return
	}
	timedLog := dvid.NewTimeLog()

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
	isSupervoxel := queryStrings.Get("supervoxels") == "true"
	var b dvid.Bounds
	b.Voxel, err = dvid.OptionalBoundsFromQueryString(r)
	if err != nil {
		server.BadRequest(w, r, "Error parsing bounds from query string: %v\n", err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		server.BadRequest(w, r, "Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	b.Block = b.Voxel.Divide(blockSize)
	data, err := d.GetSparseCoarseVol(ctx, label, b, isSupervoxel)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if data == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-type", "application/octet-stream")
	_, err = w.Write(data)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	timedLog.Infof("HTTP %s: sparsevol-coarse on label %s (%s)", r.Method, parts[4], r.URL)
}

func (d *Data) handleSparsevolsCoarse(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/sparsevols-coarse/<start label>/<end label>
	if len(parts) < 6 {
		server.BadRequest(w, r, "DVID requires start and end label ID to follow 'sparsevols-coarse' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	begLabel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	endLabel, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if begLabel == 0 || endLabel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}

	var b dvid.Bounds
	b.Voxel, err = dvid.OptionalBoundsFromQueryString(r)
	if err != nil {
		server.BadRequest(w, r, "Error parsing bounds from query string: %v\n", err)
		return
	}
	blockSize, ok := d.BlockSize().(dvid.Point3d)
	if !ok {
		server.BadRequest(w, r, "Error: BlockSize for %s wasn't 3d", d.DataName())
		return
	}
	b.Block = b.Voxel.Divide(blockSize)

	w.Header().Set("Content-type", "application/octet-stream")
	if err := d.WriteSparseCoarseVols(ctx, w, begLabel, endLabel, b); err != nil {
		server.BadRequest(w, r, err)
		return
	}
	timedLog.Infof("HTTP %s: sparsevols-coarse on label %s to %s (%s)", r.Method, parts[4], parts[5], r.URL)
}

func (d *Data) handleMaxlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/maxlabel
	// POST <api URL>/node/<UUID>/<data name>/maxlabel/<max label>
	timedLog := dvid.NewTimeLog()
	switch strings.ToLower(r.Method) {
	case "get":
		w.Header().Set("Content-Type", "application/json")
		maxlabel, ok := d.MaxLabel[ctx.VersionID()]
		if !ok {
			server.BadRequest(w, r, "No maximum label found for %s version %d\n", d.DataName(), ctx.VersionID())
			return
		}
		fmt.Fprintf(w, "{%q: %d}", "maxlabel", maxlabel)

	case "post":
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires max label ID to follow POST /maxlabel")
			return
		}
		maxlabel, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		changed, err := d.updateMaxLabel(ctx.VersionID(), maxlabel)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if changed {
			versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
			msginfo := map[string]interface{}{
				"Action":    "post-maxlabel",
				"MaxLabel":  maxlabel,
				"UUID":      string(versionuuid),
				"Timestamp": time.Now().String(),
			}
			jsonmsg, _ := json.Marshal(msginfo)
			if err = d.PublishKafkaMsg(jsonmsg); err != nil {
				dvid.Errorf("error on sending maxlabel op to kafka: %v", err)
			}
		}

	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
	timedLog.Infof("HTTP maxlabel request (%s)", r.URL)
}

func (d *Data) handleNextlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// GET <api URL>/node/<UUID>/<data name>/nextlabel
	// POST <api URL>/node/<UUID>/<data name>/nextlabel/<number of labels>
	timedLog := dvid.NewTimeLog()
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(r.Method) {
	case "get":
		if d.NextLabel == 0 {
			fmt.Fprintf(w, `{"nextlabel": %d}`, d.MaxRepoLabel+1)
		} else {
			fmt.Fprintf(w, `{"nextlabel": %d}`, d.NextLabel+1)
		}
	case "post":
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires number of requested labels to follow POST /nextlabel")
			return
		}
		numLabels, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		start, end, err := d.newLabels(ctx.VersionID(), numLabels)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		fmt.Fprintf(w, `{"start": %d, "end": %d}`, start, end)
		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":      "post-nextlabel",
			"Start Label": start,
			"End Label":   end,
			"UUID":        string(versionuuid),
			"Timestamp":   time.Now().String(),
		}
		jsonmsg, _ := json.Marshal(msginfo)
		if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending nextlabel op to kafka: %v", err)
		}
		return
	default:
		server.BadRequest(w, r, "Unknown action %q requested: %s\n", r.Method, r.URL)
		return
	}
	timedLog.Infof("HTTP nextlabel request (%s)", r.URL)
}

func (d *Data) handleSetNextlabel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/set-nextlabel/<label>
	timedLog := dvid.NewTimeLog()
	switch strings.ToLower(r.Method) {
	case "post":
		if len(parts) < 5 {
			server.BadRequest(w, r, "DVID requires a label ID to follow POST /set-nextlabel")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if label == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as next label.\n")
			return
		}
		d.mlMu.Lock()
		defer d.mlMu.Unlock()

		d.NextLabel = label
		if err := d.persistNextLabel(); err != nil {
			server.BadRequest(w, r, err)
			return
		}

		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":     "setnextlabel",
			"Next Label": label,
			"UUID":       string(versionuuid),
			"Timestamp":  time.Now().String(),
		}
		jsonmsg, _ := json.Marshal(msginfo)
		if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending set-nextlabel to kafka: %v", err)
		}
	default:
		server.BadRequest(w, r, "Unknown action %q requested for %s\n", r.Method, r.URL)
	}
	timedLog.Infof("HTTP set-nextabel request (%s)", r.URL)
}

func (d *Data) handleSplitSupervoxel(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/split-supervoxel/<supervoxel>?<options>
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Split requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
		return
	}
	queryStrings := r.URL.Query()
	splitStr := queryStrings.Get("split")
	remainStr := queryStrings.Get("remain")
	downscale := true
	if queryStrings.Get("downres") == "false" {
		downscale = false
	}
	var err error
	var split, remain uint64
	if splitStr != "" {
		split, err = strconv.ParseUint(splitStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "bad split query string provided: %s", splitStr)
			return
		}
	}
	if remainStr != "" {
		remain, err = strconv.ParseUint(remainStr, 10, 64)
		if err != nil {
			server.BadRequest(w, r, "bad remain query string provided: %s", remainStr)
			return
		}
	}

	timedLog := dvid.NewTimeLog()

	supervoxel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if supervoxel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as split target\n")
		return
	}
	info := dvid.GetModInfo(r)
	splitSupervoxel, remainSupervoxel, mutID, err := d.SplitSupervoxel(ctx.VersionID(), supervoxel, split, remain, r.Body, info, downscale)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split supervoxel %d -> %d, %d: %v", supervoxel, splitSupervoxel, remainSupervoxel, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"SplitSupervoxel": %d, "RemainSupervoxel": %d, "MutationID": %d}`, splitSupervoxel, remainSupervoxel, mutID)

	timedLog.Infof("HTTP split supervoxel of supervoxel %d request (%s)", supervoxel, r.URL)
}

func (d *Data) handleCleave(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/cleave/<label>
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Cleave requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'cleave' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	label, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if label == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as cleave target\n")
		return
	}
	modInfo := dvid.GetModInfo(r)
	cleaveLabel, mutID, err := d.CleaveLabel(ctx.VersionID(), label, modInfo, r.Body)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"CleavedLabel": %d, "MutationID": %d}`, cleaveLabel, mutID)

	timedLog.Infof("HTTP cleave of label %d request (%s)", label, r.URL)
}

func (d *Data) handleSplit(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/split/<label>[?splitlabel=X]
	if !server.AllowLabelmapSplit() {
		server.BadRequest(w, r, "Split endpoint deactivated in this DVID server's configuration.")
		return
	}
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Split requests must be POST actions.")
		return
	}
	if len(parts) < 5 {
		server.BadRequest(w, r, "ERROR: DVID requires label ID to follow 'split' command")
		return
	}
	timedLog := dvid.NewTimeLog()

	fromLabel, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		server.BadRequest(w, r, err)
		return
	}
	if fromLabel == 0 {
		server.BadRequest(w, r, "Label 0 is protected background value and cannot be used as sparse volume.\n")
		return
	}
	info := dvid.GetModInfo(r)
	toLabel, mutID, err := d.SplitLabels(ctx.VersionID(), fromLabel, r.Body, info)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("split label %d: %v", fromLabel, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"label": %d, "MutationID": %d}`, toLabel, mutID)

	timedLog.Infof("HTTP split of label %d request (%s)", fromLabel, r.URL)
}

func (d *Data) handleMerge(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request, parts []string) {
	// POST <api URL>/node/<UUID>/<data name>/merge
	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Merge requests must be POST actions.")
		return
	}
	timedLog := dvid.NewTimeLog()

	data, err := io.ReadAll(r.Body)
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
	info := dvid.GetModInfo(r)
	mutID, err := d.MergeLabels(ctx.VersionID(), mergeOp, info)
	if err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Error on merge: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"MutationID": %d}`, mutID)

	timedLog.Infof("HTTP merge request (%s)", r.URL)
}

func (d *Data) handleRenumber(ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	// POST <api URL>/node/<UUID>/<data name>/renumber

	if strings.ToLower(r.Method) != "post" {
		server.BadRequest(w, r, "Renumber requests must be POST actions.")
		return
	}
	timedLog := dvid.NewTimeLog()

	data, err := io.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "Bad POSTed data for renumber.  Should be JSON.")
		return
	}
	var renumberPairs []uint64
	if err := json.Unmarshal(data, &renumberPairs); err != nil {
		server.BadRequest(w, r, fmt.Sprintf("Bad renumber op JSON, must be list of numbers: %v", err))
		return
	}
	if len(renumberPairs)%2 != 0 {
		server.BadRequest(w, r, "Bad renumber op JSON, must be even list of numbers [new1, old1, new2, old2, ...]")
		return
	}
	info := dvid.GetModInfo(r)
	for i := 0; i < len(renumberPairs); i += 2 {
		origLabel := renumberPairs[i+1]
		newLabel := renumberPairs[i]
		if _, err := d.RenumberLabels(ctx.VersionID(), origLabel, newLabel, info); err != nil {
			server.BadRequest(w, r, fmt.Sprintf("Error on merge: %v", err))
			return
		}
	}
	timedLog.Infof("HTTP renumber request (%s)", r.URL)
}
