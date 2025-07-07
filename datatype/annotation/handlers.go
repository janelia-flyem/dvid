package annotation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(req datastore.Request, reply *datastore.Response) error {
	switch req.TypeCommand() {
	case "reload":
		var uuidStr, dataName string
		if _, err := req.FilenameArgs(1, &uuidStr, &dataName); err != nil {
			return err
		}
		uuid, v, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			return err
		}
		if err = datastore.AddToNodeLog(uuid, []string{req.Command.String()}); err != nil {
			return err
		}
		var inMemory, check bool
		setting, found := req.Setting("inmemory")
		if !found || setting != "false" {
			inMemory = true
		}
		setting, found = req.Setting("check")
		if found && setting == "true" {
			check = true
		}
		ctx := datastore.NewVersionedCtx(d, v)
		d.RecreateDenormalizations(ctx, inMemory, check)
		reply.Text = fmt.Sprintf("Asynchronously checking and restoring label and tag denormalizations for annotation %q\n", d.DataName())
		return nil

	default:
		return fmt.Errorf("unknown command.  Data type %q [%s] does not support %q command",
			d.DataName(), d.TypeName(), req.TypeCommand())
	}
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) (activity map[string]interface{}) {
	timedLog := dvid.NewTimeLog()
	// versionID := ctx.VersionID()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	d.RLock()
	if d.denormOngoing && action == "post" {
		d.RUnlock()
		server.BadRequest(w, r, "cannot run POST commands while %q instance is being reloaded", d.DataName())
		return
	}
	d.RUnlock()

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
		fmt.Fprintf(w, "Changed %q based on received configuration:\n%s\n", d.DataName(), config)
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
		fmt.Fprint(w, string(jsonBytes))

	case "sync":
		if action != "post" {
			server.BadRequest(w, r, "Only POST allowed to sync endpoint")
			return
		}
		replace := r.URL.Query().Get("replace") == "true"
		if err := datastore.SetSyncByJSON(d, uuid, replace, r.Body); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		d.cachedBlockSize = nil

	case "tags":
		if action == "post" {
			replace := r.URL.Query().Get("replace") == "true"
			if err := datastore.SetTagsByJSON(d, uuid, replace, r.Body); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		} else {
			jsonBytes, err := d.MarshalJSONTags()
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, string(jsonBytes))
		}

	case "labels":
		if action != "post" {
			server.BadRequest(w, r, "Only POST action is available on 'labels' endpoint.")
			return
		}
		if len(parts) >= 5 {
			server.BadRequest(w, r, "Extraneous arguments after POST /labels endpoint.")
			return
		}
		if err := d.handlePostLabels(ctx, w, r.Body); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "label":
		if action != "get" {
			server.BadRequest(w, r, "Only GET action is available on 'label' endpoint.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "Must include label after 'label' endpoint.")
			return
		}
		label, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if label == 0 {
			server.BadRequest(w, r, "Label 0 is protected background value and cannot be used for query.")
			return
		}
		queryStrings := r.URL.Query()
		jsonBytes, err := d.GetLabelJSON(ctx, label, queryStrings.Get("relationships") == "true")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		if _, err := w.Write(jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: get synaptic elements for label %d (%s)", r.Method, label, r.URL)

	case "tag":
		if action != "get" {
			server.BadRequest(w, r, "Only GET action is available on 'tag' endpoint.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "Must include tag string after 'tag' endpoint.")
			return
		}
		tag := Tag(parts[4])
		queryStrings := r.URL.Query()
		jsonBytes, err := d.GetTagJSON(ctx, tag, queryStrings.Get("relationships") == "true")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		if _, err := w.Write(jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: get synaptic elements for tag %s (%s)", r.Method, tag, r.URL)

	case "roi":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/roi/<ROI specification>
			if len(parts) < 5 {
				server.BadRequest(w, r, "Expect ROI specification to follow 'roi' in GET request")
				return
			}
			roiParts := strings.Split(parts[4], ",")
			var roiSpec string
			roiSpec = "roi:"
			switch len(roiParts) {
			case 1:
				roiSpec += roiParts[0] + "," + string(uuid)
			case 2:
				roiSpec += parts[4]
			default:
				server.BadRequest(w, r, "Bad ROI specification: %q", parts[4])
				return
			}
			elems, err := d.GetROISynapses(ctx, storage.FilterSpec(roiSpec))
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/json")
			jsonBytes, err := json.Marshal(elems)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if _, err := w.Write(jsonBytes); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: synapse elements in ROI (%s) (%s)", r.Method, parts[4], r.URL)

		default:
			server.BadRequest(w, r, "Only GET action is available on 'roi' endpoint.")
			return
		}

	case "all-elements":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/all-elements
			if len(parts) > 4 {
				server.BadRequest(w, r, "Do not expect additional parameters after 'all-elements' in GET request")
				return
			}
			w.Header().Set("Content-type", "application/json")
			if err := d.StreamAll(ctx, w); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: all elements (%s)", r.Method, r.URL)

		default:
			server.BadRequest(w, r, "Only GET is available on 'all-elements' endpoint.")
			return
		}

	case "scan":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/scan
			if len(parts) > 4 {
				server.BadRequest(w, r, "Do not expect additional parameters after 'scan' in GET request")
				return
			}
			byCoord := r.URL.Query().Get("byCoord") == "true"
			keysOnly := r.URL.Query().Get("keysOnly") == "true"
			w.Header().Set("Content-type", "application/json")
			if err := d.scan(ctx, w, byCoord, keysOnly); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: scan annotations %q (%s)", r.Method, d.DataName(), r.URL)

		default:
			server.BadRequest(w, r, "Only GET is available on 'scan' endpoint.")
			return
		}

	case "blocks":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>
			if len(parts) < 6 {
				server.BadRequest(w, r, "Expect size and offset to follow 'blocks' in GET request")
				return
			}
			sizeStr, offsetStr := parts[4], parts[5]
			ext3d, err := dvid.NewExtents3dFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/json")
			if err = d.StreamBlocks(ctx, w, ext3d); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: synapse elements in blocks intersecting subvolume (size %s, offset %s) (%s)", r.Method, sizeStr, offsetStr, r.URL)

		case "post":
			// POST <api URL>/node/<UUID>/<data name>/blocks
			kafkaOff := r.URL.Query().Get("kafkalog") == "off"
			numBlocks, err := d.StoreBlocks(ctx, r.Body, kafkaOff)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: posted %d synapse blocks (%s)", r.Method, numBlocks, r.URL)

		default:
			server.BadRequest(w, r, "Only GET/POST action is available on 'blocks' endpoint.")
			return
		}

	case "elements":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/elements/<size>/<offset>
			if len(parts) < 6 {
				server.BadRequest(w, r, "Expect size and offset to follow 'elements' in GET request")
				return
			}
			sizeStr, offsetStr := parts[4], parts[5]
			ext3d, err := dvid.NewExtents3dFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			elems, err := d.GetRegionSynapses(ctx, ext3d)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/json")
			jsonBytes, err := json.Marshal(elems)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if _, err := w.Write(jsonBytes); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: synapse elements in subvolume (size %s, offset %s) (%s)", r.Method, sizeStr, offsetStr, r.URL)

		case "post":
			kafkaOff := r.URL.Query().Get("kafkalog") == "off"
			if err := d.StoreElements(ctx, r.Body, kafkaOff); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		default:
			server.BadRequest(w, r, "Only GET or POST action is available on 'elements' endpoint.")
			return
		}

	case "element":
		// DELETE <api URL>/node/<UUID>/<data name>/element/<coord>
		if action != "delete" {
			server.BadRequest(w, r, "Only DELETE action is available on 'element' endpoint.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "Must include coordinate after DELETE on 'element' endpoint.")
			return
		}
		pt, err := dvid.StringToPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		kafkaOff := r.URL.Query().Get("kafkalog") == "off"
		if err := d.DeleteElement(ctx, pt, kafkaOff); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: delete synaptic element at %s (%s)", r.Method, pt, r.URL)

	case "move":
		// POST <api URL>/node/<UUID>/<data name>/move/<from_coord>/<to_coord>
		if action != "post" {
			server.BadRequest(w, r, "Only POST action is available on 'move' endpoint.")
			return
		}
		if len(parts) < 6 {
			server.BadRequest(w, r, "Must include 'from' and 'to' coordinate after 'move' endpoint.")
			return
		}
		fromPt, err := dvid.StringToPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		toPt, err := dvid.StringToPoint3d(parts[5], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		kafkaOff := r.URL.Query().Get("kafkalog") == "off"
		if err := d.MoveElement(ctx, fromPt, toPt, kafkaOff); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: move synaptic element from %s to %s (%s)", r.Method, fromPt, toPt, r.URL)

	case "reload":
		// POST <api URL>/node/<UUID>/<data name>/reload
		if action != "post" {
			server.BadRequest(w, r, "Only POST action is available on 'reload' endpoint.")
			return
		}
		inMemory := !(r.URL.Query().Get("inmemory") == "false")
		check := r.URL.Query().Get("check") == "true"
		d.RecreateDenormalizations(ctx, inMemory, check)

	default:
		server.BadAPIRequest(w, r, d)
	}
	return
}

type labelsJSONStrs map[string]string

func (d *Data) handlePostLabels(ctx *datastore.VersionedCtx, w http.ResponseWriter, r io.Reader) error {
	timedLog := dvid.NewTimeLog()

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	jsonBytes, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var data labelsJSONStrs
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return err
	}
	for k, v := range data {
		label, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return err
		}
		if label == 0 {
			continue
		}
		tk := NewLabelTKey(label)
		if err := store.Put(ctx, tk, []byte(v)); err != nil {
			return err
		}
	}
	timedLog.Infof("HTTP POST: set %d labels (%s)", len(data), ctx)
	return nil
}
