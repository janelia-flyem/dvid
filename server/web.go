/*
	This file supports the DVID REST API, breaking down URLs into
	commands and massaging attached data into appropriate data types.
*/

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const webClientUnavailableMessage = `
DVID Web Client Unavailable!  To make the web client available, you have two choices:

1) Invoke the DVID server using the full path to the DVID executable to use
   the built-in web client.

2) Specify a path to web pages that implement a web client via the "-webclient=PATH"
   option to dvid.  Example: 
   % dvid -webclient=/path/to/html/files -datastore=/path/to/db serve
`

const WebAPIHelp = `
DVID's HTTP API is a Level 2 REST API that roughly translates the dvid terminal
commands into URL form.

Commands that set or create data use POST.  Commands that return data use GET,
and the returned format will be in JSON except for "help" which returns HTML.

    GET /api/help
    GET /api/about
    GET /api/load

    GET /api/datasets/info
    GET /api/datasets/list
    POST /api/datasets/new  (Returns JSON like {"Root": "My Root UUID"})

    GET /api/dataset/<UUID>/info 
    POST /api/dataset/<UUID>/new/<datatype name>/<data name>
        Type-specific configuration settings should be sent via JSON.

    GET /api/dataset/<UUID>/<data name>/<type-specific commands>

    POST /api/node/<UUID>/lock
    POST /api/node/<UUID>/branch

    GET /api/node/<UUID>/<data name>/<type-specific commands>
    POST /api/node/<UUID>/<data name>/<type-specific commands>

To examine the data type-specific API commands available, use GET /api/dataset/.../help
shown above.
`

func BadRequest(w http.ResponseWriter, r *http.Request, message string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", message, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// Index file redirection.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/index.html", http.StatusMovedPermanently)
}

// Handler for web client
func mainHandler(w http.ResponseWriter, r *http.Request) {
	if runningService.WebClientPath != "" {
		consoleFile := strings.TrimPrefix(r.URL.Path, "/console/")
		filename := filepath.Join(runningService.WebClientPath, consoleFile)
		dvid.Fmt(dvid.Debug, "Web client: %s -> %s\n", r.URL.Path, filename)
		http.ServeFile(w, r, filename)
	} else {
		fmt.Fprintf(w, webClientUnavailableMessage)
	}
}

// Handler for API commands.  Results come back in JSON.
// We assume all DVID API commands have URLs with prefix /api/...
// See WebAPIHelp for expected calling URLs and HTTP verbs.
func apiHandler(w http.ResponseWriter, r *http.Request) {
	// Break URL request into arguments
	lenPath := len(WebAPIPath)
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	if len(parts) == 0 {
		BadRequest(w, r, "Poorly formed request")
		return
	}

	// Handle the requests
	switch parts[0] {
	case "help":
		helpRequest(w, r)
	case "about":
		aboutRequest(w, r)
	case "load":
		loadRequest(w, r)
	case "datasets":
		datasetsRequest(w, r)
	case "dataset":
		datasetRequest(w, r)
	case "node":
		nodeRequest(w, r)
	default:
		BadRequest(w, r, "Request not in API")
	}
}

func helpRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, WebAPIHelp)
}

func aboutRequest(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := runningService.AboutJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func loadRequest(w http.ResponseWriter, r *http.Request) {
	m, err := json.Marshal(map[string]int{
		"bytes read":      storage.BytesReadPerSec,
		"bytes written":   storage.BytesWrittenPerSec,
		"GET requests":    storage.GetsPerSec,
		"PUT requests":    storage.PutsPerSec,
		"handlers active": int(100 * ActiveHandlers / MaxChunkHandlers),
	})
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func datasetsRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "datasets/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	action := strings.ToLower(r.Method)

	badRequest := func() {
		BadRequest(w, r, WebAPIPath+"datasets/ must be followed with 'info', 'list' or 'new'")
	}

	if len(parts) != 1 {
		badRequest()
		return
	}

	switch parts[0] {
	case "list":
		jsonStr, err := runningService.DatasetsListJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
	case "info":
		jsonStr, err := runningService.DatasetsAllJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
	case "new":
		if action != "post" {
			BadRequest(w, r, "Datasets 'new' request must be made with HTTP POST method")
			return
		}
		root, _, err := runningService.NewDataset()
		if err != nil {
			BadRequest(w, r, err.Error())
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "Root", root)
	default:
		badRequest()
	}
}

func datasetRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "dataset/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	action := strings.ToLower(r.Method)

	if len(parts) < 2 || len(parts) > 4 {
		BadRequest(w, r, "Bad dataset request made.  Visit /api/help for help.")
		return
	}

	// Get particular dataset for this UUID
	uuidStr := parts[0]
	uuid, _, _, err := runningService.NodeIDFromString(uuidStr)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	// Handle query of dataset properties
	if parts[1] == "info" {
		jsonStr, err := runningService.DatasetJSON(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return
	}

	// Handle creation of new data in dataset via POST.
	if parts[1] == "new" {
		if action != "post" {
			BadRequest(w, r, "Dataset 'new' request must be made with HTTP POST method")
			return
		}
		if len(parts) != 4 {
			BadRequest(w, r, "Bad URL: Expecting /api/dataset/<UUID>/new/<datatype name>/<data name>")
			return
		}
		typename := parts[2]
		dataname := parts[3]
		decoder := json.NewDecoder(r.Body)
		var config dvid.Config
		err = decoder.Decode(&config)
		if err != nil {
			BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for 'new': %s", err.Error()))
			return
		}
		err = runningService.NewData(uuid, typename, dataname, config)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: 'Added %s [%s] to node %s'}", "result", dataname, typename, uuidStr)
		return
	}

	// Forward all other commands to the data service.
	dataname := datastore.DataString(parts[1])
	dataservice, err := runningService.DataService(uuid, dataname)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	err = dataservice.DoHTTP(uuid, w, r)
	if err != nil {
		BadRequest(w, r, err.Error())
	}
}

func nodeRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "node/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")

	if len(parts) < 2 {
		BadRequest(w, r, "Bad node request made.  Visit /api/help for help.")
		return
	}

	// Get particular dataset for this UUID
	uuidStr := parts[0]
	uuid, _, _, err := runningService.NodeIDFromString(uuidStr)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	// Handle the dataset command.
	switch parts[1] {
	case "lock":
		err := runningService.Lock(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
		} else {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintln(w, "Lock on node %s successful.", uuidStr)
		}

	case "branch":
		newuuid, err := runningService.NewVersion(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
		} else {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{%q: %q}", "Branch", newuuid)
		}

	default:
		dataname := datastore.DataString(parts[1])
		dataservice, err := runningService.DataService(uuid, dataname)
		if err != nil {
			BadRequest(w, r, err.Error())
		}
		err = dataservice.DoHTTP(uuid, w, r)
		if err != nil {
			BadRequest(w, r, err.Error())
		}
	}
}
