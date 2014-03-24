/*
	This file supports the DVID REST API, breaking down URLs into
	commands and massaging attached data into appropriate data types.
*/

package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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

const WebHelp = `
<!DOCTYPE html>
<html>

  <head>
    <meta charset='utf-8' />
    <meta http-equiv="X-UA-Compatible" content="chrome=1" />
    <meta name="description" content="DVID Web Server Home Page" />

    <link rel="stylesheet" type="text/css" media="screen" href="/stylesheets/stylesheet.css">

    <title>DVID Web Server</title>
  </head>

  <body>

    <!-- HEADER -->
    <div id="header_wrap" class="outer">
        <header class="inner">
          <a id="forkme_banner" href="https://github.com/janelia-flyem/dvid">View DVID on GitHub</a>

          <h1 id="project_title">DVID Web Server</h1>
          <h2 id="project_tagline">Stock help page for DVID server currently running {{.Hostname}}</h2>

        </header>
    </div>

    <!-- MAIN CONTENT -->
    <div id="main_content_wrap" class="outer">
      <section id="main_content" class="inner">
        <h3>Welcome to DVID</h3>

        <p>This page provides an introduction to the currently running DVID server.  Developers can visit
        the <a href="https://github.com/janelia-flyem/dvid">Github repo</a> for more documentation and code.
        The <a href="/console/">DVID admin console</a> may be available if you have permissions.</p>
        
        <h4>HTTP API</h4>

        <p>Please consult the
           <a href="https://github.com/janelia-flyem/dvid#dvid">DVID documentation</a> for type-specific API help.
           In the following examples, any part surrounded by curly braces like {myparam}
           should be replaced by appropriate values.
        </p>


          <ul>
            <li>GET /api/help (current page)</li>
            <li><a href="/api/load">GET /api/load</a></li>

            <li><a href="/api/server/info">GET /api/server/info</a></li>
            <li><a href="/api/server/types">GET /api/server/types</a></li>

            <li><a href="/api/datasets/info">GET /api/datasets/info</a></li>
            <li><a href="/api/datasets/list">GET /api/datasets/list</a></li>
            <li>POST /api/datasets/new</li>

            <li>GET /api/dataset/{UUID}/info</li>
            <li>POST /api/dataset/{UUID}/new/{datatype name}/{data name}<br />
                Type-specific configuration settings should be sent via JSON.</li>

            <li>GET /api/dataset/{UUID}/{data name}/{type-specific commands}</li>

            <li>POST /api/node/{UUID}/lock</li>
            <li>POST /api/node/{UUID}/branch<br /></li>

            <li>POST /api/node/{UUID}/{data name} (expects config in JSON format)</li>
            <li>GET /api/node/{UUID}/{data name}/{type-specific commands}</li>
            <li>POST /api/node/{UUID}/{data name}/{type-specific commands}</li>
        </ul>
        
        <h3>Licensing</h3>
        <p>DVID is released under the
            <a href="http://janelia-flyem.github.com/janelia_farm_license.html">Janelia Farm license</a>, a
            <a href="http://en.wikipedia.org/wiki/BSD_license#3-clause_license_.28.22New_BSD_License.22_or_.22Modified_BSD_License.22.29">
                3-clause BSD license</a>.
        </p>
      </section>
    </div>

    <!-- FOOTER  -->
    <div id="footer_wrap" class="outer">
      <footer class="inner">
      </footer>
    </div>

    

  </body>
</html>
`

func BadRequest(w http.ResponseWriter, r *http.Request, message string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", message, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// DecodeJSON decodes JSON passed in a request into a dvid.Config.
func DecodeJSON(r *http.Request) (dvid.Config, error) {
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		return dvid.Config{}, fmt.Errorf("Malformed JSON request in body: %s", err.Error())
	}
	return config, nil
}

// Index file redirection.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/index.html", http.StatusMovedPermanently)
}

// Handler for web client
func mainHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		// Respond with stock help page for root URL.
		t, _ := template.New("Help").Parse(WebHelp)
		hostname, _ := os.Hostname()
		vars := struct {
			Hostname      string
			WebClientPath string
		}{
			hostname,
			runningService.WebClientPath,
		}
		t.Execute(w, vars)
	} else if runningService.WebClientPath != "" {
		var filename string
		if r.URL.Path[len(r.URL.Path)-1:] == "/" {
			filename = filepath.Join(runningService.WebClientPath, r.URL.Path, "index.html")
		} else {
			filename = filepath.Join(runningService.WebClientPath, r.URL.Path)
		}
		dvid.Log(dvid.Debug, "Serving %s -> %s\n", r.URL.Path, filename)
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
	case "load":
		loadRequest(w, r)
	case "server":
		serverRequest(w, r)
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
	fmt.Fprintf(w, WebHelp)
}

func loadRequest(w http.ResponseWriter, r *http.Request) {
	m, err := json.Marshal(map[string]int{
		"file bytes read":     storage.FileBytesReadPerSec,
		"file bytes written":  storage.FileBytesWrittenPerSec,
		"key bytes read":      storage.StoreKeyBytesReadPerSec,
		"key bytes written":   storage.StoreKeyBytesWrittenPerSec,
		"value bytes read":    storage.StoreValueBytesReadPerSec,
		"value bytes written": storage.StoreValueBytesWrittenPerSec,
		"GET requests":        storage.GetsPerSec,
		"PUT requests":        storage.PutsPerSec,
		"handlers active":     int(100 * ActiveHandlers / MaxChunkHandlers),
		"goroutines":          runtime.NumGoroutine(),
	})
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func aboutJSON() (jsonStr string, err error) {
	data := map[string]string{
		"Cores":           fmt.Sprintf("%d", dvid.NumCPU),
		"Maximum Cores":   fmt.Sprintf("%d", runtime.NumCPU()),
		"DVID datastore":  datastore.Version,
		"Storage backend": storage.Version,
		"Storage driver":  storage.Driver,
		"Server uptime":   time.Since(startupTime).String(),
	}
	m, err := json.Marshal(data)
	if err != nil {
		return
	}
	jsonStr = string(m)
	return
}

func serverRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "server/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")

	badRequest := func() {
		BadRequest(w, r, WebAPIPath+"server/ must be followed with 'info' or 'types'")
	}

	if len(parts) != 1 {
		badRequest()
		return
	}

	switch parts[0] {
	case "info":
		jsonStr, err := aboutJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
	case "types":
		jsonStr, err := runningService.TypesJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
	default:
		badRequest()
	}
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
	uuid, err := MatchingUUID(parts[0])
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
		typename := dvid.TypeString(parts[2])
		dataname := dvid.DataString(parts[3])
		decoder := json.NewDecoder(r.Body)
		config := dvid.NewConfig()
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
		fmt.Fprintf(w, "{%q: 'Added %s [%s] to node %s'}", "result", dataname, typename, uuid)
		return
	}

	// Forward all other commands to the data service.
	dataname := dvid.DataString(parts[1])
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
	uuid, err := MatchingUUID(parts[0])
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
			fmt.Fprintln(w, "Lock on node %s successful.", uuid)
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
		dataname := dvid.DataString(parts[1])
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
}
