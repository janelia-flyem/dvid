/*
	This file supports the DVID REST API, breaking down URLs into
	commands and massaging attached data into appropriate data types.
*/

package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"bitbucket.org/tebeka/nrsc"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const HelpMessage = `
HTTP API (Level 2 REST) for general DVID commands
=================================================

POST <api URL>/repo/<UUID>/new/<datatype name>/<data name>

    Creates a new instance of the given data type.  Expects configuration data in JSON
    as the body of the POST.  Configuration data is a JSON object with each property
    corresponding to a configuration keyword for the particular data type.
`

const WebHelp = `
<!DOCTYPE html>
<html>

  <head>
    <meta charset='utf-8' />
    <meta http-equiv="X-UA-Compatible" content="chrome=1" />
    <meta name="description" content="DVID Web Server Home Page" />

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
           The DVID API is specified via RAML that can be accessed at /interface.
        </p>

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

const (
	// WebAPIVersion is the string version of the API.  Once DVID is somewhat stable,
	// this will be "v1/", "v2/", etc.
	WebAPIVersion = ""

	// The relative URL path to our Level 2 REST API
	WebAPIPath = "/api/" + WebAPIVersion
)

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion:
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func serveHttp(address, clientDir string) {
	dvid.Infof("Web server listening at %s ...\n", address)

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	// Handle RAML interface
	http.HandleFunc("/interface/", logHttpPanics(interfaceHandler))
	http.HandleFunc("/interface/version", logHttpPanics(versionHandler))

	// Handle Level 2 REST API.
	http.HandleFunc(WebAPIPath, logHttpPanics(apiHandler))

	// Handle static files through serving embedded files
	// via nrsc or loading files from a specified web client directory.
	if clientDir == "" {
		dvid.Infof("Serving web client from embedded files...")
		if err := nrsc.Initialize(); err != nil {
			dvid.Errorf("Error initializing embedded data access: %s\n", err.Error())
		}
	} else {
		dvid.Infof("Serving web pages from %s\n", clientDir)
	}
	http.HandleFunc("/", logHttpPanics(mainHandler))

	// Serve it up!
	src.ListenAndServe()
}

// Wrapper function so that http handlers recover from panics gracefully
// without crashing the entire program.  The error message is written to
// the log.
func logHttpPanics(handler func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Caught panic on HTTP request: %s", err)
				log.Printf("IP: %v, URL: %s", request.RemoteAddr, request.URL.Path)
				log.Printf("Stack Dump:\n%s", debug.Stack())
			}
		}()
		handler(writer, request)
	}
}

func NotFound(w http.ResponseWriter, r *http.Request) {
	errorMsg := fmt.Sprintf("Could not find the URL: %s", r.URL.Path)
	dvid.Infof(errorMsg)
	http.Error(w, errorMsg, http.StatusNotFound)
}

func BadRequest(w http.ResponseWriter, r *http.Request, message string) {
	errorMsg := fmt.Sprintf("ERROR: %s (%s).", message, r.URL.Path)
	dvid.Errorf(errorMsg)
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

// ---- Function types that fulfill http.Handler.  How can a bare function satisfy an interface?
//      See http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html

// Handler for web client and other static content
func mainHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// Serve from embedded files in executable if not web client directory was specified
	if config.webClientDir == "" {
		if len(path) > 0 && path[0:1] == "/" {
			path = path[1:]
		}
		dvid.Debugf("[%s] Serving from embedded files: %s\n", r.Method, path)

		resource := nrsc.Get(path)
		if resource == nil {
			http.NotFound(w, r)
			return
		}
		rsrc, err := resource.Open()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		data, err := ioutil.ReadAll(rsrc)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		dvid.SendHTTP(w, r, path, data)
	} else {
		filename := filepath.Join(config.webClientDir, path)
		dvid.Debugf("[%s] Serving from webclient directory: %s\n", r.Method, filename)
		http.ServeFile(w, r, filename)
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
	case "repos":
		reposRequest(w, r)
	case "repo":
		repoRequest(w, r)
	case "version":
		versionRequest(w, r)
	default:
		BadRequest(w, r, "Request not in API")
	}
}

// Index file redirection.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/index.html", http.StatusMovedPermanently)
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
		jsonStr, err := AboutJSON()
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

func reposRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "repos/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	action := strings.ToLower(r.Method)

	badRequest := func() {
		BadRequest(w, r, WebAPIPath+"repos/ must be followed with 'info', 'list' or 'new'")
	}

	if len(parts) != 1 {
		badRequest()
		return
	}

	switch parts[0] {
	case "info":
		jsonBytes, err := Repos.MarshalJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
	case "new":
		if action != "post" {
			BadRequest(w, r, "Repos 'new' request must be made with HTTP POST method")
			return
		}
		config, err := DecodeJSON(r)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		repo, err := Repos.NewRepo()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		alias, found, err := config.GetString("Alias")
		if err != nil || !found {
			alias = fmt.Sprintf("Repo %d", repo.RepoID())
		}
		repo.SetAlias(alias)
		description, found, err := config.GetString("Description")
		if err != nil || !found {
			description = "N/A"
		}
		repo.SetDescription(description)
		config.Remove("Alias", "Description")
		if err := repo.SetProperties(config.GetAll()); err != nil {
			BadRequest(w, r, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "Root", repo.RootUUID())
	default:
		badRequest()
	}
}

func repoRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "repo/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	action := strings.ToLower(r.Method)

	if len(parts) < 2 || len(parts) > 4 {
		BadRequest(w, r, "Bad repo request made.  Visit /api/help for help.")
		return
	}

	// Get particular repo for this UUID
	uuid, _, err := Repos.MatchingUUID(parts[0])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	repo, err := Repos.RepoFromUUID(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	// Handle query of dataset properties
	if parts[1] == "info" {
		jsonBytes, err := repo.MarshalJSON()
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))
		return
	}

	// Handle creation of new data in dataset via POST.
	if parts[1] == "new" {
		if action != "post" {
			BadRequest(w, r, "Repo 'new' request must be made with HTTP POST method")
			return
		}
		if len(parts) != 4 {
			BadRequest(w, r, "Bad URL: Expecting /api/dataset/<UUID>/new/<datatype name>/<data name>")
			return
		}
		typename := dvid.TypeString(parts[2])
		dataname := dvid.DataString(parts[3])
		config, err := DecodeJSON(r)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		typeservice, err := datastore.TypeServiceByName(typename)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		if _, err := repo.NewData(typeservice, dataname, config); err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: 'Added %s [%s] to node %s'}", "result", dataname, typename, uuid)
		return
	}

	// Forward all other commands to the data service.
	dataname := dvid.DataString(parts[1])
	dataservice, err := repo.GetDataByName(dataname)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	dataservice.ServeHTTP(w, r)
}

func versionRequest(w http.ResponseWriter, r *http.Request) {
	lenPath := len(WebAPIPath + "version/")
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")

	if len(parts) < 2 {
		BadRequest(w, r, "Bad version request made.  Visit /api/help for help.")
		return
	}

	// Get particular repo for this UUID
	uuid, _, err := Repos.MatchingUUID(parts[0])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	repo, err := Repos.RepoFromUUID(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	// Handle the repo command.
	switch parts[1] {
	case "lock":
		err := repo.Lock(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
		} else {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintln(w, "Lock on node %s successful.", uuid)
		}

	case "branch":
		newuuid, err := repo.NewChild(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
		} else {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{%q: %q}", "Branch", newuuid)
		}

	default:
		dataname := dvid.DataString(parts[1])
		dataservice, err := repo.GetDataByName(dataname)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		dataservice.ServeHTTP(w, r)
	}
}
