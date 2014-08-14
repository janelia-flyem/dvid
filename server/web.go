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
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/go/nrsc"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
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
          <h2 id="project_tagline">Stock help page for DVID server currently running %s</h2>

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

var (
	// The main web mux
	WebMux *web.Mux
)

func init() {
	WebMux = web.New()
	WebMux.Use(middleware.RequestID)
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion:
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func serveHttp(address, clientDir string) {
	dvid.Infof("Web server listening at %s ...\n", address)
	initRoutes()

	// Install our handler at the root of the standard net/http default mux.
	// This allows packages like expvar to continue working as expected.  (From goji.go)
	http.Handle("/", WebMux)

	graceful.HandleSignals()
	if err := graceful.ListenAndServe(address, http.DefaultServeMux); err != nil {
		log.Fatal(err)
	}
	graceful.Wait()
}

// High-level switchboard for DVID HTTP API.
func initRoutes() {
	silentMux := web.New()
	WebMux.Handle("/api/load", silentMux)
	silentMux.Get("/api/load", loadHandler)

	mainMux := web.New()
	WebMux.Handle("/*", mainMux)
	mainMux.Use(middleware.Logger)
	mainMux.Use(middleware.Recoverer)
	mainMux.Use(middleware.AutomaticOptions)

	// Handle RAML interface
	mainMux.Get("/interface", logHttpPanics(interfaceHandler))
	mainMux.Get("/interface/version", logHttpPanics(versionHandler))

	mainMux.Get("/api/help", helpHandler)

	mainMux.Get("/api/server/info", serverInfoHandler)
	mainMux.Get("/api/server/types", serverTypesHandler)

	mainMux.Post("/api/repos", reposPostHandler)
	mainMux.Get("/api/repos/info", reposInfoHandler)

	repoMux := web.New()
	mainMux.Handle("/api/repo/:uuid/*", repoMux)
	repoMux.Use(repoSelector)
	repoMux.Post("/api/repo/:uuid/instance", repoPostHandler)
	repoMux.Get("/api/repo/:uuid/info", repoInfoHandler)
	repoMux.Get("/api/repo/:uuid/lock", repoLockHandler)
	repoMux.Get("/api/repo/:uuid/branch", repoBranchHandler)

	instanceMux := web.New()
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword/*", instanceMux)
	instanceMux.Use(repoSelector)
	instanceMux.Use(instanceSelector)
	instanceMux.NotFound(NotFound)

	mainMux.Get("/*", mainHandler)
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

func BadRequest(w http.ResponseWriter, r *http.Request, message string, args ...interface{}) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args)
	}
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

// ---- Middleware -------------

// repoSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo.
func repoSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var err error
		var uuid dvid.UUID
		if uuid, c.Env["versionID"], err = Repos.MatchingUUID(c.URLParams["uuid"]); err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		c.Env["uuid"] = uuid
		c.Env["repo"], err = Repos.RepoFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
		} else {
			h.ServeHTTP(w, r)
		}
	}
	return http.HandlerFunc(fn)
}

// instanceSelector retrieves the data instance given its complete string name and
// forwards the request to that instance's HTTP handler.
func instanceSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var err error
		dataname := dvid.DataString(c.URLParams["dataname"])
		uuid, ok := c.Env["uuid"].(dvid.UUID)
		if !ok {
			msg := fmt.Sprintf("Bad format for UUID %q\n", c.Env["uuid"])
			BadRequest(w, r, msg)
			return
		}
		repo, err := Repos.RepoFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		versionID, err := Repos.VersionFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		dataservice, err := repo.GetDataByName(dataname)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		// Construct the Context
		ctx := datastore.NewContext(context.Background(), repo, versionID)
		dataservice.ServeHTTP(ctx, w, r)
	}
	return http.HandlerFunc(fn)
}

// ---- Function types that fulfill http.Handler.  How can a bare function satisfy an interface?
//      See http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html

func helpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}
	fmt.Fprintf(w, fmt.Sprintf(WebHelp, hostname))
}

// Handler for web client and other static content
func mainHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if config == nil {
		log.Fatalf("mainHandler() called when server was not configured!\n")
	}

	// Serve from embedded files in executable if not web client directory was specified
	if config.WebClient() == "" {
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
		filename := filepath.Join(config.WebClient(), path)
		dvid.Debugf("[%s] Serving from webclient directory: %s\n", r.Method, filename)
		http.ServeFile(w, r, filename)
	}
}

func loadHandler(w http.ResponseWriter, r *http.Request) {
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

func serverInfoHandler(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := AboutJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func serverTypesHandler(w http.ResponseWriter, r *http.Request) {
	jsonMap := make(map[dvid.TypeString]string)
	datatypes, err := Repos.Datatypes()
	if err != nil {
		msg := fmt.Sprintf("Cannot return server datatypes: %s\n", err.Error())
		BadRequest(w, r, msg)
		return
	}
	for _, datatype := range datatypes {
		jsonMap[datatype.TypeName()] = string(datatype.TypeURL())
	}
	m, err := json.Marshal(jsonMap)
	if err != nil {
		msg := fmt.Sprintf("Cannot marshal JSON datatype info: %v (%s)\n", jsonMap, err.Error())
		BadRequest(w, r, msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func reposInfoHandler(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := Repos.MarshalJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func reposPostHandler(w http.ResponseWriter, r *http.Request) {
	repo, err := Repos.NewRepo()
	if err != nil {
		BadRequest(w, r, err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %q}", "Root", repo.RootUUID())
}

func repoInfoHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo, ok := (c.Env["repo"]).(datastore.Repo)
	if !ok {
		BadRequest(w, r, "Error in retrieving Repo from URL parameters")
		return
	}
	jsonBytes, err := repo.MarshalJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func repoPostHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo, ok := (c.Env["repo"]).(datastore.Repo)
	if !ok {
		BadRequest(w, r, "Error in retrieving Repo from URL parameters")
		return
	}

	decoder := json.NewDecoder(r.Body)
	config := dvid.NewConfig()
	if err := decoder.Decode(&config); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for 'new': %s", err.Error()))
		return
	}

	// Make sure that the passed configuration has data type and instance name.
	typename, found, err := config.GetString("datatype")
	if !found || err != nil {
		BadRequest(w, r, "POST on repo endpoint requires specification of valid 'datatype'")
		return
	}
	dataname, found, err := config.GetString("dataname")
	if !found || err != nil {
		BadRequest(w, r, "POST on repo endpoint requires specification of valid 'dataname'")
		return
	}
	typeservice, err := datastore.TypeServiceByName(dvid.TypeString(typename))
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	_, err = repo.NewData(typeservice, dvid.DataString(dataname), config)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: 'Added %s [%s] to node %s'}", "result", dataname, typename, repo.RootUUID())
}

func repoLockHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo, ok := (c.Env["repo"]).(datastore.Repo)
	if !ok {
		BadRequest(w, r, "Error in retrieving Repo from URL parameters")
		return
	}
	uuid, _, err := Repos.MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	err = repo.Lock(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "Lock on node %s successful.", uuid)
	}
}

func repoBranchHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo, ok := (c.Env["repo"]).(datastore.Repo)
	if !ok {
		BadRequest(w, r, "Error in retrieving Repo from URL parameters")
		return
	}
	uuid, _, err := Repos.MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	newuuid, err := repo.NewVersion(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "Child", newuuid)
	}
}
