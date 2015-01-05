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
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/go/nrsc"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

const WebHelp = `
<!DOCTYPE html>
<html>

  <head>
	<meta charset='utf-8' />
	<meta http-equiv="X-UA-Compatible" content="chrome=1" />
	<meta name="description" content="DVID Web Server Home Page" />

	<title>Embedded DVID Web Server</title>
  </head>

  <body>

	<!-- HEADER -->
	<div id="header_wrap" class="outer">
		<header class="inner">
		  <h2 id="project_tagline">Stock help page for DVID server currently running on %s</h2>
		</header>
	</div>

	<!-- MAIN CONTENT -->
	<div id="main_content_wrap" class="outer">
	  <section id="main_content" class="inner">
		<h3>Welcome to DVID</h3>

		<p>This page provides an introduction to the currently running DVID server.  
		Developers can visit the <a href="https://github.com/janelia-flyem/dvid">Github repo</a> 
		for more documentation and code.
		The <a href="/console/">DVID admin console</a> may be available if you have downloaded the
		<a href="https://github.com/janelia-flyem/dvid-console">DVID console web client repo</a>
		and included <i>-webclient=/path/to/console</i> when running the
		<code>dvid serve</code> command.</p>
		
		<h3>HTTP API and command line use</h3>

		<h4>General commands</h4>

		<pre>
 GET  /api/help

	The current page that lists all general and type-specific commands/HTTP API.

 GET  /api/help/{typename}

	Returns help for the given datatype.

 GET  /api/load

	Returns a JSON of server load statistics.

 GET  /api/server/info

	Returns JSON for server properties.

 GET  /api/server/types

	Returns JSON with datatype names and their URLs.

 POST /api/repos

 	Creates a new repository.  Expects configuration data in JSON as the body of the POST.
 	Configuration is a JSON object with optional "alias" and "description" properties.
 	Returns the root UUID of the newly created repo in JSON object: {"Root": uuid}

 GET  /api/repos/info

	Returns JSON for the repositories under management by this server.

 HEAD /api/repo/{uuid}

 	Returns 200 if a repo with given UUID is available.

 GET  /api/repo/{uuid}/info

	Returns JSON for just the repository with given root UUID.  The UUID string can be
	shortened as long as it is uniquely identifiable across the managed repositories.

 POST /api/repo/{uuid}/lock

	Locks the node (version) with given UUID.  This is required before a version can 
	be branched or pushed to a remote server.

 POST /api/repo/{uuid}/branch

	Creates a new child node (version) of the node with given UUID.

 POST /api/repo/{uuid}/instance

	Creates a new instance of the given data type.  Expects configuration data in JSON
	as the body of the POST.  Configuration data is a JSON object with each property
	corresponding to a configuration keyword for the particular data type.  Two properties
	are required: "typename" should be set to the type name of the new instance, and
	"dataname" should be set to the desired name of the new instance.

	
 DELETE /api/repo/{uuid}/{dataname}?imsure=true

	Deletes a data instance of given name from the repository holding a node with UUID.	
		</pre>

		<h4>Data type commands</h4>

		<p>This server has compiled in the following data types, each of which have a HTTP API.
		   Click on the links below to explore each data type's command line and HTTP API.</p>

		%s

		<p>Background batch processes like generation of tiles, sparse volumes, and various
		   indices, will be paused if a single server receives more than a few data type API 
		   requests over a 5 minute moving window.  You can mark your API request as
		   non-interactive (i.e., you don't mind if it's delayed) by appending a query string
		   <code>interactive=0</code>.

		<h3>Licensing</h3>
		<p><a href="https://github.com/janelia-flyem/dvid">DVID</a> is released under the
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

type WebMux struct {
	*web.Mux
	sync.Mutex
	routesSetup bool
}

var (
	webMux WebMux
)

func init() {
	webMux.Mux = web.New()
	webMux.Use(middleware.RequestID)
}

// ServeHTTP fulfills one request using the default web Mux.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !webMux.routesSetup {
		initRoutes()
	}

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	webMux.ServeHTTP(w, r)
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion:
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func serveHttp(address, clientDir string) {
	var mode string
	if readonly {
		mode = " (read-only mode)"
	}
	dvid.Infof("Web server listening at %s%s ...\n", address, mode)
	if !webMux.routesSetup {
		initRoutes()
	}

	// Install our handler at the root of the standard net/http default mux.
	// This allows packages like expvar to continue working as expected.  (From goji.go)
	http.Handle("/", webMux)

	graceful.HandleSignals()
	if err := graceful.ListenAndServe(address, http.DefaultServeMux); err != nil {
		log.Fatal(err)
	}
	graceful.Wait()
}

// High-level switchboard for DVID HTTP API.
func initRoutes() {
	webMux.Lock()
	defer webMux.Unlock()

	if webMux.routesSetup {
		return
	}

	silentMux := web.New()
	webMux.Handle("/api/load", silentMux)
	silentMux.Use(corsHandler)
	silentMux.Get("/api/load", loadHandler)

	mainMux := web.New()
	webMux.Handle("/*", mainMux)
	mainMux.Use(middleware.Logger)
	mainMux.Use(middleware.Recoverer)
	mainMux.Use(middleware.AutomaticOptions)
	mainMux.Use(corsHandler)

	// Handle RAML interface
	mainMux.Get("/interface", logHttpPanics(interfaceHandler))
	mainMux.Get("/interface/version", logHttpPanics(versionHandler))

	mainMux.Get("/api/help", helpHandler)
	mainMux.Get("/api/help/", helpHandler)
	mainMux.Get("/api/help/:typename", typehelpHandler)

	mainMux.Get("/api/server/info", serverInfoHandler)
	mainMux.Get("/api/server/info/", serverInfoHandler)
	mainMux.Get("/api/server/types", serverTypesHandler)
	mainMux.Get("/api/server/types/", serverTypesHandler)

	if !readonly {
		mainMux.Post("/api/repos", reposPostHandler)
	}
	mainMux.Get("/api/repos/info", reposInfoHandler)

	repoMux := web.New()
	mainMux.Handle("/api/repo/:uuid", repoMux)
	mainMux.Handle("/api/repo/:uuid/*", repoMux)
	repoMux.Use(repoSelector)
	repoMux.Head("/api/repo/:uuid", repoHeadHandler)
	repoMux.Get("/api/repo/:uuid/info", repoInfoHandler)
	repoMux.Post("/api/repo/:uuid/instance", repoNewDataHandler)
	repoMux.Post("/api/repo/:uuid/lock", repoLockHandler)
	repoMux.Post("/api/repo/:uuid/branch", repoBranchHandler)
	repoMux.Delete("/api/repo/:uuid/:dataname", repoDeleteHandler)

	instanceMux := web.New()
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword", instanceMux)
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword/*", instanceMux)
	instanceMux.Use(repoSelector)
	instanceMux.Use(instanceSelector)
	instanceMux.NotFound(NotFound)

	mainMux.Get("/*", mainHandler)

	webMux.routesSetup = true
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

// corsHandler adds CORS support via header
func corsHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Allow cross-origin resource sharing.
		w.Header().Add("Access-Control-Allow-Origin", "*")

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// repoSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo.
func repoSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		action := strings.ToLower(r.Method)
		if readonly && action != "get" && action != "head" {
			BadRequest(w, r, "Server in read-only mode and will only accept GET and HEAD requests")
			return
		}

		var err error
		var uuid dvid.UUID
		if uuid, c.Env["versionID"], err = datastore.MatchingUUID(c.URLParams["uuid"]); err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		c.Env["uuid"] = uuid
		c.Env["repo"], err = datastore.RepoFromUUID(uuid)
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
		repo, err := datastore.RepoFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		versionID, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		dataservice, err := repo.GetDataByName(dataname)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}

		// Handle DVID-wide query string commands like non-interactive call designations
		queryValues := r.URL.Query()

		// All HTTP requests are interactive so let server tally request.
		interactive := queryValues.Get("interactive")
		if interactive == "" || (interactive != "false" && interactive != "0") {
			GotInteractiveRequest()
		}

		// Construct the Context
		ctx := datastore.NewServerContext(context.Background(), repo, versionID)
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

	// Get help from all compiled-in data types.
	html := "<ul>"
	for _, typeservice := range datastore.Compiled {
		name := typeservice.GetType().Name
		html += "<li>"
		html += fmt.Sprintf("<a href='/api/help/%s'>%s</a>", name, name)
		html += "</li>"
	}
	html += "</ul>"

	// Return the embedded help page.
	fmt.Fprintf(w, fmt.Sprintf(WebHelp, hostname, html))
}

func typehelpHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	typename := dvid.TypeString(c.URLParams["typename"])
	typeservice, err := datastore.TypeServiceByName(typename)
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}
	fmt.Fprintf(w, typeservice.Help())
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
	typemap, err := datastore.Types()
	if err != nil {
		msg := fmt.Sprintf("Cannot return server datatypes: %s\n", err.Error())
		BadRequest(w, r, msg)
		return
	}
	for _, typeservice := range typemap {
		t := typeservice.GetType()
		jsonMap[t.Name] = string(t.URL)
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
	jsonBytes, err := datastore.Manager.MarshalJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func reposPostHandler(w http.ResponseWriter, r *http.Request) {
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for new repo: %s", err.Error()))
		return
	}

	alias, _, err := config.GetString("alias")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint requires valid 'alias': %s", err.Error())
		return
	}
	description, _, err := config.GetString("description")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint requires valid 'description': %s", err.Error())
		return
	}

	repo, err := datastore.NewRepo(alias, description)
	if err != nil {
		BadRequest(w, r, err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %q}", "Root", repo.RootUUID())
}

func repoHeadHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo := (c.Env["repo"]).(datastore.Repo)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Repo available with root UUID %s\n", repo.RootUUID())
}

func repoInfoHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo := (c.Env["repo"]).(datastore.Repo)
	jsonBytes, err := repo.MarshalJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func repoDeleteHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	queryValues := r.URL.Query()
	imsure := queryValues.Get("imsure")
	if imsure != "true" {
		BadRequest(w, r, "Cannot delete instance unless query string 'imsure=true' is present!")
		return
	}

	repo := (c.Env["repo"]).(datastore.Repo)
	dataname, ok := c.URLParams["dataname"]
	if !ok {
		BadRequest(w, r, "Error in retrieving data instance name from URL parameters")
		return
	}

	// Do the deletion asynchronously since they can take a very long time.
	go func() {
		if err := repo.DeleteDataByName(dvid.DataString(dataname)); err != nil {
			dvid.Errorf("Error in deleting data instance %q: %s", dataname, err.Error())
		}
	}()

	// Just respond that deletion was successfully started
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"result": "Started deletion of data instance %q from repo with root %s"}`,
		dataname, repo.RootUUID())
}

func repoNewDataHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo := (c.Env["repo"]).(datastore.Repo)
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for 'new': %s", err.Error()))
		return
	}

	// Make sure that the passed configuration has data type and instance name.
	typename, found, err := config.GetString("typename")
	if !found || err != nil {
		BadRequest(w, r, "POST on repo endpoint requires specification of valid 'typename'")
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
	repo := (c.Env["repo"]).(datastore.Repo)
	uuid, _, err := datastore.MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	err = repo.Lock(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "Lock on node %s successful.\n", uuid)
	}
}

func repoBranchHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	repo := (c.Env["repo"]).(datastore.Repo)
	uuid, _, err := datastore.MatchingUUID(c.URLParams["uuid"])
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
