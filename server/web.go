/*
   This file supports the DVID REST API, breaking down URLs into
   commands and massaging attached data into appropriate data types.
*/

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/go/nrsc"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
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

		<p>All API endpoints follow this layout:
		<pre>
/serverhost:someport/api/...
		</pre>
		The online documentation doesn't show the server host prefixed to the "/api/..." URL,
		but it is required.

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

	Returns JSON with the datatypes of currently stored data instances.  Datatypes are represented
	by a name and the URL of the reference implementation.  To see all possible datatypes, i.e., the
	list of compiled datatypes, use the "compiled-types" endpoint.

 GET  /api/server/compiled-types

 	Returns JSON of all possible datatypes for this server, i.e., the list of compiled datatypes.

POST  /api/server/settings

	Sets server parameters.  Expects JSON to be posted with optional keys denoting parameters:
	{
		"gc": 500,
		"throttle": 2
	}

	 
	Possible keys:
	gc        Garbage collection target percentage.  This is a low-level server tuning
	            request that can affect overall request latency.
	            See: https://golang.org/pkg/runtime/debug/#SetGCPercent

	throttle  Maximum number of CPU-intensive requests that can be executed under throttle mode.
	            See imageblk and labelblk GET 3d voxels and POST voxels.
	            Default = 1.

-------------------------
Repo-Level REST endpoints
-------------------------

 POST /api/repos

	Creates a new repository.  Expects configuration data in JSON as the body of the POST.
	Configuration is a JSON object with optional "alias" and "description" properties.
	Returns the root UUID of the newly created repo in JSON object: {"root": uuid}

 GET  /api/repos/info

	Returns JSON for the repositories under management by this server.

 HEAD /api/repo/{uuid}

	Returns 200 if a repo with given UUID is available.

 GET  /api/repo/{uuid}/info

	Returns JSON for just the repository with given root UUID.  The UUID string can be
	shortened as long as it is uniquely identifiable across the managed repositories.

 POST /api/repo/{uuid}/instance

	Creates a new instance of the given data type.  Expects configuration data in JSON
	as the body of the POST.  Configuration data is a JSON object with each property
	corresponding to a configuration keyword for the particular data type.  

	JSON name/value pairs:

	REQUIRED "typename"   Type name of the new instance,
	REQUIRED "dataname"   Name of the new instance
	OPTIONAL "sync"       Comma-separated list of instance names. This makes the new data
	                      instance subscribe to changes in the given data instances.
	OPTIONAL "versioned"  If "false" or "0", the data is unversioned and acts as if 
	                      all UUIDs within a repo become the root repo UUID.  (True by default.)
	
  GET /api/repo/{uuid}/log
 POST /api/repo/{uuid}/log

	GETs or POSTs log data to the repo with given UUID.  The get or post body should 
	be JSON of the following format: 

	{ "log": [ "provenance data...", "provenance data...", ...] }

	The log is a list of strings that will be appended to the repo's log.  They should be
	descriptions for the entire repo and not just one node.  For particular versions, use
	node-level logging (below).

 POST /api/repo/{uuid}/merge

	Creates a conflict-free merge of a set of committed parent UUIDs into a child.  Note
	the merge will not necessarily create an error immediately, but later GETs that
	detect conflicts will produce an error at that time.  These can be resolved by
	doing a POST on the "resolve" endpoint below.

	The post body should be JSON of the following format: 

	{ 
		"mergeType": "conflict-free",
		"parents": [ "parent-uuid1", "parent-uuid2", ... ],
		"note": "this is a description of what I did on this commit"
	}

	The elements of the JSON object are:

		mergeType:  must be "conflict-free".  We will introduce other merge options in future.
		parents:    a list of the parent UUIDs to be merged. 
		note:       any note that should be set for the child version.

	A JSON response will be sent with the following format:

	{ "child": "3f01a8856" }

	The response includes the UUID of the new merged, child node.

 POST /api/repo/{uuid}/resolve

	Forces a merge of a set of committed parent UUIDs into a child by specifying a
	UUID order that establishes priorities in case of conflicts (see "parents" description
	below.  

	Unlike the very fast but lazily-enforced 'merge' endpoint, this request spawns an 
	asynchronous routine that checks all data for the given data instances (see "data" in
	JSON post), creates versions to delete conflicts, and then performs the conflict-free 
	merge to a final child.  

	The post body should be JSON of the following format: 

	{ 
		"data": [ "instance-name-1", "instance-name2", ... ],
		"parents": [ "parent-uuid1", "parent-uuid2", ... ],
		"note": "this is a description of what I did on this commit"
	}

	The elements of the JSON object are:

		data:       A list of the data instance names to be scanned for possible conflicts.
		parents:    A list of the parent UUIDs to be merged in order of priority.  If 
		             there is a conflict between the second and third UUID, the conflicting
		             data in the third UUID will be deleted in favor of the second UUID.
		note:       Any note that should be set for the child version.

	A JSON response will be sent with the following format:

	{ "child": "3f01a8856" }

	The response includes the UUID of the new merged, child node.


 DELETE /api/repo/{uuid}?imsure=true

	Deletes the repository holding a node with UUID.  


 DELETE /api/repo/{uuid}/{dataname}?imsure=true

	Deletes a data instance of given name from the repository holding a node with UUID.	


-------------------------
Node-Level REST endpoints
-------------------------

  GET /api/node/{uuid}/log
 POST /api/node/{uuid}/log

	GETs or POSTs log data to the node (version) with given UUID.  The get or post body should 
	be JSON of the following format: 

	{ "log": [ "provenance data...", "provenance data...", ...] }

	The log is a list of strings that will be appended to the node's log.  They should be
	data usable by clients to reconstruct the types of operation done to that version
	of data.

 POST /api/node/{uuid}/commit

	Commits (locks) the node/version with given UUID.  This is required before a version can 
	be branched or pushed to a remote server.  The post body should be JSON of the 
	following format: 

	{ 
		"note": "this is a description of what I did on this commit",
		"log": [ "provenance data...", "provenance data...", ...] 
	}

	The note is a human-readable commit message.  The log is a slice of strings that may
	be computer-readable.

 POST /api/node/{uuid}/branch

	Creates a new child node (version) of the node with given UUID.  

	An optional post body should be in JSON format:

	{ "note": "this is what we'll be doing on this version" }

	A JSON response will be sent with the following format:

	{ "child": "3f01a8856" }

	The response includes the UUID of the new child node.

		</pre>

		<h4>Data type commands</h4>

		<p>This server has compiled in the following data types, each of which have a HTTP API.
		   Click on the links below to explore each data type's command line and HTTP API.</p>

		%s

		<p>Background batch processes like generation of tiles, sparse volumes, and various
		   indices, will be paused if a single server receives more than a few data type API r
		   requests over a 5 minute moving window.  You can mark your API request as
		   non-interactive (i.e., you don't mind if it's delayed) by appending a query string
		   <code>interactive=false</code>.

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

	// WriteTimeout is the maximum time in seconds DVID will wait to write data down HTTP connection.
	WriteTimeout = 300 * time.Second

	// ReadTimeout is the maximum time in seconds DVID will wait to read data from HTTP connection.
	ReadTimeout = 300 * time.Second
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

// ThrottledHTTP checks if a request can continue under throttling.  If so, it returns
// false.  If it cannot (throttled state), it sends a http.StatusServiceUnavailable and
// returns true.  Throttling is controlled by
func ThrottledHTTP(w http.ResponseWriter) bool {
	curThrottleMu.Lock()
	if curThrottledOps < maxThrottledOps {
		curThrottledOps++
		curThrottleMu.Unlock()
		return false
	}
	curThrottleMu.Unlock()
	msg := fmt.Sprintf("Server already running %d throttled operations (max = %d)\n", curThrottledOps, maxThrottledOps)
	http.Error(w, msg, http.StatusServiceUnavailable)
	return true
}

// ThrottleOpDone marks the end of a throttled operation, allowing another op blocked by ThrottledHTTP() to succeed.
func ThrottledOpDone() {
	curThrottleMu.Lock()
	curThrottledOps--
	curThrottleMu.Unlock()
}

// ServeSingleHTTP fulfills one request using the default web Mux.
func ServeSingleHTTP(w http.ResponseWriter, r *http.Request) {
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

	// TODO: Could have used "graceful" goji package but doesn't allow tailoring of timeouts
	// unless package is modified.  Not sure graceful features needed whereas tailoring
	// of server is more important.

	s := &http.Server{
		Addr:         address,
		WriteTimeout: WriteTimeout,
		ReadTimeout:  ReadTimeout,
	}
	log.Fatal(s.ListenAndServe())

	// graceful.HandleSignals()
	// if err := graceful.ListenAndServe(address, http.DefaultServeMux); err != nil {
	// 	log.Fatal(err)
	// }
	// graceful.Wait()
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
	mainMux.Use(middleware.AutomaticOptions)
	mainMux.Use(recoverHandler)
	mainMux.Use(corsHandler)

	// Handle RAML interface
	mainMux.Get("/interface", interfaceHandler)
	mainMux.Get("/interface/version", versionHandler)

	mainMux.Get("/api/help", helpHandler)
	mainMux.Get("/api/help/", helpHandler)
	mainMux.Get("/api/help/:typename", typehelpHandler)

	mainMux.Get("/api/server/info", serverInfoHandler)
	mainMux.Get("/api/server/info/", serverInfoHandler)
	mainMux.Get("/api/server/types", serverTypesHandler)
	mainMux.Get("/api/server/types/", serverTypesHandler)
	mainMux.Get("/api/server/compiled-types", serverCompiledTypesHandler)
	mainMux.Get("/api/server/compiled-types/", serverCompiledTypesHandler)
	mainMux.Post("/api/server/settings", serverSettingsHandler)

	if !readonly {
		mainMux.Post("/api/repos", reposPostHandler)
	}
	mainMux.Get("/api/repos/info", reposInfoHandler)

	repoRawMux := web.New()
	mainMux.Handle("/api/repo/:uuid", repoRawMux)
	repoRawMux.Use(repoRawSelector)
	repoRawMux.Head("/api/repo/:uuid", repoHeadHandler)
	repoRawMux.Delete("/api/repo/:uuid", repoDeleteHandler)

	repoMux := web.New()
	mainMux.Handle("/api/repo/:uuid/:action", repoMux)
	repoMux.Use(repoSelector)
	repoMux.Get("/api/repo/:uuid/info", repoInfoHandler)
	repoMux.Post("/api/repo/:uuid/instance", repoNewDataHandler)
	repoMux.Delete("/api/repo/:uuid/:dataname", repoDeleteInstanceHandler)
	repoMux.Get("/api/repo/:uuid/log", getRepoLogHandler)
	repoMux.Post("/api/repo/:uuid/log", postRepoLogHandler)
	repoMux.Post("/api/repo/:uuid/merge", repoMergeHandler)
	repoMux.Post("/api/repo/:uuid/resolve", repoResolveHandler)

	nodeMux := web.New()
	mainMux.Handle("/api/node/:uuid", nodeMux)
	mainMux.Handle("/api/node/:uuid/:action", nodeMux)
	nodeMux.Use(repoSelector)
	nodeMux.Get("/api/node/:uuid/log", getNodeLogHandler)
	nodeMux.Post("/api/node/:uuid/note", postNodeNoteHandler)
	nodeMux.Post("/api/node/:uuid/log", postNodeLogHandler)
	nodeMux.Post("/api/node/:uuid/commit", repoCommitHandler)
	nodeMux.Post("/api/node/:uuid/branch", repoBranchHandler)

	instanceMux := web.New()
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword", instanceMux)
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword/*", instanceMux)
	instanceMux.Use(repoSelector)
	instanceMux.Use(instanceSelector)
	instanceMux.NotFound(NotFound)

	mainMux.Get("/*", mainHandler)

	webMux.routesSetup = true
}

// Middleware that recovers from panics, sends email if a notification email
// has been provided, and log issues.
func recoverHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetReqID(*c)

		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 1<<16)
				size := runtime.Stack(buf, false)
				stackTrace := string(buf[0:size])
				message := fmt.Sprintf("Panic detected on request %s:\n%+v\nIP: %v, URL: %s\nStack trace:\n%s\n",
					reqID, err, r.RemoteAddr, r.URL.Path, stackTrace)
				dvid.Criticalf("%s\n", message)

				if err := SendNotification(message, nil); err != nil {
					dvid.Criticalf("Couldn't send email notifcation: %v\n", err)
				}

				http.Error(w, http.StatusText(500), 500)
			}
		}()

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func NotFound(w http.ResponseWriter, r *http.Request) {
	errorMsg := fmt.Sprintf("Could not find the URL: %s", r.URL.Path)
	dvid.Infof(errorMsg)
	http.Error(w, errorMsg, http.StatusNotFound)
}

func BadRequest(w http.ResponseWriter, r *http.Request, format interface{}, args ...interface{}) {
	var message string
	switch v := format.(type) {
	case string:
		message = v
	case error:
		message = v.Error()
	case fmt.Stringer:
		message = v.String()
	default:
		dvid.Criticalf("BadRequest called with unknown format type: %v\n", format)
		return
	}
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	errorMsg := fmt.Sprintf("%s (%s).", message, r.URL.Path)
	dvid.Errorf(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// DecodeJSON decodes JSON passed in a request into a dvid.Config.
func DecodeJSON(r *http.Request) (dvid.Config, error) {
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		return dvid.Config{}, fmt.Errorf("Malformed JSON request in body: %v", err)
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

// repoRawSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo without any access restrictions.
func repoRawSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var err error
		var uuid dvid.UUID
		if uuid, c.Env["versionID"], err = datastore.MatchingUUID(c.URLParams["uuid"]); err != nil {
			BadRequest(w, r, err)
			return
		}
		c.Env["uuid"] = uuid

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// repoSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo, and imposes restrictions depending on read-only mode and locked nodes.
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
			BadRequest(w, r, err)
			return
		}
		c.Env["uuid"] = uuid

		// Make sure locked nodes can't use anything besides GET and HEAD unless we are deleting whole repo.
		locked, err := datastore.LockedUUID(uuid)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		branchRequest := (c.URLParams["action"] == "branch")
		mergeRequest := (c.URLParams["action"] == "merge")
		resolveRequest := (c.URLParams["action"] == "resolve")
		if locked && !branchRequest && !mergeRequest && !resolveRequest && action != "get" && action != "head" {
			BadRequest(w, r, "Cannot do %s on locked node %s", action, uuid)
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// instanceSelector retrieves the data instance given its complete string name and
// forwards the request to that instance's HTTP handler.
func instanceSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var err error
		dataname := dvid.InstanceName(c.URLParams["dataname"])
		uuid, ok := c.Env["uuid"].(dvid.UUID)
		if !ok {
			msg := fmt.Sprintf("Bad format for UUID %q\n", c.Env["uuid"])
			BadRequest(w, r, msg)
			return
		}
		data, err := datastore.GetDataByUUID(uuid, dataname)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		v, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err)
			return
		}

		// Check if this is unversioned.
		if !data.Versioned() {
			// Map everything to root version.
			v, err = datastore.GetRepoRootVersion(v)
			if err != nil {
				BadRequest(w, r, err)
				return
			}
		}
		ctx := datastore.NewVersionedCtx(data, v)

		// Handle DVID-wide query string commands like non-interactive call designations
		queryStrings := r.URL.Query()

		// All HTTP requests are interactive so let server tally request.
		interactive := queryStrings.Get("interactive")
		if interactive == "" || (interactive != "false" && interactive != "0") {
			GotInteractiveRequest()
		}

		// TODO: setup routing for data instances as well.
		data.ServeHTTP(uuid, ctx, w, r)
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
		name := typeservice.GetTypeName()
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
		BadRequest(w, r, err)
		return
	}
	fmt.Fprintf(w, typeservice.Help())
}

// Handler for web client and other static content
func mainHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

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
			BadRequest(w, r, err)
			return
		}
		data, err := ioutil.ReadAll(rsrc)
		if err != nil {
			BadRequest(w, r, err)
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
		"active CGo routines": dvid.NumberActiveCGo(),
	})
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func serverInfoHandler(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := AboutJSON()
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func serverTypesHandler(w http.ResponseWriter, r *http.Request) {
	jsonMap := make(map[dvid.TypeString]string)
	typemap, err := datastore.Types()
	if err != nil {
		msg := fmt.Sprintf("Cannot return server datatypes: %v\n", err)
		BadRequest(w, r, msg)
		return
	}
	for _, t := range typemap {
		jsonMap[t.GetTypeName()] = string(t.GetTypeURL())
	}
	m, err := json.Marshal(jsonMap)
	if err != nil {
		msg := fmt.Sprintf("Cannot marshal JSON datatype info: %v (%v)\n", jsonMap, err)
		BadRequest(w, r, msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func serverCompiledTypesHandler(w http.ResponseWriter, r *http.Request) {
	jsonMap := make(map[dvid.TypeString]string)
	for _, t := range datastore.Compiled {
		jsonMap[t.GetTypeName()] = string(t.GetTypeURL())
	}
	m, err := json.Marshal(jsonMap)
	if err != nil {
		msg := fmt.Sprintf("Cannot marshal JSON datatype info: %v (%v)\n", jsonMap, err)
		BadRequest(w, r, msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func serverSettingsHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for 'new': %v", err))
		return
	}
	w.Header().Set("Content-Type", "text/plain")

	// Handle GC percentage setting
	percent, found, err := config.GetInt("gc")
	if err != nil {
		BadRequest(w, r, "POST on settings endpoint had bad parsing of 'gc' key: %v", err)
		return
	}
	if found {
		old := debug.SetGCPercent(percent)
		fmt.Fprintf(w, "DVID server garbage collection target percentage set to %d from %d\n", percent, old)
	}

	// Handle max throttle ops setting
	maxOps, found, err := config.GetInt("throttle")
	if err != nil {
		BadRequest(w, r, "POST on settings endpoint had bad parsing of 'throttle' key: %v", err)
		return
	}
	if found {
		old := maxThrottledOps
		SetMaxThrottleOps(maxOps)
		fmt.Fprintf(w, "Maximum throttled ops set to %d from %d\n", maxOps, old)
	}
}

func reposInfoHandler(w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := datastore.MarshalJSON()
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

// TODO -- Maybe allow assignment of child UUID via JSON in POST.  Right now, we only
// allow this potentially dangerous function via command-line.
func reposPostHandler(w http.ResponseWriter, r *http.Request) {
	config := dvid.NewConfig()
	if r.Body != nil {
		if err := config.SetByJSON(r.Body); err != nil {
			BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for new repo: %v", err))
			return
		}
	}

	alias, _, err := config.GetString("alias")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint requires valid 'alias': %v", err)
		return
	}
	description, _, err := config.GetString("description")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint requires valid 'description': %v", err)
		return
	}

	root, err := datastore.NewRepo(alias, description, nil)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %q}", "root", root)
}

func repoHeadHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := (c.Env["uuid"]).(dvid.UUID)
	root, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Repo available with root UUID %s\n", root)
}

func repoInfoHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := (c.Env["uuid"]).(dvid.UUID)
	jsonStr, err := datastore.GetRepoJSON(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func repoDeleteHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	queryStrings := r.URL.Query()
	imsure := queryStrings.Get("imsure")
	if imsure != "true" {
		BadRequest(w, r, "Cannot delete repo unless query string 'imsure=true' is present!")
		return
	}

	err := datastore.DeleteRepo(uuid)
	if err != nil {
		BadRequest(w, r, "Error trying to delete repo with UUID %s: %v", uuid, err)
		return
	}

	// Just respond that deletion was successfully started
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"result": "Started deletion of repo containing UUID %s"}`, uuid)
}

func repoDeleteInstanceHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	queryStrings := r.URL.Query()
	imsure := queryStrings.Get("imsure")
	if imsure != "true" {
		BadRequest(w, r, "Cannot delete instance unless query string 'imsure=true' is present!")
		return
	}

	dataname, ok := c.URLParams["dataname"]
	if !ok {
		BadRequest(w, r, "Error retrieving data instance name from URL parameters")
		return
	}

	// Make sure this instance exists.
	_, err := datastore.GetDataByUUID(uuid, dvid.InstanceName(dataname))
	if err != nil {
		BadRequest(w, r, "Error trying to delete %q for UUID %s: %v", dataname, uuid, err)
		return
	}

	// Do the deletion.  Under hood, modifies metadata immediately and launches async k/v deletion.
	if err := datastore.DeleteDataByUUID(uuid, dvid.InstanceName(dataname)); err != nil {
		BadRequest(w, r, "Error deleting data instance %q: %v", dataname, err)
		return
	}

	// Just respond that deletion was successfully started
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"result": "Started deletion of data instance %q from repo with root %s"}`,
		dataname, uuid)
}

func repoNewDataHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	config := dvid.NewConfig()
	if err := config.SetByJSON(r.Body); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error decoding POSTed JSON config for 'new': %v", err))
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
		BadRequest(w, r, err)
		return
	}
	_, err = datastore.NewData(uuid, typeservice, dvid.InstanceName(dataname), config)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{%q: "Added %s [%s] to node %s"}`, "result", dataname, typename, uuid)
}

func getRepoLogHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	logdata, err := datastore.GetRepoLog(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsonStr, err := json.Marshal(struct {
		Log []string `json:"log"`
	}{
		logdata,
	})
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	fmt.Fprintf(w, string(jsonStr))
}

func postRepoLogHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := make(map[string][]string)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&jsonData); err != nil && err != io.EOF {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %s", err))
		return
	}
	logdata, ok := jsonData["log"]
	if !ok {
		BadRequest(w, r, "Could not find 'log' value in POSTed JSON.")
	}
	if err := datastore.AddToRepoLog(uuid, logdata); err != nil {
		BadRequest(w, r, err)
		return
	}
}

func getNodeLogHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	logdata, err := datastore.GetNodeLog(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsonStr, err := json.Marshal(struct {
		Log []string `json:"log"`
	}{
		logdata,
	})
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	fmt.Fprintf(w, string(jsonStr))
}

func postNodeNoteHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := make(map[string]string)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&jsonData); err != nil && err != io.EOF {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %s", err))
		return
	}
	note, ok := jsonData["note"]
	if !ok {
		BadRequest(w, r, "Could not find 'note' value in POSTed JSON.")
	}
	if err := datastore.SetNodeNote(uuid, note); err != nil {
		BadRequest(w, r, err)
		return
	}
}

func postNodeLogHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := make(map[string][]string)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&jsonData); err != nil && err != io.EOF {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %s", err))
		return
	}
	logdata, ok := jsonData["log"]
	if !ok {
		BadRequest(w, r, "Could not find 'log' value in POSTed JSON.")
	}
	if err := datastore.AddToNodeLog(uuid, logdata); err != nil {
		BadRequest(w, r, err)
		return
	}
}

func repoCommitHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		BadRequest(w, r, err)
		return
	}

	jsonData := struct {
		Note string   `json:"note"`
		Log  []string `json:"log"`
	}{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %v", err))
		return
	}

	err = datastore.Commit(uuid, jsonData.Note, jsonData.Log)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "Node %s committed and locked.\n", uuid)
	}
}

// TODO -- Might allow specification of UUID for child via HTTP, or only
// allow this potentially dangerous op via command line.
func repoBranchHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := struct {
		Note string `json:"note"`
	}{}
	if r.Body != nil {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			BadRequest(w, r, err)
			return
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &jsonData); err != nil {
				BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %v", err))
				return
			}
		}

	}
	newuuid, err := datastore.NewVersion(uuid, jsonData.Note, nil)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "child", newuuid)
	}
}

func repoMergeHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		BadRequest(w, r, "merge requires JSON to be POSTed per API documentation")
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		BadRequest(w, r, err)
		return
	}

	jsonData := struct {
		MergeType string   `json:"mergeType"`
		Note      string   `json:"note"`
		Parents   []string `json:"parents"`
	}{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %v", err))
		return
	}
	if len(jsonData.Parents) < 2 {
		BadRequest(w, r, "Must specify at least two parent UUIDs using 'parents' field")
		return
	}

	// Convert JSON of parents into []UUID
	parents := make([]dvid.UUID, len(jsonData.Parents))
	for i, uuidFrag := range jsonData.Parents {
		uuid, _, err := datastore.MatchingUUID(uuidFrag)
		if err != nil {
			BadRequest(w, r, fmt.Sprintf("can't match parent %q: %v", uuidFrag, err))
			return
		}
		parents[i] = uuid
	}

	// Convert merge type designation
	var mt datastore.MergeType
	switch jsonData.MergeType {
	case "conflict-free":
		mt = datastore.MergeConflictFree
	default:
		BadRequest(w, r, fmt.Sprintf("'mergeType' must be 'conflict-free' at this time"))
		return
	}

	// Do the merge
	newuuid, err := datastore.Merge(parents, jsonData.Note, mt)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "child", newuuid)
	}
}

func repoResolveHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid, _, err := datastore.MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	if r.Body == nil {
		BadRequest(w, r, "merge resolving requires JSON to be POSTed per API documentation")
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		BadRequest(w, r, err)
		return
	}

	jsonData := struct {
		Data    []dvid.InstanceName `json:"data"`
		Note    string              `json:"note"`
		Parents []string            `json:"parents"`
	}{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %v", err))
		return
	}
	if len(jsonData.Data) == 0 {
		BadRequest(w, r, "Must specify at least one data instance using 'data' field")
		return
	}
	if len(jsonData.Parents) < 2 {
		BadRequest(w, r, "Must specify at least two parent UUIDs using 'parents' field")
		return
	}

	// Convert JSON of parents into []UUID and whether we need a child for them to add deletions.
	numParents := len(jsonData.Parents)
	oldParents := make([]dvid.UUID, numParents)
	newParents := make([]dvid.UUID, numParents, numParents) // UUID of any parent extension for deletions.
	for i, uuidFrag := range jsonData.Parents {
		uuid, _, err := datastore.MatchingUUID(uuidFrag)
		if err != nil {
			BadRequest(w, r, fmt.Sprintf("can't match parent %q: %v", uuidFrag, err))
			return
		}
		oldParents[i] = uuid
		newParents[i] = dvid.NilUUID
	}

	// Iterate through all k/v for given data instances, making sure we find any conflicts.
	// If any are found, remove them with first UUIDs taking priority.
	for _, name := range jsonData.Data {
		data, err := datastore.GetDataByUUID(uuid, name)
		if err != nil {
			BadRequest(w, r, err)
			return
		}

		if err := datastore.DeleteConflicts(uuid, data, oldParents, newParents); err != nil {
			BadRequest(w, r, fmt.Errorf("Conflict deletion error for data %q: %v", data.DataName(), err))
			return
		}
	}

	// If we have any new nodes to accomodate deletions, commit them.
	for i, oldUUID := range oldParents {
		if newParents[i] != oldUUID {
			err := datastore.Commit(newParents[i], "Version for deleting conflicts before merge", nil)
			if err != nil {
				BadRequest(w, r, "Error while creating new nodes to handle required deletions: %v", err)
				return
			}
		}
	}

	// Do the merge
	mt := datastore.MergeConflictFree
	newuuid, err := datastore.Merge(newParents, jsonData.Note, mt)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "child", newuuid)
	}
}
