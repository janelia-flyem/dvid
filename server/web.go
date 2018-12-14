/*
   This file supports the DVID REST API, breaking down URLs into
   commands and massaging attached data into appropriate data types.
*/

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/nrsc"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

const webHelp = `
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
		The <a href="/">DVID admin console</a> may be available if you have downloaded the
		<a href="https://github.com/janelia-flyem/dvid-console">DVID console web client repo</a>
		and included <i>-webclient=/path/to/console</i> when running the
		<code>dvid serve</code> command.</p>
		
		<h3>HTTP API and command line use</h3>

		<p>All API endpoints (except for memory profiling) follow this layout:
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

 GET  /api/storage

 	Returns a JSON object for each backend store where the key is the backend store name.
	The store object has local instance ID keys with the following object value:

	{
		"Name": "grayscale",
		"DataType": "uint8blk",
		"DataUUID": ...,
		"RootUUID": ...,  // this is the UUID of this data's root
		"Bytes": ...
	}

 GET  /api/server/info

	Returns JSON for server properties.

 GET  /api/server/note 

	Returns any value of [server.note] from the configuration TOML.

 GET  /api/server/types

	Returns JSON with the datatypes of currently stored data instances.  Datatypes are represented
	by a name and the URL of the reference implementation.  To see all possible datatypes, i.e., the
	list of compiled datatypes, use the "compiled-types" endpoint.

 GET  /api/server/compiled-types

 	Returns JSON of all possible datatypes for this server, i.e., the list of compiled datatypes.

 GET  /api/server/groupcache

 	Returns JSON for groupcache statistics for this server.  See github.com/golang/groupcache package
	Stats and CacheStats for MainCache and HotCache.

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


POST  /api/server/reload-metadata

	Reloads the metadata from storage.  This is useful when using multiple DVID frontends with 
	a shared storage backend.  Typically, one DVID is designated "master" and handles any
	modifications of the repositories and data instances.  Other DVIDs simply reload the metadata 
	when prompted by an external coordinator, allowing the "slave" DVIDs to see changes made by
	the master DVID.

-------------------------
Memory Profiler endpoints
-------------------------

 GET  /profiler/start

 	Starts a memory profiler and redirects to a web page where you can see real-time
	memory usage graphs.

 GET  /profiler/stop

 	Stops the memory profiler.

 GET  /profiler/info.html

 	Shows real-time memory usage graphs.

 GET  /profiler/info

 	Returns JSON of memory usage data.


-------------------------
Repo-Level REST endpoints
-------------------------

 POST /api/repos

	Creates a new repository.  Expects configuration data in JSON as the body of the POST.
	Configuration is a JSON object with optional "alias", "description", "root" (the desired
	UUID of the root), and "passcode" properties. Returns the root UUID of the newly created 
	repo in JSON object: {"root": uuid}

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

	REQUIRED "typename"     Type name of the new instance,
	REQUIRED "dataname"     Name of the new instance
	OPTIONAL "versioned"    If "false" or "0", the data is unversioned and acts as if
							all UUIDs within a repo become the root repo UUID.  (True by default.)
	OPTIONAL "Compression"  Specify the compression format to use when serializing values to disk.
							(Applies to most instance types, but not all.)
							Choices are: “none”, “snappy”, “lz4”, “gzip”, “jpeg”.
							Where applicable, the compression level can be appended, e.g. "jpeg:80".
	OPTIONAL "Tags"         Can send list of tags as a series of equal statements separated by
							commas, e.g., "type=meshes,stuff=something-something".  This will
							create a tag "type" set to "meshes" and a tag "stuff" set to 
							"something-something".  Note that the formatting prevents spaces
							and commas from being part of a tag.

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "newinstance",
		"UUID": <UUID>,
		"Typename": <typename>,
		"Dataname": <dataname>
	}
					  	  

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


-------------------------
Node-Level REST endpoints
-------------------------

  GET /api/node/{uuid}/note
 POST /api/node/{uuid}/note

	GETs or POSTs note data to the node (version) with given UUID.  The get or post body should 
	be JSON of the following format: 

	{ "note": "this is a description of what I did on this version" }

	The log is a list of strings that will be appended to the node's log.  They should be
	data usable by clients to reconstruct the types of operation done to that version
	of data.

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "nodenote",
		"UUID": <UUID>,
		"Note": <note>
	}


  GET /api/node/{uuid}/log
 POST /api/node/{uuid}/log

	GETs or POSTs log data to the node (version) with given UUID.  The get or post body should 
	be JSON of the following format: 

	{ "log": [ "provenance data...", "provenance data...", ...] }

	The log is a list of strings that will be appended to the node's log.  They should be
	data usable by clients to reconstruct the types of operation done to that version
	of data.

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "nodelog",
		"UUID": <UUID>,
		"Log": [<msg 1>, <msg2>, ...]
	}


 GET /api/node/{uuid}/status

	Returns the commit or lock status of the node with given UUID in JSON format:

	{ "Locked": true }

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

	If successful, a valid JSON response will be sent with the following format:

	{ "committed": "3f01a8856" }

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "commit",
		"UUID": <UUID of committed node>
		"Note": <string>,
		"Log": [<msg 1>, <msg2>, ...]
	}


 POST /api/node/{uuid}/branch

	Creates a new branch child node (version) of the node with given UUID.
	The branch name must be unique across the DAG.

	The post body should be in JSON format, where "note" and "uuid" are optional:

	{
		"branch": "unique name of new branch",
		"note": "this is what we'll be doing on this version",
		"uuid": <desired UUID>
	}

	A JSON response will be sent with the following format:

	{ "child": "3f01a8856" }

	The response includes the UUID of the new child node.

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "branch",
		"Parent": <UUID of parent>
		"Child": <UUID of new child>,
		"Branch": <branch designation as string>,
		"Note": <string>
	}

	
 POST /api/node/{uuid}/newversion

	Creates a new child node (version) of the node with given
	UUID if no open node exists.

	An optional post body should be in JSON format:

	{ 
		"note": "this is what we'll be doing on this version",
		"uuid": <desired UUID>
	}

	A JSON response will be sent with the following format:

	{ "child": "3f01a8856" }

	The response includes the UUID of the new child node.

	A JSON message will be sent to any associated Kafka system with the following format:
	{ 
		"Action": "newinstance",
		"Parent": <UUID of parent>
		"Child": <UUID of new child>,
		"Note": <string>
	}


 GET /api/node/{uuid}/{data name}/blobstore/{reference}
 POST /api/node/{uuid}/{data name}/blobstore

	GETs or POSTs data into a blob store associated with the given data instance.
	The reference is a URL-friendly content hash (FNV-128) of the blob data.
	The POST will store data and return the folloing JSON:

	{ "reference": "<the content hash of blob data>" }

	Note that POST /blobstore will not be logged in any associated kafka system.

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

	// WebAPIPath is the relative URL path to our Level 2 REST API
	WebAPIPath = "/api/" + WebAPIVersion

	// WriteTimeout is the maximum time in seconds DVID will wait to write data down HTTP connection.
	WriteTimeout = 600 * time.Second

	// ReadTimeout is the maximum time in seconds DVID will wait to read data from HTTP connection.
	ReadTimeout = 600 * time.Second
)

var (
	webMux struct {
		*web.Mux
		routesSetup bool
	}
	webMuxMu sync.Mutex // Can Lock() to prevent any kind of web requests from initiating actions.
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

// ThrottledOpDone marks the end of a throttled operation, allowing another op blocked
// by ThrottledHTTP() to succeed.
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
func serveHTTP() {
	var mode string
	if readonly {
		mode = " (read-only mode)"
	} else if fullwrite {
		mode = " (full write mode)"
	}
	dvid.Infof("Web server listening at %s%s ...\n", config.HTTPAddress(), mode)
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
		Addr:         config.HTTPAddress(),
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
	if webMux.routesSetup {
		return
	}

	webMuxMu.Lock()
	silentMux := web.New()
	webMux.Handle("/api/load", silentMux)
	silentMux.Use(corsHandler)
	silentMux.Get("/api/load", loadHandler)

	mainMux := web.New()
	webMux.Handle("/*", mainMux)
	mainMux.Use(middleware.Logger)
	mainMux.Use(middleware.AutomaticOptions)
	mainMux.Use(httpAvailHandler)
	mainMux.Use(recoverHandler)
	mainMux.Use(corsHandler)

	mainMux.Get("/interface", interfaceHandler)
	mainMux.Get("/interface/version", versionHandler)

	mainMux.Get("/api/help", helpHandler)
	mainMux.Get("/api/help/", helpHandler)
	mainMux.Get("/api/help/:typename", typehelpHandler)

	mainMux.Get("/api/storage", serverStorageHandler)

	serverMux := web.New()
	mainMux.Handle("/api/server/:action", serverMux)
	serverMux.Use(activityLogHandler)
	serverMux.Get("/api/server/info", serverInfoHandler)
	serverMux.Get("/api/server/info/", serverInfoHandler)
	serverMux.Get("/api/server/note", serverNoteHandler)
	serverMux.Get("/api/server/note/", serverNoteHandler)
	serverMux.Get("/api/server/types", serverTypesHandler)
	serverMux.Get("/api/server/types/", serverTypesHandler)
	serverMux.Get("/api/server/compiled-types", serverCompiledTypesHandler)
	serverMux.Get("/api/server/compiled-types/", serverCompiledTypesHandler)
	serverMux.Get("/api/server/groupcache", serverGroupcacheHandler)
	serverMux.Get("/api/server/groupcache/", serverGroupcacheHandler)
	serverMux.Post("/api/server/settings", serverSettingsHandler)
	serverMux.Post("/api/server/reload-metadata", serverReload)
	serverMux.Post("/api/server/reload-metadata/", serverReload)

	if !readonly {
		mainMux.Post("/api/repos", reposPostHandler)
	}
	mainMux.Get("/api/repos/info", reposInfoHandler)

	repoRawMux := web.New()
	mainMux.Handle("/api/repo/:uuid", repoRawMux)
	repoRawMux.Use(activityLogHandler)
	repoRawMux.Use(repoRawSelector)
	repoRawMux.Head("/api/repo/:uuid", repoHeadHandler)

	repoMux := web.New()
	mainMux.Handle("/api/repo/:uuid/:action", repoMux)
	repoMux.Use(activityLogHandler)
	repoMux.Use(repoSelector)
	repoMux.Get("/api/repo/:uuid/info", repoInfoHandler)
	repoMux.Post("/api/repo/:uuid/instance", repoNewDataHandler)
	repoMux.Get("/api/repo/:uuid/log", getRepoLogHandler)
	repoMux.Post("/api/repo/:uuid/log", postRepoLogHandler)
	repoMux.Post("/api/repo/:uuid/merge", repoMergeHandler)
	repoMux.Post("/api/repo/:uuid/resolve", repoResolveHandler)

	nodeMux := web.New()
	mainMux.Handle("/api/node/:uuid", nodeMux)
	mainMux.Handle("/api/node/:uuid/:action", nodeMux)
	nodeMux.Use(activityLogHandler)
	nodeMux.Use(repoRawSelector)
	nodeMux.Use(nodeSelector)
	nodeMux.Get("/api/node/:uuid/note", getNodeNoteHandler)
	nodeMux.Post("/api/node/:uuid/note", postNodeNoteHandler)
	nodeMux.Get("/api/node/:uuid/log", getNodeLogHandler)
	nodeMux.Post("/api/node/:uuid/log", postNodeLogHandler)
	nodeMux.Get("/api/node/:uuid/commit", repoCommitStateHandler)
	nodeMux.Get("/api/node/:uuid/status", repoCommitStateHandler)
	nodeMux.Post("/api/node/:uuid/commit", repoCommitHandler)
	nodeMux.Post("/api/node/:uuid/branch", repoBranchHandler)
	nodeMux.Post("/api/node/:uuid/newversion", repoNewVersionHandler)

	instanceMux := web.New()
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword", instanceMux)
	mainMux.Handle("/api/node/:uuid/:dataname/:keyword/*", instanceMux)
	instanceMux.Use(repoRawSelector)
	instanceMux.Use(instanceSelector)
	instanceMux.NotFound(notFound)

	mainMux.Get("/*", mainHandler)

	webMux.routesSetup = true
	webMuxMu.Unlock()
}

// returns true and sends a 503 (Service Unavailable) status code if unavailable.
func httpUnavailable(w http.ResponseWriter) bool {
	if dvid.RequestsOK() {
		return false
	}
	http.Error(w, "DVID server is unavailable.", http.StatusServiceUnavailable)
	return true
}

type wrappedResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	status      int
	bytes       int
}

func (w *wrappedResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *wrappedResponseWriter) Write(buf []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	n, err := w.ResponseWriter.Write(buf)
	w.bytes += n
	return n, err
}

func wrapResponseWriter(w http.ResponseWriter) *wrappedResponseWriter {
	wr := wrappedResponseWriter{
		ResponseWriter: w,
	}
	return &wr
}

// Middleware that prevents any web requests if httpAvail is false.  Allows draconian
// shutdown of server when doing critical reorg of internals.
func httpAvailHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if httpUnavailable(w) {
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Middleware that recovers from panics, sends email if a notification email
// has been provided, and log issues.
func recoverHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetReqID(*c)

		defer func() {
			if e := recover(); e != nil {
				msg := fmt.Sprintf("Panic detected on request %s:\n%+v\nIP: %v, URL: %s\n",
					reqID, e, r.RemoteAddr, r.URL.Path)
				dvid.ReportPanic(msg, WebServer())
				http.Error(w, msg, 500)
			}
		}()

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Middleware that logs activity to kafka if available.
func activityLogHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		myw := wrapResponseWriter(w)
		h.ServeHTTP(myw, r)
		if KafkaAvailable() {
			user := r.URL.Query().Get("u")
			app := r.URL.Query().Get("app")
			t := time.Since(t0)
			activity := map[string]interface{}{
				"time":        t0.Unix(),
				"duration":    t.Seconds() * 1000.0,
				"status":      myw.status,
				"user":        user,
				"client":      app,
				"method":      r.Method,
				"uri":         r.RequestURI,
				"bytes_in":    r.ContentLength,
				"bytes_out":   myw.bytes,
				"remote_addr": r.RemoteAddr,
			}
			storage.LogActivityToKafka(activity)
		}
	}
	return http.HandlerFunc(fn)
}

func notFound(w http.ResponseWriter, r *http.Request) {
	errorMsg := fmt.Sprintf("Could not find the URL: %s", r.URL.Path)
	dvid.Infof(errorMsg)
	http.Error(w, errorMsg, http.StatusNotFound)
}

// BadAPIRequest writes a standard error message to http.ResponseWriter for a badly formatted API call.
func BadAPIRequest(w http.ResponseWriter, r *http.Request, d dvid.Data) {
	helpURL := path.Join("api", "help", string(d.TypeName()))
	var host string
	config := GetConfig()
	if config == nil {
		host = "unknown - config unset"
	} else {
		host = config.Host()
	}
	msg := fmt.Sprintf("Bad API call (%s) for data %q.  See API help at http://%s/%s", r.URL.Path, d.DataName(), host, helpURL)
	http.Error(w, msg, http.StatusBadRequest)
	dvid.Errorf("Bad API call (%s) for data %q\n", r.URL.Path, d.DataName())
}

// BadRequest writes an error message out to the http.ResponseWriter using format similar to fmt.Printf.
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
	dvid.Errorf(errorMsg + "\n")
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// DecodeJSON decodes JSON passed in a request into a dvid.Config.
func DecodeJSON(r *http.Request) (dvid.Config, error) {
	c := dvid.NewConfig()
	if err := c.SetByJSON(r.Body); err != nil {
		return dvid.Config{}, fmt.Errorf("Malformed JSON request in body: %v", err)
	}
	return c, nil
}

// ---- Middleware -------------

// corsHandler adds CORS support via header
func corsHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Allow cross-origin resource sharing.
		w.Header().Set("Access-Control-Allow-Origin", "*")

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// repoRawSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo without any access restrictions.
func repoRawSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		method := strings.ToLower(r.Method)
		if readonly && method != "get" && method != "head" {
			BadRequest(w, r, "Server in read-only mode and will only accept GET and HEAD requestcs")
			return
		}

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

// nodeSelector identifies a node, and imposes restrictions depending on read-only mode and locked nodes.
func nodeSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		uuid, ok := c.Env["uuid"].(dvid.UUID)
		if !ok {
			msg := fmt.Sprintf("Bad format for UUID %q\n", c.Env["uuid"])
			BadRequest(w, r, msg)
			return
		}
		// Make sure locked nodes can't use anything besides GET and HEAD unless we are deleting whole repo.
		locked, err := datastore.LockedUUID(uuid)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		method := strings.ToLower(r.Method)
		branchRequest := (c.URLParams["action"] == "branch") || (c.URLParams["action"] == "newversion")
		if !fullwrite && locked && !branchRequest && method != "get" && method != "head" {
			BadRequest(w, r, "Cannot do %s on locked node %s", method, uuid)
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// repoSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo and enforces read-only mode.
func repoSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		method := strings.ToLower(r.Method)
		if readonly && method != "get" && method != "head" {
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
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// instanceSelector retrieves the data instance given its complete string name and
// forwards the request to that instance's HTTP handler.
func instanceSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		var err error
		dataname := dvid.InstanceName(c.URLParams["dataname"])
		uuid, ok := c.Env["uuid"].(dvid.UUID)
		if !ok {
			msg := fmt.Sprintf("Bad format for UUID %q\n", c.Env["uuid"])
			BadRequest(w, r, msg)
			return
		}
		data, err := datastore.GetDataByUUIDName(uuid, dataname)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		method := strings.ToLower(r.Method)

		// handle all blobstore requests
		if c.URLParams["keyword"] == "blobstore" {
			switch method {
			case "get":
				url := r.URL.Path[len(WebAPIPath):]
				parts := strings.Split(url, "/")
				if len(parts) != 5 {
					BadRequest(w, r, fmt.Errorf("GET /blobstore requires reference"))
					return
				}
				ref := parts[4]
				value, err := data.GetBlob(ref)
				if err != nil {
					BadRequest(w, r, err)
					return
				}
				if value != nil || len(value) > 0 {
					w.Header().Set("Content-Type", "application/octet-stream")
					_, err = w.Write(value)
					if err != nil {
						BadRequest(w, r, err)
					}
				} else {
					http.Error(w, fmt.Sprintf("Reference %q not found", ref), http.StatusNotFound)
				}

			case "post":
				value, err := ioutil.ReadAll(r.Body)
				if err != nil {
					BadRequest(w, r, err)
					return
				}
				ref, err := data.PutBlob(value)
				if err != nil {
					BadRequest(w, r, err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"reference": %q}`, ref)

			default:
				BadRequest(w, r, "can only do GET and POST actions on blobstore endpoint")
			}
			return
		}

		v, err := datastore.VersionFromUUID(uuid)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		if data.Versioned() {
			// Make sure we aren't trying mutable methods on committed nodes.
			locked, err := datastore.LockedUUID(uuid)
			if err != nil {
				BadRequest(w, r, err)
				return
			}
			if !fullwrite && locked && data.IsMutationRequest(r.Method, c.URLParams["keyword"]) {
				BadRequest(w, r, "Cannot do %s on endpoint %q of locked node %s", r.Method, c.URLParams["keyword"], uuid)
				return
			}
		} else {
			// Map everything to root version.
			v, err = datastore.GetRepoRootVersion(v)
			if err != nil {
				BadRequest(w, r, err)
				return
			}
		}
		ctx := datastore.NewVersionedCtx(data, v)

		// Also set the web request information in case logging needs it downstream.
		ctx.SetRequestID(middleware.GetReqID(*c))

		// Handle DVID-wide query string commands like non-interactive call designations
		queryStrings := r.URL.Query()

		// All HTTP requests are interactive so let server tally request.
		interactive := queryStrings.Get("interactive")
		if interactive == "" || (interactive != "false" && interactive != "0") {
			GotInteractiveRequest()
		}

		// TODO: setup routing for data instances as well.
		if config != nil && config.AllowTiming() {
			w.Header().Set("Timing-Allow-Origin", "*")
		}

		if method == "post" {
			mirrors := instanceMirrors(data.DataUUID(), uuid)
			if len(mirrors) > 0 {
				buf, err := ioutil.ReadAll(r.Body)
				if err != nil {
					BadRequest(w, r, "unable to read POST for mirroring: %v", err)
					return
				}
				r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
				contentType := r.Header.Get("Content-Type")
				for _, hostAndPort := range mirrors {
					url := hostAndPort + r.URL.Path
					dvid.Infof("echoing POST asynchronously to %s\n", url)
					go func(url string) {
						resp, err := http.Post(url, contentType, bytes.NewBuffer(buf))
						if err != nil {
							dvid.Errorf("problem echoing POST (%s): %v\n", url, err)
						} else {
							dvid.Infof("echoed POST: %s (status %d)\n", url, resp.StatusCode)
						}
					}(url)
				}
			}
		}
		myw := wrapResponseWriter(w)
		activity := data.ServeHTTP(uuid, ctx, myw, r)
		if KafkaAvailable() {
			user := r.URL.Query().Get("u")
			app := r.URL.Query().Get("app")
			t := time.Since(t0)
			data := map[string]interface{}{
				"time":        t0.Unix(),
				"duration":    t.Seconds() * 1000.0,
				"status":      myw.status,
				"user":        user,
				"client":      app,
				"method":      r.Method,
				"uri":         r.RequestURI,
				"bytes_in":    r.ContentLength,
				"bytes_out":   myw.bytes,
				"remote_addr": r.RemoteAddr,
			}
			if len(activity) > 0 {
				for k, v := range activity {
					data[k] = v
				}
			}
			storage.LogActivityToKafka(data)
		}
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
	typenames := make([]string, len(datastore.Compiled))
	i := 0
	for _, typeservice := range datastore.Compiled {
		typenames[i] = string(typeservice.GetTypeName())
		i++
	}
	sort.Strings(typenames)
	html := "<ul>"
	for _, name := range typenames {
		html += "<li>"
		html += fmt.Sprintf("<a href='/api/help/%s'>%s</a>", name, name)
		html += "</li>"
	}
	html += "</ul>"

	// Return the embedded help page.
	fmt.Fprintf(w, fmt.Sprintf(webHelp, hostname, html))
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
		redirectURL := config.WebRedirectPath()
		if len(redirectURL) > 0 || config.WebDefaultFile() != "" {
			_, err := os.Stat(filename)
			if os.IsNotExist(err) {
				if len(redirectURL) > 0 {
					if redirectURL[0] != '/' {
						redirectURL = "/" + redirectURL
					}
					dvid.Debugf("[%s] Redirecting bad file (%s) to default path: %s\n", r.Method, filename, redirectURL)
					http.Redirect(w, r, redirectURL, http.StatusPermanentRedirect)
				} else {
					filename = filepath.Join(config.WebClient(), config.WebDefaultFile())
					dvid.Debugf("[%s] Serving default file from webclient directory: %s\n", r.Method, filename)
					http.ServeFile(w, r, filename)
				}
				return
			}
		}
		dvid.Debugf("[%s] Serving from webclient directory: %s\n", r.Method, filename)
		http.ServeFile(w, r, filename)
	}
}

func loadHandler(w http.ResponseWriter, r *http.Request) {
	m, err := json.Marshal(map[string]int{
		"file bytes read":      storage.FileBytesReadPerSec,
		"file bytes written":   storage.FileBytesWrittenPerSec,
		"key bytes read":       storage.StoreKeyBytesReadPerSec,
		"key bytes written":    storage.StoreKeyBytesWrittenPerSec,
		"value bytes read":     storage.StoreValueBytesReadPerSec,
		"value bytes written":  storage.StoreValueBytesWrittenPerSec,
		"GET requests":         storage.GetsPerSec,
		"PUT requests":         storage.PutsPerSec,
		"handlers active":      int(100 * ActiveHandlers / MaxChunkHandlers),
		"goroutines":           runtime.NumGoroutine(),
		"active CGo routines":  dvid.NumberActiveCGo(),
		"pending log messages": dvid.PendingLogMessages(),
	})
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(m))
}

func serverStorageHandler(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := datastore.GetStorageSummary()
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
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

func serverNoteHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, tc.Server.Note)
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

func serverGroupcacheHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := storage.GetGroupcacheStats()
	if err != nil {
		BadRequest(w, r, fmt.Sprintf("cannot get groupcache stats: %v", err))
		return
	}
	m, err := json.Marshal(stats)
	if err != nil {
		msg := fmt.Sprintf("Cannot marshal JSON groupcache stats info: %v (%v)\n", stats, err)
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
		setMaxThrottleOps(maxOps)
		fmt.Fprintf(w, "Maximum throttled ops set to %d from %d\n", maxOps, old)
	}
}

func serverReload(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) which already reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	datastore.MetadataUniversalUnlock()
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
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

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
	passcode, found, err := config.GetString("passcode")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint requires valid 'passcode': %v", err)
		return
	}
	if !found {
		passcode = ""
	}
	assignStr, found, err := config.GetString("root")
	if err != nil {
		BadRequest(w, r, "POST on repos endpoint with error on getting 'root': %v", err)
		return
	}
	var assign dvid.UUID
	var assignPtr *dvid.UUID
	if found {
		assign = dvid.UUID(assignStr)
		assignPtr = &assign
	}

	root, err := datastore.NewRepo(alias, description, assignPtr, passcode)
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

func repoNewDataHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

	uuid := c.Env["uuid"].(dvid.UUID)

	locked, err := datastore.LockedUUID(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	if !fullwrite && locked {
		BadRequest(w, r, "New data instance cannot be created on locked node %s", uuid)
		return
	}

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

	// send new instance op to kafka
	msginfo := map[string]interface{}{
		"Action":   "newinstance",
		"UUID":     string(uuid),
		"Typename": typename,
		"Dataname": dataname,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending new instance op to kafka: %v\n", err))
		return
	}
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

func getNodeNoteHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.Env["uuid"].(dvid.UUID)
	note, err := datastore.GetNodeNote(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsonStr, err := json.Marshal(struct {
		Note string `json:"note"`
	}{
		note,
	})
	if err != nil {
		BadRequest(w, r, err)
		return
	}
	fmt.Fprintf(w, string(jsonStr))
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

	msginfo := map[string]interface{}{
		"Action": "nodenote",
		"UUID":   string(uuid),
		"Note":   note,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending node note op to kafka: %v\n", err))
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

	msginfo := map[string]interface{}{
		"Action": "nodelog",
		"UUID":   string(uuid),
		"Log":    logdata,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending node log op to kafka: %v\n", err))
		return
	}
}

func repoCommitStateHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

	uuid := c.Env["uuid"].(dvid.UUID)

	locked, err := datastore.LockedUUID(uuid)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"Locked":%t}`, locked)
	}
}

func repoCommitHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

	uuid := c.Env["uuid"].(dvid.UUID)
	locked, err := datastore.LockedUUID(uuid)
	if err != nil {
		BadRequest(w, r, err)
		return
	} else if locked {
		BadRequest(w, r, "Could not post to locked node")
		return
	}

	jsonData := struct {
		Note string   `json:"note"`
		Log  []string `json:"log"`
	}{}
	if r.Body != nil {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			BadRequest(w, r, err)
			return
		}
		if err := json.Unmarshal(data, &jsonData); err != nil {
			BadRequest(w, r, fmt.Sprintf("Malformed JSON request in body: %v", err))
			return
		}
	}

	err = datastore.Commit(uuid, jsonData.Note, jsonData.Log)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "committed", uuid)
	}

	// send commit op to kafka
	msginfo := map[string]interface{}{
		"Action": "commit",
		"UUID":   string(uuid),
		"Note":   jsonData.Note,
		"Log":    jsonData.Log,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending commit op to kafka: %v\n", err))
		return
	}
}

// repoNewVersionHandler creates a new version node with the same branch as the parent
func repoNewVersionHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := struct {
		Note string `json:"note"`
		UUID string `json:"uuid"`
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

	var uuidPtr *dvid.UUID
	if len(jsonData.UUID) > 0 {
		uuid, err := dvid.StringToUUID(jsonData.UUID)
		if err != nil {
			BadRequest(w, r, fmt.Sprintf("Bad UUID provided: %v", err))
			return
		}
		uuidPtr = &uuid
	}

	// create new version with empty branch specification
	newuuid, err := datastore.NewVersion(uuid, jsonData.Note, "", uuidPtr)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "child", newuuid)
	}

	// send newversion op to kafka
	msginfo := map[string]interface{}{
		"Action": "newversion",
		"Parent": string(uuid),
		"Child":  newuuid,
		"Note":   jsonData.Note,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending newversion op to kafka: %v\n", err))
		return
	}
}

// TODO -- Might allow specification of UUID for child via HTTP, or only
// allow this potentially dangerous op via command line.
func repoBranchHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// Apply a global lock (if relevant) and reloads meta
	if err := datastore.MetadataUniversalLock(); err != nil {
		BadRequest(w, r, err)
		return
	}
	defer datastore.MetadataUniversalUnlock()

	uuid := c.Env["uuid"].(dvid.UUID)
	jsonData := struct {
		Branch string `json:"branch"`
		Note   string `json:"note"`
		UUID   string `json:"uuid"`
	}{}

	// load branch and note/uuid options
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

	var uuidPtr *dvid.UUID
	if len(jsonData.UUID) > 0 {
		uuid, err := dvid.StringToUUID(jsonData.UUID)
		if err != nil {
			BadRequest(w, r, fmt.Sprintf("Bad UUID provided: %v", err))
			return
		}
		uuidPtr = &uuid
	}

	// the default or master name should be not be specified
	if jsonData.Branch == "" || jsonData.Branch == "master" {
		BadRequest(w, r, fmt.Errorf("Cannot create a master branch"))
		return
	}

	// create new branch (will just version node if branch name is the same as the parent)
	newuuid, err := datastore.NewVersion(uuid, jsonData.Note, jsonData.Branch, uuidPtr)
	if err != nil {
		BadRequest(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "child", newuuid)
	}

	// send branch op to kafka
	msginfo := map[string]interface{}{
		"Action": "branch",
		"Parent": string(uuid),
		"Child":  newuuid,
		"Branch": jsonData.Branch,
		"Note":   jsonData.Note,
	}
	jsonmsg, _ := json.Marshal(msginfo)
	if err := datastore.LogRepoOpToKafka(uuid, jsonmsg); err != nil {
		BadRequest(w, r, fmt.Sprintf("Error on sending branch op to kafka: %v\n", err))
		return
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
		data, err := datastore.GetDataByUUIDName(uuid, name)
		if err != nil {
			BadRequest(w, r, err)
			return
		}

		if err := datastore.DeleteConflicts(uuid, data, oldParents, newParents); err != nil {
			BadRequest(w, r, fmt.Errorf("Conflict deletion error for data %q: %v", data.DataName(), err))
			return
		}
	}

	// If we have any new nodes with deletions, commit them.
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
