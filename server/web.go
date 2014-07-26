/*
	This file supports the DVID REST API, breaking down URLs into
	commands and massaging attached data into appropriate data types.
*/

package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/janelia-flyem/go/goji"
	"github.com/janelia-flyem/go/goji/web"

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

type Context struct {
	log         dvid.Logger
	data        []*datastore.DataContext
	accessToken string
}

func initRoutes(webClientDir string) {
	// Handle RAML interface
	goji.Get("/interface/", logHttpPanics(interfaceHandler))
	goji.Get("/interface/version", logHttpPanics(versionHandler))

	goji.Get("/help", helpHandler)
	goji.Get("/load", loadHandler)

	goji.Get("/api/server/info", serverInfoHandler)
	goji.Get("/api/server/types", serverTypesHandler)

	goji.Post("/api/repos", reposPostHandler)

	goji.Get("/api/repos/info", reposInfoHandler)
	goji.Get("/api/repos/list", reposListHandler)

	repoMux := web.New()
	goji.Handle("/api/repo/:uuid/*", repoMux)
	repoMux.Use(repoSelector)
	repoMux.Post("/api/repo/:uuid/instance", repoPostHandler)
	repoMux.Get("/api/repo/:uuid/info", repoInfoHandler)
	repoMux.Get("/api/repo/:uuid/lock", repoLockHandler)
	repoMux.Get("/api/repo/:uuid/branch", repoBranchHandler)

	instanceMux := web.New()
	goji.Handle("/api/repo/:uuid/:dataname/*", instanceMux)
	instanceMux.Use(repoSelector)
	instanceMux.Use(instanceSelector)
	instanceMux.NotFound(NotFound)

	goji.Get("/", mainHandler)
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
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", message, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Infof(errorMsg)
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

// AboutJSON returns a JSON string describing the properties of this server.
func AboutJSON() (jsonStr string, err error) {
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

// ---- Middleware -------------

// repoSelector retrieves the particular repo from a potentially partial string that uniquely
// identifies the repo.
func repoSelector(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var err error
		if c.Env["uuid"], err = MatchingUUID(c.URLParams["uuid"]); err != nil {
			BadRequest(w, r, err.Error())
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
		dataname := dvid.DataString(c.URLParams["dataname"])
		uuid := c.Env["uuid"].(dvid.UUID)
		dataservice, err := runningService.DataServiceByUUID(uuid, dataname)
		if err != nil {
			BadRequest(w, r, err.Error())
			return
		}
		err = dataservice.DoHTTP(uuid, w, r)
		if err != nil {
			BadRequest(w, r, err.Error())
		}
	}
	return http.HandlerFunc(fn)
}

// ---- Function types that fulfill http.Handler.  How can a bare function satisfy an interface?
//      See http://www.onebigfluke.com/2014/04/gos-power-is-in-emergent-behavior.html

func helpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, WebHelp)
}

// Handler for web client and other static content
func mainHandler(w http.ResponseWriter, r *http.Request) {
	service.sendContent(r.URL.Path, w, r)
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
	jsonStr, err := runningService.TypesJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func reposInfoHandler(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := runningService.ReposAllJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func reposListHandler(w http.ResponseWriter, r *http.Request) {
	jsonStr, err := runningService.ReposListJSON()
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func reposPostHandler(w http.ResponseWriter, r *http.Request) {
	root, _, err := runningService.NewRepo()
	if err != nil {
		BadRequest(w, r, err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: %q}", "Root", root)
}

func repoInfoHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid, err := MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	jsonStr, err := runningService.RepoJSON(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, jsonStr)
}

func repoPostHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid, err := MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	decoder := json.NewDecoder(r.Body)
	config := dvid.NewConfig()
	err = decoder.Decode(&config)
	if err != nil {
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

	err = runningService.NewData(uuid, typename, dataname, config)
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{%q: 'Added %s [%s] to node %s'}", "result", dataname, typename, uuid)
}

func repoLockHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid, err := MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	err := runningService.Lock(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "Lock on node %s successful.", uuid)
	}
}

func repoBranchHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid, err := MatchingUUID(c.URLParams["uuid"])
	if err != nil {
		BadRequest(w, r, err.Error())
		return
	}

	newuuid, err := runningService.NewVersion(uuid)
	if err != nil {
		BadRequest(w, r, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{%q: %q}", "Branch", newuuid)
	}
}
