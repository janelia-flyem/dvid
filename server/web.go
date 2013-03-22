/*
	This file supports the DVID REST API, breaking down URLs into
	commands and massaging attached data into appropriate data types. 
*/

package server

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"

	"code.google.com/p/go.net/websocket"
)

func socketHandler(c *websocket.Conn) {
	var s string
	fmt.Fscan(c, &s)
	dvid.Log(dvid.Debug, "socket received: %s\n", s)
	fmt.Fprint(c, "DVID received socket communication:", s, "\n")
}

func badRequest(w http.ResponseWriter, r *http.Request, err string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", err, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// Handler for presentation files
func mainHandler(w http.ResponseWriter, r *http.Request) {
	filename := filepath.Join(runningService.WebClientPath, r.URL.Path)
	dvid.Log(dvid.Debug, "http request: %s -> %s\n", r.URL.Path, filename)
	http.ServeFile(w, r, filename)
}

// Handler for API commands.
// We assume all DVID API commands target the URLs /api/<command or data set name>/... 
// Built-in commands are:
//    data  -- POST will add a named data set for a given data type
//       POST /api/data/<data type>/<data set name>
//    cache -- returns LRU cache status
//    
func apiHandler(w http.ResponseWriter, r *http.Request) {

	// Break URL request into arguments
	const lenPath = len(RestApiPath)
	url := r.URL.Path[lenPath:]
	parts := strings.Split(url, "/")
	if len(parts) == 0 {
		badRequest(w, r, "Poorly formed request")
		return
	}

	// Handle the requests
	switch parts[0] {
	case "cache":
		fmt.Fprintf(w, "<p>TODO -- return LRU Cache statistics</p>\n")
	case "data":
		if len(parts) != 3 {
			msg := fmt.Sprintf("Bad data set creation format (%s).  Try something like '%s' instead.",
				url, "POST /api/data/grayscale8/grayscale")
			badRequest(w, r, msg)
			return
		}
		dataType := parts[1]
		dataSetName := parts[2]
		err := runningService.NewDataSet(dataSetName, dataType)
		if err != nil {
			msg := fmt.Sprintf("Could not add data set '%s' of type '%s': %s",
				dataSetName, dataType, err.Error())
			badRequest(w, r, msg)
			return
		}
	default:
		// Pass type-specific requests to the type service
		dataSetName := parts[0]
		typeService, err := runningService.DataSetService(dataSetName)
		if err != nil {
			badRequest(w, r, fmt.Sprintf("Could not find data set '%s' in datastore [%s]",
				dataSetName, err.Error()))
			return
		}
		typeService.DoHTTP(w, r, runningService.Service, RestApiPath)
	}
}
