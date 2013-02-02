package server

import (
	"fmt"
	"log"
	"net/http"
)

// Options that could accompany commands from command line interface or web server
type Options struct {
	// Directory is the path to a DVID datastore directory
	Directory string

	// Origin (top, left corner) of a subvolume within a datastore volume space
	OriginX int
	OriginY int
	OriginZ int
}

// Handler for main page
func mainHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "<h1>DVID Server Main Page</h1>")
}

// Handler for API commands through HTTP
func apiHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "<h1>DVID Server API Handler ...</h1>")

	const lenPath = len("/api/")
	url := r.URL.Path[lenPath:]

	fmt.Fprintln(w, "<p>Processing", url, "</p>")
}

// Listen and serve HTTP requests using address.
func ServeHttp(addr string) {
	log.Printf("Starting http server for DVID at %s...", addr)
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/api/", apiHandler)

	http.ListenAndServe(addr, nil)
}
