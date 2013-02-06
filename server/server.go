package server

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

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

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion: 
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func ServeHttp(addr string) {
	log.Printf("Starting http server for DVID at %s...\n", addr)

	src := &http.Server{
		Addr:        addr,
		ReadTimeout: 1 * time.Hour,
	}
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/api/", apiHandler)
	src.ListenAndServe()
}
