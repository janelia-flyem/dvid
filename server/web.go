package server

import (
	"fmt"
	_ "log"
	"net/http"
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
