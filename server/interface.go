package server

import (
	"fmt"
	"net/http"
)

// Handler for RAML interface.
func (s *Service) interfaceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/raml+yaml")
	s.sendContent("raml/dvid.raml", w, r)
}

func (s *Service) apiHelpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	s.sendContent("raml/index.html", w, r)
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "v1")
}
