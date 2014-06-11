package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

const raml = `
#%RAML 0.8
title: "DVID Server"
version: v1
baseUri: /api/{version}
/help:
  get:
    description: returns help page
    responses:
      200:
        body:
          text/html:
/load:
  get:
    description: returns server load information
    responses:
      200:
        body:
          application/json:
            schema: |
              { "$schema": "http://json-schema.org/schema#",
                "description": "Server load data",
                "type": "object",
                "properties": {
                  "file bytes read": { "type": "number" },
                  "file bytes written": { "type": "number" },
                  "key bytes read": { "type": "number" },
                  "key bytes written": { "type": "number" },
                  "value bytes read": { "type": "number" },
                  "value bytes written": { "type": "number" },
                  "GET requests": { "type": "number" },
                  "PUT requests": { "type": "number" },
                  "handlers active": { "type": "number", "description": "number of goroutines currently active" },
                  "goroutines": { "type": "number", "description": "maximum number of goroutines allowed" }
                }
              }
/api/datasets/info:
  get:
    description: returns information on all held data sets
    responses:
      200:
        body:
          application/json:
`

// Handler for RAML interface.
func (s *Service) interfaceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/raml+yaml")
	w.WriteHeader(http.StatusOK)
	if r.Method != "HEAD" {
		io.Copy(w, strings.NewReader(raml))
	}
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "v1")
}
