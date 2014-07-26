// +build gcloud

/*
	This file sets up the web server using a Google AppEngine-friendly approach.
*/

package server

import (
	"net/http"

	"github.com/zenazn/goji"
)

func init() {
	http.Handle("/", goji.DefaultMux)
	initRoutes()
}
