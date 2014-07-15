// +build !clustered,!gcloud

/*
	This file sets up a local web server to be called within the 'serve' command
	via the dvid executable.
*/

package server

import (
	"fmt"
	"net/http"
	"time"

	"bitbucket.org/tebeka/nrsc"
	"github.com/janelia-flyem/dvid/dvid"
)

const (
	// The default URL of the DVID web server
	DefaultWebAddress = "localhost:8000"
)

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion:
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func (service *Service) ServeHttp(address, clientDir string) {
	if address == "" {
		address = DefaultWebAddress
	}
	fmt.Printf("Web server listening at %s ...\n", address)

	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}

	// Handle static files through serving embedded files
	// via nrsc or loading files from a specified web client directory.
	if clientDir == "" {
		dvid.Log(dvid.Normal, "Serving web client from embedded files...")
		if err := nrsc.Initialize(); err != nil {
			dvid.Log(dvid.Normal, "Error initializing embedded data access: %s\n", err.Error())
		}
	} else {
		dvid.Log(dvid.Normal, "Serving web pages from %s\n", clientDir)
	}
	http.HandleFunc("/", logHttpPanics(service.mainHandler))

	// Serve it up!
	src.ListenAndServe()
}
