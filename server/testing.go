/*
	This file contains functions useful for testing DVID in other packages.
	Unfortunately, due to the way Go handles compilation of *_test.go files,
	these functions cannot be in server_test.go since they will be unavailable
	to test files in external packages.  So these functions are exported and
	contain the "Test" keyword.
*/

package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

// TestHTTPResponse returns a response from a test run of the DVID server.
// Use TestHTTP if you just want the response body bytes.
func TestHTTPResponse(t *testing.T, method, urlStr string, payload io.Reader) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		t.Fatalf("Unsuccessful %s on %q: %v\n", method, urlStr, err)
	}
	resp := httptest.NewRecorder()
	ServeSingleHTTP(resp, req)
	return resp
}

// TestHTTP returns the response body bytes for a test request, making sure any response has
// status OK.
func TestHTTP(t *testing.T, method, urlStr string, payload io.Reader) []byte {
	resp := TestHTTPResponse(t, method, urlStr, payload)
	if resp.Code != http.StatusOK {
		_, fn, line, _ := runtime.Caller(1)
		var retstr string
		if resp.Body != nil {
			retbytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Error trying to read response body from request %q: %v [%s:%d]\n", urlStr, err, fn, line)
			} else {
				retstr = string(retbytes)
			}
		}
		t.Fatalf("Bad server response (%d) to %s %q: %s [%s:%d]\n", resp.Code, method, urlStr, retstr, fn, line)
	}
	return resp.Body.Bytes()
}

// TestHTTPError returns the response body bytes for a test request, making sure any response has
// status OK.
func TestHTTPError(t *testing.T, method, urlStr string, payload io.Reader) (retbytes []byte, err error) {
	resp := TestHTTPResponse(t, method, urlStr, payload)
	if resp.Code != http.StatusOK {
		_, fn, line, _ := runtime.Caller(1)
		if resp.Body != nil {
			retbytes, err = io.ReadAll(resp.Body)
			if err != nil {
				err = fmt.Errorf("error trying to read response body from request %q: %v [%s:%d]", urlStr, err, fn, line)
				return
			}
		}
		return retbytes, fmt.Errorf("bad server response (%d) to %s %q: %s [%s:%d]", resp.Code, method, urlStr, string(retbytes), fn, line)
	}
	return resp.Body.Bytes(), nil
}

// TestBadHTTP expects a HTTP response with an error status code.
func TestBadHTTP(t *testing.T, method, urlStr string, payload io.Reader) {
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("Unsuccessful %s on %q: %v [%s:%d]\n", method, urlStr, err, fn, line)
	}
	w := httptest.NewRecorder()
	ServeSingleHTTP(w, req)
	if w.Code == http.StatusOK {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("Expected bad server response to %s on %q, got %d instead. [%s:%d]\n", method, urlStr, w.Code, fn, line)
	}
}

// CreateTestInstance posts a new data instance for testing.
func CreateTestInstance(t *testing.T, uuid dvid.UUID, typename, name string, config dvid.Config) {
	config.Set("typename", typename)
	config.Set("dataname", name)
	jsonData, err := config.MarshalJSON()
	if err != nil {
		t.Fatalf("Unable to make JSON for instance creation: %v\n", config)
	}
	apiStr := fmt.Sprintf("%srepo/%s/instance", WebAPIPath, uuid)
	TestHTTP(t, "POST", apiStr, bytes.NewBuffer(jsonData))
}

// CreateTestSync posts new syncs for a given data instance / uuid.
func CreateTestSync(t *testing.T, uuid dvid.UUID, name string, syncs ...string) {
	url := fmt.Sprintf("%snode/%s/%s/sync", WebAPIPath, uuid, name)
	msg := fmt.Sprintf(`{"sync": "%s"}`, strings.Join(syncs, ","))
	TestHTTP(t, "POST", url, strings.NewReader(msg))
}

// CreateTestReplaceSync replaces syncs for a given data instance / uuid.
func CreateTestReplaceSync(t *testing.T, uuid dvid.UUID, name string, syncs ...string) {
	url := fmt.Sprintf("%snode/%s/%s/sync?replace=true", WebAPIPath, uuid, name)
	msg := fmt.Sprintf(`{"sync": "%s"}`, strings.Join(syncs, ","))
	TestHTTP(t, "POST", url, strings.NewReader(msg))
}
