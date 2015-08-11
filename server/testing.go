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
	"testing"

	"github.com/janelia-flyem/dvid/dvid"

	"encoding/json"
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
		t.Fatalf("Bad server response (%d) to %s on %q\n", resp.Code, method, urlStr)
	}
	return resp.Body.Bytes()
}

// TestBadHTTP expects a HTTP response with an error status code.
func TestBadHTTP(t *testing.T, method, urlStr string, payload io.Reader) {
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		t.Fatalf("Unsuccessful %s on %q: %v\n", method, urlStr, err)
	}
	w := httptest.NewRecorder()
	ServeSingleHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Fatalf("Expected bad server response to %s on %q, got %d instead.\n", method, urlStr, w.Code)
	}
}

// NewTestRepo returns a repo on a running server suitable for testing.
func NewTestRepo(t *testing.T) (uuid string) {
	metadata := `{"alias": "testRepo", "description": "A test repository"}`
	apiStr := WebAPIPath + "repos"
	response := TestHTTP(t, "POST", apiStr, bytes.NewBufferString(metadata))

	// Parse the returned root UUID
	parsedResponse := struct {
		Root string
	}{}
	if err := json.Unmarshal(response, &parsedResponse); err != nil {
		t.Fatalf("Couldn't decode JSON response to new repo request: %v\n", err)
	}
	return parsedResponse.Root
}

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
