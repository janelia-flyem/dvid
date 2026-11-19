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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

// TestHTTPResponse returns a response from a test run of the DVID server.  The
// request goes through OutermostHandler, as a production request does.
// Use TestHTTP if you just want the response body bytes.
func TestHTTPResponse(t *testing.T, method, urlStr string, payload io.Reader) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, urlStr, payload)
	if err != nil {
		t.Fatalf("Unsuccessful %s on %q: %v\n", method, urlStr, err)
	}
	resp := httptest.NewRecorder()
	OutermostHandler().ServeHTTP(resp, req)
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
	OutermostHandler().ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("Expected bad server response to %s on %q, got %d instead. [%s:%d]\n", method, urlStr, w.Code, fn, line)
	}
}

// SetTestDSGAuth configures DSG auth mode for tests with the given transport
// standing in for the DSG auth service, and returns a restore function.  It
// lets tests in external packages exercise DSG-admin-gated endpoints.
func SetTestDSGAuth(rt http.RoundTripper) (restore func()) {
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	origSeedToken, hadSeedToken := os.LookupEnv(DSGTokenVarName)
	tc.Auth.Enforce = "dsg"
	tc.Auth.DSGAddress = "https://dsg-auth.test"
	tc.Auth.DSGCacheTTL = 300
	tc.Auth.DSGService = "dvid"
	tc.Auth.DSGPublicRefresh = 0
	dsgHTTPClient = &http.Client{Transport: rt}
	_ = os.Setenv(DSGTokenVarName, "test-dvid-seed-token")
	clearDSGCaches()
	return func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		if hadSeedToken {
			_ = os.Setenv(DSGTokenVarName, origSeedToken)
		} else {
			_ = os.Unsetenv(DSGTokenVarName)
		}
		clearDSGCaches()
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

// ResetTestRoutes discards DVID's web routes, so the next request rebuilds them
// under the current configuration.  Routing decides once whether requests pass
// through the auth middleware, so a test that turns auth on or off resets the
// routes after doing so.  Do not call it while requests are in flight.
func ResetTestRoutes() {
	webMuxMu.Lock()
	webMux.Mux = web.New()
	webMux.Use(middleware.RequestID)
	webMux.routesSetup = false
	webMuxMu.Unlock()
}

// SetTestRWMode sets read-only and full-write modes for testing, and returns a
// function that restores the previous modes.
func SetTestRWMode(readOnly, fullWrite bool) (restore func()) {
	origReadOnly, origFullWrite := readonly, fullwrite
	readonly, fullwrite = readOnly, fullWrite
	return func() { readonly, fullwrite = origReadOnly, origFullWrite }
}

// SetTestMutationJSONStore points the JSON mutation logs at a directory for
// testing, and returns a function that restores the previous setting.
func SetTestMutationJSONStore(dir string) (restore func()) {
	orig := tc.Mutations.Jsonstore
	tc.Mutations.Jsonstore = dir
	return func() { tc.Mutations.Jsonstore = orig }
}

// TestStoredState is what a request could change: every stored key-value pair,
// the repo's mutation ID counter, and the files in the JSON mutation log
// directory with their sizes.
type TestStoredState struct {
	KV         map[string]string
	MutationID uint64
	LogFiles   map[string]int64
}

// CaptureTestState returns the stored state of the repo holding uuid.
func CaptureTestState(t *testing.T, uuid dvid.UUID) TestStoredState {
	t.Helper()
	var state TestStoredState
	var err error
	if state.KV, err = datastore.SnapshotTestStores(); err != nil {
		t.Fatalf("unable to snapshot stores: %v", err)
	}
	repoJSON, err := datastore.GetRepoJSON(uuid)
	if err != nil {
		t.Fatalf("unable to get repo JSON for %s: %v", uuid, err)
	}
	var repo struct{ MutationID uint64 }
	if err := json.Unmarshal([]byte(repoJSON), &repo); err != nil {
		t.Fatalf("unable to parse repo JSON for %s: %v", uuid, err)
	}
	state.MutationID = repo.MutationID
	state.LogFiles = make(map[string]int64)
	if dir := tc.Mutations.Jsonstore; dir != "" {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("unable to read mutation log directory %q: %v", dir, err)
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("unable to stat mutation log %q: %v", entry.Name(), err)
			}
			state.LogFiles[entry.Name()] = info.Size()
		}
	}
	return state
}

// Diff describes how other differs from s, or returns "" if they are equal.
func (s TestStoredState) Diff(other TestStoredState) string {
	var diffs []string
	if s.MutationID != other.MutationID {
		diffs = append(diffs, fmt.Sprintf("mutation ID %d -> %d", s.MutationID, other.MutationID))
	}
	var added, removed, changed int
	for k, v := range s.KV {
		v2, found := other.KV[k]
		if !found {
			removed++
		} else if v != v2 {
			changed++
		}
	}
	for k := range other.KV {
		if _, found := s.KV[k]; !found {
			added++
		}
	}
	if added+removed+changed != 0 {
		diffs = append(diffs, fmt.Sprintf("stored keys: %d added, %d removed, %d changed", added, removed, changed))
	}
	for name, size := range s.LogFiles {
		if size2, found := other.LogFiles[name]; !found {
			diffs = append(diffs, fmt.Sprintf("mutation log %s removed", name))
		} else if size != size2 {
			diffs = append(diffs, fmt.Sprintf("mutation log %s size %d -> %d", name, size, size2))
		}
	}
	for name := range other.LogFiles {
		if _, found := s.LogFiles[name]; !found {
			diffs = append(diffs, fmt.Sprintf("mutation log %s created", name))
		}
	}
	return strings.Join(diffs, "; ")
}
