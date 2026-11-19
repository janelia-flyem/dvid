package server

// Tests for dvid-0007: RequestGuard on every route, and mutation logging of
// mutation methods only.

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/zenazn/goji/web"
)

// defaultMuxHits counts requests that reach a test route on the standard
// default mux, outside webMux, where packages such as the profiler register.
var (
	defaultMuxHits    atomic.Int64
	defaultMuxRouteMu sync.Once
)

const defaultMuxTestPath = "/dvid-test-default-mux"

func registerDefaultMuxTestRoute() {
	defaultMuxRouteMu.Do(func() {
		http.HandleFunc(defaultMuxTestPath, func(w http.ResponseWriter, r *http.Request) {
			defaultMuxHits.Add(1)
			fmt.Fprintln(w, "default mux")
		})
	})
}

// guardRoutes returns one route behind each part of the outermost handler:
// mainMux, silentMux, and the default mux outside webMux.
func guardRoutes(uuid dvid.UUID) map[string]string {
	return map[string]string{
		"mainMux":     fmt.Sprintf("%snode/%s/note", WebAPIPath, uuid),
		"silentMux":   "/api/heartbeat",
		"default mux": defaultMuxTestPath,
	}
}

func serveOutermost(r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	OutermostHandler().ServeHTTP(w, r)
	return w
}

// serveUnguarded serves a request the way OutermostHandler does, but without
// RequestGuard.
func serveUnguarded(r *http.Request) *httptest.ResponseRecorder {
	OutermostHandler() // installs webMux on the default mux
	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)
	return w
}

func TestRequestGuardRefusesUnsupportedMethods(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer CloseTest()
	registerDefaultMuxTestRoute()
	uuid, _ := datastore.NewTestRepo()

	for name, url := range guardRoutes(uuid) {
		for _, method := range []string{"PATCH", "FOO", "PROPFIND", "Head", "get", "Post", "options"} {
			hits := defaultMuxHits.Load()
			w := serveOutermost(httptest.NewRequest(method, url, nil))
			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: expected 405, got %d: %s", method, name, w.Code, w.Body.String())
			}
			if allow := w.Header().Get("Allow"); allow != supportedMethods {
				t.Errorf("%s %s: expected Allow %q, got %q", method, name, supportedMethods, allow)
			}
			if defaultMuxHits.Load() != hits {
				t.Errorf("%s %s reached the default mux handler", method, name)
			}
		}
	}
}

func TestRequestGuardRefusesBodiesOnHeadAndOptions(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer CloseTest()
	registerDefaultMuxTestRoute()
	uuid, _ := datastore.NewTestRepo()

	bodies := map[string]func(r *http.Request){
		"Content-Length body": func(r *http.Request) {},
		"chunked body": func(r *http.Request) {
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
		},
		"empty chunked body": func(r *http.Request) {
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
			r.Body = http.NoBody
		},
		"unknown length": func(r *http.Request) {
			r.ContentLength = -1
		},
	}
	for name, url := range guardRoutes(uuid) {
		for _, method := range []string{http.MethodHead, http.MethodOptions} {
			for bodyName, shape := range bodies {
				r := httptest.NewRequest(method, url, strings.NewReader("[9, 9, 9]"))
				shape(r)
				hits := defaultMuxHits.Load()
				w := serveOutermost(r)
				if w.Code != http.StatusBadRequest {
					t.Errorf("%s %s with %s: expected 400, got %d: %s", method, name, bodyName, w.Code, w.Body.String())
				}
				if defaultMuxHits.Load() != hits {
					t.Errorf("%s %s with %s reached the default mux handler", method, name, bodyName)
				}
			}
		}
	}
}

// Requests the guard allows get the answer they would get without it.
func TestRequestGuardPassesSupportedRequests(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer CloseTest()
	registerDefaultMuxTestRoute()
	uuid, _ := datastore.NewTestRepo()

	type request struct {
		method string
		header map[string]string
	}
	preflight := request{http.MethodOptions, map[string]string{
		"Origin":                         "https://viewer.example.org",
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "authorization",
	}}
	for name, url := range guardRoutes(uuid) {
		for _, req := range []request{{http.MethodGet, nil}, {http.MethodHead, nil}, {http.MethodOptions, nil}, preflight} {
			newRequest := func() *http.Request {
				r := httptest.NewRequest(req.method, url, nil)
				for k, v := range req.header {
					r.Header.Set(k, v)
				}
				return r
			}
			hits := defaultMuxHits.Load()
			want := serveUnguarded(newRequest())
			wantHits := defaultMuxHits.Load() - hits
			got := serveOutermost(newRequest())
			if got.Code != want.Code || got.Body.String() != want.Body.String() {
				t.Errorf("%s %s (headers %v): guarded answer %d %q differs from unguarded %d %q",
					req.method, name, req.header, got.Code, got.Body.String(), want.Code, want.Body.String())
			}
			if gotHits := defaultMuxHits.Load() - hits - wantHits; gotHits != wantHits {
				t.Errorf("%s %s: default mux reached %d times guarded, %d unguarded", req.method, name, gotHits, wantHits)
			}
		}
	}
	// A write method passes too.
	note := fmt.Sprintf("%snode/%s/note", WebAPIPath, uuid)
	if w := serveOutermost(httptest.NewRequest(http.MethodPost, note, strings.NewReader(`{"note": "guarded"}`))); w.Code != http.StatusOK {
		t.Errorf("POST note: expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

// Through a real listener, net/http marks a chunked HEAD body the way the guard
// expects, so the guard refuses it.
func TestRequestGuardOnRealListener(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer CloseTest()
	registerDefaultMuxTestRoute()
	srv := httptest.NewServer(OutermostHandler())
	defer srv.Close()

	send := func(raw string) int {
		conn, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatalf("can't dial test server: %v", err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte(raw)); err != nil {
			t.Fatalf("can't write request: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("can't read response: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	host := srv.Listener.Addr().String()
	hits := defaultMuxHits.Load()
	for _, method := range []string{"HEAD", "OPTIONS"} {
		chunked := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: %s\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\n[9, 9, 9]\r\n0\r\n\r\n",
			method, defaultMuxTestPath, host)
		if code := send(chunked); code != http.StatusBadRequest {
			t.Errorf("%s with a chunked body: expected 400, got %d", method, code)
		}
		withLength := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: %s\r\nContent-Length: 9\r\nConnection: close\r\n\r\n[9, 9, 9]",
			method, defaultMuxTestPath, host)
		if code := send(withLength); code != http.StatusBadRequest {
			t.Errorf("%s with a Content-Length body: expected 400, got %d", method, code)
		}
	}
	if defaultMuxHits.Load() != hits {
		t.Errorf("a refused request reached the default mux handler")
	}
	bodiless := fmt.Sprintf("HEAD %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", defaultMuxTestPath, host)
	if code := send(bodiless); code != http.StatusOK {
		t.Errorf("bodiless HEAD: expected 200, got %d", code)
	}
}

// With httpstore and blobstore configured, mutationsHandler logs a POST and
// its payload, and does not log GET, HEAD, or OPTIONS, even with a body.
func TestMutationsHandlerLogsOnlyMutationMethods(t *testing.T) {
	if err := OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer CloseTest()
	uuid, _ := datastore.NewTestRepo()

	stores, err := storage.AllStores()
	if err != nil {
		t.Fatalf("can't get stores: %v", err)
	}
	var blobAlias storage.Alias
	for alias, store := range stores {
		if _, ok := store.(storage.BlobStore); ok {
			blobAlias = alias
			break
		}
	}
	if blobAlias == "" {
		t.Fatalf("test backend lacks a blob store")
	}
	// With no Kafka producer configured, a kafka httpstore accepts each record
	// and drops it, so the rest of the logging path runs.
	origMutations := tc.Mutations
	defer func() { tc.Mutations = origMutations }()
	tc.Mutations.Httpstore = "kafka:test-mutations"
	tc.Mutations.Blobstore = blobAlias

	var reached int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached++ })
	c := web.C{Env: map[interface{}]interface{}{"uuid": uuid}}
	handler := mutationsHandler(&c, next)

	mutationOrder := func() uint64 {
		mutOrderMux.RLock()
		defer mutOrderMux.RUnlock()
		return mutOrderID
	}
	url := fmt.Sprintf("%snode/%s/note", WebAPIPath, uuid)
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		before, order := CaptureTestState(t, uuid), mutationOrder()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, url, strings.NewReader("read with a body")))
		if w.Code != http.StatusOK {
			t.Errorf("%s: expected 200, got %d: %s", method, w.Code, w.Body.String())
		}
		if mutationOrder() != order {
			t.Errorf("%s was logged as a mutation", method)
		}
		if diff := before.Diff(CaptureTestState(t, uuid)); diff != "" {
			t.Errorf("%s stored its body: %s", method, diff)
		}
	}
	before, order := CaptureTestState(t, uuid), mutationOrder()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, url, strings.NewReader("a mutation payload")))
	if w.Code != http.StatusOK {
		t.Fatalf("POST: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if mutationOrder() != order+1 {
		t.Errorf("POST was not logged as a mutation")
	}
	if diff := before.Diff(CaptureTestState(t, uuid)); diff == "" {
		t.Errorf("POST payload was not stored in the blob store")
	}
	if reached != 4 {
		t.Errorf("expected all 4 requests to reach the next handler, got %d", reached)
	}
}
