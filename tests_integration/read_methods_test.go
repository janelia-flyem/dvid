package tests_integration

// Tests that GET, HEAD, and OPTIONS never write (dvid-0007).  The requests to
// the write branches go straight to DVID's web routing, without
// server.RequestGuard in front, so each datatype handler's own method check is
// tested by itself.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"

	_ "github.com/janelia-flyem/dvid/datatype/labelmap"
	_ "github.com/janelia-flyem/dvid/datatype/neuronjson"
)

const readMethodsVolSize = 64

// uniformLabels returns a readMethodsVolSize^3 volume of one label, as the raw
// uint64 voxels that POST /raw takes.
func uniformLabels(label uint64) []byte {
	n := readMethodsVolSize * readMethodsVolSize * readMethodsVolSize
	buf := make([]byte, n*8)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(buf[i*8:i*8+8], label)
	}
	return buf
}

func rawURL(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/0_0_0", server.WebAPIPath, uuid, name,
		readMethodsVolSize, readMethodsVolSize, readMethodsVolSize)
}

// blocksURL reads blocks as gzipped DVID label blocks, the format POST /blocks and
// /ingest-supervoxels take.
func blocksURL(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/blocks/%d_%d_%d/0_0_0?compression=blocks", server.WebAPIPath, uuid, name,
		readMethodsVolSize, readMethodsVolSize, readMethodsVolSize)
}

// writeBranch is one of the handler branches that wrote on GET, HEAD, or
// OPTIONS before dvid-0007.
type writeBranch struct {
	name     string
	endpoint string // path after the instance name, as a POST would use it
	instance string
	payload  []byte
	getReads bool // GET is a read here; otherwise only POST is allowed
}

func (b writeBranch) url(uuid dvid.UUID) string {
	return fmt.Sprintf("%snode/%s/%s/%s", server.WebAPIPath, uuid, b.instance, b.endpoint)
}

type readMethodsFixture struct {
	uuid      dvid.UUID
	instances []dvid.InstanceName
	branches  []writeBranch
}

// waitForUpdates waits until no instance, including synced subscribers, has
// asynchronous work in progress.  Like datastore.BlockOnUpdating, it first gives
// asynchronous work time to start, but it sleeps once for all the instances.
func (f *readMethodsFixture) waitForUpdates(t *testing.T, uuid dvid.UUID) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	for _, name := range f.instances {
		d, err := datastore.GetDataByUUIDName(uuid, name)
		if err != nil {
			t.Fatalf("can't get instance %q: %v", name, err)
		}
		syncer, isSyncer := d.(datastore.Syncer)
		updater, isUpdater := d.(interface{ Updating() bool })
		for (isSyncer && syncer.SyncPending()) || (isUpdater && updater.Updating()) {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func newReadMethodsFixture(t *testing.T) *readMethodsFixture {
	uuid, _ := datastore.NewTestRepo()
	f := &readMethodsFixture{uuid: uuid}

	// Instances whose write branches are tested, each holding label 1, and
	// source instances holding label 2 whose blocks become write payloads.
	for _, inst := range []struct{ typename, name string }{
		{"labelmap", "lmap"}, {"labelmap", "lmapsrc"},
		{"labelarray", "larr"}, {"labelarray", "larrsrc"},
		{"labelblk", "lblk"},
	} {
		server.CreateTestInstance(t, uuid, inst.typename, inst.name, dvid.Config{})
		f.instances = append(f.instances, dvid.InstanceName(inst.name))
	}
	// A synced subscriber: an annotation on the labelmap, with a synapse inside
	// the volume, so a label write would relabel it.
	server.CreateTestInstance(t, uuid, "annotation", "synapses", dvid.Config{})
	server.CreateTestSync(t, uuid, "synapses", "lmap")
	f.instances = append(f.instances, "synapses")

	for _, name := range []string{"lmap", "larr", "lblk"} {
		server.TestHTTP(t, "POST", rawURL(uuid, name), bytes.NewReader(uniformLabels(1)))
	}
	for _, name := range []string{"lmapsrc", "larrsrc"} {
		server.TestHTTP(t, "POST", rawURL(uuid, name), bytes.NewReader(uniformLabels(2)))
	}
	f.waitForUpdates(t, uuid)
	synapse := `[{"Pos":[10,10,10],"Kind":"PostSyn","Tags":[],"Prop":{}}]`
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/synapses/elements", server.WebAPIPath, uuid),
		strings.NewReader(synapse))
	f.waitForUpdates(t, uuid)

	lmapBlocks := server.TestHTTP(t, "GET", blocksURL(uuid, "lmapsrc"), nil)
	larrBlocks := server.TestHTTP(t, "GET", blocksURL(uuid, "larrsrc"), nil)
	rawPayload := uniformLabels(3)
	resolution := []byte(`[9, 9, 9]`)
	extents := []byte(`{"MinPoint": [1, 2, 3], "MaxPoint": [40, 50, 60]}`)
	rawEndpoint := fmt.Sprintf("raw/0_1_2/%d_%d_%d/0_0_0", readMethodsVolSize, readMethodsVolSize, readMethodsVolSize)

	// The nine branches in dvid-0007's table.
	f.branches = []writeBranch{
		{"labelmap extents", "extents", "lmap", extents, false},
		{"labelmap resolution", "resolution", "lmap", resolution, false},
		{"labelmap ingest-supervoxels", "ingest-supervoxels", "lmap", lmapBlocks, false},
		{"labelmap blocks", "blocks", "lmap", lmapBlocks, true},
		{"labelmap raw", rawEndpoint, "lmap", rawPayload, true},
		{"labelarray resolution", "resolution", "larr", resolution, false},
		{"labelarray blocks", "blocks", "larr", larrBlocks, true},
		{"labelarray raw", rawEndpoint, "larr", rawPayload, true},
		{"labelblk resolution", "resolution", "lblk", resolution, false},
	}
	return f
}

// caller is who sends a request: the Authorization header and query string it
// adds, if any.
type caller struct {
	authorization string
	query         string
}

var anonymous caller

// send sends a request without the request guard, waits for asynchronous work
// to settle, and returns the response and how the stored state changed.
func (f *readMethodsFixture) send(t *testing.T, uuid dvid.UUID, who caller, method, url string, payload []byte) (int, string, string) {
	t.Helper()
	before := server.CaptureTestState(t, f.uuid)
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	if who.query != "" {
		url += "?" + who.query
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("can't create %s request for %q: %v", method, url, err)
	}
	if who.authorization != "" {
		req.Header.Set("Authorization", who.authorization)
	}
	resp := httptest.NewRecorder()
	server.ServeSingleHTTP(resp, req)
	f.waitForUpdates(t, uuid)
	after := server.CaptureTestState(t, f.uuid)
	return resp.Code, resp.Body.String(), before.Diff(after)
}

// checkReadMethods sends GET, HEAD, and OPTIONS, with a valid write payload and
// with no body, to every branch on the version, and checks that nothing changed.
func (f *readMethodsFixture) checkReadMethods(t *testing.T, uuid dvid.UUID, who caller, state string) {
	for _, b := range f.branches {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
			for _, payload := range [][]byte{b.payload, nil} {
				desc := fmt.Sprintf("%s %s on %s node, body %t", method, b.name, state, payload != nil)
				code, body, diff := f.send(t, uuid, who, method, b.url(uuid), payload)
				if diff != "" {
					t.Errorf("%s changed stored state: %s", desc, diff)
				}
				// GET on a read branch reads (and the POST URL of blocks has no
				// subvolume, so it is a bad read); every other case must be
				// refused by the method check.
				if method == http.MethodGet && b.getReads {
					continue
				}
				if code != http.StatusBadRequest {
					t.Errorf("%s: expected status 400, got %d: %s", desc, code, body)
				}
				if !strings.Contains(body, "only") {
					t.Errorf("%s: expected the method check's refusal, got: %s", desc, body)
				}
			}
		}
	}
}

func TestReadMethodsNeverWrite(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer server.CloseTest()
	defer server.SetTestMutationJSONStore(t.TempDir())()

	f := newReadMethodsFixture(t)
	root := f.uuid

	// GET on the read branches still reads the stored labels.
	for _, name := range []string{"lmap", "larr", "lblk"} {
		got := server.TestHTTP(t, "GET", rawURL(root, name), nil)
		if !bytes.Equal(got, uniformLabels(1)) {
			t.Errorf("GET raw of %q did not return its stored labels", name)
		}
	}

	// An unlocked node: read methods change nothing, and PUT and DELETE are
	// refused by the handlers.
	f.checkReadMethods(t, root, anonymous, "unlocked")
	for _, b := range f.branches {
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			code, body, diff := f.send(t, root, anonymous, method, b.url(root), b.payload)
			if diff != "" {
				t.Errorf("%s %s changed stored state: %s", method, b.name, diff)
			}
			if code != http.StatusBadRequest {
				t.Errorf("%s %s: expected status 400, got %d: %s", method, b.name, code, body)
			}
		}
	}

	// A locked node: read methods still change nothing, and the locked-node
	// guard refuses every mutation method, including one it does not know.
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, root),
		strings.NewReader(`{"note": "lock for read-method tests"}`))
	f.checkReadMethods(t, root, anonymous, "locked")
	for _, b := range f.branches {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, "FOO"} {
			code, body, diff := f.send(t, root, anonymous, method, b.url(root), b.payload)
			if diff != "" {
				t.Errorf("%s %s on locked node changed stored state: %s", method, b.name, diff)
			}
			if code != http.StatusBadRequest || !strings.Contains(body, "locked node") {
				t.Errorf("%s %s on locked node: expected the locked-node refusal, got %d: %s", method, b.name, code, body)
			}
		}
	}

	// POST still writes on every branch, on an unlocked child.
	respData := server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, root), nil)
	var child struct {
		Child dvid.UUID `json:"child"`
	}
	if err := json.Unmarshal(respData, &child); err != nil {
		t.Fatalf("bad newversion response %q: %v", string(respData), err)
	}
	for _, b := range f.branches {
		code, body, diff := f.send(t, child.Child, anonymous, http.MethodPost, b.url(child.Child), b.payload)
		if code != http.StatusOK {
			t.Errorf("POST %s: expected status 200, got %d: %s", b.name, code, body)
		}
		if diff == "" {
			t.Errorf("POST %s changed no stored state", b.name)
		}
	}
	got := server.TestHTTP(t, "GET", rawURL(child.Child, "lmap"), nil)
	if !bytes.Equal(got, uniformLabels(3)) {
		t.Errorf("labelmap POST raw on the child did not store its labels")
	}
}

// A GET of mutations or mutations-range on an instance with no mutation log
// returns an empty history and creates no log file.
func TestReadMutationsCreatesNoLog(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer server.CloseTest()
	defer server.SetTestMutationJSONStore(t.TempDir())()

	uuid, _ := datastore.NewTestRepo()
	server.CreateTestInstance(t, uuid, "labelmap", "lmap", dvid.Config{})
	server.CreateTestInstance(t, uuid, "keyvalue", "kv", dvid.Config{})
	for _, url := range []string{
		fmt.Sprintf("%snode/%s/lmap/mutations", server.WebAPIPath, uuid),
		fmt.Sprintf("%snode/%s/lmap/mutations-range/%s/%s", server.WebAPIPath, uuid, uuid, uuid),
		fmt.Sprintf("%snode/%s/kv/mutations", server.WebAPIPath, uuid),
	} {
		before := server.CaptureTestState(t, uuid)
		got := server.TestHTTP(t, "GET", url, nil)
		after := server.CaptureTestState(t, uuid)
		if diff := before.Diff(after); diff != "" {
			t.Errorf("GET %s changed stored state: %s", url, diff)
		}
		if strings.TrimSpace(string(got)) != "[]" {
			t.Errorf("GET %s: expected an empty history, got %q", url, string(got))
		}
	}
}

// viewOnlyDSG stands in for DSG.  The token "viewer" belongs to a user who is
// not an admin and may view every version but edit none.
type viewOnlyDSG struct{}

func (viewOnlyDSG) RoundTrip(r *http.Request) (*http.Response, error) {
	respond := func(status int, value interface{}) (*http.Response, error) {
		body, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	}
	switch r.URL.Path {
	case "/api/dsg/v1/user":
		if r.Header.Get("Authorization") != "Bearer viewer" {
			return respond(http.StatusUnauthorized, map[string]string{"error": "invalid token"})
		}
		return respond(http.StatusOK, map[string]interface{}{"email": "viewer@example.org", "name": "viewer", "admin": false})
	case "/api/dsg/v1/authorize":
		var request struct {
			Entries []map[string]string `json:"entries"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			return respond(http.StatusBadRequest, map[string]string{"error": "bad request"})
		}
		decisions := []map[string]interface{}{}
		for _, entry := range request.Entries {
			decision, roles := "deny", []string{}
			if entry["permission"] == "view" {
				decision, roles = "allow", []string{"view"}
			}
			decisions = append(decisions, map[string]interface{}{
				"name": entry["name"], "branch": entry["branch"], "version": entry["version"],
				"decision": decision, "roles": roles,
			})
		}
		return respond(http.StatusOK, map[string]interface{}{"entries": decisions})
	default:
		return respond(http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

// Every way a request can be admitted still cannot write with GET, HEAD, or
// OPTIONS: read-only mode, full-write and admin on a locked node, an anonymous
// read of a public version, and a view-only user on a private one.
func TestReadMethodsNeverWriteOnAccessPaths(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer server.CloseTest()
	defer server.SetTestMutationJSONStore(t.TempDir())()

	f := newReadMethodsFixture(t)
	root := f.uuid
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, root),
		strings.NewReader(`{"note": "public release"}`))
	respData := server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, root), nil)
	var resp struct {
		Child dvid.UUID `json:"child"`
	}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Fatalf("bad newversion response %q: %v", string(respData), err)
	}
	child := resp.Child

	restoreMode := server.SetTestRWMode(true, false)
	f.checkReadMethods(t, child, anonymous, "read-only")
	restoreMode()

	restoreMode = server.SetTestRWMode(false, true)
	f.checkReadMethods(t, root, anonymous, "full-write, locked")
	restoreMode()

	server.SetAdminToken("read-methods-admin")
	defer server.SetAdminToken("")
	f.checkReadMethods(t, root, caller{query: "admintoken=read-methods-admin"}, "admin, locked")
	server.SetAdminToken("")

	// DSG auth decides at route setup whether requests pass the auth
	// middleware, so rebuild the routes with auth on, and again after.
	restoreAuth := server.SetTestDSGAuth(viewOnlyDSG{})
	server.ResetTestRoutes()
	defer func() {
		restoreAuth()
		server.ResetTestRoutes()
	}()
	if err := datastore.SetPublicVersions([]string{string(root)}); err != nil {
		t.Fatalf("can't make %s public: %v", root, err)
	}
	defer datastore.SetPublicVersions(nil)

	viewer := caller{authorization: "Bearer viewer"}
	resolutionURL := fmt.Sprintf("%snode/%s/lmap/resolution", server.WebAPIPath, child)
	readURLs := map[dvid.UUID]string{root: rawURL(root, "lmap"), child: rawURL(child, "lmap")}

	// The auth layer admits these reads and refuses these writes, so the
	// checks below run on the paths that matter.
	for _, check := range []struct {
		desc   string
		who    caller
		uuid   dvid.UUID
		method string
		url    string
		status int
	}{
		{"anonymous GET on the public version", anonymous, root, http.MethodGet, readURLs[root], http.StatusOK},
		{"anonymous GET on the private version", anonymous, child, http.MethodGet, readURLs[child], http.StatusUnauthorized},
		{"view-only GET on the private version", viewer, child, http.MethodGet, readURLs[child], http.StatusOK},
		{"view-only POST on the private version", viewer, child, http.MethodPost, resolutionURL, http.StatusForbidden},
	} {
		var payload []byte
		if check.method == http.MethodPost {
			payload = []byte(`[9, 9, 9]`)
		}
		code, body, diff := f.send(t, check.uuid, check.who, check.method, check.url, payload)
		if code != check.status {
			t.Errorf("%s: expected status %d, got %d: %s", check.desc, check.status, code, body)
		}
		if diff != "" {
			t.Errorf("%s changed stored state: %s", check.desc, diff)
		}
	}

	f.checkReadMethods(t, root, anonymous, "public, anonymous")
	f.checkReadMethods(t, child, viewer, "private, view-only")
}

// Immutable POSTs still work on a locked node, and the two datatypes that mark
// them keep doing so while every other method follows the fail-closed default.
func TestImmutablePostsOnLockedNode(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	server.CreateTestInstance(t, uuid, "neuronjson", "neurons", dvid.Config{})
	server.CreateTestInstance(t, uuid, "roi", "roi", dvid.Config{})
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/neurons/key/1000?u=tester", server.WebAPIPath, uuid),
		strings.NewReader(`{"bodyid": 1000, "type": "KC"}`))
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/roi/roi", server.WebAPIPath, uuid),
		strings.NewReader(`[[0, 0, 0, 0]]`)) // block (0,0,0): voxels 0-31 on each axis
	server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid),
		strings.NewReader(`{"note": "lock for immutable POSTs"}`))

	got := server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/neurons/query", server.WebAPIPath, uuid),
		strings.NewReader(`{"bodyid": [1000]}`))
	var neurons []map[string]interface{}
	if err := json.Unmarshal(got, &neurons); err != nil {
		t.Fatalf("bad neuronjson query response %q: %v", string(got), err)
	}
	if len(neurons) != 1 || neurons[0]["type"] != "KC" {
		t.Errorf("neuronjson query on a locked node returned %s", string(got))
	}

	got = server.TestHTTP(t, "POST", fmt.Sprintf("%snode/%s/roi/ptquery", server.WebAPIPath, uuid),
		strings.NewReader(`[[5, 5, 5], [100, 100, 100]]`))
	var inside []bool
	if err := json.Unmarshal(got, &inside); err != nil {
		t.Fatalf("bad roi ptquery response %q: %v", string(got), err)
	}
	if len(inside) != 2 || !inside[0] || inside[1] {
		t.Errorf("roi ptquery on a locked node returned %s, expected [true,false]", string(got))
	}

	for name, endpoint := range map[dvid.InstanceName]string{"neurons": "query", "roi": "ptquery"} {
		d, err := datastore.GetDataByUUIDName(uuid, name)
		if err != nil {
			t.Fatalf("can't get instance %q: %v", name, err)
		}
		if d.IsMutationRequest("POST", endpoint) {
			t.Errorf("%s: POST %s counted as a mutation", name, endpoint)
		}
		for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
			if !d.IsMutationRequest(method, "key") {
				t.Errorf("%s: %s key not counted as a mutation", name, method)
			}
		}
		for _, method := range []string{"PUT", "PATCH"} {
			if !d.IsMutationRequest(method, endpoint) {
				t.Errorf("%s: %s %s not counted as a mutation", name, method, endpoint)
			}
		}
		if d.IsMutationRequest("GET", endpoint) {
			t.Errorf("%s: GET %s counted as a mutation", name, endpoint)
		}
	}
}
