package labelmap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

func (svm *SVMap) checkMapping(t *testing.T, mappedVersions distFromRoot, from, to uint64) {
	svm.fmMu.RLock()
	mappedLabel, found := svm.mapLabel(from, mappedVersions)
	svm.fmMu.RUnlock()
	if !found {
		t.Fatalf("expected mapping of %d to be found\n", from)
	}
	if mappedLabel != to {
		t.Fatalf("expected mapping of %d -> %d, got %d\n", from, to, mappedLabel)
	}
}

func TestSVMap(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()
	uuid, v := initTestRepo()
	var config dvid.Config
	server.CreateTestInstance(t, uuid, "labelmap", "labels", config)

	d, err := GetByVersionName(v, "labels")
	if err != nil {
		t.Fatalf("can't get labelmap data service: %v\n", err)
	}
	svm := initMapping(d, v)

	mapping := map[uint64]uint64{
		1: 3,
		2: 3,
		4: 6,
		5: 6,
	}
	for from, to := range mapping {
		svm.setMapping(v, from, to)
	}

	mappedVersions := svm.getMappedVersionsDist(v)

	for from, to := range mapping {
		svm.checkMapping(t, mappedVersions, from, to)
	}

	// Create a new version
	apiStr := fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid)
	payload := bytes.NewBufferString(`{"note": "first version"}`)
	server.TestHTTP(t, "POST", apiStr, payload)

	versionReq := fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, uuid)
	respData := server.TestHTTP(t, "POST", versionReq, nil)
	resp := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	v2, err := datastore.VersionFromUUID(resp.Child)
	if err != nil {
		t.Fatalf("error getting version from UUID: %v\n", err)
	}

	// Add a different mapping to the new version
	mapping2 := map[uint64]uint64{
		2: 5,
		4: 7,
		5: 8,
	}
	for from, to := range mapping2 {
		svm.setMapping(v2, from, to)
	}

	// Make sure that the mapping is the new one
	mappedVersions2 := svm.getMappedVersionsDist(v2)

	svm.checkMapping(t, mappedVersions2, 1, 3) // unset should fall back to version 1
	for from, to := range mapping2 {
		svm.checkMapping(t, mappedVersions2, from, to)
	}

	// Verify that the old mapping is still there
	mappedVersions = svm.getMappedVersionsDist(v)

	for from, to := range mapping {
		svm.checkMapping(t, mappedVersions, from, to)
	}

	// Create a new branch off of the first version
	apiStr = fmt.Sprintf("%snode/%s/branch", server.WebAPIPath, uuid)
	payload = bytes.NewBufferString(`{"branch": "branch 1", "note": "version 3"}`)
	respData = server.TestHTTP(t, "POST", apiStr, payload)
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	v3, err := datastore.VersionFromUUID(resp.Child)
	if err != nil {
		t.Fatalf("error getting version from UUID: %v\n", err)
	}

	// Get a new mapping
	mapping3 := map[uint64]uint64{
		3: 15,
		4: 17,
		6: 18,
	}
	for from, to := range mapping3 {
		svm.setMapping(v3, from, to)
	}

	// Make sure that the mapping is the new one
	mappedVersions3 := svm.getMappedVersionsDist(v3)

	svm.checkMapping(t, mappedVersions3, 1, 3) // unset should fall back to version 1
	svm.checkMapping(t, mappedVersions3, 2, 3) // unset should fall back to version 1
	svm.checkMapping(t, mappedVersions3, 5, 6) // unset should fall back to version 1
	for from, to := range mapping3 {
		svm.checkMapping(t, mappedVersions3, from, to)
	}

	// Verify that the old mapping is still there
	mappedVersions = svm.getMappedVersionsDist(v)

	for from, to := range mapping {
		svm.checkMapping(t, mappedVersions, from, to)
	}

}
