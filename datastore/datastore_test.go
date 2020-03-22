package datastore

import (
	"reflect"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

// Make sure we get unique IDs even when doing things concurrently.
func TestNewInstanceIDs(t *testing.T) {
	OpenTest()
	defer CloseTest()

	n := 1000 // number of IDs
	ch := make(chan dvid.InstanceID, n)
	for i := 0; i < n; i++ {
		go func() {
			id, err := manager.newInstanceID()
			if err != nil {
				t.Fatalf("error getting instance id: %v\n", err)
			}
			ch <- id
		}()
	}
	got := make(map[dvid.InstanceID]struct{}, n)
	for i := 0; i < n; i++ {
		select {
		case id := <-ch:
			_, found := got[id]
			if found {
				t.Fatalf("duplicate instance id created: %d\n", id)
			}
			got[id] = struct{}{}
		case <-time.After(time.Second * 2):
			t.Fatalf("took longer than 2 seconds to run duplicate instance id test")
			break
		}
	}
}

func TestBranching(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root, err := NewRepo("my alias", "my desc", nil, "")
	if err != nil {
		t.Fatalf("couldn't create repo: %v\n", err)
	}
	if err := Commit(root, "", []string{}); err != nil {
		t.Fatalf("couldn't commit node\n")
	}

	master2, err := NewVersion(root, "master note", "", nil)
	if err != nil {
		t.Fatalf("couldn't create new version: %v\n", err)
	}
	if err := Commit(master2, "", []string{}); err != nil {
		t.Fatalf("couldn't commit node\n")
	}

	other1, err := NewVersion(root, "other note", "other", nil)
	if err != nil {
		t.Fatalf("couldn't create new version: %v\n", err)
	}
	if err := Commit(other1, "", []string{}); err != nil {
		t.Fatalf("couldn't commit node\n")
	}

	master3, err := NewVersion(master2, "master note 2", "", nil)
	if err != nil {
		t.Fatalf("couldn't create new version: %v\n", err)
	}
	if err := Commit(master3, "", []string{}); err != nil {
		t.Fatalf("couldn't commit node\n")
	}

	_, err = NewVersion(master3, "random note", "random", nil)
	if err != nil {
		t.Fatalf("couldn't create new version: %v\n", err)
	}

	other2, err := NewVersion(other1, "other note 2", "other", nil)
	if err != nil {
		t.Fatalf("couldn't create new version: %v\n", err)
	}
	if err := Commit(other2, "", []string{}); err != nil {
		t.Fatalf("couldn't commit node\n")
	}

	masterUUIDs, err := GetBranchVersions(root, "")
	if err != nil {
		t.Fatalf("couldn't get branch versions: %v\n", err)
	}
	expectedUUIDs := []dvid.UUID{master3, master2, root}
	if !reflect.DeepEqual(masterUUIDs, expectedUUIDs) {
		t.Fatalf("For master branch expected versions %v, got %v\n", masterUUIDs, expectedUUIDs)
	}

	otherUUIDs, err := GetBranchVersions(root, "other")
	if err != nil {
		t.Fatalf("couldn't get branch versions: %v\n", err)
	}
	expectedUUIDs = []dvid.UUID{other2, other1, root}
	if !reflect.DeepEqual(otherUUIDs, expectedUUIDs) {
		t.Fatalf("For other branch expected versions %v, got %v\n", otherUUIDs, expectedUUIDs)
	}

	masterQuery := string(master2[:5]) + ":master"
	masterLeaf, _, err := MatchingUUID(masterQuery)
	if err != nil {
		t.Fatalf("couldn't get match for query %q: %v\n", masterQuery, err)
	}
	if masterLeaf != master3 {
		t.Fatalf("Expected master leaf %s from query %q, got %s\n", master3, masterQuery, masterLeaf)
	}

	otherQuery := string(master2[:5]) + ":other"
	otherLeaf, _, err := MatchingUUID(otherQuery)
	if err != nil {
		t.Fatalf("couldn't get match for query %q: %v\n", otherQuery, err)
	}
	if otherLeaf != other2 {
		t.Fatalf("Expected other leaf %s from query %q, got %s\n", other2, otherQuery, otherLeaf)
	}
}
