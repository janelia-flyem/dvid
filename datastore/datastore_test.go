package datastore

import (
	"reflect"
	"testing"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/dvid"
)

// Returns a mockRepo with limited functionality for testing.
func mockRepo() *repoT {
	return &repoT{
		repoID:     0,
		rootID:     dvid.UUID("test uuid"),
		properties: make(map[string]interface{}),
		data:       make(map[dvid.DataString]DataService),
	}
}

func TestServerContext(t *testing.T) {
	repo := mockRepo()
	versionID := dvid.VersionID(1003)
	ctx := NewServerContext(context.Background(), repo, versionID)
	repo2, versions, err := FromContext(ctx)
	if err != nil {
		t.Errorf("Server context retrieval error: %s\n", err.Error())
	}
	if !reflect.DeepEqual(repo, repo2) {
		t.Errorf("Server context retrieval error: bad repo\n")
	}
	if len(versions) != 1 {
		t.Errorf("Server context retrieval error: bad versions %v (expected just %d)\n",
			versions, versionID)
	}
}
