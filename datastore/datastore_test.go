package datastore

import (
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
