// +build graphkeyvalue

package storage_test

import (
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/dvid/tests"
)

func TestBasicGraph(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	graphDB, err := storage.GraphStore()
	if err != nil {
		t.Fatalf("Can't open graph store: %s\n", err.Error())
	}

	ctx := storage.GetTestDataContext(storage.TestUUID1, "graph", dvid.InstanceID(13))

	if err = graphDB.CreateGraph(ctx); err != nil {
		t.Errorf("Can't create graph: %s\n", err.Error())
	}

	if err = graphDB.AddVertex(ctx, 1, 5); err != nil {
		t.Errorf("Can't add vertex: %s\n", err.Error())
	}

	if err = graphDB.AddVertex(ctx, 2, 11); err != nil {
		t.Errorf("Can't add vertex: %s\n", err.Error())
	}

	if err = graphDB.AddEdge(ctx, 1, 2, 0.3); err != nil {
		t.Errorf("Can't add edge: %s\n", err.Error())
	}

	vert1, err := graphDB.GetVertex(ctx, 1)
	if err != nil {
		t.Errorf("Can't get vertex: %s\n", err.Error())
	}
	if vert1.Weight != float64(5) {
		t.Errorf("Bad weight.  Should be %f, was %f\n", 5, vert1.Weight)
	}

	vert2, err := graphDB.GetVertex(ctx, 2)
	if err != nil {
		t.Errorf("Can't get vertex: %s\n", err.Error())
	}
	if vert2.Weight != float64(11) {
		t.Errorf("Bad weight.  Should be %f, was %f\n", 11, vert1.Weight)
	}

	edge, err := graphDB.GetEdge(ctx, 1, 2)
	if err != nil {
		t.Errorf("Can't get edge: %s\n", err.Error())
	}
	if edge.Weight != float64(0.3) {
		t.Errorf("Bad edge.  Should be %f, was %f\n", 0.3, edge.Weight)
	}

	edge, err = graphDB.GetEdge(ctx, 2, 1)
	if err != nil {
		t.Errorf("Can't get edge: %s\n", err.Error())
	}
	if edge.Weight != float64(0.3) {
		t.Errorf("Bad edge.  Should be %f, was %f\n", 0.3, edge.Weight)
	}

	if err = graphDB.RemoveGraph(ctx); err != nil {
		t.Errorf("Error removing graph: %s\n", err.Error())
	}
}
