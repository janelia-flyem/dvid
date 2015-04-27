package labelgraph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"
)

var (
	dtype datastore.TypeService
)

// return example label graph
func getTestGraph() LabelGraph {
	graph := new(LabelGraph)
	graph.Vertices = append(graph.Vertices, labelVertex{1, 2.3})
	graph.Vertices = append(graph.Vertices, labelVertex{2, 10.1})
	graph.Edges = append(graph.Edges, labelEdge{1, 2, 10})
	graph.Transactions = make([]transactionItem, 0)

	return *graph
}

// convert sample labelgraph to bytes
func getGraphJSON() io.Reader {
	graph := getTestGraph()
	// graph json should add transaction information
	//graph.Transactions = make([]transactionItem, 0)
	jsonBytes, err := json.Marshal(graph)
	if err != nil {
		log.Fatalf("Can't encode graph into JSON: %s\n", err.Error())
	}
	return bytes.NewReader(jsonBytes)
}

// convert bytes to labelgraph
func loadGraphJSON(data []byte) (LabelGraph, error) {
	var graph LabelGraph
	if err := json.Unmarshal(data, &graph); err != nil {
		return graph, err
	}
	return graph, nil
}

// Sets package-level testRepo and TestVersionID
func initTestRepo() (datastore.Repo, dvid.VersionID) {
	if dtype == nil {
		var err error
		dtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get labelgraph type: %s\n", err)
		}
	}
	return tests.NewRepo()
}

// Make sure new labelgraph data have different IDs.
func TestNewLabelgraphDifferent(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice1, err := repo.NewData(dtype, "lg1", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 1: %s\n", err.Error())
	}
	data1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not labelgraph.Data\n")
	}
	dataservice2, err := repo.NewData(dtype, "lg2", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 2: %s\n", err.Error())
	}
	data2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not labelgraph.Data\n")
	}
	if data1.InstanceID() == data2.InstanceID() {
		t.Errorf("Instance IDs should be different: %d == %d\n",
			data1.InstanceID(), data2.InstanceID())
	}
}

func TestLabelgraphRepoPersistence(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	repo, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := repo.NewData(dtype, "lg", config)
	if err != nil {
		t.Errorf("Unable to create labelgraph instance: %s\n", err.Error())
	}
	lgdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labelgraph data service into labelgraph.Data\n")
	}
	oldData := *lgdata

	// Restart test datastore and see if datasets are still there.
	if err = repo.Save(); err != nil {
		t.Fatalf("Unable to save repo during labelgraph persistence test: %s\n", err.Error())
	}
	oldUUID := repo.RootUUID()
	tests.CloseReopenStore()

	repo2, err := datastore.RepoFromUUID(oldUUID)
	if err != nil {
		t.Fatalf("Can't get repo %s from reloaded test db: %s\n", oldUUID, err.Error())
	}
	dataservice2, err := repo2.GetDataByName("lg")
	if err != nil {
		t.Fatalf("Can't get labelgraph instance from reloaded test db: %s\n", err.Error())
	}
	lgdata2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not labelgraph.Data\n")
	}
	if !oldData.Equals(lgdata2) {
		t.Errorf("Expected %v, got %v\n", oldData, *lgdata2)
	}
}

// check subgraph endpoint
func TestLabelgraphPostAndDelete(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	// Create the ROI dataservice.
	repo, versionID := initTestRepo()
	uuid, err := datastore.UUIDFromVersion(versionID)
	if err != nil {
		t.Errorf(err.Error())
	}

	config := dvid.NewConfig()
	dataservice, err := repo.NewData(dtype, "lg", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance: %s\n", err.Error())
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Returned new data instance is not labelgraph.Data\n")
	}

	// PUT a labelraph
	subgraphRequest := fmt.Sprintf("%snode/%s/%s/subgraph", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", subgraphRequest, getGraphJSON())

	// Get back the labelgraph
	returnedData := server.TestHTTP(t, "GET", subgraphRequest, nil)
	retgraph, err := loadGraphJSON(returnedData)
	if err != nil {
		t.Errorf("Error on getting back JSON from roi GET: %s\n", err.Error())
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(retgraph, getTestGraph()) {
		t.Errorf("Bad PUT/GET ROI roundtrip\nOriginal:\n%s\nReturned:\n%s\n", getTestGraph(), retgraph)
	}

	// Delete the labelgraph
	_ = server.TestHTTP(t, "DELETE", subgraphRequest, nil)

	// Subgraph should now be empty
	returnedData = server.TestHTTP(t, "GET", subgraphRequest, nil)
	expectedResp := "{\"Transactions\":[],\"Vertices\":[],\"Edges\":[]}"
	if string(returnedData) != expectedResp {
		t.Errorf("Bad ROI after ROI delete.  Should be %s got: %s\n", expectedResp, string(returnedData))
	}
}
