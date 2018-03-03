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
		log.Fatalf("Can't encode graph into JSON: %v\n", err)
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
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	if dtype == nil {
		var err error
		dtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get labelgraph type: %v\n", err)
		}
	}
	return datastore.NewTestRepo()
}

// Make sure new labelgraph data have different IDs.
func TestNewLabelgraphDifferent(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	// Add data
	config := dvid.NewConfig()
	dataservice1, err := datastore.NewData(uuid, dtype, "lg1", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 1: %v\n", err)
	}
	data1, ok := dataservice1.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 1 is not labelgraph.Data\n")
	}
	dataservice2, err := datastore.NewData(uuid, dtype, "lg2", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance 2: %v\n", err)
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, dtype, "lg", config)
	if err != nil {
		t.Errorf("Unable to create labelgraph instance: %v\n", err)
	}
	lgdata, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast labelgraph data service into labelgraph.Data\n")
	}
	oldData := *lgdata

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, lgdata); err != nil {
		t.Fatalf("Unable to save repo during labelgraph persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUIDName(uuid, "lg")
	if err != nil {
		t.Fatalf("Can't get labelgraph instance from reloaded test db: %v\n", err)
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
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create the ROI dataservice.
	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, dtype, "lg", config)
	if err != nil {
		t.Errorf("Error creating new labelgraph instance: %v\n", err)
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
		t.Errorf("Error on getting back JSON from roi GET: %v\n", err)
	}

	// Make sure the two are the same.
	if !reflect.DeepEqual(retgraph, getTestGraph()) {
		t.Errorf("Bad PUT/GET ROI roundtrip\nOriginal:\n%v\nReturned:\n%v\n", getTestGraph(), retgraph)
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
