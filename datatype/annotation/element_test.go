package annotation

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

var (
	syntype datastore.TypeService
	testMu  sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if syntype == nil {
		var err error
		syntype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get synapse type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func TestSynapseRepoPersistence(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	// Make labels and set various properties
	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, syntype, "synapses", config)
	if err != nil {
		t.Errorf("Unable to create keyvalue instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Errorf("Can't cast data service into synapse.Data\n")
	}
	oldData := *data

	// Restart test datastore and see if datasets are still there.
	if err = datastore.SaveDataByUUID(uuid, data); err != nil {
		t.Fatalf("Unable to save repo during synapse persistence test: %v\n", err)
	}
	datastore.CloseReopenTest()

	dataservice2, err := datastore.GetDataByUUID(uuid, "synapses")
	if err != nil {
		t.Fatalf("Can't get synapse instance from reloaded test db: %v\n", err)
	}
	data2, ok := dataservice2.(*Data)
	if !ok {
		t.Errorf("Returned new data instance 2 is not synapse.Data\n")
	}
	if !oldData.Equals(data2) {
		t.Errorf("Expected %v, got %v\n", oldData, *data2)
	}
}

var testData = Elements{
	{
		Pos:  dvid.Point3d{15, 27, 35},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{20, 30, 40}}, {Rel: PreSynTo, To: dvid.Point3d{14, 25, 37}}, {Rel: PreSynTo, To: dvid.Point3d{33, 30, 31}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
		Prop: map[string]string{
			"Im a T-Bar":         "yes",
			"I'm not a PSD":      "sure",
			"i'm really special": "",
		},
	},
	{
		Pos:  dvid.Point3d{20, 30, 40},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1"},
	},
	{
		Pos:  dvid.Point3d{14, 25, 37},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{33, 30, 31},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{128, 63, 99},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{88, 47, 80}}, {Rel: PreSynTo, To: dvid.Point3d{120, 65, 100}}, {Rel: PreSynTo, To: dvid.Point3d{151, 67, 98}}},
		Tags: []Tag{"Synapse2"},
		Prop: map[string]string{
			"Im a T-Bar":             "no",
			"I'm not a PSD":          "not really",
			"i'm not really special": "at all",
		},
	},
	{
		Pos:  dvid.Point3d{88, 47, 80},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: GroupedWith, To: dvid.Point3d{14, 25, 37}}, {Rel: PostSynTo, To: dvid.Point3d{128, 63, 99}}, {Rel: GroupedWith, To: dvid.Point3d{20, 30, 40}}},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{120, 65, 100},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{128, 63, 99}}},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{151, 67, 98},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{128, 63, 99}}},
		Tags: []Tag{"Synapse2"},
	},
}

var expected3 = Elements{
	{
		Pos:  dvid.Point3d{128, 63, 99},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{88, 47, 80}}, {Rel: PreSynTo, To: dvid.Point3d{120, 65, 100}}, {Rel: PreSynTo, To: dvid.Point3d{151, 67, 98}}},
		Tags: []Tag{"Synapse2"},
		Prop: map[string]string{
			"Im a T-Bar":             "no",
			"I'm not a PSD":          "not really",
			"i'm not really special": "at all",
		},
	},
}

var afterMove = Elements{
	{
		Pos:  dvid.Point3d{15, 27, 35},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{20, 30, 40}}, {Rel: PreSynTo, To: dvid.Point3d{14, 25, 37}}, {Rel: PreSynTo, To: dvid.Point3d{33, 30, 31}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
		Prop: map[string]string{
			"Im a T-Bar":         "yes",
			"I'm not a PSD":      "sure",
			"i'm really special": "",
		},
	},
	{
		Pos:  dvid.Point3d{20, 30, 40},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1"},
	},
	{
		Pos:  dvid.Point3d{14, 25, 37},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{33, 30, 31},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{127, 64, 100},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{88, 47, 80}}, {Rel: PreSynTo, To: dvid.Point3d{120, 65, 100}}, {Rel: PreSynTo, To: dvid.Point3d{151, 67, 98}}},
		Tags: []Tag{"Synapse2"},
		Prop: map[string]string{
			"Im a T-Bar":             "no",
			"I'm not a PSD":          "not really",
			"i'm not really special": "at all",
		},
	},
	{
		Pos:  dvid.Point3d{88, 47, 80},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: GroupedWith, To: dvid.Point3d{14, 25, 37}}, {Rel: PostSynTo, To: dvid.Point3d{127, 64, 100}}, {Rel: GroupedWith, To: dvid.Point3d{20, 30, 40}}},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{120, 65, 100},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{127, 64, 100}}},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{151, 67, 98},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{127, 64, 100}}},
		Tags: []Tag{"Synapse2"},
	},
}

var afterDelete = Elements{
	{
		Pos:  dvid.Point3d{15, 27, 35},
		Kind: PreSyn,
		Rels: []Relationship{{Rel: PreSynTo, To: dvid.Point3d{20, 30, 40}}, {Rel: PreSynTo, To: dvid.Point3d{14, 25, 37}}, {Rel: PreSynTo, To: dvid.Point3d{33, 30, 31}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
		Prop: map[string]string{
			"Im a T-Bar":         "yes",
			"I'm not a PSD":      "sure",
			"i'm really special": "",
		},
	},
	{
		Pos:  dvid.Point3d{20, 30, 40},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1"},
	},
	{
		Pos:  dvid.Point3d{14, 25, 37},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{33, 30, 31},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: PostSynTo, To: dvid.Point3d{15, 27, 35}}},
		Tags: []Tag{"Synapse1", "Zlt90"},
	},
	{
		Pos:  dvid.Point3d{88, 47, 80},
		Kind: PostSyn,
		Rels: []Relationship{{Rel: GroupedWith, To: dvid.Point3d{14, 25, 37}}, {Rel: GroupedWith, To: dvid.Point3d{20, 30, 40}}},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{120, 65, 100},
		Kind: PostSyn,
		Rels: []Relationship{},
		Tags: []Tag{"Synapse2"},
	},
	{
		Pos:  dvid.Point3d{151, 67, 98},
		Kind: PostSyn,
		Rels: []Relationship{},
		Tags: []Tag{"Synapse2"},
	},
}

func getTag(tag Tag, elems Elements) Elements {
	var result Elements
	for _, elem := range elems {
		for _, etag := range elem.Tags {
			if etag == tag {
				result = append(result, elem)
				break
			}
		}
	}
	return result
}

func TestRequests(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := initTestRepo()

	config := dvid.NewConfig()
	dataservice, err := datastore.NewData(uuid, syntype, "mysynapses", config)
	if err != nil {
		t.Fatalf("Error creating new data instance: %v\n", err)
	}
	data, ok := dataservice.(*Data)
	if !ok {
		t.Fatalf("Returned new data instance is not synapse.Data\n")
	}

	// PUT first batch of synapses
	testJSON, err := json.Marshal(testData)
	if err != nil {
		t.Fatal(err)
	}
	url1 := fmt.Sprintf("%snode/%s/%s/elements", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", url1, strings.NewReader(string(testJSON)))

	// GET synapses back within superset bounding box and make sure all data is there.
	url2 := fmt.Sprintf("%snode/%s/%s/elements/1000_1000_1000/0_0_0", server.WebAPIPath, uuid, data.DataName())
	returnValue := server.TestHTTP(t, "GET", url2, nil)
	var got Elements
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(testData.Normalize(), got.Normalize()) {
		t.Errorf("Error on POSTing of synaptic elements:\nExpected this:\n%s\nGot this:\n%s\n", string(testJSON), string(returnValue))
	}

	// Test subset GET
	url3 := fmt.Sprintf("%snode/%s/%s/elements/5_5_5/126_60_97", server.WebAPIPath, uuid, data.DataName())
	returnValue = server.TestHTTP(t, "GET", url3, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected3.Normalize(), got.Normalize()) {
		t.Errorf("Error on subset GET of synaptic elements. Got:\n%s\n", string(returnValue))
	}

	// Test Tag 1
	tag := Tag("Synapse2")
	url4 := fmt.Sprintf("%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag)
	returnValue = server.TestHTTP(t, "GET", url4, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	synapse2 := getTag(tag, testData)
	if !reflect.DeepEqual(synapse2.Normalize(), got.Normalize()) {
		t.Errorf("Error on tag %q GET.  Got:\n%s\n", tag, string(returnValue))
	}

	// Test Tag 2
	tag2 := Tag("Zlt90")
	url4a := fmt.Sprintf("%snode/%s/%s/tag/%s", server.WebAPIPath, uuid, data.DataName(), tag2)
	returnValue = server.TestHTTP(t, "GET", url4a, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	zlt90 := getTag(tag2, testData)
	if !reflect.DeepEqual(zlt90.Normalize(), got.Normalize()) {
		t.Errorf("Error on tag %q GET.  Got:\n%s\n", tag2, string(returnValue))
	}

	// Test move
	url5 := fmt.Sprintf("%snode/%s/%s/move/128_63_99/127_64_100", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "POST", url5, nil)

	// --- check all elements
	returnValue = server.TestHTTP(t, "GET", url2, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterMove.Normalize(), got.Normalize()) {
		t.Errorf("Error after move.  Got:\n%s\n", string(returnValue))
	}

	// --- check tag
	returnValue = server.TestHTTP(t, "GET", url4, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	synapse2 = getTag(tag, afterMove)
	if !reflect.DeepEqual(synapse2.Normalize(), got.Normalize()) {
		t.Errorf("Error on tag %q GET after move.  Got:\n%s\n", tag, string(returnValue))
	}

	// Test delete
	url6 := fmt.Sprintf("%snode/%s/%s/element/127_64_100", server.WebAPIPath, uuid, data.DataName())
	server.TestHTTP(t, "DELETE", url6, nil)

	// --- check all elements
	returnValue = server.TestHTTP(t, "GET", url2, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterDelete.Normalize(), got.Normalize()) {
		t.Errorf("Error after delete.  Got:\n%v\nExpected:\n%v\n", got.Normalize(), afterDelete.Normalize())
	}

	// --- check tag
	returnValue = server.TestHTTP(t, "GET", url4, nil)
	got = Elements{}
	if err := json.Unmarshal(returnValue, &got); err != nil {
		t.Fatal(err)
	}
	synapse2 = getTag(tag, afterDelete)
	if !reflect.DeepEqual(synapse2.Normalize(), got.Normalize()) {
		t.Errorf("Error on tag %q GET after delete.  Got:\n%s\n", tag, string(returnValue))
	}
}
