package datastore

import (
	"encoding/gob"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func init() {
	gob.Register(&TestType{})
	gob.Register(&TestData{})
}

type TestType struct {
	Type
}

func (t *TestType) Help() string {
	return "no help here!"
}

func (t *TestType) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (DataService, error) {
	basedata, err := NewDataService(t, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	return &TestData{basedata}, nil
}

type TestData struct {
	*Data
}

func (d *TestData) DoRPC(request Request, reply *Response) error {
	return nil
}

func (d *TestData) ServeHTTP(uuid dvid.UUID, ctx *VersionedCtx, w http.ResponseWriter, r *http.Request) map[string]interface{} {
	return nil
}

func (d *TestData) Help() string {
	return "no help here!"
}

func TestJsonSet(t *testing.T) {
	testJson := `{"versioned": true}`
	r := strings.NewReader(testJson)
	var config dvid.Config
	if err := config.SetByJSON(r); err != nil {
		t.Fatalf("couldn't set config by JSON: %v\n", err)
	}
	var d Data
	if err := d.ModifyConfig(config); err != nil {
		t.Fatalf("couldn't modify Data via config: %v\n", err)
	}
	if d.unversioned {
		t.Fatalf("bad set of Data unversioned via bool in config %v", config)
	}
	testJson = `{"versioned": false}`
	r = strings.NewReader(testJson)
	var config2 dvid.Config
	var d2 Data
	if err := config2.SetByJSON(r); err != nil {
		t.Fatalf("couldn't set config by JSON: %v\n", err)
	}
	if err := d2.ModifyConfig(config2); err != nil {
		t.Fatalf("couldn't modify Data via config: %v\n", err)
	}
	if !d2.unversioned {
		t.Fatalf("bad set of Data unversioned via bool string in config %v", config)
	}
	testJson = `{"versioned": "true"}`
	r = strings.NewReader(testJson)
	if err := config2.SetByJSON(r); err != nil {
		t.Fatalf("couldn't set config by JSON: %v\n", err)
	}
	if err := d2.ModifyConfig(config2); err != nil {
		t.Fatalf("couldn't modify Data via config: %v\n", err)
	}
	if d2.unversioned {
		t.Fatalf("bad set of Data unversioned via bool string in config %v", config)
	}
}

func TestDataGobEncoding(t *testing.T) {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &TestData{&Data{
		typename:    "testtype",
		typeurl:     "foo.bar.baz/testtype",
		typeversion: "1.0",
		id:          dvid.InstanceID(13),
		name:        "my fabulous data",
		rootUUID:    dvid.UUID("42"),
		dataUUID:    dvid.NewUUID(),
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		syncData:    dvid.UUIDSet{"moo": struct{}{}, "bar": struct{}{}, "baz": struct{}{}},
		tags:        map[string]string{"type": "meshes"},
	}}

	encoding, err := data.GobEncode()
	if err != nil {
		t.Fatalf("Couldn't Gob encode test data: %v\n", err)
	}
	data2 := &TestData{new(Data)}
	if err = data2.GobDecode(encoding); err != nil {
		t.Fatalf("Couldn't Gob decode test data: %v\n", err)
	}
	if !reflect.DeepEqual(data, data2) {
		t.Errorf("Bad Gob roundtrip:\nOriginal: %v\nReturned: %v\n", data.Data, data2.Data)
	}
}
