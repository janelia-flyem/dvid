package datastore

import (
	"encoding/gob"
	"net/http"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
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

func (d *TestData) Send(s rpc.Session, t rpc.Transmit, filter dvid.Filter, v map[dvid.VersionID]struct{}) error {
	return nil
}

func (d *TestData) DoRPC(request Request, reply *Response) error {
	return nil
}

func (d *TestData) ServeHTTP(uuid dvid.UUID, ctx *VersionedCtx, w http.ResponseWriter, r *http.Request) {
}

func (d *TestData) Help() string {
	return "no help here!"
}

func TestDataGobEncoding(t *testing.T) {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &TestData{&Data{
		typename:    "testtype",
		typeurl:     "foo.bar.baz/testtype",
		typeversion: "1.0",
		name:        "my fabulous data",
		id:          dvid.InstanceID(13),
		uuid:        dvid.UUID("42"),
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		syncs:       []dvid.InstanceName{"moo", "bar", "baz"},
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
		t.Errorf("Bad Gob roundtrip:\nOriginal: %v\nReturned: %v\n", data, data2)
	}
}
