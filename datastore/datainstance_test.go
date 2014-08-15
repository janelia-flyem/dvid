package datastore

import (
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestDataGobEncoding(t *testing.T) {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &Data{
		typename:    "testtype",
		typeurl:     "foo.bar.baz/testtype",
		typeversion: "1.0",
		name:        "my fabulous data",
		id:          dvid.InstanceID(13),
		uuid:        dvid.UUID("42"),
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		versioned:   true,
	}

	encoding, err := data.GobEncode()
	if err != nil {
		t.Fatalf("Couldn't Gob encode Data: %s\n", err.Error())
	}
	data2 := &Data{}
	if err = data2.GobDecode(encoding); err != nil {
		t.Fatalf("Couldn't Gob decode Data: %s\n", err.Error())
	}
	if !reflect.DeepEqual(data, data2) {
		t.Errorf("Bad Gob roundtrip:\nOriginal: %v\nReturned: %v\n", data, data2)
	}
}
