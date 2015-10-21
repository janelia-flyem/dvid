package storage

import (
	"bytes"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

var (
	TestUUID1         dvid.UUID = "01"
	TestUUID2         dvid.UUID = "02"
	TestUUID3         dvid.UUID = "03"
	testUUIDToVersion           = map[dvid.UUID]dvid.VersionID{
		TestUUID1: 1,
		TestUUID2: 2,
		TestUUID3: 3,
	}
)

// Satisfies dvid.Data interface
type testData struct {
	uuid       dvid.UUID
	name       dvid.InstanceName
	instanceID dvid.InstanceID
}

func (d *testData) DataName() dvid.InstanceName {
	return d.name
}

func (d *testData) InstanceID() dvid.InstanceID {
	return d.instanceID
}

func (d *testData) SetInstanceID(id dvid.InstanceID) {
	d.instanceID = id
}

func (d *testData) Versioned() bool {
	return true
}

func (d *testData) TypeName() dvid.TypeString {
	return "testType"
}

func (d *testData) TypeURL() dvid.URLString {
	return "foo.baz.com/go/testData"
}

func (d *testData) TypeVersion() string {
	return "1.0"
}

func GetTestDataContext(uuid dvid.UUID, name string, instanceID dvid.InstanceID) *DataContext {
	versionID, found := testUUIDToVersion[uuid]
	if !found {
		return nil
	}
	data := &testData{uuid, dvid.InstanceName(name), instanceID}
	return NewDataContext(data, versionID)
}

func TestUnversionedKey(t *testing.T) {
	// make a metadata key
	var mctx MetadataContext
	tk := TKey([]byte{0x08, 0x33, 0x71, 0x00, 0x00, 0xFF})
	mk := mctx.ConstructKey(tk)

	// make a data key with same TKey.
	dctx := GetTestDataContext(TestUUID3, "mydata", 23)
	dk := dctx.ConstructKey(tk)

	// When we decompose the metadata key we should get proper components.
	isMetadata, unversK, v, err := UnversionedKey(mk)
	if err != nil {
		t.Errorf("Error on UnversionedKey(mk): %v\n", err)
	}
	if !isMetadata {
		t.Errorf("Expected metadata key returned from UnversionedKey(mk)\n")
	}
	if bytes.Compare(unversK, mk) != 0 {
		t.Errorf("Expected unversioned metadata key to equal metadata key\n")
	}
	if v != 0 {
		t.Errorf("Expected metadata key to return no version\n")
	}

	// When we decompose the data key we should get proper components.
	isMetadata2, unversK2, v2, err := UnversionedKey(dk)
	if err != nil {
		t.Errorf("Error on UnversionedKey(dk): %v\n", err)
	}
	if isMetadata2 {
		t.Errorf("Expected data key returned from UnversionedKey(dk)\n")
	}
	unversK3, v3, err := dctx.UnversionedKey(tk)
	if err != nil {
		t.Errorf("Error getting UnversionedKey from data ctx: %v\n", err)
	}
	if bytes.Compare(unversK2, unversK3) != 0 {
		t.Errorf("Expected unversioned data key to be same when deciphering full key and using context.\n")
	}
	if v2 != 3 {
		t.Errorf("Expected version id of data key from full key to be 3, got %d\n", v2)
	}
	if v3 != 3 {
		t.Errorf("Expected version id of data key from using context to be 3, got %d\n", v3)
	}
}
