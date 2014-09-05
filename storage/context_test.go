package storage

import "github.com/janelia-flyem/dvid/dvid"

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
	name       dvid.DataString
	instanceID dvid.InstanceID
}

func (d *testData) DataName() dvid.DataString {
	return d.name
}

func (d *testData) InstanceID() dvid.InstanceID {
	return d.instanceID
}

func (d *testData) SetInstanceID(id dvid.InstanceID) {
	d.instanceID = id
}

func (d *testData) Versioned() bool {
	return false
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
	data := &testData{uuid, dvid.DataString(name), instanceID}
	return &DataContext{data, versionID}
}
