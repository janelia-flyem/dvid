package storage

import (
	"bytes"
	"sync/atomic"
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
	dataUUID   dvid.UUID
	rootUUID   dvid.UUID
	name       dvid.InstanceName
	instanceID dvid.InstanceID
	kvStore    dvid.Store
	logStore   dvid.Store
	syncData   dvid.UUIDSet
	mutID      uint64
	tags       map[string]string
	deleted    bool
}

func (d *testData) DataName() dvid.InstanceName {
	return d.name
}

func (d *testData) InstanceID() dvid.InstanceID {
	return d.instanceID
}

func (d *testData) RootUUID() dvid.UUID {
	return d.rootUUID
}

func (d *testData) DAGRootUUID() (dvid.UUID, error) {
	return d.rootUUID, nil
}

func (d *testData) RootVersionID() (dvid.VersionID, error) {
	return dvid.VersionID(0), nil
}

func (d *testData) DataUUID() dvid.UUID {
	return d.dataUUID
}

func (d *testData) SetInstanceID(id dvid.InstanceID) {
	d.instanceID = id
}

func (d *testData) SetDataUUID(uuid dvid.UUID) {
	d.dataUUID = uuid
}

func (d *testData) SetRootUUID(uuid dvid.UUID) {
	d.rootUUID = uuid
}

func (d *testData) SetName(name dvid.InstanceName) {
	d.name = name
}

func (d *testData) SetSync(syncs dvid.UUIDSet) {
	d.syncData = syncs
}

func (d *testData) SetTags(tags map[string]string) {
	d.tags = tags
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

func (d *testData) Tags() map[string]string {
	return nil
}

func (d *testData) NewMutationID() uint64 {
	return atomic.AddUint64(&(d.mutID), 1)
}

func (d *testData) KVStore() (dvid.Store, error) {
	if d.kvStore != nil {
		return d.kvStore, nil
	}
	return DefaultKVStore()
}

func (d *testData) SetKVStore(kvStore dvid.Store) {
	d.kvStore = kvStore
}

func (d *testData) SetLogStore(logStore dvid.Store) {
	d.logStore = logStore
}

func (d *testData) IsDeleted() bool {
	return d.deleted
}

func (d *testData) SetDeleted(deleted bool) {
	d.deleted = deleted
}

func GetTestDataContext(uuid dvid.UUID, name string, instanceID dvid.InstanceID) *DataContext {
	versionID, found := testUUIDToVersion[uuid]
	if !found {
		return nil
	}
	data := &testData{
		dataUUID:   dvid.NewUUID(),
		rootUUID:   uuid,
		name:       dvid.InstanceName(name),
		instanceID: instanceID,
	}
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
	unversK, v, err := SplitKey(mk)
	if err != nil {
		t.Errorf("Error on SplitKey(mk): %v\n", err)
	}
	if !bytes.Equal(unversK, mk) {
		t.Errorf("Expected unversioned metadata key to equal metadata key\n")
	}

	empty := make([]byte, 0)
	if !bytes.Equal(v, empty) {
		t.Errorf("Expected metadata key to return no version\n")
	}

	// When we decompose the data key we should get proper components.
	unversK2, v2, err := SplitKey(dk)
	if err != nil {
		t.Errorf("Error on SplitKey(dk): %v\n", err)
	}

	unversK3, v3, err := dctx.SplitKey(tk)
	if err != nil {
		t.Errorf("Error getting SplitKey from data ctx: %v\n", err)
	}
	if !bytes.Equal(unversK2, unversK3) {
		t.Errorf("Expected unversioned data key to be same when deciphering full key and using context.\n")
	}
	if bytes.Equal(v2, []byte{0x02}) {
		t.Errorf("Expected version id of data key from full key to be 3, got %d\n", v2)
	}
	if bytes.Equal(v3, []byte{0x03}) {
		t.Errorf("Expected version id of data key from using context to be 3, got %d\n", v3)
	}
}
