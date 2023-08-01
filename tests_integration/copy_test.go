package tests_integration

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	// Declare the data types the DVID server should support
	"github.com/janelia-flyem/dvid/datatype/keyvalue"
)

type mockDB struct {
	name string
	kvs  storage.KeyValues
}

// -- functions so mockDB fulfills storage.Store interface.
func (db *mockDB) String() string {
	return db.name
}

func (db *mockDB) Close() {}

func (db *mockDB) Equal(cfg dvid.StoreConfig) bool {
	return db.name == cfg.Engine
}

func (db *mockDB) GetStoreConfig() dvid.StoreConfig {
	return dvid.StoreConfig{Engine: db.name}
}

// -- function so mockDB fulfills datastore.rawQueryDB interface
func (db *mockDB) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue, cancel <-chan struct{}) error {
	for i, kv := range db.kvs {
		if bytes.Compare(kv.K, kStart) >= 0 && bytes.Compare(kv.K, kEnd) <= 0 {
			out <- &(db.kvs[i])
		}
	}
	out <- nil
	return nil
}

// -- function so mockDB fulfills datastore.rawPutDB interface
func (db *mockDB) RawPut(k storage.Key, v []byte) error {
	kv := storage.KeyValue{
		K: k,
		V: v,
	}
	db.kvs = append(db.kvs, kv)
	dvid.Infof("put value %q, now db %q has %d kv\n", string(v), db, len(db.kvs))
	sort.Sort(db.kvs)
	return nil
}

var testDataDB1 = storage.KeyValues{
	{
		K: storage.Key("aaa"),
		V: []byte("one"),
	},
	{
		K: storage.Key("aab"),
		V: []byte("two"),
	},
	{
		K: storage.Key("aba"),
		V: []byte("three"),
	},
	{
		K: storage.Key("caad"),
		V: []byte("four"),
	},
}

func checkKV(db *mockDB, expected storage.KeyValues) (err error) {
	var wg sync.WaitGroup
	wg.Add(1)

	ch := make(chan *storage.KeyValue, 100)
	i := 0
	go func() {
		for {
			kv := <-ch
			if kv == nil {
				wg.Done()
				break
			}
			if bytes.Compare(kv.K, expected[i].K) != 0 {
				err = fmt.Errorf("bad kv %d transmitted with key %q != expected %q", i, string(kv.K), string(testDataDB1[i].K))
				wg.Done()
				break
			}
			if bytes.Compare(kv.V, expected[i].V) != 0 {
				err = fmt.Errorf("bad kv %d transmitted with value %q != expected %q", i, string(kv.V), string(testDataDB1[i].V))
				wg.Done()
				break
			}
			i++
		}
	}()
	db.RawRangeQuery(storage.Key([]byte{0x00}), storage.Key([]byte{0xFF, 0xFF, 0xFF, 0xFF}), false, ch, nil)
	wg.Wait()
	if i != len(db.kvs) {
		err = fmt.Errorf("expected %d kv pairs but only got %d", len(db.kvs), i)
	}
	return
}

func TestMockDB(t *testing.T) {
	var db mockDB
	for _, kv := range testDataDB1 {
		db.RawPut(kv.K, kv.V)
	}
	if err := checkKV(&db, testDataDB1); err != nil {
		t.Fatalf("bad check: %v\n", err)
	}
}

func checkVersionedKV(db *mockDB, expected []versionedKV) (err error) {
	dvid.Infof("Checking db %q with %d kvs...\n", db, len(db.kvs))
	var wg sync.WaitGroup
	wg.Add(1)

	ch := make(chan *storage.KeyValue, 100)
	i := 0
	go func() {
		for {
			kv := <-ch
			if kv == nil {
				wg.Done()
				break
			}
			if i >= len(expected) {
				err = fmt.Errorf("got %d kvs when only %d expected", i+1, len(expected))
				wg.Done()
				break
			}
			var curV dvid.VersionID
			if curV, err = storage.VersionFromDataKey(kv.K); err != nil {
				wg.Done()
				break
			}
			var tk storage.TKey
			if tk, err = storage.TKeyFromKey(kv.K); err != nil {
				wg.Done()
				break
			}
			dvid.Infof("Received tkey %q, version %d, value %q\n", tk, curV, kv.V)
			if curV != expected[i].v {
				err = fmt.Errorf("expected version %d, got %d for key %q, value %q", expected[i].v, curV, string(tk), string(kv.V))
				wg.Done()
				break
			}
			if bytes.Compare(tk, expected[i].K) != 0 {
				err = fmt.Errorf("bad kv %d transmitted with key %q != expected %q", i, string(tk), string(expected[i].K))
				wg.Done()
				break
			}
			if bytes.Compare(kv.V, expected[i].V) != 0 {
				err = fmt.Errorf("bad kv %d transmitted with value %q != expected %q", i, string(kv.V), string(expected[i].V))
				wg.Done()
				break
			}
			i++
		}
	}()
	db.RawRangeQuery(storage.Key([]byte{0x00}), storage.Key([]byte{0xFF, 0xFF, 0xFF, 0xFF}), false, ch, nil)
	wg.Wait()
	if err != nil {
		return
	}
	if i != len(expected) {
		err = fmt.Errorf("expected %d kv, got %d kv", len(expected), i)
	}
	return
}

type versionedKV struct {
	v dvid.VersionID
	K storage.TKey
	V []byte
}

var kvTestData2 = []versionedKV{
	{
		v: 1,
		K: storage.TKey("key1"),
		V: []byte("val1-1"),
	},
	{
		v: 1,
		K: storage.TKey("key2"),
		V: []byte("val2-1"),
	},
	{
		v: 1,
		K: storage.TKey("key3"),
		V: []byte("val3-1"),
	},
	{
		v: 1,
		K: storage.TKey("key6"),
		V: []byte("val6-1"),
	},
	{
		v: 2,
		K: storage.TKey("key1"),
		V: []byte("val1-2"),
	},
	{
		v: 2,
		K: storage.TKey("key4"),
		V: []byte("val4-2"),
	},
	{
		v: 3,
		K: storage.TKey("key2"),
		V: []byte("val2-3"),
	},
	{
		v: 4,
		K: storage.TKey("key2"),
		V: []byte("val2-4"),
	},
	{
		v: 4,
		K: storage.TKey("key3"),
		V: []byte("val3-4"),
	},
	{
		v: 4,
		K: storage.TKey("key6"),
		V: []byte("val6-1"), // this is a repeat value that should be removed in migration
	},
	{
		v: 5,
		K: storage.TKey("key3"),
		V: []byte("val3-5"),
	},
	{
		v: 6,
		K: storage.TKey("key2"),
		V: []byte("val2-6"),
	},
	{
		v: 7, // none of the version 7 kvs should make it to migrated db since it's a branch
		K: storage.TKey("key1"),
		V: []byte("val1-7"),
	},
	{
		v: 7,
		K: storage.TKey("key3"),
		V: []byte("val3-7"),
	},
	{
		v: 7,
		K: storage.TKey("key4"),
		V: []byte("val4-7"),
	},
	{
		v: 8,
		K: storage.TKey("key1"),
		V: []byte("val1-8"),
	},
	{
		v: 8,
		K: storage.TKey("key3"),
		V: []byte("val3-8"),
	},
	{
		v: 9,
		K: storage.TKey("key1"),
		V: []byte("val1-9"),
	},
	{
		v: 9,
		K: storage.TKey("key2"),
		V: []byte("val2-9"),
	},
	{
		v: 9,
		K: storage.TKey("key5"),
		V: []byte("val5-9"),
	},
}

// expected for migration of versions 1, 4, 8
var expectedKV2 = []versionedKV{
	{
		v: 1,
		K: storage.TKey("key1"),
		V: []byte("val1-1"),
	},
	{
		v: 4,
		K: storage.TKey("key1"),
		V: []byte("val1-2"),
	},
	{
		v: 8,
		K: storage.TKey("key1"),
		V: []byte("val1-8"),
	},
	{
		v: 1,
		K: storage.TKey("key2"),
		V: []byte("val2-1"),
	},
	{
		v: 4,
		K: storage.TKey("key2"),
		V: []byte("val2-4"),
	},
	{
		v: 8,
		K: storage.TKey("key2"),
		V: []byte("val2-6"), // note that this is not "val2-7" which is on branch
	},
	{
		v: 1,
		K: storage.TKey("key3"),
		V: []byte("val3-1"),
	},
	{
		v: 4,
		K: storage.TKey("key3"),
		V: []byte("val3-4"),
	},
	{
		v: 8,
		K: storage.TKey("key3"),
		V: []byte("val3-8"),
	},
	{
		v: 4,
		K: storage.TKey("key4"),
		V: []byte("val4-2"),
	},
	{
		v: 1,
		K: storage.TKey("key6"),
		V: []byte("val6-1"), // no version 4 because it's duplicate
	},
}

// expected for migration of versions 2, 7
var expectedKV3 = []versionedKV{
	{
		v: 2,
		K: storage.TKey("key1"),
		V: []byte("val1-2"),
	},
	{
		v: 7, // none of the version 7 kvs should make it to migrated db since it's a branch
		K: storage.TKey("key1"),
		V: []byte("val1-7"),
	},
	{
		v: 2,
		K: storage.TKey("key2"),
		V: []byte("val2-1"),
	},
	{
		v: 7,
		K: storage.TKey("key2"),
		V: []byte("val2-4"),
	},
	{
		v: 2,
		K: storage.TKey("key3"),
		V: []byte("val3-1"),
	},
	{
		v: 7,
		K: storage.TKey("key3"),
		V: []byte("val3-7"),
	},
	{
		v: 2,
		K: storage.TKey("key4"),
		V: []byte("val4-2"),
	},
	{
		v: 7,
		K: storage.TKey("key4"),
		V: []byte("val4-7"),
	},
	{
		v: 2,
		K: storage.TKey("key6"),
		V: []byte("val6-1"), // no version 4 because it's duplicate
	},
}

func commitAndNewVersion(t *testing.T, parent dvid.UUID) (child dvid.UUID, curV dvid.VersionID) {
	err := datastore.Commit(parent, "", []string{})
	if err != nil {
		t.Fatalf("couldn't commit node %q: %s\n", parent, err)
	}
	child, err = datastore.NewVersion(parent, "master note", "", nil)
	if err != nil {
		t.Fatalf("couldn't create new version on %q: %v\n", parent, err)
	}
	curV, err = datastore.VersionFromUUID(child)
	if err != nil {
		t.Fatalf("couldn't get version from uuid %q: %v\n", child, err)
	}
	return
}

func commitAndNewBranch(t *testing.T, parent dvid.UUID) (child dvid.UUID, curV dvid.VersionID) {
	err := datastore.Commit(parent, "", []string{})
	if err != nil {
		t.Fatalf("couldn't commit node %q: %s\n", parent, err)
	}
	tm := time.Now()
	branch := fmt.Sprintf("new branch %s", tm)
	child, err = datastore.NewVersion(parent, "master note", branch, nil)
	if err != nil {
		t.Fatalf("couldn't create new version on %q: %v\n", parent, err)
	}
	curV, err = datastore.VersionFromUUID(child)
	if err != nil {
		t.Fatalf("couldn't get version from uuid %q: %v\n", child, err)
	}
	return
}

func newBranch(t *testing.T, parent dvid.UUID) (child dvid.UUID, curV dvid.VersionID) {
	var err error
	tm := time.Now()
	branch := fmt.Sprintf("new branch %s", tm)
	child, err = datastore.NewVersion(parent, "master note", branch, nil)
	if err != nil {
		t.Fatalf("couldn't create new version on %q: %v\n", parent, err)
	}
	curV, err = datastore.VersionFromUUID(child)
	if err != nil {
		t.Fatalf("couldn't get version from uuid %q: %v\n", child, err)
	}
	return
}

func writeTestDataForVersion(db *mockDB, d dvid.Data, v dvid.VersionID) {
	ctx := datastore.NewVersionedCtx(d, v)
	for _, vkv := range kvTestData2 {
		if v == vkv.v {
			k := ctx.ConstructKeyVersion(vkv.K, v)
			db.RawPut(k, vkv.V)
			dvid.Infof("Writing key (%s), value (%s) into version %d...\n", string(vkv.K), string(vkv.V), v)
		}
	}
}

func TestMigrateInstance(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	var srcDB, dstDB, dstDB2 mockDB
	srcDB.name = "source"
	dstDB.name = "destination"
	dstDB2.name = "destination2"
	uuid1, curV := datastore.NewTestRepo()
	if curV != 1 {
		t.Fatalf("new version for test repo wasn't 0: %d\n", curV)
	}
	server.CreateTestInstance(t, uuid1, "keyvalue", "mykv", dvid.Config{})
	data, err := keyvalue.GetByUUIDName(uuid1, "mykv")
	if err != nil {
		t.Fatalf("unable to get created keyvalue instance: %v\n", err)
	}

	writeTestDataForVersion(&srcDB, data, curV) // version 1

	uuid2, curV := commitAndNewVersion(t, uuid1)
	writeTestDataForVersion(&srcDB, data, curV) // version 2

	uuid3, curV := commitAndNewVersion(t, uuid2)
	writeTestDataForVersion(&srcDB, data, curV) // version 3

	uuid4, curV := commitAndNewVersion(t, uuid3)
	writeTestDataForVersion(&srcDB, data, curV) // version 4

	uuid5, curV := commitAndNewVersion(t, uuid4)
	writeTestDataForVersion(&srcDB, data, curV) // version 5

	uuid6, curV := commitAndNewVersion(t, uuid5)
	writeTestDataForVersion(&srcDB, data, curV) // version 6

	uuid7, curV := newBranch(t, uuid5)
	writeTestDataForVersion(&srcDB, data, curV) // version 7 (new branch off 5)

	uuid8, curV := commitAndNewVersion(t, uuid6)
	writeTestDataForVersion(&srcDB, data, curV) // version 8

	uuid9, curV := commitAndNewVersion(t, uuid8)
	writeTestDataForVersion(&srcDB, data, curV) // version 9

	commitAndNewVersion(t, uuid9) // version 10

	cfg := dvid.NewConfig()
	uuids := string(uuid1[:8]) + "," + string(uuid4[:10]) + "," + string(uuid8[:9])
	cfg.Set("transmit", uuids)
	done := make(chan bool)
	if err := datastore.MigrateInstance(uuid1, data.DataName(), &srcDB, &dstDB, cfg, done); err != nil {
		t.Fatalf("error migrating instance: %v\n", err)
	}
	<-done
	if err := checkVersionedKV(&dstDB, expectedKV2); err != nil {
		t.Fatalf("error in expected: %v\n", err)
	}

	cfg = dvid.NewConfig()
	uuids = string(uuid2[:8]) + "," + string(uuid7[:10])
	cfg.Set("transmit", uuids)
	done = make(chan bool)
	if err := datastore.MigrateInstance(uuid1, data.DataName(), &srcDB, &dstDB2, cfg, done); err != nil {
		t.Fatalf("error migrating instance: %v\n", err)
	}
	<-done
	if err := checkVersionedKV(&dstDB2, expectedKV3); err != nil {
		t.Fatalf("error in expected: %v\n", err)
	}
}
