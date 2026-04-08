//go:build badger
// +build badger

package badger_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	badgerstore "github.com/janelia-flyem/dvid/storage/badger"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type testData struct {
	rootUUID   dvid.UUID
	name       dvid.InstanceName
	instanceID dvid.InstanceID
	kvStore    dvid.Store
	logStore   dvid.Store
	mutID      uint64
	deleted    bool
}

func (d *testData) InstanceID() dvid.InstanceID { return d.instanceID }
func (d *testData) DataUUID() dvid.UUID         { return dvid.UUID(fmt.Sprintf("data-%d", d.instanceID)) }
func (d *testData) DataName() dvid.InstanceName { return d.name }
func (d *testData) RootUUID() dvid.UUID         { return d.rootUUID }
func (d *testData) RootVersionID() (dvid.VersionID, error) {
	return datastore.VersionFromUUID(d.rootUUID)
}
func (d *testData) DAGRootUUID() (dvid.UUID, error)  { return d.rootUUID, nil }
func (d *testData) TypeName() dvid.TypeString        { return "testType" }
func (d *testData) TypeURL() dvid.URLString          { return "test://badger" }
func (d *testData) TypeVersion() string              { return "1.0" }
func (d *testData) Tags() map[string]string          { return nil }
func (d *testData) Versioned() bool                  { return true }
func (d *testData) NewMutationID() uint64            { return atomic.AddUint64(&d.mutID, 1) }
func (d *testData) KVStore() (dvid.Store, error)     { return d.kvStore, nil }
func (d *testData) SetKVStore(store dvid.Store)      { d.kvStore = store }
func (d *testData) SetLogStore(store dvid.Store)     { d.logStore = store }
func (d *testData) SetInstanceID(id dvid.InstanceID) { d.instanceID = id }
func (d *testData) SetDataUUID(uuid dvid.UUID)       {}
func (d *testData) SetName(name dvid.InstanceName)   { d.name = name }
func (d *testData) SetRootUUID(uuid dvid.UUID)       { d.rootUUID = uuid }
func (d *testData) SetSync(syncs dvid.UUIDSet)       {}
func (d *testData) SetTags(tags map[string]string)   {}
func (d *testData) PersistMetadata() error           { return nil }
func (d *testData) IsDeleted() bool                  { return d.deleted }
func (d *testData) SetDeleted(deleted bool)          { d.deleted = deleted }

func openTempBadgerDB(tb testing.TB) *badgerstore.BadgerDB {
	tb.Helper()
	path := tb.TempDir()
	var c dvid.Config
	c.SetAll(map[string]interface{}{"path": path})
	store, _, err := (badgerstore.Engine{}).NewStore(dvid.StoreConfig{Config: c, Engine: "badger"})
	if err != nil {
		tb.Fatalf("open temp badger: %v", err)
	}
	db, ok := store.(*badgerstore.BadgerDB)
	if !ok {
		tb.Fatalf("unexpected store type %T", store)
	}
	tb.Cleanup(func() {
		db.Close()
	})
	return db
}

func makeLinearVersionedCtxs(tb testing.TB, store dvid.Store, n int) ([]*datastore.VersionedCtx, *testData) {
	tb.Helper()
	datastore.OpenTest()
	tb.Cleanup(datastore.CloseTest)
	if n < 1 {
		tb.Fatalf("need at least one version, got %d", n)
	}

	rootUUID, _ := datastore.NewTestRepo()
	if err := datastore.Commit(rootUUID, "root", nil); err != nil {
		tb.Fatalf("commit root: %v", err)
	}
	rootV, err := datastore.VersionFromUUID(rootUUID)
	if err != nil {
		tb.Fatalf("root version: %v", err)
	}
	data := &testData{
		rootUUID:   rootUUID,
		name:       "badger-test",
		instanceID: 101,
		kvStore:    store,
	}
	ctxs := []*datastore.VersionedCtx{datastore.NewVersionedCtx(data, rootV)}
	parentUUID := rootUUID
	for i := 1; i < n; i++ {
		childUUID, err := datastore.NewVersion(parentUUID, fmt.Sprintf("child-%d", i), "", nil)
		if err != nil {
			tb.Fatalf("new version %d: %v", i, err)
		}
		childV, err := datastore.VersionFromUUID(childUUID)
		if err != nil {
			tb.Fatalf("child version %d: %v", i, err)
		}
		ctxs = append(ctxs, datastore.NewVersionedCtx(data, childV))
		if i != n-1 {
			if err := datastore.Commit(childUUID, fmt.Sprintf("child-%d", i), nil); err != nil {
				tb.Fatalf("commit child %d: %v", i, err)
			}
		}
		parentUUID = childUUID
	}
	return ctxs, data
}

func makeVersionedCtx(tb testing.TB, store dvid.Store) (*datastore.VersionedCtx, *datastore.VersionedCtx, *testData) {
	tb.Helper()
	ctxs, data := makeLinearVersionedCtxs(tb, store, 2)
	return ctxs[0], ctxs[1], data
}

func benchIntEnv(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func makeBenchTKey(i int) storage.TKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return storage.NewTKey(0x7A, buf)
}

func collectLegacyVersionedRange(db *badgerstore.BadgerDB, vctx storage.VersionedCtx, begTKey, endTKey storage.TKey) ([]storage.TKeyValue, int, int64, error) {
	minKey, err := vctx.MinVersionKey(begTKey)
	if err != nil {
		return nil, 0, 0, err
	}
	maxKey, err := vctx.MaxVersionKey(endTKey)
	if err != nil {
		return nil, 0, 0, err
	}
	maxVersionKey, err := vctx.MaxVersionKey(begTKey)
	if err != nil {
		return nil, 0, 0, err
	}

	ch := make(chan *storage.KeyValue, 64)
	cancel := make(chan struct{})
	defer close(cancel)
	go func() {
		_ = db.RawRangeQuery(minKey, maxKey, false, ch, cancel)
	}()

	var out []storage.TKeyValue
	var scanned int
	var bytesRead int64
	values := make([]*storage.KeyValue, 0, 8)
	flush := func() error {
		if len(values) == 0 {
			return nil
		}
		kv, err := vctx.VersionedKeyValue(values)
		if err != nil {
			return err
		}
		if kv != nil {
			tk, err := storage.TKeyFromKey(kv.K)
			if err != nil {
				return err
			}
			out = append(out, storage.TKeyValue{K: tk, V: kv.V})
		}
		values = values[:0]
		return nil
	}

	for {
		kv := <-ch
		if kv == nil {
			break
		}
		scanned++
		bytesRead += int64(len(kv.V))
		if bytes.Compare(kv.K, maxVersionKey) > 0 {
			if storage.Key(kv.K).IsDataKey() {
				indexBytes, err := storage.TKeyFromKey(kv.K)
				if err != nil {
					return nil, 0, 0, err
				}
				maxVersionKey, err = vctx.MaxVersionKey(indexBytes)
				if err != nil {
					return nil, 0, 0, err
				}
			}
			if err := flush(); err != nil {
				return nil, 0, 0, err
			}
		}
		values = append(values, kv)
	}
	if err := flush(); err != nil {
		return nil, 0, 0, err
	}
	return out, scanned, bytesRead, nil
}

func collectOptimizedProcessRange(db *badgerstore.BadgerDB, ctx storage.Context, begTKey, endTKey storage.TKey) ([]storage.TKeyValue, int64, error) {
	return collectProcessRangeForStrategy(db, ctx, begTKey, endTKey, badgerstore.VersionedReadOptimized)
}

func collectPipelinedProcessRange(db *badgerstore.BadgerDB, ctx storage.Context, begTKey, endTKey storage.TKey) ([]storage.TKeyValue, int64, error) {
	return collectProcessRangeForStrategy(db, ctx, begTKey, endTKey, badgerstore.VersionedReadPipelined)
}

func collectProcessRangeForStrategy(db *badgerstore.BadgerDB, ctx storage.Context, begTKey, endTKey storage.TKey, strategy badgerstore.VersionedReadStrategy) ([]storage.TKeyValue, int64, error) {
	var out []storage.TKeyValue
	var bytesRead int64
	err := db.ProcessRangeWithStrategy(ctx, begTKey, endTKey, strategy, nil, func(c *storage.Chunk) error {
		bytesRead += int64(len(c.V))
		out = append(out, storage.TKeyValue{
			K: append(storage.TKey(nil), c.K...),
			V: append([]byte(nil), c.V...),
		})
		return nil
	})
	return out, bytesRead, err
}

func TestVersionedRangeMatchesLegacySelection(t *testing.T) {
	db := openTempBadgerDB(t)
	rootCtx, childCtx, _ := makeVersionedCtx(t, db)

	key1 := makeBenchTKey(1)
	key2 := makeBenchTKey(2)
	key3 := makeBenchTKey(3)
	if err := db.Put(rootCtx, key1, []byte("root-only")); err != nil {
		t.Fatalf("put root key1: %v", err)
	}
	if err := db.Put(rootCtx, key2, []byte("root-key2")); err != nil {
		t.Fatalf("put root key2: %v", err)
	}
	if err := db.Put(childCtx, key2, []byte("child-key2")); err != nil {
		t.Fatalf("put child key2: %v", err)
	}
	if err := db.Put(rootCtx, key3, []byte("root-key3")); err != nil {
		t.Fatalf("put root key3: %v", err)
	}
	if err := db.Delete(childCtx, key3); err != nil {
		t.Fatalf("delete child key3: %v", err)
	}

	got1, err := db.Get(childCtx, key1)
	if err != nil || string(got1) != "root-only" {
		t.Fatalf("unexpected inherited get: %q, %v", got1, err)
	}
	got2, err := db.Get(childCtx, key2)
	if err != nil || string(got2) != "child-key2" {
		t.Fatalf("unexpected overridden get: %q, %v", got2, err)
	}
	got3, err := db.Get(childCtx, key3)
	if err != nil || got3 != nil {
		t.Fatalf("unexpected tombstoned get: %q, %v", got3, err)
	}

	exists3, err := db.Exists(childCtx, key3)
	if err != nil || exists3 {
		t.Fatalf("unexpected exists for tombstoned key: %t, %v", exists3, err)
	}
	version2, err := db.GetVersion(childCtx, key2)
	if err != nil || version2 != childCtx.VersionID() {
		t.Fatalf("unexpected winning version: %d, %v", version2, err)
	}

	legacy, _, _, err := collectLegacyVersionedRange(db, childCtx, key1, key3)
	if err != nil {
		t.Fatalf("legacy range: %v", err)
	}
	optimized, _, err := collectOptimizedProcessRange(db, childCtx, key1, key3)
	if err != nil {
		t.Fatalf("optimized range: %v", err)
	}
	pipelined, _, err := collectPipelinedProcessRange(db, childCtx, key1, key3)
	if err != nil {
		t.Fatalf("pipelined range: %v", err)
	}
	if len(legacy) != len(optimized) {
		t.Fatalf("legacy and optimized range lengths differ: %d vs %d", len(legacy), len(optimized))
	}
	for i := range legacy {
		if !bytes.Equal(legacy[i].K, optimized[i].K) || !bytes.Equal(legacy[i].V, optimized[i].V) {
			t.Fatalf("legacy and optimized range results differ at %d", i)
		}
	}
	if len(legacy) != len(pipelined) {
		t.Fatalf("legacy and pipelined range lengths differ: %d vs %d", len(legacy), len(pipelined))
	}
	for i := range legacy {
		if !bytes.Equal(legacy[i].K, pipelined[i].K) || !bytes.Equal(legacy[i].V, pipelined[i].V) {
			t.Fatalf("legacy and pipelined range results differ at %d", i)
		}
	}
}

func benchmarkVersionedRange(b *testing.B, strategy badgerstore.VersionedReadStrategy) {
	db := openTempBadgerDB(b)
	versionsPerKey := benchIntEnv("DVID_BADGER_BENCH_VERSIONS", 4)
	ctxs, _ := makeLinearVersionedCtxs(b, db, versionsPerKey)
	rootCtx := ctxs[0]
	leafCtx := ctxs[len(ctxs)-1]

	numKeys := benchIntEnv("DVID_BADGER_BENCH_KEYS", 20000)
	valueSize := benchIntEnv("DVID_BADGER_BENCH_VALUE_SIZE", 8192)
	value := bytes.Repeat([]byte{0xAB}, valueSize)

	for i := 0; i < numKeys; i++ {
		tk := makeBenchTKey(i)
		if err := db.Put(rootCtx, tk, value); err != nil {
			b.Fatalf("put root: %v", err)
		}
		for v := 1; v < len(ctxs); v++ {
			if v%2 == 1 {
				if err := db.Put(ctxs[v], tk, value); err != nil {
					b.Fatalf("put version %d: %v", v, err)
				}
			}
		}
	}

	beg := makeBenchTKey(0)
	end := makeBenchTKey(numKeys - 1)
	scanned := numKeys * len(ctxs)
	var bytesRead int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if strategy == badgerstore.VersionedReadLegacy {
			_, _, bytesRead, err = collectLegacyVersionedRange(db, leafCtx, beg, end)
		} else {
			_, bytesRead, err = collectProcessRangeForStrategy(db, leafCtx, beg, end, strategy)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(scanned), "version_keys/op")
	b.ReportMetric(float64(bytesRead), "value_bytes/op")
}

func BenchmarkVersionedRangeBaseline(b *testing.B) {
	benchmarkVersionedRange(b, badgerstore.VersionedReadLegacy)
}

func BenchmarkVersionedRangeOptimized(b *testing.B) {
	benchmarkVersionedRange(b, badgerstore.VersionedReadOptimized)
}

func BenchmarkVersionedRangePipelined(b *testing.B) {
	benchmarkVersionedRange(b, badgerstore.VersionedReadPipelined)
}
