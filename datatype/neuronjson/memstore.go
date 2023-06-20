package neuronjson

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/keyvalue"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// memdbs is a map of all in-memory key-value stores for a given data instance.
// This data structure has to handle both branch HEAD dbs that will allow
// mutations and track the HEAD of the branch, as well as UUID dbs that are
// read-only.
type memdbs struct {
	static map[dvid.UUID]*memdb
	head   map[string]*memdb
	mu     sync.RWMutex
}

// in-memory neuron annotations with sorted body id list for optional sorted iteration.
type memdb struct {
	db     map[uint64]NeuronJSON
	ids    []uint64            // sorted list of body ids
	fields map[string]struct{} // list of all fields among the annotations
	dbMu   sync.RWMutex
}

// initializes the in-memory dbs for the given list of UUIDs + branch names in
// addition to the default HEAD of main/master branch.
func (d *Data) initMemoryDB(versions []string) error {
	dbs := &memdbs{
		static: make(map[dvid.UUID]*memdb),
		head:   make(map[string]*memdb),
	}
	versions = append(versions, ":main")
	for _, versionSpec := range versions {
		if strings.HasPrefix(versionSpec, ":") {
			branch := strings.TrimPrefix(versionSpec, ":")
			_, v, err := datastore.GetBranchHead(d.RootUUID(), branch)
			if err != nil {
				return fmt.Errorf("could not find branch %q specified for neuronjson %q in-memory db: %v",
					branch, d.DataName(), err)
			}
			newdb, err := newMemDB(d, v)
			if err != nil {
				return err
			}
			dbs.head[branch] = newdb
		} else {
			uuid, v, err := datastore.MatchingUUID(versionSpec)
			if err != nil {
				return err
			}
			newdb, err := newMemDB(d, v)
			if err != nil {
				return err
			}
			dbs.static[uuid] = newdb
		}
	}
	d.dbsMu.Lock()
	d.dbs = dbs
	d.dbsMu.Unlock()
	return nil
}

func newMemDB(d dvid.Data, v dvid.VersionID) (*memdb, error) {
	mdb := &memdb{
		db:     make(map[uint64]NeuronJSON),
		fields: make(map[string]struct{}),
		ids:    []uint64{},
	}

	ctx := datastore.NewVersionedCtx(d, v)

	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, fmt.Errorf("can't setup ordered keyvalue db for neuronjson %q: %v", d.DataName(), err)
	}

	tlog := dvid.NewTimeLog()
	numLoaded := 0
	err = db.ProcessRange(ctx, MinAnnotationTKey, MaxAnnotationTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil {
			return nil
		}
		kv := c.TKeyValue
		if kv.V == nil {
			return nil
		}
		key, err := DecodeTKey(kv.K)
		if err != nil {
			return err
		}

		bodyid, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return fmt.Errorf("received non-integer key %q during neuronjson load from database: %v", key, err)
		}

		var annotation NeuronJSON
		if err := json.Unmarshal(kv.V, &annotation); err != nil {
			return fmt.Errorf("unable to decode annotation for bodyid %d, skipping: %v", bodyid, err)
		}
		mdb.addAnnotation(bodyid, annotation)

		numLoaded++
		if numLoaded%1000 == 0 {
			tlog.Infof("Loaded %d annotations into neuronjson instance %q, version id %d",
				numLoaded, d.DataName(), v)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error loading neuron annotations into in-memory db for neuronjson %q, version id %d: %v",
			d.DataName(), v, err)
	}
	sort.Slice(mdb.ids, func(i, j int) bool { return mdb.ids[i] < mdb.ids[j] })
	tlog.Infof("Completed loading of %d annotations into neuronjson instance %q version %s in-memory db",
		numLoaded, d.DataName(), v)
	return mdb, nil
}

// add bodyid to sorted in-memory list of bodyids
func (mdb *memdb) addBodyID(bodyid uint64) {
	i := sort.Search(len(mdb.ids), func(i int) bool { return mdb.ids[i] >= bodyid })
	if i < len(mdb.ids) && mdb.ids[i] == bodyid {
		return
	}
	mdb.ids = append(mdb.ids, 0)
	copy(mdb.ids[i+1:], mdb.ids[i:])
	mdb.ids[i] = bodyid
}

// delete bodyid from sorted in-memory list of bodyids
func (mdb *memdb) deleteBodyID(bodyid uint64) {
	i := sort.Search(len(mdb.ids), func(i int) bool { return mdb.ids[i] == bodyid })
	if i == len(mdb.ids) {
		return
	}
	mdb.ids = append(mdb.ids[:i], mdb.ids[i+1:]...)
}

// add an annotation to the in-memory DB in batch mode assuming ids are sorted later
func (mdb *memdb) addAnnotation(bodyid uint64, annotation NeuronJSON) {
	mdb.db[bodyid] = annotation
	mdb.ids = append(mdb.ids, bodyid)
	for field := range annotation {
		mdb.fields[field] = struct{}{}
	}
}

// importKV imports a keyvalue instance into the neuronjson instance.
func (d *Data) importKV(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 5 {
		return fmt.Errorf("keyvalue instance name must be specified after importKV")
	}
	var uuidStr, dataName, cmdStr, kvName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &kvName)

	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	sourceKV, err := keyvalue.GetByUUIDName(uuid, dvid.InstanceName(kvName))
	if err != nil {
		return err
	}
	go d.loadFromKV(versionID, sourceKV)

	reply.Output = []byte(fmt.Sprintf("Started loading from keyvalue instance %q into neuronjson instance %q, uuid %s\n",
		kvName, d.DataName(), uuidStr))
	return nil
}

// kvType is an interface for keyvalue instances we wish to migrate to neuronjson.
type kvType interface {
	DataName() dvid.InstanceName
	StreamKV(v dvid.VersionID) (chan storage.KeyValue, error)
}

// goroutine-friendly ingest from a keyvalue instance
func (d *Data) loadFromKV(v dvid.VersionID, kvData kvType) {
	tlog := dvid.NewTimeLog()

	db, err := datastore.GetKeyValueDB(d)
	if err != nil {
		dvid.Criticalf("unable to get keyvalue database: %v", err)
		return
	}
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		dvid.Criticalf("unable to get UUID from version %d: %v", v, err)
		return
	}
	if d.dbs[uuid], err = newMemDB(d, v); err != nil {
		dvid.Criticalf("unable to initialize in-memory database: %v", err)
		return
	}

	ch, err := kvData.StreamKV(v)
	if err != nil {
		dvid.Errorf("Error in getting stream of data from keyvalue instance %q: %v\n", kvData.DataName(), err)
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	numLoaded := 0
	numFromKV := 0
	for kv := range ch {
		key := string(kv.K)
		numFromKV++

		// Handle metadata string keys
		switch key {
		case JSONSchema.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, JSONSchema); err != nil {
				dvid.Errorf("Unable to handle JSON schema metadata transfer, skipping: %v\n", err)
			}
			continue
		case NeuSchema.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, NeuSchema); err != nil {
				dvid.Errorf("Unable to handle neutu/neu3 schema metadata transfer, skipping: %v\n", err)
			}
			continue
		case NeuSchemaBatch.String():
			dvid.Infof("Transferring metadata %q from keyvalue instance %q to neuronjson instance %q",
				key, kvData.DataName(), d.DataName())
			if err := d.putMetadata(ctx, kv.V, NeuSchemaBatch); err != nil {
				dvid.Errorf("Unable to handle neutu/neu3 batch schema metadata transfer, skipping: %v\n", err)
			}
			continue
		}

		// Handle numeric keys for neuron annotations
		bodyid, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			dvid.Errorf("Received non-integer key %q during neuronjson load from keyvalue: ignored\n", key)
			continue
		}

		// a) Persist to storage first
		tk, err := NewTKey(key)
		if err != nil {
			dvid.Errorf("unable to encode neuronjson %q key %q, skipping: %v\n", d.DataName(), key, err)
			continue
		}
		if err := db.Put(ctx, tk, kv.V); err != nil {
			dvid.Errorf("unable to persist neuronjson %q key %s annotation, skipping: %v\n", d.DataName(), key, err)
			continue
		}

		// b) Add to in-memory annotations db
		var annotation NeuronJSON
		if err := json.Unmarshal(kv.V, &annotation); err != nil {
			dvid.Errorf("Unable to decode annotation for bodyid %d, skipping: %v\n", bodyid, err)
			continue
		}
		d.addAnnotation(bodyid, annotation)

		numLoaded++
		if numLoaded%1000 == 0 {
			tlog.Infof("Loaded %d annotations into neuronjson instance %q", numLoaded, d.DataName())
		}
	}
	sort.Slice(d.ids, func(i, j int) bool { return d.ids[i] < d.ids[j] })
	errored := numFromKV - numLoaded
	tlog.Infof("Completed loading of %d annotations into neuronjson instance %q (%d skipped)",
		numLoaded, d.DataName(), errored)
}
