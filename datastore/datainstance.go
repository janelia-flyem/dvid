/*
	This file contains code supporting data instances of a DVID datatype.
*/

package datastore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func init() {
	gob.Register(&Data{})
}

// ------------------------
// TODO -- Deprecate RPC commands to datatypes.  All commands should be via HTTP.

// Request supports RPC requests to the DVID server.
type Request struct {
	dvid.Command
	Input []byte
}

// Response supports RPC responses from DVID.
type Response struct {
	dvid.Response
	Output []byte
}

// Writes a RPC response to a writer.
func (r *Response) Write(w io.Writer) error {
	if len(r.Response.Text) != 0 {
		fmt.Fprintf(w, r.Response.Text)
		return nil
	} else if len(r.Output) != 0 {
		_, err := w.Write(r.Output)
		if err != nil {
			return err
		}
	}
	return nil
}

// ------------------------

// VersionedCtx implements storage.VersionedCtx for data instances that
// have a version DAG.
type VersionedCtx struct {
	*storage.DataContext
}

func NewVersionedCtx(data dvid.Data, versionID dvid.VersionID) *VersionedCtx {
	return &VersionedCtx{storage.NewDataContext(data, versionID)}
}

// VersionedKeyValue returns the key-value pair corresponding to this key's version
// given a list of key-value pairs across many versions.  If no suitable key-value
// pair is found or a tombstone is encounterd closest to version, nil is returned.
func (vctx *VersionedCtx) VersionedKeyValue(values []*storage.KeyValue) (*storage.KeyValue, error) {
	// Set up a map[VersionID]KeyValue
	versionMap := make(kvVersions, len(values))
	for _, kv := range values {
		vid, err := vctx.VersionFromKey(kv.K)
		if err != nil {
			return nil, err
		}
		versionMap[vid] = kvvNode{kv: kv}
		// fmt.Printf("Found version %d for %s\n", vid, ctx)
	}

	// Get the correct key-value for this version among all ancestors, some of which might have
	// a value.
	kv, _, err := versionMap.FindMatch(vctx.VersionID())
	return kv, err
}

// Head checks whether this the open head of the master branch
func (vctx *VersionedCtx) Head() bool {

	rootversion, _ := UUIDFromVersion(vctx.VersionID())
	r, _ := manager.repos[rootversion]
	node, _ := r.dag.nodes[vctx.VersionID()]

	if node.locked {
		return false
	}
	if node.branch != "" {
		// requires branching info in new DVID
		return false
	}
	return true
}

// Head checks whether specified version is on the  master branch
func (vctx *VersionedCtx) MasterVersion(version dvid.VersionID) bool {

	rootversion, _ := UUIDFromVersion(version)
	r, _ := manager.repos[rootversion]
	node, _ := r.dag.nodes[version]

	if node.branch != "" {
		// requires branching info in new DVID
		return false
	}
	return true
}

// NumVersions returns the number of versions for a given
func (vctx *VersionedCtx) NumVersions() int32 {
	rootversion, _ := UUIDFromVersion(vctx.VersionID())
	r, _ := manager.repos[rootversion]
	return int32(len(r.dag.nodes))
}

// RepoRoot returns the root uuid.
func (vctx *VersionedCtx) RepoRoot() (dvid.UUID, error) {
	rootversion, _ := UUIDFromVersion(vctx.VersionID())
	return GetRepoRoot(rootversion)
}

func (vctx *VersionedCtx) Versioned() bool {
	return true
}

func (vctx *VersionedCtx) String() string {
	return fmt.Sprintf("Versioned data context for %q (local id %d, version id %d)", vctx.DataName(),
		vctx.InstanceID(), vctx.VersionID())
}

// GetKeyValueDB returns a key-value store associated with this context on an
// error if one is unavailable.
func (vctx *VersionedCtx) GetKeyValueDB() (storage.KeyValueDB, error) {
	d := vctx.DataContext.Data()
	if d == nil {
		return nil, fmt.Errorf("invalid data %v in GetKeyValueDB", d)
	}
	return GetKeyValueDB(d)
}

// GetOrderedKeyValueDB returns an ordered key-value store associated with this
// context or an error if one is unavailable.
func (vctx *VersionedCtx) GetOrderedKeyValueDB() (storage.OrderedKeyValueDB, error) {
	d := vctx.DataContext.Data()
	if d == nil {
		return nil, fmt.Errorf("invalid data %v in GetOrderedKeyValueDB", d)
	}
	return GetOrderedKeyValueDB(d)
}

// GetGraphDB returns a graph store associated with this context or an error
// if one is not available.
func (vctx *VersionedCtx) GetGraphDB() (storage.GraphDB, error) {
	d := vctx.DataContext.Data()
	if d == nil {
		return nil, fmt.Errorf("invalid data %v in GetGraphDB", d)
	}
	return GetGraphDB(d)
}

// DataService is an interface for operations on an instance of a supported datatype.
type DataService interface {
	dvid.Data
	json.Marshaler
	BlobService

	Help() string
	GetType() TypeService

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(dvid.Config) error

	// IsMutationRequest returns true if the given HTTP method on the endpoint results in mutations.
	// This allows datatypes to define actions on an endpoint as immutable even if they use POST.
	// The endpoint is the keyword immediately following a data instance name in the URL:
	//    /api/node/483f/grayscale/raw/xy/...
	// In the above URL, "raw" is the endpoint.
	IsMutationRequest(action, endpoint string) bool

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(Request, *Response) error

	// ServeHTTP handles HTTP requests in the context of a particular version and
	// returns activity information that can be logged for monitoring.
	ServeHTTP(dvid.UUID, *VersionedCtx, http.ResponseWriter, *http.Request) (activity map[string]interface{})

	// DescribeTKeyClass explains in a string what a particular TKeyClass
	// is used for.  For example, one class of TKey for the label data types is the block-indexed
	// voxel values.
	DescribeTKeyClass(storage.TKeyClass) string

	// PushData handles DVID-to-DVID transmission of key-value pairs, optionally
	// delimited by type-specific filter specifications (e.g., "roi:roiname,uuid")
	// and a set of versions.  DataService implementations can automatically embed
	// this via datastore.Data or can add filters by providing their own PushData(),
	// Filterer, and Filter implementations.  (datatype/imageblk/distributed.go)
	PushData(*PushSession) error
}

// TypeMigrator is an interface for a DataService that can migrate itself to another DataService.
// A deprecated DataService implementation can implement this interface to auto-convert on metadata load.
type TypeMigrator interface {
	MigrateData([]dvid.VersionID) (DataService, error)
}

// TypeUpgrader is an interface for a DataService that can upgrade itself to another version of data storage.
// A deprecated DataService implementation can implement this interface to auto-convert on metadata load.
type TypeUpgrader interface {
	DataService
	UpgradeData() (upgraded bool, err error)
}

// InstanceMutator provides a hook for data instances to load mutable data
// on startup.  It is assumed that the data instances store data whenever
// its data mutates, e.g., extents for labelblk or max label for labelvol.
type InstanceMutator interface {
	// Loads all mutable properties and applies any necessary migration to
	// transform the internal data from the stored to expected version.
	LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (saveNeeded bool, err error)
}

// MutationMutexer is an interface for mutexes on particular mutation IDs.
type MutationMutexer interface {
	MutAdd(mutID uint64) (newOp bool)
	MutDone(mutID uint64)
	MutWait(mutID uint64)
	MutDelete(mutID uint64)
}

// VersionInitializer provides a hook for data instances to receive branch (new version)
// events and modify their properties as needed.
type VersionInitializer interface {
	InitVersion(dvid.UUID, dvid.VersionID) error
}

// DataInitializer is a data instance that needs to be initialized, e.g., start
// long-lived goroutines that handle data syncs, etc.  Initialization should only
// constitute supporting data and goroutines and not change the data itself like
// InstanceMutator, so no saveNeeded flag needs to be returned.
type DataInitializer interface {
	InitDataHandlers() error
}

// VersionRemapper provides a hook for data instances to remap properties
// that depend on server-specific version ids.  During DVID-to-DVID transfer
// of data, these version ids need to be remapped as part of the push/pull.
type VersionRemapper interface {
	RemapVersions(dvid.VersionMap) error
}

// PropertyCopier are types that can copy data instance properties from another (typically identically typed)
// data instance with an optional filter.  This is used to create copies of data instances locally or
// when pushing to a remote DVID.
type PropertyCopier interface {
	CopyPropertiesFrom(DataService, storage.FilterSpec) error
}

// DataShutdownTime is the maximum number of seconds a data instance can delay when terminating
// goroutines during Shutdown.
const DataShutdownTime = 20

// Shutdowner is a data instance that has a function to call during shutdown.
// Typically, this exits goroutines used for background data processing.
type Shutdowner interface {
	Shutdown(wg *sync.WaitGroup) // Expects wg.Done() to be called in Shutdown function.
}

// Initializer is a data instance that can be initialized.
// Typically this sets up any goroutines necessary after server configuration, etc.
type Initializer interface {
	Initialize()
}

type Updater struct {
	updates uint32
	sync.RWMutex
}

func (u *Updater) StartUpdate() {
	u.Lock()
	u.updates++
	u.Unlock()
}

func (u *Updater) StopUpdate() {
	u.Lock()
	u.updates--
	if u.updates < 0 {
		dvid.Criticalf("StopUpdate() called more than StartUpdate().  Issue with data instance code.\n")
	}
	u.Unlock()
}

// Returns true if the data is currently being updated.
func (u *Updater) Updating() bool {
	u.RLock()
	updating := u.updates > 0
	u.RUnlock()
	return updating
}

type dataUpdater interface {
	Updating() bool
}

// MutationDumper is a dataservice that suppports the flatten-mutations command via
// a DumpMutations() function.
type MutationDumper interface {
	DumpMutations(versionUUID dvid.UUID, filename string) (comment string, err error)
}

// BlockOnUpdating blocks until the given data is not updating from syncs or has events
// waiting in sync channels.  Primarily used during testing.
func BlockOnUpdating(uuid dvid.UUID, name dvid.InstanceName) error {
	time.Sleep(100 * time.Millisecond)
	d, err := GetDataByUUIDName(uuid, name)
	if err != nil {
		return err
	}

	syncer, isSyncer := d.(Syncer)
	updater, isUpdater := d.(dataUpdater)

	for (isSyncer && syncer.SyncPending()) || (isUpdater && updater.Updating()) {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// PropertyTKeyClass is the TKeyClass reserved for data instance properties, and should
// be reserved in each data type implementation.
const PropertyTKeyClass = 1

// Data is the base struct of repo-specific data instances.  It should be embedded
// in a datatype's DataService implementation and handle datastore-wide key partitioning.
type Data struct {
	typename    dvid.TypeString
	typeurl     dvid.URLString
	typeversion string

	tags map[string]string

	id dvid.InstanceID // local data instance id

	dataUUID dvid.UUID // Unique id for this data instance.

	name     dvid.InstanceName // name, which can be changed.
	rootUUID dvid.UUID         // Root uuid of sub-DAG for this data instance, which can be changed on flattening.

	// Compression of serialized data, e.g., the value in a key-value.
	compression dvid.Compression

	// Checksum approach for serialized data.
	checksum dvid.Checksum

	// a list of the instances to which this data should be synced
	syncNames []dvid.InstanceName // deprecated but used for legacy serialization
	syncData  dvid.UUIDSet        // data UUIDs of syncs.

	// unversioned = true if all UUIDs should be mapped to the root UUID.
	// Only one version exists for an entire repo, so it's repo-wide.
	// Requires the data type to actually check if versioned and handle
	// UUIDs differently.  (See keyvalue type.)
	unversioned bool

	// the assigned backend kv and log store for a data instance.  If nil, we
	// will use the default store.
	kvStore  dvid.Store // key-value store
	logStore dvid.Store // append-only log

	// true if deleted (or in processing of deleting)
	deleted bool

	// handle waiting based on operation ID.
	opWG    map[uint64]*sync.WaitGroup
	opWG_mu sync.RWMutex
}

// IsDeleted returns true if data has been deleted or is deleting.
func (d *Data) IsDeleted() bool {
	return d.deleted
}

// SetDeleted is used to indicate whether data has been deleted or is deleting.
func (d *Data) SetDeleted(deleted bool) {
	d.deleted = deleted
}

// ---------------------------------------

func (d *Data) MarshalJSON() ([]byte, error) {
	// convert sync UUIDs to names so its more understandable.
	syncs := []dvid.InstanceName{}
	for uuid := range d.syncData {
		synced, err := GetDataByDataUUID(uuid)
		if err != nil {
			syncs = append(syncs, "undefined")
			dvid.Errorf("In data %q found synced data UUID %q that cannot be found: %v\n", d.DataName(), uuid, err)
		} else {
			syncs = append(syncs, synced.DataName())
		}
	}
	var kvStore, logStore string
	if d.kvStore != nil {
		kvStore = d.kvStore.String()
	} else {
		kvStore = "no key-value store set"
	}
	if d.logStore != nil {
		logStore = d.logStore.String()
	} else {
		logStore = "no mutation log set"
	}
	return json.Marshal(struct {
		TypeName    dvid.TypeString
		TypeURL     dvid.URLString
		TypeVersion string
		DataUUID    dvid.UUID
		Name        dvid.InstanceName
		RepoUUID    dvid.UUID
		Compression string
		Checksum    string
		Syncs       []dvid.InstanceName
		Versioned   bool
		KVStore     string
		LogStore    string
		Tags        map[string]string
	}{
		TypeName:    d.typename,
		TypeURL:     d.typeurl,
		TypeVersion: d.typeversion,
		DataUUID:    d.dataUUID,
		Name:        d.name,
		RepoUUID:    d.rootUUID,
		Compression: d.compression.String(),
		Checksum:    d.checksum.String(),
		Syncs:       syncs,
		Versioned:   !d.unversioned,
		KVStore:     kvStore,
		LogStore:    logStore,
		Tags:        d.tags,
	})
}

var reservedNames = map[string]struct{}{
	"log":        struct{}{},
	"branch":     struct{}{},
	"note":       struct{}{},
	"commit":     struct{}{},
	"newversion": struct{}{},
}

// NewDataService returns a new Data instance that fulfills the DataService interface.
// The UUID passed in corresponds to the root UUID of the DAG subgraph that should hold the data.
// This returned Data struct is usually embedded by datatype-specific data instances.
// By default, LZ4 and the default checksum is used.
func NewDataService(t TypeService, rootUUID dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	if _, reserved := reservedNames[string(name)]; reserved {
		return nil, fmt.Errorf("cannot use reserved name %q", name)
	}

	// // Don't allow identical names to be used in the same repo.
	// d, err := GetDataByUUIDName(rootUUID, name)
	// if err == nil && d != nil {
	// 	return nil, fmt.Errorf("cannot create data instance %q when one already exists in repo with UUID %s", name, rootUUID)
	// }

	// Make sure we generate a valid UUID for the data instance.
	dataUUID := dvid.NewUUID()
	if dataUUID == dvid.NilUUID {
		return nil, fmt.Errorf("Unable to generate new UUID for data %q creation", name)
	}

	// Setup the basic data instance structure.
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &Data{
		typename:    t.GetTypeName(),
		typeurl:     t.GetTypeURL(),
		typeversion: t.GetTypeVersion(),
		dataUUID:    dataUUID,
		id:          id,
		name:        name,
		rootUUID:    rootUUID,
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		syncNames:   []dvid.InstanceName{},
		syncData:    dvid.UUIDSet{},
		unversioned: false,
	}
	if err := data.ModifyConfig(c); err != nil {
		return nil, err
	}

	// Cache assigned store and/or log.
	var err error
	data.kvStore, err = storage.GetAssignedStore(name, rootUUID, data.tags, t.GetTypeName())
	if err != nil {
		return nil, err
	}
	data.logStore, err = storage.GetAssignedLog(name, rootUUID, data.tags, t.GetTypeName())
	if err != nil {
		return nil, err
	}

	return data, nil
}

// ---- dvid.Data implementation ----

func (d *Data) InstanceID() dvid.InstanceID { return d.id }

func (d *Data) DataName() dvid.InstanceName { return d.name }

func (d *Data) DataUUID() dvid.UUID { return d.dataUUID }

func (d *Data) RootUUID() dvid.UUID { return d.rootUUID }

func (d *Data) RootVersionID() (dvid.VersionID, error) { return VersionFromUUID(d.rootUUID) }

func (d *Data) DAGRootUUID() (dvid.UUID, error) {
	return GetRepoRoot(d.rootUUID)
}

func (d *Data) TypeName() dvid.TypeString { return d.typename }

func (d *Data) TypeURL() dvid.URLString { return d.typeurl }

func (d *Data) TypeVersion() string { return d.typeversion }

func (d *Data) Tags() map[string]string { return d.tags }

func (d *Data) Versioned() bool { return !d.unversioned }

func (d *Data) KVStore() (dvid.Store, error) {
	if d.kvStore == nil {
		return storage.DefaultKVStore()
	}
	return d.kvStore, nil
}

func (d *Data) GetWriteLog() storage.WriteLog {
	var err error
	var s dvid.Store
	if d.logStore == nil {
		s, err = storage.DefaultLogStore()
		if err != nil {
			return nil
		}
	} else {
		s = d.logStore
	}
	wl, ok := s.(storage.WriteLog)
	if ok {
		return wl
	}
	return nil
}

func (d *Data) GetReadLog() storage.ReadLog {
	var err error
	var s dvid.Store
	if d.logStore == nil {
		s, err = storage.DefaultLogStore()
		if err != nil {
			return nil
		}
	} else {
		s = d.logStore
	}
	rl, ok := s.(storage.ReadLog)
	if ok {
		return rl
	}
	return nil
}

func (d *Data) NewMutationID() uint64 {
	if manager == nil {
		dvid.Criticalf("New mutation ID requested for data %q but manager not initialized!\n", d.DataName())
		return 0
	}
	repo, err := manager.repoFromUUID(d.RootUUID())
	if err != nil {
		dvid.Criticalf("New mutation ID requested for data %q but no repo associated with root %s\n", d.DataName(), d.RootUUID())
		return 0
	}
	return repo.newMutationID()
}

// ---- dvid.DataSetter implementation ----

func (d *Data) SetInstanceID(id dvid.InstanceID) {
	d.id = id
}

func (d *Data) SetDataUUID(uuid dvid.UUID) {
	d.dataUUID = uuid
}

func (d *Data) SetRootUUID(uuid dvid.UUID) {
	d.rootUUID = uuid
}

func (d *Data) SetName(name dvid.InstanceName) {
	d.name = name
}

// SetSync sets the list of synced UUIDs for this data instance.  It does not modify
// the sync graph.
func (d *Data) SetSync(syncs dvid.UUIDSet) {
	d.syncData = syncs
	d.syncNames = nil
}

func (d *Data) SetKVStore(kvStore dvid.Store) {
	d.kvStore = kvStore
}

func (d *Data) SetLogStore(logStore storage.WriteLog) {
	d.logStore = logStore
}

// ---------------

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.typename)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.typeurl)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.typeversion)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.name)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.id)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.rootUUID)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.compression)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.checksum)); err != nil {
		return err
	}

	// legacy: we load but in future will be removed.
	if err := dec.Decode(&(d.syncNames)); err != nil {
		return err
	}

	if err := dec.Decode(&(d.unversioned)); err != nil {
		dvid.Infof("Data %q had no explicit versioning flag: assume it's versioned.\n", d.name)
	}
	if err := dec.Decode(&(d.dataUUID)); err != nil {
		dvid.Infof("Data %q had no data UUID.\n", d.name)
	}
	if err := dec.Decode(&(d.syncData)); err != nil {
		if len(d.syncNames) != 0 {
			dvid.Infof("Data %q has legacy sync names, will convert to data UUIDs...\n", d.name)
		}
	}
	if err := dec.Decode(&(d.tags)); err != nil {
		dvid.Infof("Serialization of data %q had no tags.  Skipping reading of tags.\n", d.name)
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.typename); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.typeurl); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.typeversion); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.name); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.id); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.rootUUID); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.compression); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.checksum); err != nil {
		return nil, err
	}
	oldsyncs := []dvid.InstanceName{}
	if err := enc.Encode(oldsyncs); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.unversioned); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.dataUUID); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.syncData); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.tags); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Equals returns true if the two data instances are identical, not just referring
// to the same data instance (e.g., different names but same data UUID, etc).
func (d *Data) Equals(d2 *Data) bool {
	if d.typename != d2.typename ||
		d.typeurl != d2.typeurl ||
		d.typeversion != d2.typeversion ||
		d.dataUUID != d2.dataUUID ||
		d.id != d2.id ||
		d.name != d2.name ||
		d.rootUUID != d2.rootUUID ||
		d.compression != d2.compression ||
		d.checksum != d2.checksum ||
		len(d.tags) != len(d2.tags) ||
		!d.syncData.Equals(d2.syncData) {
		return false
	}
	return true
}

// -----------

func (d *Data) Compression() dvid.Compression {
	return d.compression
}

func (d *Data) Checksum() dvid.Checksum {
	return d.checksum
}

// --- DataService implementation -----

// IsMutationRequest is the default definition of mutation requests.
// All POST, PUT, and DELETE actions return true, while others return false.
func (d *Data) IsMutationRequest(action, endpoint string) bool {
	lc := strings.ToLower(action)
	switch lc {
	case "post", "put", "delete":
		return true
	default:
		return false
	}
}

func (d *Data) GetType() TypeService {
	typeservice, err := TypeServiceByURL(d.typeurl)
	if err != nil {
		dvid.Errorf("Data %q: %v\n", d.name, err)
	}
	return typeservice
}

func (d *Data) ModifyConfig(config dvid.Config) error {
	// Set compression for this instance
	s, found, err := config.GetString("Compression")
	if err != nil {
		return err
	}
	if found {
		format := strings.ToLower(s)
		switch format {
		case "none":
			d.compression, _ = dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
		case "snappy":
			d.compression, _ = dvid.NewCompression(dvid.Snappy, dvid.DefaultCompression)
		case "lz4":
			d.compression, _ = dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
		case "gzip":
			d.compression, _ = dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
		case "jpeg":
			// Jpeg should only be used on datatypes with a BlockSize property
			// and should only be used on uint8blk dim1 < 256 -- not enforced

			// default dim1 setting
			firstdim := 32

			blockstr, found, err := config.GetString("BlockSize")
			if err != nil {
				return err
			}
			if found {
				// extract the first block dimension size
				bparts := strings.Split(blockstr, ",")
				firstdim, err = strconv.Atoi(bparts[0])
				if err != nil {
					return fmt.Errorf("unable to parse first block dim (%q)", bparts[0])
				}
				if firstdim <= 0 {
					return fmt.Errorf("Invalid blocksize dim for jpeg compression")
				}
			}
			// all data stored must be divisible by firstdim -- which will be the case for block datatypes
			d.compression, _ = dvid.NewCompression(dvid.JPEG, dvid.CompressionLevel(firstdim))
		default:
			// Check for gzip + compression level
			parts := strings.Split(format, ":")
			if len(parts) == 2 && parts[0] == "gzip" {
				level, err := strconv.Atoi(parts[1])
				if err != nil {
					return fmt.Errorf("unable to parse gzip compression level (%q).  Should be 'gzip:<level>'", parts[1])
				}
				d.compression, _ = dvid.NewCompression(dvid.Gzip, dvid.CompressionLevel(level))
			} else {
				return fmt.Errorf("Illegal compression specified: %s", s)
			}
		}
	}

	// Set checksum for this instance
	s, found, err = config.GetString("Checksum")
	if err != nil {
		return err
	}
	if found {
		checksum := strings.ToLower(s)
		switch checksum {
		case "none":
			d.checksum = dvid.NoChecksum
		case "crc32":
			d.checksum = dvid.CRC32
		default:
			return fmt.Errorf("Illegal checksum specified: %s", s)
		}
	}

	// Set versioning
	s, found, err = config.GetString("Versioned")
	if err != nil {
		return err
	}
	if found {
		versioned := strings.ToLower(s)
		switch versioned {
		case "false", "0":
			d.unversioned = true
		case "true", "1":
			d.unversioned = false
		default:
			return fmt.Errorf("Illegal setting for 'versioned' (needs to be 'false', '0', 'true', or '1'): %s", s)
		}
	}

	// Check for tags
	s, found, err = config.GetString("Tags")
	if err != nil {
		return err
	}
	if found {
		tagassigns := strings.Split(s, ",")
		d.tags = make(map[string]string, len(tagassigns))
		for _, tagassign := range tagassigns {
			elems := strings.Split(tagassign, "=")
			if len(elems) == 2 && len(elems[0]) != 0 {
				d.tags[elems[0]] = elems[1]
			}
		}
	}

	return nil
}

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  This will be overriden in data types, but if not, this provides
// a fallback for all data types.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	return fmt.Sprintf("generic %s key", d.TypeName())
}

// PushData is the base implementation of pushing data instance key-value pairs
// to a remote DVID without any datatype-specific filtering of data.
func (d *Data) PushData(p *PushSession) error {
	return PushData(d, p)
}

// ------ handle operation id waiting --------

// MutAdd adds a wait to this operation.  Returns true if this is a new operation for this Data
func (d *Data) MutAdd(mutID uint64) (newOp bool) {
	d.opWG_mu.Lock()
	if d.opWG == nil {
		d.opWG = make(map[uint64]*sync.WaitGroup)
	}
	wg, found := d.opWG[mutID]
	if !found || wg == nil {
		wg = new(sync.WaitGroup)
		d.opWG[mutID] = wg
		newOp = true
	}
	wg.Add(1)
	d.opWG_mu.Unlock()
	return
}

// MutDone marks the end of operations corresponding to MutAdd.
func (d *Data) MutDone(mutID uint64) {
	d.opWG_mu.Lock()
	defer d.opWG_mu.Unlock()
	if d.opWG == nil {
		return
	}
	wg, found := d.opWG[mutID]
	if !found || wg == nil {
		return
	}
	wg.Done()
}

// MutWait blocks until all operations with the given ID are completed.
func (d *Data) MutWait(mutID uint64) {
	d.opWG_mu.RLock()
	wg, found := d.opWG[mutID]
	d.opWG_mu.RUnlock()
	if found && wg != nil {
		wg.Wait()
	}
}

// MutDelete removes a wait for this operation.  Should only be done after all MutWait()
// have completed.
func (d *Data) MutDelete(mutID uint64) {
	d.opWG_mu.Lock()
	delete(d.opWG, mutID)
	d.opWG_mu.Unlock()
}

// GetKeyValueDB returns a kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func GetKeyValueDB(d dvid.Data) (db storage.KeyValueDB, err error) {
	store, err := d.KVStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, ErrInvalidStore
	}
	var ok bool
	db, ok = store.(storage.KeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Store assigned to data %q (%s) is not a key-value db", d.DataName(), store)
	}
	return
}

// GetOrderedKeyValueDB returns the ordered kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func GetOrderedKeyValueDB(d dvid.Data) (db storage.OrderedKeyValueDB, err error) {
	store, err := d.KVStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, ErrInvalidStore
	}
	var ok bool
	db, ok = store.(storage.OrderedKeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Store assigned to data %q (%s) is not an ordered key-value db: %v", d.DataName(), store, store)
	}
	return
}

// GetKeyValueBatcher returns a batch-capable kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func GetKeyValueBatcher(d dvid.Data) (db storage.KeyValueBatcher, err error) {
	store, err := d.KVStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, ErrInvalidStore
	}
	var ok bool
	db, ok = store.(storage.KeyValueBatcher)
	if !ok {
		return nil, fmt.Errorf("Store assigned to data %q (%s) is not able to batch key-value ops", d.DataName(), store)
	}
	return
}

// GetGraphDB returns a graph store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func GetGraphDB(d dvid.Data) (db storage.GraphDB, err error) {
	store, err := d.KVStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, ErrInvalidStore
	}
	var ok bool
	db, ok = store.(storage.GraphDB)
	if !ok {
		return nil, fmt.Errorf("Store assigned to data %q (%s) is not a graph db", d.DataName(), store)
	}
	return
}

// GetBlobStore returns a blob store or nil if not available.
// TODO -- Add configurable option to use a file system.
func GetBlobStore(d dvid.Data) (store storage.BlobStore, err error) {
	db, err := GetKeyValueDB(d)
	if err != nil {
		return nil, err
	}
	var ok bool
	store, ok = db.(storage.BlobStore)
	if !ok {
		return nil, fmt.Errorf("data %q assigned key-value DB (%s) cannot be used as blob store", d.DataName(), db)
	}
	return store, nil
}

// GetBlob retrieves data given a reference that was received during PutBlob.
func (d *Data) GetBlob(ref string) (data []byte, err error) {
	var blobStore storage.BlobStore
	if blobStore, err = GetBlobStore(d); err != nil {
		return
	}
	if blobStore != nil {
		data, err = blobStore.GetBlob(ref)
		if err != nil {
			err = fmt.Errorf("bad GET BLOB for blob store %s assigned to data %q: %v", blobStore, d.DataName(), err)
		}
	}
	return
}

// PutBlob stores data and returns a reference to that data.
func (d *Data) PutBlob(b []byte) (ref string, err error) {
	var blobStore storage.BlobStore
	if blobStore, err = GetBlobStore(d); err != nil {
		return
	}
	if blobStore != nil {
		ref, err = blobStore.PutBlob(b)
		if err != nil {
			err = fmt.Errorf("bad PUT BLOB for blob store %s assigned to data %q: %v", blobStore, d.DataName(), err)
		}
	}
	return
}

func (d *Data) ProduceKafkaMsg(b []byte) error {
	// create topic (repo ID + data instance uuid)
	// NOTE: Kafka server must be configured to allow topic creation from
	// messages sent to a non-existent topic
	rootuuid, _ := d.DAGRootUUID()
	datauuid := d.DataUUID()
	topic := storage.KafkaTopicPrefix + "dvidrepo-" + string(rootuuid) + "-data-" + string(datauuid)
	suffix := storage.KafkaTopicSuffix(d.DataUUID())
	if suffix != "" {
		topic += "-" + suffix
	}

	// send message if kafka initialized
	return storage.KafkaProduceMsg(b, topic)
}
