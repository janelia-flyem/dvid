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

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
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
	return getKeyValueDB(d)
}

// GetOrderedKeyValueDB returns an ordered key-value store associated with this
// context or an error if one is unavailable.
func (vctx *VersionedCtx) GetOrderedKeyValueDB() (storage.OrderedKeyValueDB, error) {
	d := vctx.DataContext.Data()
	if d == nil {
		return nil, fmt.Errorf("invalid data %v in GetOrderedKeyValueDB", d)
	}
	return getOrderedKeyValueDB(d)
}

// GetGraphDB returns a graph store associated with this context or an error
// if one is not available.
func (vctx *VersionedCtx) GetGraphDB() (storage.GraphDB, error) {
	d := vctx.DataContext.Data()
	if d == nil {
		return nil, fmt.Errorf("invalid data %v in GetGraphDB", d)
	}
	return getGraphDB(d)
}

// DataService is an interface for operations on an instance of a supported datatype.
type DataService interface {
	dvid.Data
	storage.Accessor
	json.Marshaler

	Help() string
	GetType() TypeService

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(dvid.Config) error

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(Request, *Response) error

	// ServeHTTP handles HTTP requests in the context of a particular version.
	ServeHTTP(dvid.UUID, *VersionedCtx, http.ResponseWriter, *http.Request)

	// Send allows peer-to-peer transmission of data instance metadata and
	// all normalized key-value pairs associated with it.  Transmitted data
	// can be delimited by an optional filter specification (e.g., "roi:roiname,uuid")
	// and set of versions.  Use an empty string for the roiname parameter to transmit
	// the full extents.
	Send(s rpc.Session, t rpc.Transmit, filter string, versions map[dvid.VersionID]struct{}) error

	// NOTE: Requiring these interfaces breaks Gob encoding/decoding
	//  TODO -- figure out why
	//gob.GobEncoder
	//gob.GobDecoder
}

// TypeMigrator is an interface for a DataService that can migrate itself to another DataService.
// A deprecated DataService implementation can implement this interface to auto-convert on metadata load.
type TypeMigrator interface {
	MigrateData() (DataService, error)
}

// InstanceMutator provides a hook for data instances to load mutable data
// on startup.  It is assumed that the data instances store data whenever
// its data mutates, e.g., extents for labelblk or max label for labelvol.
type InstanceMutator interface {
	// Loads all mutable properties and applies any necessary migration to
	// transform the internal data from the stored to expected version.
	LoadMutable(root dvid.VersionID, storedVersion, expectedVersion uint64) (saveNeeded bool, err error)
}

// VersionInitializer provides a hook for data instances to receive branch (new version)
// events and modify their properties as needed.
type VersionInitializer interface {
	InitVersion(dvid.UUID, dvid.VersionID) error
}

// SyncEvent identifies an event in which a data instance has modified its data
type SyncEvent struct {
	Instance dvid.InstanceName
	Event    string
}

// SyncMessage describes changes to a data instance for a given version.
type SyncMessage struct {
	Version dvid.VersionID
	Delta   interface{}
}

// SyncSub is a subscription request from an instance to be notified via a channel when
// a given data instance has a given event.
type SyncSub struct {
	Event  SyncEvent
	Notify dvid.InstanceName
	Ch     chan SyncMessage
	Done   chan struct{}
}

// SyncSubs is a slice of sync subscriptions.
type SyncSubs []SyncSub

// Add returns a SyncSubs with the added SyncSub, making sure that only one subscription exists for any
// (Event, Notify) tuple.  If a previous (Event, Notify) exists, it is replaced by the passed SyncSub.
func (subs SyncSubs) Add(added SyncSub) SyncSubs {
	if len(subs) == 0 {
		return SyncSubs{added}
	}
	for i, sub := range subs {
		if sub.Event == added.Event && sub.Notify == added.Notify {
			subs[i] = added
			return subs
		}
	}
	return append(subs, added)
}

// Syncer types support syncing of data between different data instances.
type Syncer interface {
	// SetSync establishes a sync link between the receiver data instance and a list of named
	// data instances.
	SetSync(uuid dvid.UUID, in io.ReadCloser) error

	// GetSyncSubs returns the subscriptions that need to be created to keep this data
	// synced and may launch goroutines that will consume inbound channels of changes
	// from associated data.
	GetSyncSubs(dvid.Data) SyncSubs

	// SyncedNames returns a slice of instance names to which the receiver is synced.
	SyncedNames() []dvid.InstanceName
}

// CommitSyncer want to be notified when a node is committed.
type CommitSyncer interface {
	// SyncOnCommit is an asynchronous function that should be called when a node is committed.
	SyncOnCommit(dvid.UUID, dvid.VersionID)
}

// NotifySubscribers sends a message to any data instances subscribed to the event.
func NotifySubscribers(e SyncEvent, m SyncMessage) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	// Get the repo from the version
	repo, err := manager.repoFromVersion(m.Version)
	if err != nil {
		return err
	}

	// Use the repo notification system
	return repo.notifySubscribers(e, m)
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
	u.Unlock()
}

// Returns true if the data is currently being updated.
func (u *Updater) Updating() bool {
	u.RLock()
	updating := u.updates > 0
	u.RUnlock()
	return updating
}

// Data is the base struct of repo-specific data instances.  It should be embedded
// in a datatype's DataService implementation and handle datastore-wide key partitioning.
type Data struct {
	typename    dvid.TypeString
	typeurl     dvid.URLString
	typeversion string

	name dvid.InstanceName
	id   dvid.InstanceID

	uuid dvid.UUID // Root uuid of repo

	// Compression of serialized data, e.g., the value in a key-value.
	compression dvid.Compression

	// Checksum approach for serialized data.
	checksum dvid.Checksum

	// a list of the instances to which this data should be synced
	syncs []dvid.InstanceName

	// unversioned = true if all UUIDs should be mapped to the root UUID.
	// Only one version exists for an entire repo, so it's repo-wide.
	// Requires the data type to actually check if versioned and handle
	// UUIDs differently.  (See keyvalue type.)
	unversioned bool

	// the assigned backend store for a data instance.  If nil, we
	// will use the default store.
	store dvid.Store

	// these sync management vars aren't serialized
	syncmu     sync.RWMutex
	syncInited map[dvid.InstanceName]struct{}
}

// CloneToType returns a clone of Data with modified data type information but same instance id.
// This is useful for migrating data from one type to another.
func (d *Data) CloneToType(typename dvid.TypeString, typeurl dvid.URLString, typeversion string) *Data {
	d2 := new(Data)
	d2.typename = typename
	d2.typeurl = typeurl
	d2.typeversion = typeversion

	d2.name = d.name
	d2.id = d.id
	d2.uuid = d.uuid

	d2.compression = d.compression
	d2.checksum = d.checksum
	copy(d2.syncs, d.syncs)
	d2.unversioned = d.unversioned

	return d2
}

// -- Syncer interface implementation is in base Data type because generic implementations will do.

func (d *Data) SetSync(uuid dvid.UUID, in io.ReadCloser) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}
	jsonData := make(map[string]string)
	decoder := json.NewDecoder(in)
	if err := decoder.Decode(&jsonData); err != nil && err != io.EOF {
		return fmt.Errorf("Malformed JSON request in sync request: %v", err)
	}
	syncedCSV, ok := jsonData["sync"]
	if !ok {
		return fmt.Errorf("Could not find 'sync' value in POSTed JSON to sync request.")
	}

	syncedNames := strings.Split(syncedCSV, ",")
	if len(syncedNames) == 0 {
		return nil
	}
	for _, name := range syncedNames {
		d.syncs = append(d.syncs, dvid.InstanceName(name))
	}
	if err := SyncData(uuid, dvid.InstanceName(d.DataName()), syncedNames...); err != nil {
		return err
	}
	return nil
}

func (d *Data) SyncedNames() []dvid.InstanceName {
	return d.syncs
}

// ---------------------------------------

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TypeName    dvid.TypeString
		TypeURL     dvid.URLString
		TypeVersion string
		Name        dvid.InstanceName
		RepoUUID    dvid.UUID
		Compression string
		Checksum    string
		Syncs       []dvid.InstanceName
		Versioned   bool
	}{
		TypeName:    d.typename,
		TypeURL:     d.typeurl,
		TypeVersion: d.typeversion,
		Name:        d.name,
		RepoUUID:    d.uuid,
		Compression: d.compression.String(),
		Checksum:    d.checksum.String(),
		Syncs:       d.syncs,
		Versioned:   !d.unversioned,
	})
}

var reservedNames = map[string]struct{}{
	"log":    struct{}{},
	"lock":   struct{}{},
	"branch": struct{}{},
}

// NewDataService returns a new Data instance that fulfills the DataService interface.
// The UUID passed in corresponds to the root UUID of the repo that should hold the data.
// This returned Data struct is usually embedded by datatype-specific data instances.
// By default, LZ4 and the default checksum is used.
func NewDataService(t TypeService, uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	if _, reserved := reservedNames[string(name)]; reserved {
		return nil, fmt.Errorf("cannot use reserved name %q", name)
	}

	// Get the store for this particular data instance.
	store, err := storage.GetAssignedStore(t.GetTypeName())
	if err != nil {
		return nil, err
	}

	// Setup the basic data instance structure.
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &Data{
		typename:    t.GetTypeName(),
		typeurl:     t.GetTypeURL(),
		typeversion: t.GetTypeVersion(),
		name:        name,
		id:          id,
		uuid:        uuid,
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		syncs:       []dvid.InstanceName{},
		unversioned: false,
		store:       store,
	}
	return data, data.ModifyConfig(c)
}

// ---- dvid.Data implementation ----

func (d *Data) DataName() dvid.InstanceName { return d.name }

func (d *Data) InstanceID() dvid.InstanceID { return d.id }

func (d *Data) UUID() dvid.UUID { return d.uuid }

func (d *Data) SetInstanceID(id dvid.InstanceID) {
	d.id = id
}

func (d *Data) SetUUID(uuid dvid.UUID) {
	d.uuid = uuid
}

func (d *Data) TypeName() dvid.TypeString { return d.typename }

func (d *Data) TypeURL() dvid.URLString { return d.typeurl }

func (d *Data) TypeVersion() string { return d.typeversion }

func (d *Data) Versioned() bool { return !d.unversioned }

func (d *Data) BackendStore() (dvid.Store, error) {
	if d.store == nil {
		return storage.DefaultStore()
	}
	return d.store, nil
}

func (d *Data) SetBackendStore(store dvid.Store) {
	d.store = store
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
	if err := dec.Decode(&(d.uuid)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.compression)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.checksum)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.syncs)); err != nil {
		return err
	}
	err := dec.Decode(&(d.unversioned))
	if err != nil {
		dvid.Infof("Data %q had no explicit versioning flag: assume it's versioned.\n", d.name)
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
	if err := enc.Encode(d.uuid); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.compression); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.checksum); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.syncs); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.unversioned); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Data) Equals(d2 *Data) bool {
	if d.typename != d2.typename ||
		d.typeurl != d2.typeurl ||
		d.typeversion != d2.typeversion ||
		d.name != d2.name ||
		d.id != d2.id ||
		d.uuid != d2.uuid ||
		d.compression != d2.compression ||
		d.checksum != d2.checksum ||
		!syncsEqual(d.syncs, d2.syncs) {
		return false
	}
	return true
}

func syncsEqual(a, b []dvid.InstanceName) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
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
		default:
			// Check for gzip + compression level
			parts := strings.Split(format, ":")
			if len(parts) == 2 && parts[0] == "gzip" {
				level, err := strconv.Atoi(parts[1])
				if err != nil {
					return fmt.Errorf("Unable to parse gzip compression level ('%d').  Should be 'gzip:<level>'.", parts[1])
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
	return nil
}

// Send is the default peer-to-peer transmission of data for an instance
// that ignores any filtering per the ROI specification.  Filtering of
// transmitted data needs to be implemented in the datatype-specific
// implementation.
func (d *Data) Send(s rpc.Session, transmit rpc.Transmit, filter string, versions map[dvid.VersionID]struct{}) error {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return err
	}

	// pick any version because flatten transmit will only have one version, and all or branch transmit will
	// be looking at all versions anyway.
	if len(versions) == 0 {
		return fmt.Errorf("need at least one version to send")
	}
	var v dvid.VersionID
	for v = range versions {
		break
	}
	ctx := storage.NewDataContext(d, v)

	// Send the initial data instance start message
	dmsg := DataTxInit{
		Session:    s.ID(),
		DataName:   d.DataName(),
		TypeName:   d.TypeName(),
		InstanceID: d.InstanceID(),
	}
	if _, err := s.Call()(StartDataMsg, dmsg); err != nil {
		dvid.Errorf("couldn't send data instance start: %v\n", err)
	}

	// Send all the key-value pairs
	keysOnly := false
	if transmit == rpc.TransmitFlatten {
		// Start goroutine to receive tkey-value pairs and transmit to remote.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					endmsg := KVMessage{Session: s.ID(), Terminate: true}
					if _, err := s.Call()(PutKVMsg, endmsg); err != nil {
						dvid.Errorf("couldn't send data instance termination: %v\n", err)
					}
					s.StopJob()
					return
				}
				kv := storage.KeyValue{
					K: ctx.ConstructKey(tkv.K),
					V: tkv.V,
				}
				kvmsg := KVMessage{s.ID(), kv, false}
				if _, err := s.Call()(PutKVMsg, kvmsg); err != nil {
					dvid.Errorf("Error pushing data %q: %v", d.DataName(), err)
				}
			}
		}()

		s.StartJob()
		begKey, endKey := ctx.TKeyRange()
		err := store.ProcessRange(ctx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("got nil chunk in datainstance flatten send")
			}
			ch <- c.TKeyValue
			return nil
		})
		if err != nil {
			return fmt.Errorf("%q flatten range query: %v\n", d.DataName(), err)
		}
	} else {
		// Start goroutine to receive key-value pairs and transmit to remote.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					endmsg := KVMessage{Session: s.ID(), Terminate: true}
					if _, err := s.Call()(PutKVMsg, endmsg); err != nil {
						dvid.Errorf("couldn't send data instance termination: %v\n", err)
					}
					s.StopJob()
					return
				}
				if !ctx.ValidKV(kv, versions) {
					continue
				}
				kvmsg := KVMessage{s.ID(), *kv, false}
				if _, err := s.Call()(PutKVMsg, kvmsg); err != nil {
					dvid.Errorf("Error pushing data %q: %v", d.DataName(), err)
				}
			}
		}()

		s.StartJob()
		begKey, endKey := ctx.KeyRange()
		if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch); err != nil {
			return fmt.Errorf("%q all/branch transmit range query: %v", d.DataName(), err)
		}
	}

	return nil
}

// ------ storage.Accessor interface implementation -------

func getKeyValueDB(d dvid.Data) (db storage.KeyValueDB, err error) {
	store, err := d.BackendStore()
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

func getOrderedKeyValueDB(d dvid.Data) (db storage.OrderedKeyValueDB, err error) {
	store, err := d.BackendStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, ErrInvalidStore
	}
	var ok bool
	db, ok = store.(storage.OrderedKeyValueDB)
	if !ok {
		return nil, fmt.Errorf("Store assigned to data %q (%s) is not an ordered key-value db", d.DataName(), store)
	}
	return
}

func getKeyValueBatcher(d dvid.Data) (db storage.KeyValueBatcher, err error) {
	store, err := d.BackendStore()
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

func getGraphDB(d dvid.Data) (db storage.GraphDB, err error) {
	store, err := d.BackendStore()
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

// GetKeyValueDB returns a kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func (d *Data) GetKeyValueDB() (storage.KeyValueDB, error) {
	return getKeyValueDB(d)
}

// GetOrderedKeyValueDB returns the ordered kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func (d *Data) GetOrderedKeyValueDB() (storage.OrderedKeyValueDB, error) {
	return getOrderedKeyValueDB(d)
}

// GetKeyValueBatcher returns a batch-capable kv data store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func (d *Data) GetKeyValueBatcher() (storage.KeyValueBatcher, error) {
	return getKeyValueBatcher(d)
}

// GetGraphDB returns a graph store assigned to this data instance.
// If the store is nil or not available, an error is returned.
func (d *Data) GetGraphDB() (storage.GraphDB, error) {
	return getGraphDB(d)
}
