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

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
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

// VersionedContext implements storage.VersionedContext for data instances that
// have a version DAG.
type VersionedContext struct {
	*storage.DataContext
}

func NewVersionedContext(data dvid.Data, versionID dvid.VersionID) *VersionedContext {
	return &VersionedContext{storage.NewDataContext(data, versionID)}
}

func (ctx *VersionedContext) GetIterator() (storage.VersionIterator, error) {
	uuid, err := UUIDFromVersion(ctx.VersionID())
	if err != nil {
		return nil, err
	}
	repo, err := RepoFromUUID(uuid)
	if err != nil {
		return nil, err
	}
	return repo.GetIterator(ctx.VersionID())
}

// VersionedKeyValue returns the key-value pair corresponding to this key's version
// given a list of key-value pairs across many versions.  If no suitable key-value
// pair is found or a tombstone is encounterd closest to version, nil is returned.
func (ctx *VersionedContext) VersionedKeyValue(values []*storage.KeyValue) (*storage.KeyValue, error) {
	// Set up a map[VersionID]KeyValue
	versionMap := make(map[dvid.VersionID]*storage.KeyValue, len(values))
	for _, kv := range values {
		vid, err := ctx.VersionFromKey(kv.K)
		if err != nil {
			return nil, err
		}
		versionMap[vid] = kv
		// fmt.Printf("Found version %d for %s\n", vid, ctx)
	}

	// Iterate from the current node up the ancestors in the version DAG, checking if
	// current best is present.
	it, err := ctx.GetIterator()
	if err != nil {
		return nil, fmt.Errorf("Couldn't get versioned data iterator: %s\n", err.Error())
	}
	for {
		if it.Valid() {
			if kv, found := versionMap[it.VersionID()]; found {
				if ctx.IsTombstoneKey(kv.K) {
					// fmt.Printf("Found tombstone and returning nil value.\n")
					return nil, nil
				}
				// fmt.Printf("Found version %d and returning value %d bytes\n", it.VersionID(), len(kv.V))
				return kv, nil
			}
		} else {
			break
		}
		it.Next()
	}
	return nil, nil
}

func (ctx *VersionedContext) Versioned() bool {
	return true
}

// DataService is an interface for operations on an instance of a supported datatype.
type DataService interface {
	dvid.Data

	GetType() TypeService

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(config dvid.Config) error

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// ServeHTTP handles HTTP requests in the context of a particular version of a Repo
	// for this instance of a datatype.
	ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request)

	Help() string

	// Send allows peer-to-peer transmission of data instance metadata and
	// all normalized key-value pairs associated with it.  Transmitted data
	// can be delimited by an optional ROI, specified by the ROI data name
	// and its UUID.  Use an empty string for the roiname parameter to transmit
	// the full extents.
	Send(s message.Socket, roiname string, uuid dvid.UUID) error

	// DataService must allow serialization into JSON and binary I/O
	json.Marshaler

	// NOTE: Requiring these interfaces breaks Gob encoding/decoding
	//  TODO -- figure out why
	//gob.GobEncoder
	//gob.GobDecoder
}

// InstanceMutator provides a hook for data instances to load mutable data
// on startup.  It is assumed that the data instances store data whenever
// its data mutates, e.g., extents for labelblk or max label for labelvol.
type InstanceMutator interface {
	// Loads all mutable properties.
	LoadMutable() error
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

// Syncer types support syncing of data between different data instances.
type Syncer interface {
	// InitSync establishes a sync link between the receiver data instance and the
	// named instance.  It returns the subscriptions that need to be created to keep this data
	// synced and also launches any necessary goroutines that will consume inbound channels of changes
	// from associated data.
	InitSync(dvid.InstanceName) []SyncSub

	// SyncedNames returns a slice of instance names to which the receiver is synced.
	SyncedNames() []dvid.InstanceName
}

// NotifySubscribers sends a message to any data instances subscribed to the event.
func NotifySubscribers(e SyncEvent, m SyncMessage) error {
	// Get the repo from the version
	repo, err := RepoFromVersionID(m.Version)
	if err != nil {
		return err
	}

	// Use the repo notification system
	return repo.NotifySubscribers(e, m)
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

	// these sync management vars aren't serialized
	syncmu     sync.RWMutex
	syncInited map[dvid.InstanceName]struct{}
}

// IsSyncEstablished returns true if a sync subscriptions have already been marked
// as established to the given data instance.  Thread-safe.
func (d *Data) IsSyncEstablished(name dvid.InstanceName) bool {
	d.syncmu.RLock()
	defer d.syncmu.RUnlock()
	if _, found := d.syncInited[name]; found {
		return true
	}
	return false
}

// SyncEstablished marks the establishment of sync subscriptions with the given
// data instance.  Thread-safe.
func (d *Data) SyncEstablished(name dvid.InstanceName) {
	d.syncmu.Lock()
	defer d.syncmu.Unlock()
	if d.syncInited == nil {
		d.syncInited = make(map[dvid.InstanceName]struct{})
	}
	d.syncInited[name] = struct{}{}
}

func (d *Data) SyncedNames() []dvid.InstanceName {
	return d.syncs
}

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
	}{
		TypeName:    d.typename,
		TypeURL:     d.typeurl,
		TypeVersion: d.typeversion,
		Name:        d.name,
		RepoUUID:    d.uuid,
		Compression: d.compression.String(),
		Checksum:    d.checksum.String(),
		Syncs:       d.syncs,
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
	}
	err := data.ModifyConfig(c)
	return data, err
}

// ---- dvid.Data implementation ----

func (d *Data) DataName() dvid.InstanceName { return d.name }

func (d *Data) InstanceID() dvid.InstanceID { return d.id }

func (d *Data) SetInstanceID(id dvid.InstanceID) {
	d.id = id
}

func (d *Data) TypeName() dvid.TypeString { return d.typename }

func (d *Data) TypeURL() dvid.URLString { return d.typeurl }

func (d *Data) TypeVersion() string { return d.typeversion }

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
		dvid.Errorf("Data %q: %s\n", d.name, err.Error())
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

	// Set data instances for syncing.
	s, found, err = config.GetString("sync")
	if err != nil {
		return err
	}
	if found {
		names := strings.Split(s, ",")
		if len(names) > 0 {
			for _, name := range names {
				d.syncs = append(d.syncs, dvid.InstanceName(name))
			}
		}
	}
	return nil
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.name, d.TypeName(), request.TypeCommand())
}
