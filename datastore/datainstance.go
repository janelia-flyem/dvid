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
// pair is found, nil is returned.
func (ctx *VersionedContext) VersionedKeyValue(values []*storage.KeyValue) (*storage.KeyValue, error) {

	// Set up a map[VersionID]KeyValue
	versionMap := make(map[dvid.VersionID]*storage.KeyValue, len(values))
	for _, kv := range values {
		pos := len(kv.K) - dvid.VersionIDSize
		vid := dvid.VersionIDFromBytes(kv.K[pos:])
		versionMap[vid] = kv
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
				return kv, nil
			}
		} else {
			break
		}
		it.Next()
	}
	return nil, nil
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
	// GetSyncSubs returns the subscriptions that need to be created to keep this data synced
	// and also launches any necessary goroutines that will consume inbound channels of changes
	// from associated data.
	//
	// This function is called whenever a new data instance is created or loaded on startup, and
	// it is used to modify the sync graph between data instances.
	GetSyncSubs() []SyncSub
}

// Persistence indicates the level of persistence needed for data within this instance.
// It's a method to mark how critical it is to protect data.
type Persistence uint8

const (
	// DataDefault  - no backups made.  Normal replication using redundant copies or erasure coding.
	DataDefault Persistence = iota

	// DataCritical - must be serializable and DVID will asynchronously backup data in cheaper storage.
	DataCritical

	// DataCached - can be deleted after X hours, say 48 hours.
	DataCached
)

func (p Persistence) String() string {
	switch p {
	case DataDefault:
		return "standard data persistence"
	case DataCritical:
		return "critical data persistence"
	case DataCached:
		return "cached data"
	default:
		return "unknown persistence model"
	}
}

// Data is the base struct of repo-specific data instances.  It should be embedded
// in a datatype's DataService implementation and handle datastore-wide key partitioning.
type Data struct {
	TypeService

	name dvid.InstanceName
	id   dvid.InstanceID
	uuid dvid.UUID // Root uuid of repo

	// Compression of serialized data, e.g., the value in a key-value.
	compression dvid.Compression

	// Checksum approach for serialized data.
	checksum dvid.Checksum

	// Persistence describes how data should be persisted.
	persistence Persistence

	// If true (default), we allow changes along nodes.
	versioned bool
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
		Persistence string
		Versioned   bool
	}{
		TypeName:    d.TypeService.GetTypeName(),
		TypeURL:     d.TypeService.GetTypeURL(),
		TypeVersion: d.TypeService.GetTypeVersion(),
		Name:        d.name,
		RepoUUID:    d.uuid,
		Compression: d.compression.String(),
		Checksum:    d.checksum.String(),
		Persistence: d.persistence.String(),
		Versioned:   d.versioned,
	})
}

var reservedNames = map[string]struct{}{
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
		TypeService: t,
		name:        name,
		id:          id,
		uuid:        uuid,
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		persistence: DataDefault,
		versioned:   true,
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

func (d *Data) TypeName() dvid.TypeString { return d.GetTypeName() }

func (d *Data) TypeURL() dvid.URLString { return d.GetTypeURL() }

func (d *Data) TypeVersion() string { return d.GetTypeVersion() }

func (d *Data) Versioned() bool { return d.versioned }

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var t TypeService
	if err := dec.Decode(&t); err != nil {
		return err
	}
	d.TypeService = t
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
	if err := dec.Decode(&(d.persistence)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.versioned)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.TypeService); err != nil {
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
	if err := enc.Encode(d.persistence); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.versioned); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// -----------

func (d *Data) Compression() dvid.Compression {
	return d.compression
}

func (d *Data) Checksum() dvid.Checksum {
	return d.checksum
}

func (d *Data) Persistence() Persistence {
	return d.persistence
}

// --- DataService implementation -----

func (d *Data) GetType() TypeService {
	return d.TypeService
}

func (d *Data) ModifyConfig(config dvid.Config) error {
	versioned, err := config.IsVersioned()
	if err != nil {
		return err
	}
	d.versioned = versioned

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
	return nil
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.name, d.TypeName(), request.TypeCommand())
}
