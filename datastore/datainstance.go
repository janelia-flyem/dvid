/*
	This file contains code supporting data instances of a DVID datatype.
*/

package datastore

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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
	TypeService

	DataType() TypeService

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(config dvid.Config) error

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// ServeHTTP handles HTTP requests in the context of a particular version of a Repo
	// for this instance of a datatype.
	ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request)
}

// Data is the base struct of repo-specific data instances.  It should be embedded
// in a datatype's DataService implementation and handle datastore-wide key partitioning.
type Data struct {
	TypeService

	name dvid.DataString
	id   dvid.InstanceID
	uuid dvid.UUID // Root uuid of repo

	// Compression of serialized data, e.g., the value in a key-value.
	compression dvid.Compression

	// Checksum approach for serialized data.
	checksum dvid.Checksum

	// If true (default), we allow changes along nodes.
	versioned bool
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TypeName    dvid.TypeString
		TypeURL     URLString
		Name        dvid.DataString
		RepoUUID    dvid.UUID
		Compression string
		Checksum    string
		Versioned   bool
	}{
		TypeName:    d.TypeName(),
		TypeURL:     d.TypeURL(),
		Name:        d.name,
		RepoUUID:    d.uuid,
		Compression: d.compression.String(),
		Checksum:    d.checksum.String(),
		Versioned:   d.versioned,
	})
}

// NewDataService returns a new Data instance that fulfills the DataService interface.
// This returned Data struct is usually embedded by datatype-specific data instances.
// By default, LZ4 and the default checksum is used.
func NewDataService(t TypeService, r Repo, id dvid.InstanceID, name dvid.DataString, c dvid.Config) (*Data, error) {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &Data{
		TypeService: t,
		name:        name,
		id:          id,
		uuid:        r.RootUUID(),
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		versioned:   true,
	}
	err := data.ModifyConfig(c)
	return data, err
}

// ---- dvid.Data implementation ----

func (d *Data) DataName() dvid.DataString { return d.name }

func (d *Data) InstanceID() dvid.InstanceID { return d.id }

func (d *Data) Versioned() bool {
	return d.versioned
}

// -----------

func (d *Data) Compression() dvid.Compression {
	return d.compression
}

func (d *Data) Checksum() dvid.Checksum {
	return d.checksum
}

// --- DataService implementation -----

func (d *Data) DataType() TypeService {
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

// --- Handle version-specific data mutexes -----

type nodeID struct {
	instance dvid.InstanceID
	version  dvid.VersionID
}

// VersionMutex returns a Mutex that is specific for data at a particular version.
func (d *Data) VersionMutex(versionID dvid.VersionID) *sync.Mutex {
	var mutex sync.Mutex
	mutex.Lock()
	id := nodeID{d.id, versionID}
	vmutex, found := versionMutexes[id]
	if !found {
		vmutex = new(sync.Mutex)
		versionMutexes[id] = vmutex
	}
	mutex.Unlock()
	return vmutex
}
