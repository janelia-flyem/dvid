/*
	This file contains code supporting data instances of a DVID datatype.
*/

package datastore

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datatype"
	"github.com/janelia-flyem/dvid/dvid"
)

// This message is used for all data types to explain options.
const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 
`

type Context struct {
	*Service
	config dvid.Config
}

type DataContext struct {
	*Context
	uuid   dvid.UUID
	dataID dvid.DataLocalID
}

// Subsetter is a type that can tell us its range of Index and how much it has
// actually available in this server.  It's used to implement limited cloning,
// e.g., only cloning a quarter of an image volume.
// TODO: Fulfill implementation for voxels data type.
type Subsetter interface {
	// MaximumExtents returns a range of indices for which data is available at
	// some DVID server.
	MaximumExtents() dvid.IndexRange

	// AvailableExtents returns a range of indices for which data is available
	// at this DVID server.  It is the currently available extents.
	AvailableExtents() dvid.IndexRange
}

// Request supports requests to the DVID server.
type Request struct {
	dvid.Command
	Input []byte
}

var (
	HelpRequest = Request{Command: []string{"help"}}
)

// Response supports responses from DVID.
type Response struct {
	dvid.Response
	Output []byte
}

// Writes a response to a writer.
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

// DataService is an interface for operations on an instance of a supported datatype.
type DataService interface {
	dvid.Data

	datatype.Service

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(config dvid.Config) error

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// ServeHTTP fulfills the http.Handler interface.
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

// DataInstance is the base struct of repo-specific data instances.  It should be embedded
// in a datatype's DataService implementation and handle datastore-wide key partitioning.
type DataInstance struct {
	datatype.Service

	name dvid.DataString
	id   dvid.InstanceID
	repo *Repo

	// Compression of serialized data, e.g., the value in a key-value.
	compression dvid.Compression

	// Checksum approach for serialized data.
	checksum dvid.Checksum

	// If true (default), we allow changes along nodes.
	versioned bool
}

// NewDataInstance returns a new instance.  By default, LZ4 and the default checksum is used.
func NewDataInstance(name dvid.DataString, t datatype.Service, r *Repo) *DataInstance {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	return &DataInstance{
		t,
		name,
		id:          NewInstanceID(),
		repo:        r,
		compression: compression,
		checksum:    dvid.DefaultChecksum,
		versioned:   true,
	}
}

// ---- Partial DataService implementation ----

func (data *DataInstance) DataName() dvid.DataString { return data.name }

func (data *DataInstance) InstanceID() InstanceID { return data.id }

func (data *DataInstance) RepoID() RepoID { return data.repo.ID() }

// DataKey returns a DataKey for this data given a local version and a data-specific Index. If
// the data is to be unversioned, the versionID should represent the root node of the version DAG.
func (d *Data) DataKey(versionID dvid.VersionID, index dvid.Index) *DataKey {
	return &DataKey{d.ID, versionID, index}
}

func (d *Data) UseCompression() dvid.Compression {
	return d.Compression
}

func (d *Data) UseChecksum() dvid.Checksum {
	return d.Checksum
}

func (d *Data) IsVersioned() bool {
	return !d.Unversioned
}

func (d *Data) ModifyConfig(config dvid.Config) error {
	versioned, err := config.IsVersioned()
	if err != nil {
		return err
	}
	d.Unversioned = !versioned

	// Set compression for this instance
	s, found, err := config.GetString("Compression")
	if err != nil {
		return err
	}
	if found {
		format := strings.ToLower(s)
		switch format {
		case "none":
			d.Compression, _ = dvid.NewCompression(dvid.Uncompressed, dvid.DefaultCompression)
		case "snappy":
			d.Compression, _ = dvid.NewCompression(dvid.Snappy, dvid.DefaultCompression)
		case "lz4":
			d.Compression, _ = dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
		case "gzip":
			d.Compression, _ = dvid.NewCompression(dvid.Gzip, dvid.DefaultCompression)
		default:
			// Check for gzip + compression level
			parts := strings.Split(format, ":")
			if len(parts) == 2 && parts[0] == "gzip" {
				level, err := strconv.Atoi(parts[1])
				if err != nil {
					return fmt.Errorf("Unable to parse gzip compression level ('%d').  Should be 'gzip:<level>'.", parts[1])
				}
				d.Compression, _ = dvid.NewCompression(dvid.Gzip, dvid.CompressionLevel(level))
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
			d.Checksum = dvid.NoChecksum
		case "crc32":
			d.Checksum = dvid.CRC32
		default:
			return fmt.Errorf("Illegal checksum specified: %s", s)
		}
	}
	return nil
}

func (d *Data) UnknownCommand(request Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		d.Name, d.DatatypeName(), request.TypeCommand())
}

// --- Handle version-specific data mutexes -----

type nodeID struct {
	Repo    dvid.RepoLocalID
	Data    dvid.DataLocalID
	Version dvid.VersionLocalID
}

// VersionMutex returns a Mutex that is specific for data at a particular version.
func (d *Data) VersionMutex(versionID dvid.VersionLocalID) *sync.Mutex {
	var mutex sync.Mutex
	mutex.Lock()
	id := nodeID{d.DsetID, d.ID, versionID}
	vmutex, found := versionMutexes[id]
	if !found {
		vmutex = new(sync.Mutex)
		versionMutexes[id] = vmutex
	}
	mutex.Unlock()
	return vmutex
}
