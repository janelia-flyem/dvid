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

// DataService is an interface for operations on an instance of a supported datatype.
type DataService interface {
	datatype.Service

	// DataName returns the name of the data (e.g., grayscale data that is grayscale8 data type).
	DataName() dvid.DataString

	// ModifyConfig modifies a configuration in a type-specific way.
	ModifyConfig(config dvid.Config) error

	// DoRPC handles command line and RPC commands specific to a data type
	DoRPC(request Request, reply *Response) error

	// ServeHTTP fulfills the http.Handler interface.
	ServeHTTP(w http.ResponseWriter, r *http.Request)
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

// NewDataService returns a Data instance.  If the configuration doesn't explicitly
// set compression and checksum, LZ4 and the default checksum (chosen by -crc32 flag)
// is used.
func NewDataService(id *DataInstance, t datatype.Service, config dvid.Config) (*Data, error) {
	compression, _ := dvid.NewCompression(dvid.LZ4, dvid.DefaultCompression)
	data := &Data{
		DataInstance: id,
		Service:      t,
		Compression:  compression,
		Checksum:     dvid.DefaultChecksum,
	}
	err := data.ModifyConfig(config)
	return data, err
}

// ---- DataService implementation ----

// DataInstance identifies data within a DVID server.
type DataInstance struct {
	Name   dvid.DataString
	ID     dvid.DataLocalID
	DsetID dvid.RepoLocalID
}

func (id DataInstance) DataName() dvid.DataString { return id.Name }

func (id DataInstance) LocalID() dvid.DataLocalID { return id.ID }

func (id DataInstance) RepoID() dvid.RepoLocalID { return id.DsetID }

// Data is an instance of a data type with some identifiers and it satisfies
// a DataService interface.  Each Data is repo-specific.
type Data struct {
	*DataInstance
	datatype.Service

	// Compression of serialized data, e.g., the value in a key-value.
	Compression dvid.Compression

	// Checksum approach for serialized data.
	Checksum dvid.Checksum

	// If false (default), we allow changes along nodes.
	Unversioned bool
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
