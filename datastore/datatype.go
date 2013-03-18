/*
	This file contains code useful for arbitrary data types supported in DVID.
	It includes the base Datatype struct which are embedded in user-supplied
	data types as well as useful functions like image loading likely to be used
	by many data types.
*/

package datastore

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"github.com/janelia-flyem/dvid/dvid"
)

// This message is used for all data types to explain options.
const helpMessage = `
    DVID data type information

    name: %s 
    url: %s 

    The following set options are available on the command line:

        uuid=<UUID>    Example: uuid=3f7a088

        rpc=<address>  Example: rpc=my.server.com:1234
`

type UrlString string

type DataFormat string

const (
	PNG DataFormat = "png"
	JPG            = "jpg"
	HDF            = "hdf5"
)

// DataStruct is an interface to structs that know their shape and data.
// Slices of various orientation and subvolumes should satisfy this interface.
type DataStruct interface {
	DataShaper
	DataPacker
}

// DataPacker encapsulates data of arbitrary type, although it may require
// additional data to figure out how to unpack the data.
type DataPacker interface {
	TypeService

	// The data itself.  Go image data is usually held in []uint8.
	Data() []uint8
}

// SliceVoxels aggregates the size and orientation of a slice and its data
type SliceVoxels struct {
	TypeService

	dvid.Slice

	// The data itself
	data []uint8
}

func (s *SliceVoxels) String() string {
	size := s.Size()
	return fmt.Sprintf("%s %s of size %d x %d @ %s",
		s.TypeName(), s.DataShape(), size[0], size[1], s.Origin())
}

// Subvolume packages the location, extent, and data of a data type corresponding
// to a rectangular box of voxels.  The "Sub" prefix emphasizes that the data is 
// usually a smaller portion of the volume held by the DVID datastore.  Although
// this type usually holds voxel values, it's possible to transmit other types
// of data that is associated with this region of the volume, e.g., a region
// adjacency graph or a serialized label->label map.
type Subvolume struct {
	TypeService

	// 3d offset
	offset dvid.VoxelCoord

	// 3d size of data
	size dvid.VoxelCoord

	// The data itself
	data []uint8
}

func (s *Subvolume) DataShape() dvid.DataShape {
	return Vol
}

func (s *Subvolume) String() string {
	return fmt.Sprintf("%s (%d x %d x %d) at offset (%d, %d, %d)",
		s.TypeName(), s.size[0], s.size[1], s.size[2], s.offset[0], s.offset[1], s.offset[2])
}

func (s *Subvolume) NumVoxels() int {
	if s == nil {
		return 0
	}
	return int(s.size[0] * s.size[1] * s.size[2])
}

func (s *Subvolume) Origin() dvid.VoxelCoord {
	return s.offset
}

func (s *Subvolume) TopRight() dvid.VoxelCoord {
	tr := s.offset
	tr[0] += s.size[0] - 1
	return tr
}

func (s *Subvolume) BottomLeft() dvid.VoxelCoord {
	tr := s.offset
	tr[1] += s.size[1] - 1
	tr[2] += s.size[2] - 1
	return tr
}

func (s *Subvolume) EndVoxel() dvid.VoxelCoord {
	return s.offset.AddSize(s.size)
}

func (s *Subvolume) NonZeroBytes(message string) {
	nonZeros := 0
	for _, b := range s.Data {
		if b != 0 {
			nonZeros++
		}
	}
	fmt.Printf("%s> Number of non-zeros: %d\n", message, nonZeros)
}

// VoxelCoordToDataIndex returns an index that can be used to access the first byte
// corresponding to the given voxel coordinate in the subvolume's Data slice.  The
// data element will constitute p.BytesPerVoxel bytes.
func (s *Subvolume) VoxelCoordToDataIndex(c dvid.VoxelCoord) (index int) {
	pt := c.Sub(s.offset)
	index = int(pt[2]*s.size[0]*s.size[1] + pt[1]*s.size[0] + pt[0])
	index *= s.BytesPerVoxel
	return
}

// SubvolCommand fulfills a Request interface and uses Subvol for input.
type SubvolRequest struct {
	dvid.Request
	Subvolume
}

// SliceRequest fulfills a Request interface and uses SliceVoxels for input.
type SliceRequest struct {
	dvid.Request
	SliceVoxels
}

// SliceResponse is a Response with SliceVoxels payload
type SliceResponse struct {
	dvid.SimpleResponse
	SliceVoxels
}

// TypeService is an interface for operations using arbitrary data types.
type TypeService interface {
	// Name describes a data type and may not be unique.
	Name() string

	// Url returns the unique package name that fulfills the DVID Data interface
	Url() UrlString

	// Version describes the version identifier of this data type code
	Version() string

	// BlockSize returns the block size for this data type
	BlockSize() dvid.VoxelCoord

	// SpatialIndexing returns the spatial indexing scheme employed by this data type
	SpatialIndexing() SpatialIndexScheme

	// IsolatedKeys returns true if this type's data keys should be grouped by itself.
	IsolatedKeys() bool

	// Help returns a string explaining how to use a data type's service
	Help(textHelp string) string

	// Do handles RPC commands specific to a data type
	DoRPC(request *Request, reply dvid.Response) error

	// DoHTTP handles HTTP requests specific to a data type
	DoHTTP(w http.ResponseWriter, r *http.Request, svc *Service, apiPrefixURL string)

	// Returns standard error response for unknown commands
	UnknownCommand(request *Request) error
}

// DatatypeID uniquely identifies a DVID-supported data type and provides a 
// shorthand name.
type DatatypeID struct {
	// Name describes a data type and may not be unique.
	TypeName string

	// Url specifies the unique package name that fulfills the DVID Data interface
	TypeUrl UrlString

	// Version describes the version identifier of this data type code
	TypeVersion string
}

func MakeDatatypeID(name string, url UrlString, version string) DatatypeID {
	return DatatypeID{name, url, version}
}

func (id DatatypeID) Name() string { return id.TypeName }

func (id DatatypeID) Url() UrlString { return id.TypeUrl }

func (id DatatypeID) Version() string { return id.TypeVersion }

// Datatype adds instance-specific information on how a data type is being used
// and for what named data sets it's being used.
type Datatype struct {
	DatatypeID

	// Block size
	BlockMax dvid.VoxelCoord

	// Spatial indexing scheme
	Indexing SpatialIndexScheme

	// isolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// The following functions supply standard operations necessary across all supported
// data types and are centralized here for DRY reasons.  Each supported data type
// embeds the datastore.Datatype type and gets these functions for free.

func (datatype *Datatype) BlockSize() dvid.VoxelCoord {
	return datatype.BlockMax
}

func (datatype *Datatype) SpatialIndexing() SpatialIndexScheme {
	return datatype.Indexing
}

func (datatype *Datatype) IsolatedKeys() bool {
	return datatype.IsolateData
}

func (datatype *Datatype) Help(typeHelp string) string {
	return fmt.Sprintf(helpMessage+typeHelp, datatype.TypeName, datatype.TypeUrl)
}

func (datatype *Datatype) UnknownCommand(request *Request) error {
	return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
		datatype.TypeName, datatype.TypeUrl, request.TypeCommand())
}

// CompiledTypes is the set of registered data types compiled into DVID and
// held as a global variable initialized at runtime.
var CompiledTypes map[UrlString]TypeService

// CompiledTypeNames returns a list of data type names compiled into this DVID. 
func CompiledTypeNames() string {
	var names []string
	for _, datatype := range CompiledTypes {
		names = append(names, datatype.Name())
	}
	return strings.Join(names, ", ")
}

// CompiledTypeUrls returns a list of data type urls supported by this DVID. 
func CompiledTypeUrls() string {
	var urls []string
	for url, _ := range CompiledTypes {
		urls = append(urls, string(url))
	}
	return strings.Join(urls, ", ")
}

// CompiledTypeChart returns a chart (names/urls) of data types compiled into this DVID. 
func CompiledTypeChart() string {
	var text string = "\nData types compiled into this DVID\n\n"
	writeLine := func(name string, url UrlString) {
		text += fmt.Sprintf("%-15s   %s\n", name, url)
	}
	writeLine("Name", "Url")
	for _, datatype := range CompiledTypes {
		writeLine(datatype.Name(), datatype.Url())
	}
	return text + "\n"
}

// RegisterDatatype registers a data type for DVID use.
func RegisterDatatype(t TypeService) {
	if CompiledTypes == nil {
		CompiledTypes = make(map[UrlString]TypeService)
	}
	CompiledTypes[t.Url()] = t
}

/***** Image Utilities ******/

func LoadImage(filename string) (img image.Image, format string, err error) {
	var file *os.File
	file, err = os.Open(filename)
	if err != nil {
		err = fmt.Errorf("Unable to open image (%s).  Is this visible to server process?",
			filename)
		return
	}
	img, format, err = image.Decode(file)
	if err != nil {
		return
	}
	err = file.Close()
	return
}
