/*
Package googlevoxels implements DVID support for multi-scale tiles and volumes in XY, XZ,
and YZ orientation using the Google BrainMaps API.
*/
package googlevoxels

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/imagetile"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/golang/snappy"
	lz4 "github.com/janelia-flyem/go/golz4-updated"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/googlevoxels"
	TypeName = "googlevoxels"
)

const helpMessage = `
API for datatypes derived from googlevoxels (github.com/janelia-flyem/dvid/datatype/googlevoxels)
=================================================================================================

Command-line:

$ dvid repo <UUID> new googlevoxels <data name> <settings...>

	Adds voxel support using Google BrainMaps API.

	Example:

	$ dvid repo 3f8c new googlevoxels grayscale volumeid=281930192:stanford jwtfile=/foo/myname-319.json

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "mygrayscale"
    settings       Configuration settings in "key=value" format separated by spaces.

    Required Configuration Settings (case-insensitive keys)

    volumeid       The globally unique identifier of the volume within Google BrainMaps API.
    jwtfile        Path to JSON Web Token file downloaded from http://console.developers.google.com.
                   Under the BrainMaps API, visit the Credentials area, create credentials for a
                   service account key, then download that JWT file.

    Optional Configuration Settings (case-insensitive keys)

    tilesize       Default size in pixels along one dimension of square tile.  If unspecified, 512.


$ dvid googlevoxels volumes <jwtfile>

	Contacts Google BrainMaps API and returns the available volume ids for a user identified by a 
	JSON Web Token (JWT) file.

	Example:

	$ dvid googlevoxels volumes /foo/myname-319.json

    Arguments:

    jwtfile        Path to JSON Web Token file downloaded from http://console.developers.google.com.
                   Under the BrainMaps API, visit the Credentials area, create credentials for a
                   service account key, then download that JWT file.


    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info

    Retrieves characteristics of this data in JSON format.

    Example: 

    GET <api URL>/node/3f8c/grayscale/info

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of googlevoxels data.


GET  <api URL>/node/<UUID>/<data name>/tile/<dims>/<scaling>/<tile coord>[?options]

    Retrieves a tile of named data within a version node.  The default tile size is used unless
    the query string "tilesize" is provided.

    Example: 

    GET <api URL>/node/3f8c/grayscale/tile/xy/0/10_10_20

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  Example: "0_2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    scaling       Value from 0 (original resolution) to N where each step is downres by 2.
    tile coord    The tile coordinate in "x_y_z" format.  See discussion of scaling above.

  	Query-string options:

    tilesize      Size in pixels along one dimension of square tile.
  	noblanks	  If true, any tile request for tiles outside the currently stored extents
  				  will return a placeholder.
    format        "png", "jpeg" (default: "png")
                    jpeg allows lossy quality setting, e.g., "jpeg:80"  (0 <= quality <= 100)
                    png allows compression levels, e.g., "png:7"  (0 <= level <= 9)


GET  <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>[/<format>][?queryopts]

    Retrieves either 2d images (PNG by default) or 3d binary data, depending on the dims parameter.  
    The 3d binary data response has "Content-type" set to "application/octet-stream" and is an array of 
    voxel values in ZYX order (X iterates most rapidly).

    Example: 

    GET <api URL>/node/3f8c/segmentation/raw/0_1/512_256/0_0_100/jpg:80

    Returns a raw XY slice (0th and 1st dimensions) with width (x) of 512 voxels and
    height (y) of 256 voxels with offset (0,0,100) in JPG format with quality 80.
    By "raw", we mean that no additional processing is applied based on voxel
    resolutions to make sure the retrieved image has isotropic pixels.
    The example offset assumes the "grayscale" data in version node "3f8c" is 3d.
    The "Content-type" of the HTTP response should agree with the requested format.
    For example, returned PNGs will have "Content-type" of "image/png", and returned
    nD data will be "application/octet-stream". 

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    dims          The axes of data extraction in form "i_j_k,..."  
                    Slice strings ("xy", "xz", or "yz") are also accepted.
                    Example: "0_2" is XZ, and "0_1_2" is a 3d subvolume.
    size          Size in voxels along each dimension specified in <dims>.
    offset        Gives coordinate of first voxel using dimensionality of data.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: uses default "octet-stream".

    Query-string Options:

    compression   Allows retrieval or submission of 3d data in "snappy (default) or "lz4" format.  
                     The 2d data will ignore this and use the image-based codec.
  	scale         Default is 0.  For scale N, returns an image down-sampled by a factor of 2^N.
    throttle      Only works for 3d data requests.  If "true", makes sure only N compute-intense operation 
    				(all API calls that can be throttled) are handled.  If the server can't initiate the API 
    				call right away, a 503 (Service Unavailable) status code is returned.
`

func init() {
	datastore.Register(NewType())

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

var (
	DefaultTileSize   int32  = 512
	DefaultTileFormat string = "png"
	bmapsPrefix       string = "https://brainmaps.googleapis.com/v1beta2"
)

// Type embeds the datastore's Type to create a unique type with tile functions.
// Refinements of general tile types can be implemented by embedding this type,
// choosing appropriate # of channels and bytes/voxel, overriding functions as
// needed, and calling datastore.Register().
// Note that these fields are invariant for all instances of this type.  Fields
// that can change depending on the type of data (e.g., resolution) should be
// in the Data type.
type Type struct {
	datastore.Type
}

// NewDatatype returns a pointer to a new voxels Datatype with default values set.
func NewType() *Type {
	return &Type{
		datastore.Type{
			Name:    "googlevoxels",
			URL:     "github.com/janelia-flyem/dvid/datatype/googlevoxels",
			Version: "0.1",
			Requirements: &storage.Requirements{
				Batcher: true,
			},
		},
	}
}

// --- TypeService interface ---

// NewData returns a pointer to new googlevoxels data with default values.
func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	// Make sure we have needed volumeid and authentication key.
	volumeid, found, err := c.GetString("volumeid")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make googlevoxels data without valid 'volumeid' setting.")
	}
	jwtfile, found, err := c.GetString("jwtfile")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Cannot make googlevoxels data without valid 'jwtfile' specifying path to JSON Web Token")
	}

	// Read in the JSON Web Token
	jwtdata, err := ioutil.ReadFile(jwtfile)
	if err != nil {
		return nil, fmt.Errorf("Cannot load JSON Web Token file (%s): %v", jwtfile, err)
	}
	conf, err := google.JWTConfigFromJSON(jwtdata, "https://www.googleapis.com/auth/brainmaps")
	if err != nil {
		return nil, fmt.Errorf("Cannot establish JWT Config file from Google: %v", err)
	}
	client := conf.Client(oauth2.NoContext)

	// Make URL call to get the available scaled volumes.
	url := fmt.Sprintf("%s/volumes/%s", bmapsPrefix, volumeid)
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Error getting volume metadata from Google: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code %d returned when getting volume metadata for %q", resp.StatusCode, volumeid)
	}
	metadata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	var m struct {
		Geoms Geometries `json:"geometry"`
	}
	if err := json.Unmarshal(metadata, &m); err != nil {
		return nil, fmt.Errorf("Error decoding volume JSON metadata: %v", err)
	}
	dvid.Infof("Successfully got geometries:\nmetadata:\n%s\nparsed JSON:\n%v\n", metadata, m)

	// Compute the mapping from tile scale/orientation to scaled volume index.
	geomMap := GeometryMap{}

	// (1) Find the highest resolution geometry.
	var highResIndex GeometryIndex
	minVoxelSize := dvid.NdFloat32{10000, 10000, 10000}
	for i, geom := range m.Geoms {
		if geom.PixelSize[0] < minVoxelSize[0] || geom.PixelSize[1] < minVoxelSize[1] || geom.PixelSize[2] < minVoxelSize[2] {
			minVoxelSize = geom.PixelSize
			highResIndex = GeometryIndex(i)
		}
	}
	dvid.Infof("Google voxels %q: found highest resolution was geometry %d: %s\n", name, highResIndex, minVoxelSize)

	// (2) For all geometries, find out what the scaling is relative to the highest resolution pixel size.
	for i, geom := range m.Geoms {
		if i == int(highResIndex) {
			geomMap[GSpec{0, XY}] = highResIndex
			geomMap[GSpec{0, XZ}] = highResIndex
			geomMap[GSpec{0, YZ}] = highResIndex
			geomMap[GSpec{0, XYZ}] = highResIndex
		} else {
			scaleX := geom.PixelSize[0] / minVoxelSize[0]
			scaleY := geom.PixelSize[1] / minVoxelSize[1]
			scaleZ := geom.PixelSize[2] / minVoxelSize[2]
			var shape Shape
			switch {
			case scaleX > scaleZ && scaleY > scaleZ:
				shape = XY
			case scaleX > scaleY && scaleZ > scaleY:
				shape = XZ
			case scaleY > scaleX && scaleZ > scaleX:
				shape = YZ
			default:
				shape = XYZ
			}
			var mag float32
			if scaleX > mag {
				mag = scaleX
			}
			if scaleY > mag {
				mag = scaleY
			}
			if scaleZ > mag {
				mag = scaleZ
			}
			scaling := log2(mag)
			geomMap[GSpec{scaling, shape}] = GeometryIndex(i)
			dvid.Infof("%s at scaling %d set to geometry %d: resolution %s\n", shape, scaling, i, geom.PixelSize)
		}
	}

	// Create a client that will be authorized and authenticated on behalf of the account.

	// Initialize the googlevoxels data
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data: basedata,
		Properties: Properties{
			VolumeID:     volumeid,
			JWT:          string(jwtdata),
			TileSize:     DefaultTileSize,
			GeomMap:      geomMap,
			Scales:       m.Geoms,
			HighResIndex: highResIndex,
		},
		client: client,
	}
	return data, nil
}

// Do handles command-line requests to the Google BrainMaps API
func (dtype *Type) Do(cmd datastore.Request, reply *datastore.Response) error {
	switch cmd.Argument(1) {
	case "volumes":
		// Read in the JSON Web Token
		jwtdata, err := ioutil.ReadFile(cmd.Argument(2))
		if err != nil {
			return fmt.Errorf("Cannot load JSON Web Token file (%s): %v", cmd.Argument(2), err)
		}
		conf, err := google.JWTConfigFromJSON(jwtdata, "https://www.googleapis.com/auth/brainmaps")
		if err != nil {
			return fmt.Errorf("Cannot establish JWT Config file from Google: %v", err)
		}
		client := conf.Client(oauth2.NoContext)

		// Make the call.
		url := fmt.Sprintf("%s/volumes", bmapsPrefix)
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("Error getting volumes metadata from Google: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Unexpected status code %d returned when getting volumes for user", resp.StatusCode)
		}
		metadata, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		reply.Text = string(metadata)
		return nil

	default:
		return fmt.Errorf("unknown command for type %s", dtype.GetTypeName())
	}
}

// log2 returns the power of 2 necessary to cover the given value.
func log2(value float32) Scaling {
	var exp Scaling
	pow := float32(1.0)
	for {
		if pow >= value {
			return exp
		}
		pow *= 2
		exp++
	}
}

func (dtype *Type) Help() string {
	return helpMessage
}

// GSpec encapsulates the scale and orientation of a tile.
type GSpec struct {
	scaling Scaling
	shape   Shape
}

func (ts GSpec) MarshalBinary() ([]byte, error) {
	return []byte{byte(ts.scaling), byte(ts.shape)}, nil
}

func (ts *GSpec) UnmarshalBinary(data []byte) error {
	if len(data) != 2 {
		return fmt.Errorf("GSpec serialization is 2 bytes.  Got %d bytes instead: %v", len(data), data)
	}
	ts.scaling = Scaling(data[0])
	ts.shape = Shape(data[1])
	return nil
}

// GetGSpec returns a GSpec for a given scale and dvid Geometry.
func GetGSpec(scaling Scaling, shape dvid.DataShape) (*GSpec, error) {
	ts := new(GSpec)
	ts.scaling = scaling
	if err := ts.shape.FromShape(shape); err != nil {
		return nil, err
	}
	return ts, nil
}

// Scaling describes the resolution where 0 is the highest resolution
type Scaling uint8

// Shape describes the orientation of a 2d or 3d image.
type Shape uint8

const (
	XY Shape = iota
	XZ
	YZ
	XYZ
)

func (s *Shape) FromShape(shape dvid.DataShape) error {
	switch {
	case shape.Equals(dvid.XY):
		*s = XY
	case shape.Equals(dvid.XZ):
		*s = XZ
	case shape.Equals(dvid.YZ):
		*s = YZ
	case shape.Equals(dvid.Vol3d):
		*s = XYZ
	default:
		return fmt.Errorf("No Google BrainMaps shape corresponds to DVID %s shape", shape)
	}
	return nil
}

func (s Shape) String() string {
	switch s {
	case XY:
		return "XY"
	case XZ:
		return "XZ"
	case YZ:
		return "YZ"
	case XYZ:
		return "XYZ"
	default:
		return "Unknown orientation"
	}
}

// GeometryMap provides a mapping from DVID scale (0 is highest res) and tile orientation
// to the specific geometry (Google "scale" value) that supports it.
type GeometryMap map[GSpec]GeometryIndex

func (gm GeometryMap) MarshalJSON() ([]byte, error) {
	s := "{"
	mapStr := make([]string, len(gm))
	i := 0
	for ts, gi := range gm {
		mapStr[i] = fmt.Sprintf(`"%s:%d": %d`, ts.shape, ts.scaling, gi)
		i++
	}
	s += strings.Join(mapStr, ",")
	s += "}"
	return []byte(s), nil
}

type GeometryIndex int

// Geometry corresponds to a Volume Geometry in Google BrainMaps API
type Geometry struct {
	VolumeSize   dvid.Point3d   `json:"volumeSize"`
	ChannelCount uint32         `json:"channelCount"`
	ChannelType  string         `json:"channelType"`
	PixelSize    dvid.NdFloat32 `json:"pixelSize"`
}

// JSON from Google API encodes unsigned long as string because javascript has limited max
// integers due to Javascript number types using double float.

type uint3d struct {
	X uint32
	Y uint32
	Z uint32
}

func (u *uint3d) UnmarshalJSON(b []byte) error {
	var m struct {
		X string `json:"x"`
		Y string `json:"y"`
		Z string `json:"z"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	x, err := strconv.Atoi(m.X)
	if err != nil {
		return fmt.Errorf("Could not parse X coordinate with unsigned long: %v", err)
	}
	u.X = uint32(x)

	y, err := strconv.Atoi(m.Y)
	if err != nil {
		return fmt.Errorf("Could not parse Y coordinate with unsigned long: %v", err)
	}
	u.Y = uint32(y)

	z, err := strconv.Atoi(m.Z)
	if err != nil {
		return fmt.Errorf("Could not parse Z coordinate with unsigned long: %v", err)
	}
	u.Z = uint32(z)
	return nil
}

func (i uint3d) String() string {
	return fmt.Sprintf("%d x %d x %d", i.X, i.Y, i.Z)
}

type float3d struct {
	X float32 `json:"x"`
	Y float32 `json:"y"`
	Z float32 `json:"z"`
}

func (f float3d) String() string {
	return fmt.Sprintf("%f x %f x %f", f.X, f.Y, f.Z)
}

func (g *Geometry) UnmarshalJSON(b []byte) error {
	if g == nil {
		return fmt.Errorf("Can't unmarshal JSON into nil Geometry")
	}
	var m struct {
		VolumeSize   uint3d  `json:"volumeSize"`
		ChannelCount string  `json:"channelCount"`
		ChannelType  string  `json:"channelType"`
		PixelSize    float3d `json:"pixelSize"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	g.VolumeSize = dvid.Point3d{int32(m.VolumeSize.X), int32(m.VolumeSize.Y), int32(m.VolumeSize.Z)}
	g.PixelSize = dvid.NdFloat32{m.PixelSize.X, m.PixelSize.Y, m.PixelSize.Z}
	channels, err := strconv.Atoi(m.ChannelCount)
	if err != nil {
		return fmt.Errorf("Could not parse channelCount: %v", err)
	}
	g.ChannelCount = uint32(channels)
	g.ChannelType = m.ChannelType
	return nil
}

type Geometries []Geometry

// GoogleSubvolGeom encapsulates all information needed for voxel retrieval (aside from authentication)
// from the Google BrainMaps API, as well as processing the returned data.
type GoogleSubvolGeom struct {
	shape    Shape
	offset   dvid.Point3d
	size     dvid.Point3d // This is the size we can retrieve, not necessarily the requested size
	sizeWant dvid.Point3d // This is the requested size.
	gi       GeometryIndex
	edge     bool // Is the tile on the edge, i.e., partially outside a scaled volume?
	outside  bool // Is the tile totally outside any scaled volume?

	// cached data that immediately follows from the geometry index
	channelCount  uint32
	channelType   string
	bytesPerVoxel int32
}

// GetGoogleSubvolGeom returns a google-specific voxel spec, which includes how the data is positioned relative to
// scaled volume boundaries.  Not that the size parameter is the desired size and not what is required to fit
// within a scaled volume.
func (d *Data) GetGoogleSubvolGeom(scaling Scaling, shape dvid.DataShape, offset dvid.Point3d, size dvid.Point) (*GoogleSubvolGeom, error) {
	gsg := new(GoogleSubvolGeom)
	if err := gsg.shape.FromShape(shape); err != nil {
		return nil, err
	}
	gsg.offset = offset

	// If 2d plane, convert combination of plane and size into 3d size.
	if size.NumDims() == 2 {
		size2d := size.(dvid.Point2d)
		sizeWant, err := dvid.GetPoint3dFrom2d(shape, size2d, 1)
		if err != nil {
			return nil, err
		}
		gsg.sizeWant = sizeWant
	} else {
		var ok bool
		gsg.sizeWant, ok = size.(dvid.Point3d)
		if !ok {
			return nil, fmt.Errorf("Can't convert %v to dvid.Point3d", size)
		}
	}

	// Determine which geometry is appropriate given the scaling and the shape/orientation
	tileSpec, err := GetGSpec(scaling, shape)
	if err != nil {
		return nil, err
	}
	geomIndex, found := d.GeomMap[*tileSpec]
	if !found {
		return nil, fmt.Errorf("Could not find scaled volume in %q for %s with scaling %d", d.DataName(), shape, scaling)
	}
	geom := d.Scales[geomIndex]
	gsg.gi = geomIndex
	gsg.channelCount = geom.ChannelCount
	gsg.channelType = geom.ChannelType

	// Get the # bytes for each pixel
	switch geom.ChannelType {
	case "UINT8":
		gsg.bytesPerVoxel = 1
	case "FLOAT":
		gsg.bytesPerVoxel = 4
	case "UINT64":
		gsg.bytesPerVoxel = 8
	default:
		return nil, fmt.Errorf("Unknown volume channel type in %s: %s", d.DataName(), geom.ChannelType)
	}

	// Check if the requested area is completely outside the volume.
	volumeSize := geom.VolumeSize
	if offset[0] >= volumeSize[0] || offset[1] >= volumeSize[1] || offset[2] >= volumeSize[2] {
		gsg.outside = true
		return gsg, nil
	}

	// Check if the requested shape is on the edge and adjust size.
	adjSize := gsg.sizeWant
	maxpt := offset.Add(adjSize)
	for i := uint8(0); i < 3; i++ {
		if maxpt.Value(i) > volumeSize[i] {
			gsg.edge = true
			adjSize[i] = volumeSize[i] - offset[i]
		}
	}
	gsg.size = adjSize

	return gsg, nil
}

// GetURL returns the base API URL for retrieving an image.  Note that the authentication key
// or token needs to be added to the returned string to form a valid URL.  The formatStr
// parameter is of the form "jpeg" or "jpeg:80" or "png:8" where an optional compression
// level follows the image format and a colon.  Leave formatStr empty for default.
func (gsg GoogleSubvolGeom) GetURL(volumeid, formatStr string) (string, error) {
	url := fmt.Sprintf("%s/volumes/%s/binary", bmapsPrefix, volumeid)
	if gsg.shape == XYZ {
		url += "/subvolume"
	} else {
		url += "/tile"
	}
	url += fmt.Sprintf("/corner=%d,%d,%d", gsg.offset[0], gsg.offset[1], gsg.offset[2])
	url += fmt.Sprintf("/size=%d,%d,%d", gsg.size[0], gsg.size[1], gsg.size[2])
	url += fmt.Sprintf("/scale=%d", gsg.gi)

	if formatStr != "" {
		format := strings.Split(formatStr, ":")
		var gformat string
		switch format[0] {
		case "jpg", "jpeg", "JPG":
			gformat = "JPEG"
		case "png":
			gformat = "PNG"
		default:
			return "", fmt.Errorf("Google tiles only support JPEG or PNG formats, not %q", format[0])
		}
		url += fmt.Sprintf("/imageFormat=%s", gformat)
		if len(format) > 1 {
			level, err := strconv.Atoi(format[1])
			if err != nil {
				return url, err
			}
			switch format[0] {
			case "jpeg":
				url += fmt.Sprintf("/jpegQuality=%d", level)
			case "png":
				url += fmt.Sprintf("/pngCompressionLevel=%d", level)
			}
		}
	}
	if gsg.shape == XYZ {
		if formatStr != "" {
			url += "/subvolumeFormat=SINGLE_IMAGE"
		} else {
			url += "/subvolumeFormat=raw_snappy"
		}
	}
	url += "?alt=media"

	return url, nil
}

// padData takes returned data and pads it to full expected size.
// currently assumes that data padding needed on far edges, not near edges.
func (gsg GoogleSubvolGeom) padData(data []byte) ([]byte, error) {
	if gsg.size[0]*gsg.size[1]*gsg.size[2]*gsg.bytesPerVoxel != int32(len(data)) {
		return nil, fmt.Errorf("Before padding, for %d x %d x %d bytes/voxel tile, received %d bytes",
			gsg.size[0], gsg.size[1], gsg.bytesPerVoxel, len(data))
	}

	inRowBytes := gsg.size[0] * gsg.bytesPerVoxel
	outRowBytes := gsg.sizeWant[0] * gsg.bytesPerVoxel
	outBytes := outRowBytes * gsg.sizeWant[1]
	out := make([]byte, outBytes, outBytes)
	inI := int32(0)
	outI := int32(0)
	for y := int32(0); y < gsg.size[1]; y++ {
		copy(out[outI:outI+inRowBytes], data[inI:inI+inRowBytes])
		inI += inRowBytes
		outI += outRowBytes
	}
	return out, nil
}

// Properties are additional properties for keyvalue data instances beyond those
// in standard datastore.Data.   These will be persisted to metadata storage.
type Properties struct {
	// Necessary information to select data from Google BrainMaps API.
	VolumeID string
	JWT      string

	// Default size in pixels along one dimension of square tile.
	TileSize int32

	// GeomMap provides mapping between scale and various image shapes to Google scaling index.
	GeomMap GeometryMap

	// Scales is the list of available precomputed scales ("geometries" in Google terms) for this data.
	Scales Geometries

	// HighResIndex is the geometry that is the highest resolution among the available scaled volumes.
	HighResIndex GeometryIndex

	// OAuth2 configuration
	oa2conf *oauth2.Config
}

// CopyPropertiesFrom copies the data instance-specific properties from a given
// data instance into the receiver's properties.  Fulfills the datastore.PropertyCopier interface.
func (d *Data) CopyPropertiesFrom(src datastore.DataService, fs storage.FilterSpec) error {
	d2, ok := src.(*Data)
	if !ok {
		return fmt.Errorf("unable to copy properties from non-imageblk data %q", src.DataName())
	}
	// These should all be immutable so can have shared reference with source.
	d.VolumeID = d2.VolumeID
	d.JWT = d2.JWT
	d.TileSize = d2.TileSize
	d.GeomMap = d2.GeomMap
	d.Scales = d2.Scales
	d.HighResIndex = d2.HighResIndex
	d.oa2conf = d2.oa2conf

	return nil
}

// MarshalJSON handles JSON serialization for googlevoxels Data.  It adds "Levels" metadata equivalent
// to imagetile's tile specification so clients can treat googlevoxels tile API identically to
// imagetile.  Sensitive information like AuthKey are withheld.
func (p Properties) MarshalJSON() ([]byte, error) {
	var minTileCoord, maxTileCoord dvid.Point3d
	if len(p.Scales) > 0 {
		vol := p.Scales[0].VolumeSize
		maxX := vol[0] / p.TileSize
		if vol[0]%p.TileSize > 0 {
			maxX++
		}
		maxY := vol[1] / p.TileSize
		if vol[1]%p.TileSize > 0 {
			maxY++
		}
		maxZ := vol[2] / p.TileSize
		if vol[2]%p.TileSize > 0 {
			maxZ++
		}
		maxTileCoord = dvid.Point3d{maxX, maxY, maxZ}
	}
	return json.Marshal(struct {
		VolumeID     string
		MinTileCoord dvid.Point3d
		MaxTileCoord dvid.Point3d
		TileSize     int32
		GeomMap      GeometryMap
		Scales       Geometries
		HighResIndex GeometryIndex
		Levels       imagetile.TileSpec
	}{
		p.VolumeID,
		minTileCoord,
		maxTileCoord,
		p.TileSize,
		p.GeomMap,
		p.Scales,
		p.HighResIndex,
		getGSpec(p.TileSize, p.Scales[p.HighResIndex], p.GeomMap),
	})
}

// Converts Google BrainMaps scaling to imagetile-style tile specifications.
// This assumes that Google levels always downsample by 2.
func getGSpec(tileSize int32, hires Geometry, geomMap GeometryMap) imagetile.TileSpec {
	// Determine how many levels we have by the max of any orientation.
	// TODO -- Warn user in some way if BrainMaps API has levels in one orientation but not in other.
	var maxScale Scaling
	for tileSpec := range geomMap {
		if tileSpec.scaling > maxScale {
			maxScale = tileSpec.scaling
		}
	}

	// Create the levels from 0 (hires) to max level.
	levelSpec := imagetile.LevelSpec{
		TileSize: dvid.Point3d{tileSize, tileSize, tileSize},
	}
	levelSpec.Resolution = make(dvid.NdFloat32, 3)
	copy(levelSpec.Resolution, hires.PixelSize)
	ms2dGSpec := make(imagetile.TileSpec, maxScale+1)
	for scale := Scaling(0); scale <= maxScale; scale++ {
		curSpec := levelSpec.Duplicate()
		ms2dGSpec[imagetile.Scaling(scale)] = imagetile.TileScaleSpec{LevelSpec: curSpec}
		levelSpec.Resolution[0] *= 2
		levelSpec.Resolution[1] *= 2
		levelSpec.Resolution[2] *= 2
	}
	return ms2dGSpec
}

// Data embeds the datastore's Data and extends it with voxel-specific properties.
type Data struct {
	*datastore.Data
	Properties

	client *http.Client // HTTP client that provides Authorization headers
}

// Returns a potentially cached client that handles authorization to Google.
// Assumes a JSON Web Token has been loaded into Data or else returns an error.
func (d *Data) GetClient() (*http.Client, error) {
	if d.client != nil {
		return d.client, nil
	}
	if d.Properties.JWT == "" {
		return nil, fmt.Errorf("No JSON Web Token has been set for this data")
	}
	conf, err := google.JWTConfigFromJSON([]byte(d.Properties.JWT), "https://www.googleapis.com/auth/brainmaps")
	if err != nil {
		return nil, fmt.Errorf("Cannot establish JWT Config file from Google: %v", err)
	}
	client := conf.Client(oauth2.NoContext)
	d.client = client
	return client, nil
}

func (d *Data) GetVoxelSize(ts *GSpec) (dvid.NdFloat32, error) {
	if d.Scales == nil || len(d.Scales) == 0 {
		return nil, fmt.Errorf("%s has no geometries and therefore no volumes for access", d.DataName())
	}
	if d.GeomMap == nil {
		return nil, fmt.Errorf("%s has not been initialized and can't return voxel sizes", d.DataName())
	}
	if ts == nil {
		return nil, fmt.Errorf("Can't get voxel sizes for nil tile spec!")
	}
	scaleIndex := d.GeomMap[*ts]
	if int(scaleIndex) > len(d.Scales) {
		return nil, fmt.Errorf("Can't map tile spec (%v) to available geometries", *ts)
	}
	geom := d.Scales[scaleIndex]
	return geom.PixelSize, nil
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- DataService interface ---

func (d *Data) Help() string {
	return helpMessage
}

// getBlankTileData returns a background 2d tile data
func (d *Data) getBlankTileImage(tile *GoogleSubvolGeom) (image.Image, error) {
	if tile == nil {
		return nil, fmt.Errorf("Can't get blank tile for unknown tile spec")
	}
	if d.Scales == nil || len(d.Scales) <= int(tile.gi) {
		return nil, fmt.Errorf("Scaled volumes for %s not suitable for tile spec: %d scales <= %d tile scales", d.DataName(), len(d.Scales), int(tile.gi))
	}

	// Generate the blank image
	numBytes := tile.sizeWant[0] * tile.sizeWant[1] * tile.bytesPerVoxel
	data := make([]byte, numBytes, numBytes)
	return dvid.GoImageFromData(data, int(tile.sizeWant[0]), int(tile.sizeWant[1]))
}

func (d *Data) serveTile(w http.ResponseWriter, r *http.Request, geom *GoogleSubvolGeom, formatStr string, noblanks bool) error {
	// If it's outside, write blank tile unless user wants no blanks.
	if geom.outside {
		if noblanks {
			http.NotFound(w, r)
			return fmt.Errorf("Requested tile is outside of available volume.")
		}
		img, err := d.getBlankTileImage(geom)
		if err != nil {
			return err
		}
		return dvid.WriteImageHttp(w, img, formatStr)
	}

	// If we are within volume, get data from Google.
	url, err := geom.GetURL(d.VolumeID, formatStr)
	if err != nil {
		return err
	}

	timedLog := dvid.NewTimeLog()
	client, err := d.GetClient()
	if err != nil {
		dvid.Errorf("Can't get OAuth2 connection to Google: %v\n", err)
		return err
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	timedLog.Infof("PROXY HTTP to Google: %s, returned response %d", url, resp.StatusCode)
	defer resp.Body.Close()

	// Set the image header
	if err := dvid.SetImageHeader(w, formatStr); err != nil {
		return err
	}

	// If it's on edge, we need to pad the tile to the tile size.
	if geom.edge {
		// We need to read whole thing in to pad it.
		data, err := ioutil.ReadAll(resp.Body)
		timedLog.Infof("Got edge tile from Google, %d bytes\n", len(data))
		if err != nil {
			return err
		}
		paddedData, err := geom.padData(data)
		if err != nil {
			return err
		}
		_, err = w.Write(paddedData)
		return err
	}

	// If we aren't on edge or outside, our return status should be OK.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Unexpected status code %d on tile request (%q, volume id %q)", resp.StatusCode, d.DataName(), d.VolumeID)
	}

	// Just send the data as we get it from Google in chunks.
	respBytes := 0
	const BufferSize = 32 * 1024
	buf := make([]byte, BufferSize)
	for {
		n, err := resp.Body.Read(buf)
		respBytes += n
		eof := (err == io.EOF)
		if err != nil && !eof {
			return err
		}
		if _, err = w.Write(buf[:n]); err != nil {
			return err
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if eof {
			break
		}
	}
	timedLog.Infof("Got non-edge tile from Google, %d bytes\n", respBytes)
	return nil
}

func (d *Data) serveVolume(w http.ResponseWriter, r *http.Request, geom *GoogleSubvolGeom, noblanks bool, formatstr string) error {
	// If it's outside, write blank tile unless user wants no blanks.
	if geom.outside {
		if noblanks {
			http.NotFound(w, r)
			return fmt.Errorf("Requested subvolume is outside of available volume.")
		}
		return nil
	}

	// If we are within volume, get data from Google.
	url, err := geom.GetURL(d.VolumeID, formatstr)
	if err != nil {
		return err
	}

	timedLog := dvid.NewTimeLog()
	client, err := d.GetClient()
	if err != nil {
		dvid.Errorf("Can't get OAuth2 connection to Google: %v\n", err)
		return err
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	timedLog.Infof("PROXY HTTP to Google: %s, returned response %d", url, resp.StatusCode)
	defer resp.Body.Close()

	// If it's on edge, we need to pad the subvolume to the requested size.
	if geom.edge {
		return fmt.Errorf("Googlevoxels subvolume GET does not pad data on edge at this time")
	}

	// If we aren't on edge or outside, our return status should be OK.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Unexpected status code %d on volume request (%q, volume id %q)", resp.StatusCode, d.DataName(), d.VolumeID)
	}

	w.Header().Set("Content-type", "application/octet-stream")

	queryStrings := r.URL.Query()
	compression := queryStrings.Get("compression")

	switch compression {
	case "lz4":
		// Decompress snappy
		sdata, err := ioutil.ReadAll(resp.Body)
		timedLog.Infof("Got snappy-encoded subvolume from Google, %d bytes\n", len(sdata))
		if err != nil {
			return err
		}
		data, err := snappy.Decode(nil, sdata)
		if err != nil {
			return err
		}
		// Recompress and transmit as lz4
		lz4data := make([]byte, lz4.CompressBound(data))
		outSize, err := lz4.Compress(data, lz4data)
		if err != nil {
			return err
		}
		if _, err := w.Write(lz4data[:outSize]); err != nil {
			return err
		}
		timedLog.Infof("Sent lz4-encoded subvolume from DVID, %d bytes\n", outSize)

	default: // "snappy"
		// Just stream data from Google
		respBytes := 0
		const BufferSize = 32 * 1024
		buf := make([]byte, BufferSize)
		for {
			n, err := resp.Body.Read(buf)
			respBytes += n
			eof := (err == io.EOF)
			if err != nil && !eof {
				return err
			}
			if _, err = w.Write(buf[:n]); err != nil {
				return err
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if eof {
				break
			}
		}
		timedLog.Infof("Proxied encoded subvolume from Google, %d bytes\n", respBytes)
	}

	return nil
}

// See if scaling was specified in query string, otherwise return high-res (scale 0)
func getScale(r *http.Request) (Scaling, error) {
	var scale Scaling
	queryStrings := r.URL.Query()
	scalingStr := queryStrings.Get("scale")
	if scalingStr != "" {
		scale64, err := strconv.ParseUint(scalingStr, 10, 8)
		if err != nil {
			return 0, fmt.Errorf("Illegal tile scale: %s (%v)", scalingStr, err)
		}
		scale = Scaling(scale64)
	}
	return scale, nil
}

func (d *Data) handleImage2d(w http.ResponseWriter, r *http.Request, parts []string) error {
	return nil
}

// handleImageReq returns an image with appropriate Content-Type set.  This function differs
// from handleTileReq in the way parameters are passed to it.  handleTileReq accepts a tile coordinate.
// This function allows arbitrary offset and size, unconstrained by tile sizes.
func (d *Data) handleImageReq(w http.ResponseWriter, r *http.Request, parts []string) error {
	if len(parts) < 7 {
		return fmt.Errorf("%q must be followed by shape/size/offset", parts[3])
	}
	shapeStr, sizeStr, offsetStr := parts[4], parts[5], parts[6]
	planeStr := dvid.DataShapeString(shapeStr)
	plane, err := planeStr.DataShape()
	if err != nil {
		return err
	}

	var size dvid.Point
	if size, err = dvid.StringToPoint(sizeStr, "_"); err != nil {
		return err
	}
	offset, err := dvid.StringToPoint3d(offsetStr, "_")
	if err != nil {
		return err
	}

	// Determine how this request sits in the available scaled volumes.
	scale, err := getScale(r)
	if err != nil {
		return err
	}
	geom, err := d.GetGoogleSubvolGeom(scale, plane, offset, size)
	if err != nil {
		return err
	}

	switch plane.ShapeDimensions() {
	case 2:
		var formatStr string
		if len(parts) >= 8 {
			formatStr = parts[7]
		}
		if formatStr == "" {
			formatStr = DefaultTileFormat
		}

		return d.serveTile(w, r, geom, formatStr, false)
	case 3:
		if len(parts) >= 8 {
			return d.serveVolume(w, r, geom, false, parts[7])
		} else {
			return d.serveVolume(w, r, geom, false, "")
		}
	}
	return nil
}

// handleTileReq returns a tile with appropriate Content-Type set.
func (d *Data) handleTileReq(w http.ResponseWriter, r *http.Request, parts []string) error {

	if len(parts) < 7 {
		return fmt.Errorf("'tile' request must be following by plane, scale level, and tile coordinate")
	}
	planeStr, scalingStr, coordStr := parts[4], parts[5], parts[6]
	queryStrings := r.URL.Query()

	var noblanks bool
	noblanksStr := dvid.InstanceName(queryStrings.Get("noblanks"))
	if noblanksStr == "true" {
		noblanks = true
	}

	var tilesize int32 = DefaultTileSize
	tileSizeStr := queryStrings.Get("tilesize")
	if tileSizeStr != "" {
		tilesizeInt, err := strconv.Atoi(tileSizeStr)
		if err != nil {
			return err
		}
		tilesize = int32(tilesizeInt)
	}
	size := dvid.Point2d{tilesize, tilesize}

	var formatStr string
	if len(parts) >= 8 {
		formatStr = parts[7]
	}
	if formatStr == "" {
		formatStr = DefaultTileFormat
	}

	// Parse the tile specification
	plane := dvid.DataShapeString(planeStr)
	shape, err := plane.DataShape()
	if err != nil {
		err = fmt.Errorf("Illegal tile plane: %s (%v)", planeStr, err)
		server.BadRequest(w, r, err)
		return err
	}
	scale, err := strconv.ParseUint(scalingStr, 10, 8)
	if err != nil {
		err = fmt.Errorf("Illegal tile scale: %s (%v)", scalingStr, err)
		server.BadRequest(w, r, err)
		return err
	}
	tileCoord, err := dvid.StringToPoint(coordStr, "_")
	if err != nil {
		err = fmt.Errorf("Illegal tile coordinate: %s (%v)", coordStr, err)
		server.BadRequest(w, r, err)
		return err
	}

	// Convert tile coordinate to offset.
	var ox, oy, oz int32
	switch {
	case shape.Equals(dvid.XY):
		ox = tileCoord.Value(0) * tilesize
		oy = tileCoord.Value(1) * tilesize
		oz = tileCoord.Value(2)
	case shape.Equals(dvid.XZ):
		ox = tileCoord.Value(0) * tilesize
		oy = tileCoord.Value(1)
		oz = tileCoord.Value(2) * tilesize
	case shape.Equals(dvid.YZ):
		ox = tileCoord.Value(0)
		oy = tileCoord.Value(1) * tilesize
		oz = tileCoord.Value(2) * tilesize
	default:
		return fmt.Errorf("Unknown tile orientation: %s", shape)
	}

	// Determine how this request sits in the available scaled volumes.
	geom, err := d.GetGoogleSubvolGeom(Scaling(scale), shape, dvid.Point3d{ox, oy, oz}, size)
	if err != nil {
		server.BadRequest(w, r, err)
		return err
	}

	// Send the tile.
	return d.serveTile(w, r, geom, formatStr, noblanks)
}

// DoRPC handles the 'generate' command.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("Unknown command.  Data instance %q does not support any commands.  See API help.", d.DataName())
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()

	action := strings.ToLower(r.Method)
	switch action {
	case "get":
		// Acceptable
	default:
		server.BadRequest(w, r, "googlevoxels can only handle GET HTTP verbs at this time")
		return
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	if len(parts) < 4 {
		server.BadRequest(w, r, "incomplete API request")
		return
	}

	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "tile":
		if err := d.handleTileReq(w, r, parts); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: tile (%s)", r.Method, r.URL)

	case "raw":
		queryStrings := r.URL.Query()
		if throttle := queryStrings.Get("throttle"); throttle == "on" || throttle == "true" {
			if server.ThrottledHTTP(w) {
				return
			}
			defer server.ThrottledOpDone()
		}
		if err := d.handleImageReq(w, r, parts); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: image (%s)", r.Method, r.URL)
	default:
		server.BadAPIRequest(w, r, d)
	}
}
