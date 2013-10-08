/*
	Package multichan16 tailors the voxels data type for 16-bit fluorescent images with multiple
	channels that can be read from V3D Raw format.  Note that this data type has multiple
	channels but segregates its channel data in (c, z, y, x) fashion rather than interleave
	it within a block of data in (z, y, x, c) fashion.  There is not much advantage at
	using interleaving; most forms of RGB compression fails to preserve the
	independence of the channels.  Segregating the channel data lets us use straightforward
	compression on channel slices.

	Specific channels of multichan16 data are addressed by adding a numerical suffix to the
	data name.  For example, if we have "mydata" multichan16 data, we reference channel 1
	as "mydata1" and channel 2 as "mydata2".  Up to the first 3 channels are composited
	into a RGBA volume that is addressible using "mydata" or "mydata0".
*/
package multichan16

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/voxels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/datatype/multichan16"
)

const HelpMessage = `
API for datatypes derived from multichan16 (github.com/janelia-flyem/dvid/datatype/multichan16)
===============================================================================================

Command-line:

$ dvid node <UUID> <data name> load local <V3D raw filename>
$ dvid node <UUID> <data name> load remote <V3D raw filename>

    Adds multichannel data to a version node when the server can see the local files ("local")
    or when the server must be sent the files via rpc ("remote").

    Example: 

    $ dvid node 3f8c mydata load local mydata.v3draw

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data to add.
    filename      Filename of a V3D Raw format file.
	
    ------------------

HTTP API (Level 2 REST):

GET  /api/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  /api/node/<UUID>/<data name>/info
POST /api/node/<UUID>/<data name>/info

    Retrieves or puts data properties.

    Example: 

    GET /api/node/3f8c/multichan16/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of multichan16 data.


GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]
POST /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]

    Retrieves or puts orthogonal plane image data to named multichannel 16-bit data.

    Example: 

    GET /api/node/3f8c/mydata2/xy/200,200/0,0,100/jpg:80  (channel 2 of mydata)

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of data.  Optionally add a numerical suffix for the channel number.
    dims          The axes of data extraction in form "i,j,k,..."  Example: "0,2" can be XZ.
                    Slice strings ("xy", "xz", or "yz") are also accepted.
    size          Size in pixels in the format "dx,dy".
    offset        3d coordinate in the format "x,y,z".  Gives coordinate of top upper left voxel.
    format        Valid formats depend on the dimensionality of the request and formats
                    available in server implementation.
                  2D: "png", "jpg" (default: "png")
                    jpg allows lossy quality setting, e.g., "jpg:80"
                  nD: "thrift" (default: "thrift")

`

// DefaultBlockMax specifies the default size for each block of this data type.
var (
	DefaultBlockMax voxels.Point3d = voxels.Point3d{16, 16, 16}
	typeService     datastore.TypeService
)

func init() {
	dtype := &Datatype{voxels.NewDatatype(1, 2)}
	dtype.DatatypeID = datastore.MakeDatatypeID("multichan16", RepoUrl, Version)
	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.RegisterDatatype(dtype)
	typeService = dtype

	gob.Register(&Datatype{})
	gob.Register(&Data{})
}

// -------  VoxelHandler interface implementation -------------

// Channel is an image volumes that fulfills the voxels.VoxelHandler interface.
// The term "channels" is used to reflect RGBA channels that are interleaved in
// a voxel compared to "Channel", which is entirely different data in multichannel
// images.  Perhaps we should use different names...
type Channel struct {
	voxels.Geometry

	channelNum int32

	channels        int32
	bytesPerChannel int32

	// The data itself
	data []uint8

	// The stride for 2d iteration.  For 3d subvolumes, we don't reuse standard Go
	// images but maintain fully packed data slices, so stride isn't necessary.
	stride int32

	byteOrder binary.ByteOrder
}

func (c *Channel) String() string {
	size := c.Size()
	return fmt.Sprintf("Channel of size %d x %d x %d @ %s",
		size[0], size[1], size[2], c.StartVoxel())
}

func (c *Channel) Data() []uint8 {
	return c.data
}

func (c *Channel) Stride() int32 {
	return c.stride
}

// BlockIndex returns a channel-specific ZYXIndexer
func (c *Channel) BlockIndex(x, y, z int32) voxels.ZYXIndexer {
	return &IndexCZYX{c.channelNum, voxels.BlockCoord{x, y, z}}
}

func (c *Channel) BytesPerVoxel() int32 {
	return c.channels * c.bytesPerChannel
}

func (c *Channel) VoxelFormat() (channels, bytesPerChannel int32) {
	return c.channels, c.bytesPerChannel
}

func (c *Channel) ByteOrder() binary.ByteOrder {
	return c.byteOrder
}

type Datatype struct {
	*voxels.Datatype
}

// --- TypeService interface ---

// NewData returns a pointer to a new Voxels with default values.
func (dtype *Datatype) NewDataService(dset *datastore.Dataset, id *datastore.DataID,
	config dvid.Config) (service datastore.DataService, err error) {

	var voxelservice datastore.DataService
	voxelservice, err = dtype.Datatype.NewDataService(dset, id, config)
	if err != nil {
		return
	}
	basedata := voxelservice.(*voxels.Data)
	data := &Data{
		Data:        *basedata,
		datasetUUID: dset.Root,
	}
	data.BlockSize = DefaultBlockMax
	data.TypeService = typeService
	service = data
	return
}

func (dtype *Datatype) Help() string {
	return HelpMessage
}

// Data of multichan16 type embeds voxels and extends it with channels.
type Data struct {
	voxels.Data

	// Number of channels for this data.  The names are referenced by
	// adding a number onto the data name, e.g., mydata1, mydata2, etc.
	NumChannels int

	// Pointer to the owning Dataset so we can force Put() if we modify
	// data properties based on loaded data, e.g., initialize NumChannels.
	datasetUUID datastore.UUID
}

// JSONString returns the JSON for this Data's configuration
func (d *Data) JSONString() (jsonStr string, err error) {
	m, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// --- DataService interface ---

// Do acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	if request.TypeCommand() != "load" {
		return d.UnknownCommand(request)
	}
	if len(request.Command) < 6 {
		return fmt.Errorf("Poorly formatted load command.  See command-line help.")
	}
	source := request.Command[4]
	switch source {
	case "local":
		return d.LoadLocal(request, reply)
	case "remote":
		return fmt.Errorf("load remote not yet implemented")
	default:
		return d.UnknownCommand(request)
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this dataset.
func (d *Data) DoHTTP(uuid datastore.UUID, w http.ResponseWriter, r *http.Request) error {
	startTime := time.Now()

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)
	var op voxels.OpType
	switch action {
	case "get":
		op = voxels.GetOp
	case "post":
		op = voxels.PutOp
	default:
		return fmt.Errorf("Can only handle GET or POST HTTP verbs")
	}

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")

	// Get the running datastore service from this DVID instance.
	service := server.DatastoreService()

	_, versionID, err := service.LocalIDFromUUID(uuid)
	if err != nil {
		return err
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, d.Help())
		return nil
	case "info":
		jsonStr, err := d.JSONString()
		if err != nil {
			server.BadRequest(w, r, err.Error())
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, jsonStr)
		return nil
	default:
	}

	// Get the data name and parse out the channel number or see if composite is required.
	var channelNum int32
	channumStr := strings.TrimPrefix(parts[2], string(d.Name))
	if len(channumStr) == 0 {
		channelNum = 0
	} else {
		n, err := strconv.ParseInt(channumStr, 10, 32)
		if err != nil {
			return fmt.Errorf("Error parsing channel number from data name '%s': %s",
				parts[2], err.Error())
		}
		if int(n) > d.NumChannels {
			minChannelName := fmt.Sprintf("%s1", d.DataName())
			maxChannelName := fmt.Sprintf("%s%d", d.DataName(), d.NumChannels)
			return fmt.Errorf("Data only has %d channels.  Use names '%s' -> '%s'", d.NumChannels,
				minChannelName, maxChannelName)
		}
		channelNum = int32(n)
	}

	// Get the data shape.
	shapeStr := voxels.DataShapeString(parts[3])
	dataShape, err := shapeStr.DataShape()
	if err != nil {
		return fmt.Errorf("Bad data shape given '%s'", shapeStr)
	}

	switch dataShape {
	case voxels.XY, voxels.XZ, voxels.YZ:
		sizeStr, offsetStr := parts[4], parts[5]
		slice, err := voxels.NewSliceFromStrings(shapeStr, offsetStr, sizeStr)
		if err != nil {
			return err
		}
		if op == voxels.PutOp {
			return fmt.Errorf("DVID does not yet support POST of slices into multichannel data")
		} else {
			var channels, bytesPerChannel int32
			if channelNum == 0 {
				bytesPerChannel = 1
				channels = 4
			} else {
				channels = 1
				bytesPerChannel = 2
			}
			numBytes := int64(channels*bytesPerChannel) * slice.NumVoxels()
			channel := &Channel{
				Geometry:        slice,
				channelNum:      channelNum,
				channels:        channels,
				bytesPerChannel: bytesPerChannel,
				data:            make([]uint8, numBytes),
				stride:          slice.Width() * channels * bytesPerChannel,
				byteOrder:       d.ByteOrder,
			}
			img, err := d.GetImage(versionID, channel)
			var formatStr string
			if len(parts) >= 7 {
				formatStr = parts[6]
			}
			//dvid.ElapsedTime(dvid.Normal, startTime, "%s %s upto image formatting", op, slice)
			err = dvid.WriteImageHttp(w, img, formatStr)
			if err != nil {
				return err
			}
		}
	case voxels.Vol:
		sizeStr, offsetStr := parts[4], parts[5]
		_, err := voxels.NewSubvolumeFromStrings(offsetStr, sizeStr)
		if err != nil {
			return err
		}
		if op == voxels.GetOp {
			return fmt.Errorf("DVID does not yet support GET of thrift-encoded volume data")
		} else {
			return fmt.Errorf("DVID does not yet support POST of thrift-encoded volume data")
		}
	case voxels.Arb:
		return fmt.Errorf("DVID does not yet support arbitrary planes.")
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "HTTP %s: %s", r.Method, dataShape)
	return nil
}

// LoadLocal adds image data to a version node.  See HelpMessage for example of
// command-line use of "load local".
func (d *Data) LoadLocal(request datastore.Request, reply *datastore.Response) error {
	startTime := time.Now()

	// Get the running datastore service from this DVID instance.
	service := server.DatastoreService()

	// Parse the request
	var uuidStr, dataName, cmdStr, sourceStr, filename string
	_ = request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &sourceStr, &filename)

	// Get the version ID from a uniquely identifiable string
	_, _, versionID, err := service.NodeIDFromString(uuidStr)
	if err != nil {
		return fmt.Errorf("Could not find node with UUID %s: %s", uuidStr, err.Error())
	}

	// Load the V3D Raw file.
	ext := filepath.Ext(filename)
	switch ext {
	case ".raw", ".v3draw":
	default:
		return fmt.Errorf("Unknown extension '%s' when expected V3D Raw file", ext)
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	unmarshaler := V3DRawMarshaler{}
	channels, err := unmarshaler.UnmarshalV3DRaw(file)
	if err != nil {
		return err
	}

	// Store the metadata
	d.NumChannels = len(channels)
	if d.NumChannels > 0 {
		d.ByteOrder = channels[0].byteOrder
		reply.Text = fmt.Sprintf("Loaded %s into data '%s': found %d channels",
			d.DataName(), filename, d.NumChannels)
		reply.Text += fmt.Sprintf(" (%d x %d x %d)", channels[0].Width(),
			channels[0].Height(), channels[0].Depth())
	} else {
		reply.Text = fmt.Sprintf("Found no channels in file %s", filename)
		return nil
	}
	err = service.SaveDataset(d.datasetUUID)
	if err != nil {
		return err
	}

	// PUT each channel of the file into the datastore using a separate data name.
	for _, channel := range channels {
		dvid.Fmt(dvid.Debug, "Processing channel %d... \n", channel.channelNum)
		err = d.PutImage(versionID, channel)
		if err != nil {
			return err
		}
	}

	// Create a RGB composite from the first 3 channels.  This is considered to be channel 0
	// or can be accessed with the base data name.
	dvid.Fmt(dvid.Debug, "Creating composite image from channels...\n")
	err = d.storeComposite(versionID, channels)
	if err != nil {
		return err
	}

	dvid.ElapsedTime(dvid.Debug, startTime, "RPC load local '%s' completed", filename)
	return nil
}

// Create a RGB interleaved volume.
func (d *Data) storeComposite(versionID datastore.VersionLocalID, channels []*Channel) error {
	// Setup the composite Channel
	geom := channels[0].Geometry
	pixels := int(geom.Width() * geom.Height() * geom.Depth())
	composite := &Channel{
		Geometry:        geom,
		channelNum:      0,
		channels:        4,
		bytesPerChannel: 1,
		data:            make([]uint8, pixels*4),
		stride:          geom.Width(),
	}

	// Get the min/max of each channel.
	numChannels := len(channels)
	if numChannels > 3 {
		numChannels = 3
	}
	var min, max [3]uint16
	min[0] = uint16(0xFFFF)
	min[1] = uint16(0xFFFF)
	min[2] = uint16(0xFFFF)
	for c := 0; c < numChannels; c++ {
		channel := channels[c]
		data := channel.Data()
		beg := 0
		for i := 0; i < pixels; i++ {
			value := d.ByteOrder.Uint16(data[beg : beg+2])
			if value < min[c] {
				min[c] = value
			}
			if value > max[c] {
				max[c] = value
			}
			beg += 2
		}
	}

	// Do second pass, normalizing each channel and storing it into the appropriate byte.
	compdata := composite.data
	for c := 0; c < numChannels; c++ {
		channel := channels[c]
		window := int(max[c] - min[c])
		if window == 0 {
			window = 1
		}
		data := channel.Data()
		beg := 0
		begC := c // Channel 0 -> R, Channel 1 -> G, Channel 2 -> B
		for i := 0; i < pixels; i++ {
			value := d.ByteOrder.Uint16(data[beg : beg+2])
			normalized := 255 * int(value-min[c]) / window
			if normalized > 255 {
				normalized = 255
			}
			compdata[begC] = uint8(normalized)
			beg += 2
			begC += 4
		}
	}

	// Set the alpha channel to 255.
	alphaI := 3
	for i := 0; i < pixels; i++ {
		compdata[alphaI] = 255
		alphaI += 4
	}

	// Store the result
	return d.PutImage(versionID, composite)
}
