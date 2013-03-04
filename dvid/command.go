/*
	This file holds types and functions supporting command-related activity in DVID.
	These Command types bundle operation specification and data payloads for use in
	RPC and HTTP APIs.
*/

package dvid

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Keys for setting various arguments within the command line via "key=value" strings.
const (
	KeyUuid         = "uuid"
	KeyRpc          = "rpc"
	KeyWeb          = "web"
	KeyDatastoreDir = "dir"
	KeyConfigFile   = "config"
	KeyPlane        = "plane"
)

var setKeys = map[string]bool{
	"uuid":   true,
	"rpc":    true,
	"web":    true,
	"dir":    true,
	"config": true,
	"plane":  true,
}

// Request supports requests to DVID.  It extends the standard
// command package Command by bundling a packet used for input data since Go's
// rpc convention is to pass a single struct for input.  Once we pass the data
// through the rpc connection, we unpack the command and input packet to pass
// it to data types-specific handling.
//
// Since input and reply payloads are different depending on the command, we
// use an interface and have different types (SliceCommand, SubvolCommand, etc)
// that fulfill that interface.
type Request interface {
	// Name returns the command or data type of the request.
	// If it is a data type, we use TypeCommand() to get the type-specific command.
	Name() string

	// TypeCommand returns the name of a type-specific command, e.g., "get".
	TypeCommand() string

	// String returns the full command with its arguments.
	String() string

	// GetSetting scans a command for any "key=value" argument and returns
	// the value of the passed 'key'.
	GetSetting(key string) (value string, found bool)

	// SetCommandArgs sets a variadic argument set of string pointers to data
	// command arguments, ignoring setting arguments of the form "<key>=<value>".  
	// If there aren't enough arguments to set a target, the target is set to the 
	// empty string.  It returns an 'overflow' slice that has all arguments
	// beyond those needed for targets.
	SetCommandArgs(targets ...*string) (overflow []string)

	// SetDatatypeArgs sets a variadic argument set of string pointers to data
	// type-specific command arguments, ignoring setting arguments of the form 
	// "<key>=<value>".  If there aren't enough arguments to set a target, the 
	// target is set to the empty string.    It returns an 'overflow' slice 
	// that has all arguments beyond those needed for targets.
	SetDatatypeArgs(targets ...*string) (overflow []string)
}

// Response abstracts possible DVID responses to a Request.
type Response interface {
	// ContentType classifies the type of payload and in some cases
	// is identical to HTTP response content type, e.g., "image/png".
	ContentType() string

	// Text is some response string.
	Text() string

	// Status classifies the response.
	Status() string
}

// SubvolCommand fulfills a Request interface and uses Subvol for input.
type SubvolCommand struct {
	Command
	Subvolume
}

// SliceCommand fulfills a Request interface and uses SliceVoxels for input.
type SliceCommand struct {
	Command
	SliceVoxels
}

// Command fulfills a Request interface for command-based interaction with DVID.
// The first item in the string slice is the command, which may be "help"
// or the name of DVID data type ("grayscale8").  If the first item is the name
// of a data type, the second item will have a type-specific command like "get".
// The other arguments are command arguments or optional settings of the form
// "<key>=<value>".
type Command []string

// String returns a space-separated command line
func (cmd Command) String() string {
	return strings.Join([]string(cmd), " ")
}

// Name returns the first argument which is assumed to be the name of the command.
func (cmd Command) Name() string {
	if len(cmd) == 0 {
		return ""
	}
	return cmd[0]
}

// TypeCommand returns the name of a type-specific command assuming this command 
// is for a supported data type.
func (cmd Command) TypeCommand() string {
	if len(cmd) < 2 {
		return ""
	}
	return cmd[1]
}

// GetSetting scans a command for any "key=value" argument and returns
// the value of the passed 'key'.
func (cmd Command) GetSetting(key string) (value string, found bool) {
	if len(cmd) > 1 {
		for _, arg := range cmd[1:] {
			elems := strings.Split(arg, "=")
			if len(elems) == 2 && elems[0] == key {
				value = elems[1]
				found = true
				return
			}
		}
	}
	return
}

// GetDatastoreDir returns a directory specified in the arguments via "dir=..." or
// defaults to the current directory.
func (cmd Command) GetDatastoreDir() string {
	datastoreDir, found := cmd.GetSetting(KeyDatastoreDir)
	if !found {
		currentDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get current directory:", err)
		}
		return currentDir
	}
	return datastoreDir
}

// SetCommandArgs sets a variadic argument set of string pointers to data
// command arguments, ignoring setting arguments of the form "<key>=<value>".  
// If there aren't enough arguments to set a target, the target is set to the 
// empty string.  It returns an 'overflow' slice that has all arguments
// beyond those needed for targets.
func (cmd Command) SetCommandArgs(targets ...*string) (overflow []string) {
	overflow = setArgs(cmd, 1, targets...)
	return
}

// SetDatatypeArgs sets a variadic argument set of string pointers to data
// type-specific command arguments, ignoring setting arguments of the form 
// "<key>=<value>".  If there aren't enough arguments to set a target, the 
// target is set to the empty string.    It returns an 'overflow' slice 
// that has all arguments beyond those needed for targets.
func (cmd Command) SetDatatypeArgs(targets ...*string) (overflow []string) {
	overflow = setArgs(cmd, 2, targets...)
	return
}

func setArgs(cmd Command, startPos int, targets ...*string) (overflow []string) {
	overflow = make([]string, 0, len(cmd))
	for _, target := range targets {
		*target = ""
	}
	if len(cmd) > startPos {
		numTargets := len(targets)
		curTarget := 0
		for _, arg := range cmd[startPos:] {
			optionalSet := false
			elems := strings.Split(arg, "=")
			if len(elems) == 2 {
				_, optionalSet = setKeys[elems[0]]
			}
			if !optionalSet {
				if curTarget >= numTargets {
					overflow = append(overflow, arg)
				} else {
					*(targets[curTarget]) = arg
				}
				curTarget++
			}
		}
	}
	return
}

// SimpleResponse is the simplest type to fulfill the Response interface.
type SimpleResponse struct {
	ctype  string
	text   string
	status string
}

func NewSimpleResponse(ctype, text, status string) Response {
	return &SimpleResponse{ctype, text, status}
}

func (r *SimpleResponse) ContentType() string {
	return r.ctype
}

func (r *SimpleResponse) Text() string {
	return r.text
}

func (r *SimpleResponse) Status() string {
	return r.status
}

// SliceResponse is a Response with SliceVoxels payload
type SliceResponse struct {
	SimpleResponse
	SliceVoxels
}

// PointStr is a n-dimensional coordinate in string format "x,y,z,..."
// where each coordinate is a 32-bit integer.
type PointStr string

func (s PointStr) VoxelCoord() (coord VoxelCoord, err error) {
	_, err = fmt.Sscanf(string(s), "%d,%d,%d", &coord[0], &coord[1], &coord[2])
	return
}

func (s PointStr) Point2d() (point Point2d, err error) {
	_, err = fmt.Sscanf(string(s), "%d,%d", &point[0], &point[1])
	return
}

// VectorStr is a n-dimensional coordinate in string format "x,y,z,....""
// where each coordinate is a 32-bit float.
type VectorStr string

func (s VectorStr) Vector3d() (v Vector3d, err error) {
	_, err = fmt.Sscanf(string(s), "%f,%f,%f", &v[0], &v[1], &v[2])
	return
}
