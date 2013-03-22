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
	KeyOffset       = "offset"
	KeySize         = "size"
)

var setKeys = map[string]bool{
	"uuid":   true,
	"rpc":    true,
	"web":    true,
	"dir":    true,
	"config": true,
	"plane":  true,
}

// Response provides a few string fields to pass information back from
// a remote operation.
type Response struct {
	ContentType string
	Text        string
	Status      string
}

// Command supports command-line interaction with DVID.
// The first item in the string slice is the command, which may be "help"
// or the name of DVID data name ("grayscale8").  If the first item is the name
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

// Parameter scans a command for any "key=value" argument and returns
// the value of the passed 'key'.
func (cmd Command) Parameter(key string) (value string, found bool) {
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

// DatastoreDir returns a directory specified in the arguments via "dir=..." or
// defaults to the current directory.
func (cmd Command) DatastoreDir() string {
	datastoreDir, found := cmd.Parameter(KeyDatastoreDir)
	if !found {
		currentDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get current directory:", err)
		}
		return currentDir
	}
	return datastoreDir
}

// CommandArgs sets a variadic argument set of string pointers to data
// command arguments, ignoring setting arguments of the form "<key>=<value>".  
// If there aren't enough arguments to set a target, the target is set to the 
// empty string.  It returns an 'overflow' slice that has all arguments
// beyond those needed for targets.
//
// Example: Given the command string "add param1 param2 42 data/*.png"
// 
//   var s1, s2, s3, s4 string
//   filenames := CommandArgs(0, &s1, &s2, &s3, &s4)
//   fmt.Println(filenames)
//   fmt.Println(s1)
//   fmt.Println(s2, s3)
//   fmt.Println(s4)
//
//   Would print out:
//      ["data/foo-1.png", "data/foo-2.png", "data/foo-3.png"] 
//      add
//      param1 param2 
//      42
func (cmd Command) CommandArgs(startPos int, targets ...*string) (overflow []string) {
	overflow = getArgs(cmd, startPos, targets...)
	return
}

func getArgs(cmd Command, startPos int, targets ...*string) (overflow []string) {
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
