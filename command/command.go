package command

import (
	"log"
	"os"
	"strings"
)

// Keys for setting various parameters within the command line via "key=value" arguments.
const (
	KeyUuid         = "uuid"
	KeyRpc          = "rpc"
	KeyWeb          = "web"
	KeyDatastoreDir = "dir"
)

var setKeys = map[string]bool{
	"uuid": true,
	"rpc":  true,
	"web":  true,
	"dir": 	true,
}

// Packet packages data that are transmitted as part of commands
type Packet struct {
	Text string
	Data [][]byte
}

// Command supports command-based interaction with DVID
type Command struct {
	// Args lists the elements of the command where Args[0] is the command string
	// and the other arguments are command arguments or optional settings of
	// the form "<key>=<value>"
	Args []string
}

func (cmd *Command) String() string {
	return strings.Join(cmd.Args, " ")
}

// Name returns the first argument which is assumed to be the name of the command.
func (cmd *Command) Name() string {
	if len(cmd.Args) == 0 {
		return ""
	}
	return cmd.Args[0]
}

// TypeCommand returns the name of a type-specific command assuming this command 
// is for a supported data type.
func (cmd *Command) TypeCommand() string {
	if len(cmd.Args) < 2 {
		return ""
	}
	return cmd.Args[1]
}

// GetSetting scans a command for any "key=value" argument and returns
// the value of the passed 'key'.
func (cmd *Command) GetSetting(key string) (value string, found bool) {
	if len(cmd.Args) > 1 {
		for _, arg := range cmd.Args[1:] {
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
func (cmd *Command) GetDatastoreDir() string {
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
func (cmd *Command) SetCommandArgs(targets ...*string) (overflow []string) {
	overflow = setArgs(cmd.Args, 1, targets...)
	return
}

// SetDatatypeArgs sets a variadic argument set of string pointers to data
// type-specific command arguments, ignoring setting arguments of the form 
// "<key>=<value>".  If there aren't enough arguments to set a target, the 
// target is set to the empty string.    It returns an 'overflow' slice 
// that has all arguments beyond those needed for targets.
func (cmd *Command) SetDatatypeArgs(targets ...*string) (overflow []string) {
	overflow = setArgs(cmd.Args, 2, targets...)
	return
}

func setArgs(args []string, startPos int, targets ...*string) (overflow []string) {
	overflow = make([]string, 0, len(args))
	log.Println("len(args) =", len(args))
	for _, target := range targets {
		*target = ""
	}
	if len(args) > startPos {
		numTargets := len(targets)
		curTarget := 0
		for _, arg := range args[startPos:] {
			optionalSet := false
			elems := strings.Split(arg, "=")
			if len(elems) == 2 {
				_, optionalSet = setKeys[elems[0]]
			}
			if !optionalSet {
				if curTarget >= numTargets {
					overflow = append(overflow, arg)
					log.Println("done")
				} else {
					*(targets[curTarget]) = arg
				}
				curTarget++
			}
		}
	}
	return
}

