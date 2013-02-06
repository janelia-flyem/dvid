package cli

import (
	"dvid/datastore"
	"dvid/versioning"
	_ "fmt"
	"log"
)

const HelpMessage = `
Version control commands (note use of [vhash] for optional version hash string):

  init <max x> <max y> <max z>     Initialize a datastore in current directory.
  add [vhash] <filenames glob>     Add images.  Specify top left corner with 
                                      optional x,y,z flags.
  commit [vhash] <message>         Commit additions.  Message can be on command-line 
                                      with leading and ending quotes, or you can 
                                      specify a file name, which will have its 
                                      contents used as a message.
  log                              List image versions in datastore.
`

// Options that could accompany commands from command line interface or web server
type Options struct {
	// Directory is the path to a DVID datastore directory
	Directory string

	// Origin (top, left corner) of a subvolume within a datastore volume space
	OriginX int
	OriginY int
	OriginZ int

	// Version identifier.
	Version versioning.NodeHash
}

func DoInit(args []string, options Options) (err error) {
	log.Println("Initializing datastore at", options.Directory)
	create := true
	datastore.InitDatastore(options.Directory, create)
	return
}

func DoCommand(args []string, options Options) (err error) {
	switch args[0] {
	case "init":
		err = DoInit(args, options)
	}
	return
}
