// +build !clustered,!gcloud

// Command-line backup program.

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Delete old snapshot directory if it exists.
	deleteSnapshot = flag.Bool("delete", false, "")

	// Run in verbose mode if true.
	runVerbose = flag.Bool("verbose", false, "")

	// Just make snapshot
	snapshotOnly = flag.Bool("snapshot", false, "")
)

const helpMessage = `
dvid-backup does a cold backup of a local leveldb storage engine.

Usage: dvid-backup [options] <database directory> <backup directory>

	  -delete     (flag)    Remove old snapshot directory.
	  -snapshot   (flag)    Only create snapshot directory; don't rsync.
	  -verbose    (flag)    Run in verbose mode.
	  -h, -help   (flag)    Show help message

`

func duplicateFile(src, dst string) {
	r, err := os.Open(src)
	if err != nil {
		fmt.Printf("Could not open file %q for copy: %v\n", src, err)
		os.Exit(1)
	}
	defer r.Close()

	w, err := os.Create(dst)
	if err != nil {
		fmt.Printf("Could not create %q: %v\n", dst, err)
		os.Exit(1)
	}
	defer w.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		fmt.Printf("Error copying %q: %v\n", src, err)
		os.Exit(1)
	}
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = func() {
		fmt.Printf(helpMessage)
	}
	flag.Parse()

	if *showHelp || flag.NArg() != 2 {
		flag.Usage()
		os.Exit(0)
	}

	// Make sure we have rsync
	rsyncPath, err := exec.LookPath("rsync")
	if err != nil {
		fmt.Printf("Unable to find rsync command; alter PATH?\nError: %v\n", err)
		os.Exit(1)
	}

	// Make sure db directory exists
	pathDb := flag.Args()[0]
	pathBackup := flag.Args()[1]

	var fileinfo os.FileInfo
	if fileinfo, err = os.Stat(pathDb); os.IsNotExist(err) {
		fmt.Printf("Supplied DB path (%s) doesn't exist.", pathDb)
		os.Exit(1)
	}
	if !fileinfo.IsDir() {
		fmt.Printf("Supplied DB path (%s) is not a directory.", pathDb)
		os.Exit(1)
	}

	if !(*snapshotOnly) {
		// Create backup directory if it doesn't exist
		if fileinfo, err = os.Stat(pathBackup); os.IsNotExist(err) {
			fmt.Printf("Creating backup directory: %s\n", pathBackup)
			err := os.MkdirAll(pathBackup, 0744)
			if err != nil {
				fmt.Printf("Can't make backup directory: %v\n", err)
				os.Exit(1)
			}
		} else if !fileinfo.IsDir() {
			fmt.Printf("Supplied backup path (%s) is not a directory.", pathBackup)
			os.Exit(1)
		}
	}

	// Create snapshot directory for hard SST links in sibling directory to pathDb.
	// Must be same device as pathDb or else we get "Invalid cross-device link" errors.
	parentDir, dbName := path.Split(pathDb)
	snapshotDir := path.Join(parentDir, dbName+"-snapshot")
	if fileinfo, err = os.Stat(snapshotDir); !os.IsNotExist(err) {
		if *deleteSnapshot {
			err = os.RemoveAll(snapshotDir)
			if err != nil {
				fmt.Printf("Error removing snapshot directory %q: %v\n", snapshotDir, err)
				os.Exit(1)
			}
		} else {
			fmt.Printf("Already existing snapshot directory: %s\n", snapshotDir)
			os.Exit(1)
		}
	}

	if err := os.MkdirAll(snapshotDir, 0744); err != nil {
		fmt.Printf("Unable to create snapshot directory: %s\n", snapshotDir)
		os.Exit(1)
	}

	// Make hard links of all files in sst folders and copy files not in sst_* directories.
	err = filepath.Walk(pathDb, func(fullpath string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error traversing the database directory @ %s: %v\n", fullpath, err)
			os.Exit(1)
		}
		srcpath, name := path.Split(fullpath)
		if strings.HasPrefix(name, "sst_") && f.IsDir() {
			err = os.MkdirAll(path.Join(snapshotDir, name), 0744)
			if err != nil {
				fmt.Printf("Error trying to create new SST directory: %v\n", err)
				os.Exit(1)
			}
			return nil
		}
		if !f.IsDir() {
			parentDir := filepath.Base(srcpath)
			if strings.HasPrefix(parentDir, "sst_") {
				if path.Ext(fullpath) != ".sst" {
					fmt.Printf("Ignoring non-sst file in sst directory: %s\n", fullpath)
					return nil
				}
				dstName := path.Join(snapshotDir, parentDir, name)
				os.Link(fullpath, dstName)
				fmt.Printf("Linked SST file %q to %q\n", fullpath, dstName)
			} else {
				dstName := path.Join(snapshotDir, name)
				duplicateFile(fullpath, dstName)
				fmt.Printf("Duplicated non-SST file %q to %q\n", fullpath, dstName)
			}
		}
		return nil
	})

	if *snapshotOnly {
		fmt.Printf("Stored snapshot of %s in %s\n", pathDb, snapshotDir)
	} else {
		// Launch background process to do rsync of sst files
		go func() {
			cmd := exec.Command(rsyncPath, "-a", "--delete", snapshotDir+"/", pathBackup)
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error running rsync: %v\n", err)
				os.Exit(1)
			}
		}()
		time.Sleep(1 * time.Second) // Hack to allow time for goroutine to spawn.
		fmt.Printf("Running rsync in background: %s -> %s\n", snapshotDir, pathBackup)
	}
}
