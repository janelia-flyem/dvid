// Command-line code generation for git-derived version information.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
)

var (
	// Name of file to output with Go version code
	outputfile = flag.String("o", "", "")

	// Display usage if true.
	showHelp = flag.Bool("help", false, "")
)

const helpMessage = `
dvid-gen-version calls git to generation Go code with source code version info.

Usage: dvid-gen-version -o version.go

      -h, -help   (flag)    Show help message

`

const code = `
package server

func init() {
	gitVersion = %q
}
`

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = func() {
		fmt.Printf(helpMessage)
	}
	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if len(*outputfile) < 4 {
		fmt.Printf("The %q is required for this program\n", "-o foo.go")
		os.Exit(1)
	}

	// Make sure we have rsync
	gitPath, err := exec.LookPath("git")
	if err != nil {
		fmt.Printf("Unable to find git command; alter PATH?\nError: %s\n", err.Error())
		os.Exit(1)
	}

	// Get version string
	cmd := exec.Command(gitPath, "describe", "--abbrev=8", "--dirty", "--always", "--tags")
	out, err := cmd.Output()
	if err != nil {
		fmt.Printf("Error running git: %s\n", err.Error())
		os.Exit(1)
	}

	// Inject the version string into Go code that gets written to desired location.
	goCode := fmt.Sprintf(code, string(out))
	if err := ioutil.WriteFile(*outputfile, []byte(goCode), 0644); err != nil {
		fmt.Printf("Error save go code: %s\n", err.Error())
		os.Exit(1)
	}
}
