// Command-line code generation for git-derived version information.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
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

const code = `package server

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

	// Make sure we have git
	gitPath, err := exec.LookPath("git")
	if err != nil {
		fmt.Printf("Unable to find git command; alter PATH?\nError: %v\n", err)
		os.Exit(1)
	}

	// Get version string
	cmd := exec.Command(gitPath, "describe", "--abbrev=5", "--tags")
	out, err := cmd.Output()
	if err != nil {
		out = []byte("notag")
	}

	// Inject the version string into Go code that gets written to desired location.
	versionID := strings.TrimSpace(string(out))
	goCode := fmt.Sprintf(code, versionID)
	if err := ioutil.WriteFile(*outputfile, []byte(goCode), 0644); err != nil {
		fmt.Printf("Error save go code: %v\n", err)
		os.Exit(1)
	}
}
