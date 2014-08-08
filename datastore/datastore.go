/*
	This file provides the highest-level view of the datastore via a Service.
*/

package datastore

import (
	"fmt"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.9"
)

var (
	// Map of mutexes at the granularity of repo node ID
	versionMutexes map[nodeID]*sync.Mutex
)

func init() {
	versionMutexes = make(map[nodeID]*sync.Mutex)
}

// The ctxkey type is unexported to prevent collisions with context keys defined in
// other packages.  See Context article at http://blog.golang.org/context
type ctxkey int

const repoCtxKey ctxkey = 0

type repoContext struct {
	repo     Repo
	versions []dvid.VersionID
}

// NewContext returns a Context extended with the Repo and optionally one or more
// versions within that Repo for this request.
func NewContext(ctx context.Context, repo Repo, versions ...dvid.VersionID) context.Context {
	return context.WithValue(ctx, repoCtxKey, repoContext{repo, versions})
}

// FromContext returns Repo and optional versions within that Repo from a Context.
func FromContext(ctx context.Context) (Repo, []dvid.VersionID, bool) {
	value, ok := ctx.Value(repoCtxKey).(repoContext)
	return value.repo, value.versions, ok
}

// The following identifiers are more compact than the global identifiers such as
// UUID or URLs, and therefore useful for compressing key sizes.

// Versions returns a chart of version identifiers for data types and and DVID's datastore
// fixed at compile-time for this DVID executable
func Versions() string {
	var text string = "\nCompile-time version information for this DVID executable:\n\n"
	writeLine := func(name dvid.TypeString, version string) {
		text += fmt.Sprintf("%-15s   %s\n", name, version)
	}
	writeLine("Name", "Version")
	writeLine("DVID datastore", Version)
	writeLine("Storage engines", storage.EnginesAvailable())
	for _, datatype := range Compiled {
		writeLine(datatype.TypeName(), datatype.TypeVersion())
	}
	return text
}
