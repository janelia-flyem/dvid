// +build !fuse

package keyvalue

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
)

// Mount creates (if not already present) a FUSE file system for this data.
func (d *Data) Mount(request datastore.Request, reply *datastore.Response) error {
	return fmt.Errorf("keyvalue Mount command not built into this DVID server!")
}
