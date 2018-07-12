package tarsupervoxels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

// GetSyncSubs implements the datastore.Syncer interface, but tarsupervoxels doesn't need
// to process events.
func (d *Data) GetSyncSubs(synced dvid.Data) (datastore.SyncSubs, error) {
	return datastore.SyncSubs{}, nil
}
