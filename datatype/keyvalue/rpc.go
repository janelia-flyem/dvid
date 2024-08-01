package keyvalue

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"

	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// put handles a PUT command-line request.
func (d *Data) put(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("the key name must be specified after 'put'")
	}
	if len(cmd.Input) == 0 {
		return fmt.Errorf("no data was passed into standard input")
	}
	var uuidStr, dataName, cmdStr, keyStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &keyStr)

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Store data
	if !d.Versioned() {
		// Map everything to root version.
		versionID, err = datastore.GetRepoRootVersion(versionID)
		if err != nil {
			return err
		}
	}
	ctx := datastore.NewVersionedCtx(d, versionID)
	if err = d.PutData(ctx, keyStr, cmd.Input); err != nil {
		return fmt.Errorf("error on put to key %q for keyvalue %q: %v", keyStr, d.DataName(), err)
	}

	reply.Output = []byte(fmt.Sprintf("Put %d bytes into key %q for keyvalue %q, uuid %s\n",
		len(cmd.Input), keyStr, d.DataName(), uuidStr))
	return nil
}

// dumpCloud stores all key-value pairs in cloud storage.
func (d *Data) dumpCloud(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("dump-cloud must be followed by <cloud path>")
	}
	var uuidStr, dataName, cmdStr, refStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &refStr)

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	if !d.Versioned() {
		// Map everything to root version.
		versionID, err = datastore.GetRepoRootVersion(versionID)
		if err != nil {
			return err
		}
	}
	// ctx := datastore.NewVersionedCtx(d, versionID)
	_ = datastore.NewVersionedCtx(d, versionID)

	// TODO: Dump to cloud store

	reply.Output = []byte(fmt.Sprintf("Dumped all key-values to cloud %q for keyvalue %q, uuid %s\n",
		refStr, d.DataName(), uuidStr))
	return nil
}

// TODO: Create transfer function using channels to get decent parallel write to cloud, assume
// getting from disk will be much faster than writing to cloud.
