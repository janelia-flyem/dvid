package neuronjson

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/keyvalue"
	"github.com/janelia-flyem/dvid/dvid"
)

// versionChanges writes JSON file for all changes by versions, including tombstones.
func (d *Data) versionChanges(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 5 {
		return fmt.Errorf("path to output file must be specified after 'versionchanges'")
	}
	var uuidStr, dataName, cmdStr, filePath string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &filePath)

	go d.writeVersions(filePath)

	reply.Output = []byte(fmt.Sprintf("Started writing version changes of neuronjson instance %q into %s ...\n",
		d.DataName(), filePath))
	return nil
}

// putCmd handles a PUT command-line request.
func (d *Data) putCmd(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("key name must be specified after 'put'")
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
	if err = d.PutData(ctx, keyStr, cmd.Input, nil, false); err != nil {
		return fmt.Errorf("error on put to key %q for neuronjson %q: %v", keyStr, d.DataName(), err)
	}

	reply.Output = []byte(fmt.Sprintf("Put %d bytes into key %q for neuronjson %q, uuid %s\n",
		len(cmd.Input), keyStr, d.DataName(), uuidStr))
	return nil
}

// importKV imports a keyvalue instance into the neuronjson instance.
func (d *Data) importKV(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 5 {
		return fmt.Errorf("keyvalue instance name must be specified after importKV")
	}
	var uuidStr, dataName, cmdStr, kvName string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &kvName)

	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	sourceKV, err := keyvalue.GetByUUIDName(uuid, dvid.InstanceName(kvName))
	if err != nil {
		return err
	}
	go d.loadFromKV(versionID, sourceKV)

	reply.Output = []byte(fmt.Sprintf("Started loading from keyvalue instance %q into neuronjson instance %q, uuid %s\n",
		kvName, d.DataName(), uuidStr))
	return nil
}

// ingestNeuronJSON modifies a neuronjson instance from data in a JSON file.
func (d *Data) ingestNeuronJSON(request datastore.Request, reply *datastore.Response) error {
	if len(request.Command) < 6 {
		return fmt.Errorf("JSON file path and user string must be specified after ingest-neuronjson")
	}
	var uuidStr, dataName, cmdStr, jsonFilePath, userStr string
	request.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &jsonFilePath, &userStr)

	uuid, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}
	ctx := datastore.NewVersionedCtx(d, versionID)
	ctx.User = userStr
	ctx.App = "ingest-neuronjson"

	if err := d.ingestJson(ctx, uuid, jsonFilePath, userStr); err != nil {
		return fmt.Errorf("error ingesting neuronjson from %q: %v", jsonFilePath, err)
	}

	reply.Output = []byte(fmt.Sprintf("Finished ingesting data from %q into neuronjson instance %q, uuid %s\n",
		jsonFilePath, d.DataName(), uuidStr))
	return nil
}
