/*
	This file handles RPC connections, usually from DVID clients.
	TODO: Remove all command-line commands aside from the most basic ones, and
	   force use of the HTTP API.  Curl can be used from command line.
*/

package server

import (
	"fmt"
	"os"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/valyala/gorpc"
)

const rpcHelp = `

Commands executed on the server (rpc address = %s):

	help
	shutdown

	repos new  <alias> <description> <settings...>
		where <settings> are optional "key=value" strings:
		
		uuid=<uuid>
		
		passcode=<passcode>
	
			The optional passcode will have to be provided to delete the repo
			or any contained data instance.
	
	repo <UUID> branch name [optional UUID]

		Create a new branch version node of the given parent UUID.  If an optional UUID is 
		specified, the child node is assigned this UUID.

	repo <UUID> newversion [optional UUID]

		Create a new child node of the given parent UUID.  If an optional UUID is 
		specified, the child node is assigned this UUID.

	repo <UUID> new <datatype name> <data name> <datatype-specific config>...
	
	repo <UUID> rename <old data name> <new data name> <repo passcode if any>

	node <UUID> <data name> <type-specific commands>

DANGEROUS COMMANDS (only available via command line)

	repos delete <UUID> <repo passcode if any>

		Deletes an entire repo with the given root UUID.

	repo <UUID> delete <data name or UUID> <repo passcode if any>

		Delete the given data instance. First checks by data UUID, and if a
		data instance is not found, checks by name.

	repo <UUID> make-master <old-master-branch-name>

		Makes the branch at given UUID (that node and all its children) the
		new master branch and renames the old master branch to the given
		branch name.  NOTE: This command will fail if the given UUID is not
		a node that is directly branched off master.

		This command should be used after committing the branches under
		consideration. After renaming the master branch, the DVID server
		should be restarted so datatypes like neuronjson can reload its
		caches appropriately.

	repo <UUID> hide-branch <branch-name>

		Removes metadata for the given branch so that it is not visible.
		This does not remove the key-value pairs associated wiith the given
		nodes, since doing so would be very expensive.  Instead, the old
		key values aren't accessible.


EXPERIMENTAL COMMANDS

	repo <UUID> storage-details

		Print information on leaf/interior nodes.

	repo <UUID> migrate <instance name> <src store> <dst store> <settings...>
    
		Migrates all of this instance's data from a source store (specified by 
		the nickname in TOML file) to a new store.  Before running this command,
		you must modify the config TOML file so the destination store is available.
		To start using the destination store for the instance, you must restart
		the server and specify that store via a TOML configuration change.

		Settings:

		transmit=[all | flatten | list of versions]

			The default transmit "all" copies all versions of the source.
			
			A transmit "flatten" will copy just the version specified and
			flatten the key/values so there is no history.

			If the value of transmit is a string with UUIDs separated by commas,
			e.g., "transmit=881e9,52a13,57e8d"
			where the first UUID must be oldest and will become the flattened root,
			and all other versions after that will accumulate deltas from versions
			that are not on the list.  In the example above, all previous versions to
			881e9 for the given instance will be flattened and then any key-values
			after 881e9 up to 52a13 will be flattened into the 52a13 version.

	repo <UUID> migrate-batch <migrate config file>

		Migrates instances specified in the config file from a source to a 
		destination store, similar to the above "migrate" command but in a batch.  

		The migrate config file contains JSON with the following format:
			{
				"Versions": ["2881e9","52a13","57e8d"],
				"Migrations": [
					{
						"Name": "instance-name",
						"SrcStore": "source store",
						"DstStore": {
							"Path": "/path/to/store",
							"Engine": "badger"
						}
					},
					{
						"Name": "#datatype",
						"SrcStore": "source store",
						"DstStore": {
							"Path": "/another/store",
							"Engine": "badger"
						}
					},
				],
				"Exclusions": ["name1", "name2"]
			}
			
		Each migration is done sequentially, one not starting until the other
		is fully completed.  This batch command is similar to many calls of the
		"migrate" command with "transmit=<versions>" option or "transmit=all" if
		no Versions are specified.

		Individual instance names must precede general data type migration 
		specifications.
	
	repo <UUID> flatten-metadata <flatten config file>
    
		Creates reduced nodes metadata into a destination metadata store.

		NOTE: If versions are skipped, the skipped portion cannot be branched or the
		created metadata will not work.

		The flatten config file contains JSON compatible with the "merge-batch" command
		and adds optional fields to set version notes and logs:

			{
				"Versions": ["2881e9","52a13","57e8d"],
				"Exclusions": ["name1", "name2"],
				"DstStore": {
					"Engine": "badger",
					"Path": "/path/to/new/metadata_db"
				},
				"Alias": "my new repo alias",
				"Description": "my new description",
				"RepoLog": ["some new", "log statements", "for the repo itself"],
				"VersionsMeta": [
					{
						"Version": "2881e9",
						"NodeNote": "a new commit message for this node",
						"NodeLog": ["some new", "log statements", "for the repo node"],
						"Branch": "another branch"	
					},
					{
						"Version": "52a13",
						"NodeNote": "a new commit message for this node",
						"NodeLog": ["some new", "log statements", "for the repo node"],
						"Branch": "some other branch"
					}
				]
			}

	repo <UUID> flatten-mutations <data UUID> <start UUID> <end UUID> <output filename>

		Flattens a log of all mutations from start to end UUID and outputs it into
		given file.
	
	repo <UUID> transfer-data <old store> <new store> <transfer config file>

		Transfers data from an old store to a new store (specified by nicknames in 
		TOML file) using settings specified in a transfer JSON config file.  The
		transfer config file must establish the set of versions to be transferred.

		An example of the transfer JSON configuration file format:
		{
			"Versions": [
				"8a90ec0d257c415cae29f8c46603bcae",
				"a5682904bb824c06aba470c0a0cbffab",
				...
			],
			"Metadata": true,
		}
		If no versions are specified, a copy of all versions is done.  If some
		versions are specified, key-value pair transfer only occurs if the version 
		in which it was saved is specified on the list.  This is useful for editing 
		a preexisting store with new versions.

		If Metadata property is true, then if metadata exists in the old store,
		it is transferred to the new store with only the versions specified
		appearing in the DAG.
	
	repo <UUID> limit-versions <version config file>

		Removes versions not present in the transfer config file from the metadata.
		An example of the version JSON configuration file format:
		{
			"Versions": [
				"8a90ec0d257c415cae29f8c46603bcae",
				"a5682904bb824c06aba470c0a0cbffab",
				...
			}
		}
					
	repo <UUID> copy <source instance name> <clone instance name> <settings...>
    
        A local data instance copy with optional datatype-specific delimiter,
        where <settings> are optional "key=value" strings:
				
		filter=<filter0>/<filter1>/...
		
			Separate filters by the forward slash.  See datatype help
            for the types of filters they will use for pushes.  Examples
            include "roi:name,uuid" and "tile:xy,xz".
		
		transmit=[all | flatten]

			The default transmit "all" copies all versions of the source.
			
			A transmit "flatten" will copy just the version specified and
			flatten the key/values so there is no history.

	repo <UUID> push <remote DVID address> <settings...>

        A DVID-to-DVID repo copy with optional datatype-specific delimiter,
		where <settings> are optional "key=value" strings:

		data=<data1>[,<data2>[,<data3>...]]
		
			If supplied, the transmitted data will be limited to the listed
			data instance names.
				
		filter=<filter0>/<filter1>/...
		
			Separate filters by the forward slash.  See datatype help
            for the types of filters they will use for pushes.  Examples
            include "roi:name,uuid" and "tile:xy,xz".
		
		transmit=[all | branch | flatten | version]

			The default transmit "all" sends all versions necessary to 
			make the remote equivalent or a superset of the local repo.
			
			A transmit "flatten" will send just the version specified and
			flatten the key/values so there is no history.
			
			A transmit "branch" will send just the ancestor path of the
			version specified.

			A transmit "version" sends just the deltas associated with
			the single version specified.

	repo <UUID> merge <UUID> [, <UUID>, ...]

		This requires all UUIDs to be committed and generates a new
		child with merged data.  This merge assumes the parents
		have no conflicts, i.e., each data type can easily resolve
		the merging of key-value pairs, and will generate an error
		message if this is not the case.


For further information, use a web browser to visit the server for this
datastore:  

	http://%s
`

const (
	commandMsg = "server.Command"
)

func init() {
	d := rpc.Dispatcher()
	d.AddFunc(commandMsg, handleCommand)

	gorpc.RegisterType(&datastore.Request{})
}

// SendRPC sends a request to a remote DVID.
func SendRPC(addr string, req datastore.Request) error {
	c := gorpc.NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	dc := rpc.Dispatcher().NewFuncClient(c)
	resp, err := dc.Call(commandMsg, req)
	if err != nil {
		return fmt.Errorf("RPC error for %q: %v", req.Command, err)
	}

	reply, ok := resp.(*datastore.Response)
	if !ok {
		return fmt.Errorf("bad response to request %s: %v", req, resp)
	}
	return reply.Write(os.Stdout)
}

// switchboard for remote command execution
func handleCommand(cmd *datastore.Request) (reply *datastore.Response, err error) {
	if cmd.Name() == "" {
		err = fmt.Errorf("server error: got empty command")
		return
	}
	if !dvid.RequestsOK() {
		err = fmt.Errorf("Server currently locked from fulfilling requests")
		return
	}
	reply = new(datastore.Response)

	switch cmd.Name() {

	case "help":
		reply.Text = fmt.Sprintf(rpcHelp, RPCAddress(), HTTPAddress())

	case "shutdown":
		dvid.Infof("DVID server halting due to 'shutdown' command.")
		reply.Text = fmt.Sprintf("DVID server at %s is being shutdown...\n", RPCAddress())
		// launch goroutine shutdown so we can concurrently return shutdown message to client.
		go Shutdown()

	case "types":
		if len(cmd.Command) == 1 {
			text := "\nData Types within this DVID Server\n"
			text += "----------------------------------\n"
			var mapTypes map[dvid.URLString]datastore.TypeService
			if mapTypes, err = datastore.Types(); err != nil {
				err = fmt.Errorf("error trying to retrieve data types within this DVID server")
				return
			}
			for url, typeservice := range mapTypes {
				text += fmt.Sprintf("%-20s %s\n", typeservice.GetTypeName(), url)
			}
			reply.Text = text
		} else {
			if len(cmd.Command) != 3 || cmd.Command[2] != "help" {
				err = fmt.Errorf("Unknown types command: %q", cmd.Command)
				return
			}
			var typename string
			var typeservice datastore.TypeService
			cmd.CommandArgs(1, &typename)
			if typeservice, err = datastore.TypeServiceByName(dvid.TypeString(typename)); err != nil {
				return
			}
			reply.Text = typeservice.Help()
		}

	case "repos":
		var subcommand string
		cmd.CommandArgs(1, &subcommand)

		switch subcommand {
		case "new":
			var alias, description string
			cmd.CommandArgs(2, &alias, &description)

			config := cmd.Settings()
			var uuidStr, passcode string
			var found bool
			if uuidStr, found, err = config.GetString("uuid"); err != nil {
				return
			}
			var assign *dvid.UUID
			if !found {
				assign = nil
			} else {
				uuid := dvid.UUID(uuidStr)
				assign = &uuid
			}
			if passcode, found, err = config.GetString("passcode"); err != nil {
				return
			}
			var root dvid.UUID
			root, err = datastore.NewRepo(alias, description, assign, passcode)
			if err != nil {
				return
			}
			if err = datastore.SetRepoAlias(root, alias); err != nil {
				return
			}
			if err = datastore.SetRepoDescription(root, description); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("New repo %q created with head node %s\n", alias, root)

		case "delete":
			var uuidStr, passcode string
			cmd.CommandArgs(2, &uuidStr, &passcode)

			var uuid dvid.UUID
			if uuid, _, err = datastore.MatchingUUID(uuidStr); err != nil {
				return
			}
			if err = datastore.DeleteRepo(uuid, passcode); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Started deletion of repo %s.\n", uuid)

		default:
			err = fmt.Errorf("Unknown repos command: %q", subcommand)
			return
		}

	case "repo":
		var uuidStr, subcommand string
		cmd.CommandArgs(1, &uuidStr, &subcommand)
		var uuid dvid.UUID
		if uuid, _, err = datastore.MatchingUUID(uuidStr); err != nil {
			return
		}

		switch subcommand {
		case "new":
			var typename, dataname string
			cmd.CommandArgs(3, &typename, &dataname)

			// Get TypeService
			var typeservice datastore.TypeService
			if typeservice, err = datastore.TypeServiceByName(dvid.TypeString(typename)); err != nil {
				return
			}

			// Create new data
			config := cmd.Settings()
			if _, err = datastore.NewData(uuid, typeservice, dvid.InstanceName(dataname), config); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Data %q [%s] added to node %s\n", dataname, typename, uuid)
			datastore.AddToRepoLog(uuid, []string{cmd.String()})

		case "rename":
			var name1, name2, passcode string
			cmd.CommandArgs(3, &name1, &name2, &passcode)
			oldname := dvid.InstanceName(name1)
			newname := dvid.InstanceName(name2)

			// Make sure this instance exists.
			if _, err = datastore.GetDataByUUIDName(uuid, oldname); err != nil {
				err = fmt.Errorf("Error trying to rename %q for UUID %s: %v", oldname, uuid, err)
				return
			}

			// Do the rename.
			if err = datastore.RenameData(uuid, oldname, newname, passcode); err != nil {
				err = fmt.Errorf("Error renaming data instance %q to %q: %v", oldname, newname, err)
				return
			}
			reply.Text = fmt.Sprintf("Renamed data instance %q to %q from DAG subgraph @ root %s\n", oldname, newname, uuid)

		case "branch":
			var branchname string
			cmd.CommandArgs(3, &branchname, &uuidStr)

			var assign *dvid.UUID

			// non-master branch name must be specified
			if branchname == "" || branchname == "master" {
				return
			}

			if uuidStr == "" {
				assign = nil
			} else {
				u := dvid.UUID(uuidStr)
				assign = &u
			}
			var child dvid.UUID
			if child, err = datastore.NewVersion(uuid, fmt.Sprintf("branch of %s", uuid), branchname, assign); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Branch %s added to node %s\n", child, uuid)
			datastore.AddToRepoLog(uuid, []string{cmd.String()})

		case "newversion":
			cmd.CommandArgs(3, &uuidStr)

			var assign *dvid.UUID
			if uuidStr == "" {
				assign = nil
			} else {
				u := dvid.UUID(uuidStr)
				assign = &u
			}
			var child dvid.UUID
			if child, err = datastore.NewVersion(uuid, fmt.Sprintf("branch of %s", uuid), "", assign); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Branch %s added to node %s\n", child, uuid)
			datastore.AddToRepoLog(uuid, []string{cmd.String()})

		case "merge":
			uuids := cmd.CommandArgs(2)

			parents := make([]dvid.UUID, len(uuids)+1)
			parents[0] = dvid.UUID(uuid)
			i := 1
			for _, uuid := range uuids {
				parents[i] = dvid.UUID(uuid)
				i++
			}
			var child dvid.UUID
			child, err = datastore.Merge(parents, fmt.Sprintf("merge of parents %v", parents), datastore.MergeConflictFree)
			if err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Parents %v merged into node %s\n", parents, child)
			datastore.AddToRepoLog(uuid, []string{cmd.String()})

		case "storage-details":
			go func() {
				_, err := datastore.GetStorageDetails()
				if err != nil {
					dvid.Errorf("storage-details: %v\n", err)
				}
			}()
			reply.Text = "Started storage details dump in log..."

		case "flatten-mutations":
			var dataStr, startStrUUID, endStrUUID, filename string
			cmd.CommandArgs(3, &dataStr, &startStrUUID, &endStrUUID, &filename)
			var d datastore.DataService
			d, err = datastore.GetDataByDataUUID(dvid.UUID(dataStr))
			if err != nil {
				return
			}
			var startUUID, endUUID dvid.UUID
			if startUUID, _, err = datastore.MatchingUUID(startStrUUID); err != nil {
				return
			}
			if endUUID, _, err = datastore.MatchingUUID(endStrUUID); err != nil {
				return
			}
			dumper, ok := d.(datastore.MutationDumper)
			if !ok {
				reply.Text = fmt.Sprintf("The data UUID %s (name %q) does not support mutation dumping\n", dataStr, d.DataName())
			}
			reply.Text, err = dumper.DumpMutations(startUUID, endUUID, filename)
			if err != nil {
				return
			}

		case "flatten-metadata":
			var dstStoreName, configFName string
			cmd.CommandArgs(3, &configFName)
			if err = datastore.FlattenMetadata(uuid, configFName); err != nil {
				return
			}
			reply.Text = fmt.Sprintf("Created metadata for uuid %s in store %q\n", uuid, dstStoreName)

		case "migrate":
			var source, srcStoreName, dstStoreName string
			cmd.CommandArgs(3, &source, &srcStoreName, &dstStoreName)
			var srcStore, dstStore dvid.Store
			srcStore, err = storage.GetStoreByAlias(storage.Alias(srcStoreName))
			if err != nil {
				return
			}
			dstStore, err = storage.GetStoreByAlias(storage.Alias(dstStoreName))
			if err != nil {
				return
			}
			config := cmd.Settings()
			go func() {
				if err = datastore.MigrateInstance(uuid, dvid.InstanceName(source), srcStore, dstStore, config, nil); err != nil {
					dvid.Errorf("migrate error: %v\n", err)
				}
			}()
			reply.Text = fmt.Sprintf("Started migration of uuid %s data instance %q from store %q to %q\n", uuid, source, srcStoreName, dstStoreName)

		case "migrate-batch":
			var configFName string
			cmd.CommandArgs(3, &configFName)
			go func() {
				if err = datastore.MigrateBatch(uuid, configFName); err != nil {
					dvid.Errorf("migrate error: %v\n", err)
				}
			}()
			reply.Text = fmt.Sprintf("Started batch migration of uuid %s with configuration file %q\n", uuid, configFName)

		case "copy":
			var source, target string
			cmd.CommandArgs(3, &source, &target)
			config := cmd.Settings()
			go func() {
				if err = datastore.CopyInstance(uuid, dvid.InstanceName(source), dvid.InstanceName(target), config); err != nil {
					dvid.Errorf("copy error: %v\n", err)
				}
			}()
			reply.Text = fmt.Sprintf("Started copy of uuid %s data instance %q to %q...\n", uuid, source, target)

		case "transfer-data":
			var oldStoreName, dstStoreName, configFName string
			cmd.CommandArgs(3, &oldStoreName, &dstStoreName, &configFName)
			var srcStore, dstStore dvid.Store
			srcStore, err = storage.GetStoreByAlias(storage.Alias(oldStoreName))
			if err != nil {
				return
			}
			dstStore, err = storage.GetStoreByAlias(storage.Alias(dstStoreName))
			if err != nil {
				return
			}
			go func() {
				SetReadOnly(true)
				if err = datastore.TransferData(uuid, srcStore, dstStore, configFName); err != nil {
					dvid.Errorf("transfer-data error: %v\n", err)
				}
				SetReadOnly(false)
			}()
			reply.Text = fmt.Sprintf("Started data transfer of repo %s from store %q to %q.  Server now in read-only mode.\n", uuid, oldStoreName, dstStoreName)

		case "limit-versions":
			var configFName string
			cmd.CommandArgs(3, &configFName)
			if err = datastore.LimitVersions(uuid, configFName); err != nil {
				dvid.Errorf("limit-versions error: %v\n", err)
			}
			reply.Text = fmt.Sprintf("Limited metadata versions for repo %s\n", uuid)

		case "make-master":
			var newMasterBranchName string
			cmd.CommandArgs(3, &newMasterBranchName)
			if err = datastore.MakeMaster(uuid, newMasterBranchName); err != nil {
				dvid.Errorf("make-master error: %v\n", err)
			}
			reply.Text = fmt.Sprintf("Made branch from %s the new master\n", uuid)

		case "hide-branch":
			var branchName string
			cmd.CommandArgs(3, &branchName)
			if err = datastore.HideBranch(uuid, branchName); err != nil {
				dvid.Errorf("make-master error: %v\n", err)
			}
			reply.Text = fmt.Sprintf("Hid branch %s in repo with UUID %s\n", branchName, uuid)

		case "push":
			var target string
			cmd.CommandArgs(3, &target)
			config := cmd.Settings()
			go func() {
				if err = datastore.PushRepo(uuid, target, config); err != nil {
					dvid.Errorf("push error: %v\n", err)
				}
			}()
			reply.Text = fmt.Sprintf("Started push of repo %s to %q...\n", uuid, target)

			/*
				case "pull":
					var target string
					cmd.CommandArgs(3, &target)
					config := cmd.Settings()
					if err = datastore.Pull(uuid, target, config); err != nil {
						return
					}
					reply.Text = fmt.Sprintf("Repo %s pulled from %q\n", uuid, target)
			*/

		case "delete":
			var dataId string // either data name or data UUID
			var passcode string
			cmd.CommandArgs(3, &dataId, &passcode)

			// Try to delete a data UUID first, and if doesn't work, try by name.
			if err = datastore.DeleteDataByDataUUID(dvid.UUID(dataId), passcode); err != nil {
				err = datastore.DeleteDataByName(uuid, dvid.InstanceName(dataId), passcode)
			}
			if err != nil {
				err = fmt.Errorf("error deleting data instance %q: %v", dataId, err)
				return
			}
			reply.Text = fmt.Sprintf("Started deletion of data instance %q from repo with root %s\n", dataId, uuid)

		default:
			err = fmt.Errorf("unknown command: %q", cmd)
			return
		}

	case "node":
		var uuidStr, descriptor string
		cmd.CommandArgs(1, &uuidStr, &descriptor)
		var uuid dvid.UUID
		if uuid, _, err = datastore.MatchingUUID(uuidStr); err != nil {
			return
		}

		// Get the DataService
		dataname := dvid.InstanceName(descriptor)
		var subcommand string
		cmd.CommandArgs(3, &subcommand)
		var dataservice datastore.DataService
		if dataservice, err = datastore.GetDataByUUIDName(uuid, dataname); err != nil {
			return
		}
		if subcommand == "help" {
			reply.Text = dataservice.Help()
			return
		}
		err = dataservice.DoRPC(*cmd, reply)
		return

	default:
		// Check to see if it's a name of a compiled data type, in which case we refer it to the data type.
		types := datastore.CompiledTypes()
		for name, typeservice := range types {
			if name == dvid.TypeString(cmd.Argument(0)) {
				err = typeservice.Do(*cmd, reply)
				return
			}
		}

		err = fmt.Errorf("Unknown command: '%s'", *cmd)
	}
	return
}
