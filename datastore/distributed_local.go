// +build !clustered,!gcloud

/*
	This file contains local server code supporting distributed operations such as push/pull with
	optional delimiting using an ROI.  Note that the actual sending of data is implemented at the
    datatype level (if supporting ROI filtering) or the generic datastore package level if
    no ROI filtering is necessary.  In the latter case, we do not have to interpret the keys
    to determine their spatial context.

    TODO: When we actually have multiple varieties of target platform (cluster, gcloud managed vm, amzn ec2),
    the repo/DAG data structure needs to be cross-platform since the receiving DVID could be an entirely
    different platform.  For example, clustered DVID frontends could use an in-memory distributed
    kv store.
*/

package datastore

import (
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	CommandPushStart = "PUSH_START"
	CommandPushStop  = "PUSH_STOP"
)

const MaxBatchSize = 1000

func init() {
	message.RegisterCommand(CommandPushStart, message.NewSessionFunc(pushStart))
}

// TODO -- If we are actively reading instead of passively taking messages, consider
// the way lexer is written:
// http://golang.org/src/pkg/text/template/parse/lex.go?h=lexer#L86

// stateFn represents the state of the push as the next function to be called
type stateFn func(*pusher, *message.Message) error

type pusher struct {
	state       stateFn
	repo        *repoT                // TODO: assumes this is local not cluster.
	instanceMap dvid.InstanceMap      // map from pushed to local instance ids
	versionMap  dvid.VersionMap       // map from pushed to local version ids
	procQueue   message.PostProcQueue // queue of post-processing commands

	// tracking variables when reading in the key-value pairs
	storeType  storage.DataStoreType
	instanceID dvid.InstanceID
	versionID  dvid.VersionID

	store storage.KeyValueDB

	// messages     chan *message.Message // channel for passing messages from client
}

// ProcessMessage fulfills the server.Command interface
func (p *pusher) ProcessMessage(m *message.Message) error {
	switch m.Type {
	case message.CommandType:
		dvid.Debugf("Received command %q\n", m.Name)
		if m.Name != CommandPushStop {
			return fmt.Errorf("Command %q not expected within dvid push command processing", m.Name)
		}
		p.state = nil
		return finishPush(p)

	case message.KeyValueType, message.BinaryType:
		//p.messages <- m
		return p.state(p, m)

	case message.PostProcType:
		dvid.Debugf("Received post-processing command %q\n", m.Name)
		if err := p.procQueue.Add(m); err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown message type sent to dvid push handler: %v", m.Type)
	}
	return nil
}

func pushStart(m *message.Message) (message.Session, error) {
	dvid.Debugf("Push start\n")
	if manager == nil {
		return nil, fmt.Errorf("Can't process pushes when datastore manager not running!")
	}

	p := &pusher{
		// messages:  make(chan *message.Message),
		procQueue: message.NewPostProcQueue(),
	}

	// Next step: get the repo
	p.state = readRepo

	return p, nil
}

func readRepo(p *pusher, m *message.Message) error {
	dvid.Debugf("Reading Repo message...\n")
	if m.Type != message.BinaryType {
		return fmt.Errorf("Expected binary message for repo in dvid push.  Got %s message instead", m.Type)
	}

	// Get the repo metadata
	// TODO -- get additional information indicating origin and push configuration
	p.repo = new(repoT)
	if err := p.repo.GobDecode(m.Data); err != nil {
		return err
	}
	repoID, err := manager.newRepoID()
	if err != nil {
		return err
	}
	p.repo.id = repoID

	p.instanceMap, p.versionMap, err = p.repo.remapLocalIDs()
	if err != nil {
		return err
	}

	// Make sure pushed UUID doesn't already exist here.
	// TODO -- allow version-level pushes, not just repos
	duplicateRepo, err := manager.repoFromUUID(p.repo.uuid)
	if err != nil {
		return err
	}
	if duplicateRepo != nil {
		return fmt.Errorf("Repo %s already exists", duplicateRepo.uuid)
	}

	// Next step: read key-value pairs
	p.state = readKeyValue

	return nil
}

func readKeyValue(p *pusher, m *message.Message) error {
	if m.Type != message.KeyValueType {
		return fmt.Errorf("Expected key-value messages in dvid push.  Got %s message instead", m.Type)
	}

	if m.KV == nil || m.KV.K == nil || m.KV.V == nil {
		dvid.Debugf("Received bad keyvalue from socket: %v\n", m)
	}
	oldInstance, oldVersion, _, err := storage.DataKeyToLocalIDs(m.KV.K)
	if err != nil {
		dvid.Debugf("Received %s: %s => key %v, value %d bytes\n", m.Type, m.Name,
			m.KV.K, len(m.KV.V))
		return err
	}

	// Modify the transmitted key-value to have local instance and version ids.
	newInstanceID, found := p.instanceMap[oldInstance]
	if !found {
		return fmt.Errorf("Received key with instance id (%d) not present in repo: %v",
			oldInstance, p.instanceMap)
	}
	newVersionID, found := p.versionMap[oldVersion]
	if !found {
		return fmt.Errorf("Received key with version id (%d) not present in repo: %v",
			oldVersion, p.versionMap)
	}

	// If this is first read or a new instance, get the assigned store.
	var checkStore bool
	if newInstanceID != p.instanceID {
		checkStore = true
		p.instanceID = newInstanceID
	}
	if newVersionID != p.versionID {
		p.versionID = newVersionID
	}
	if checkStore || p.store == nil {
		if p.store, err = getInstanceKeyValueDB(p.instanceID); err != nil {
			return err
		}
	}

	// Store the updated key-value
	if err = storage.UpdateDataKey(m.KV.K, p.instanceID, p.versionID, 0); err != nil {
		return fmt.Errorf("Unable to update data key %v", m.KV.K)
	}
	p.store.RawPut(m.KV.K, m.KV.V)

	// No need to alter state since we stay in this state until we receive STOP signal
	return nil
}

func finishPush(p *pusher) error {
	// Add this repo to current DVID server
	if err := manager.addRepo(p.repo); err != nil {
		return err
	}

	// Run any post-processing requests asynchronously since they may take a long time.
	go p.procQueue.Run()

	return nil
}

// Push pushes a Repo to a remote DVID server at the target address.  An ROI delimiter
// can be specified in the Config.
func Push(uuid dvid.UUID, target string, config dvid.Config) error {
	if manager == nil {
		return ErrManagerNotInitialized
	}

	if target == "" {
		target = message.DefaultAddress
		dvid.Infof("No target specified for push, defaulting to %q\n", message.DefaultAddress)
	}

	// Get the repo
	repo, err := manager.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	// Get the push configuration
	roiname, err := getROI(config)
	if err != nil {
		return err
	}
	data, err := repo.getDataInstances(config)
	if err != nil {
		return err
	}

	// Establish connection with target, which may be itself
	s, err := message.NewPushSocket(target)
	if err != nil {
		return fmt.Errorf("Unable to create new push socket: %s", err.Error())
	}

	// Send PUSH command start
	if err = s.SendCommand(CommandPushStart); err != nil {
		return err
	}

	// Send the repo metadata, transmit type, and other useful data for remote server.
	// TODO -- add additional information indicating origin and push configuration
	dvid.Infof("Sending repo %s data to %q\n", uuid, target)
	repoSerialization, err := repo.GobEncode()
	if err != nil {
		return err
	}
	if err = s.SendBinary("repo", repoSerialization); err != nil {
		return err
	}

	// For each data instance, send the data delimited by the roi
	for _, instance := range data {
		dvid.Infof("Sending instance %q data to %q\n", instance.DataName(), target)
		if err := instance.Send(s, roiname, repo.uuid); err != nil {
			dvid.Errorf("Aborting send of instance %q data\n", instance.DataName())
			return err
		}
	}

	// Send PUSH command end
	dvid.Debugf("Sending PUSH STOP command to %q\n", target)
	if err = s.SendCommand(CommandPushStop); err != nil {
		return err
	}
	return nil
}

/*
func Pull(repo Repo, target string, config dvid.Config) error {
	// To Pull() we initiate a push from target.
	// It's up to target whether it will push or not.
	return nil
}
*/

// Return roi name or empty string
func getROI(config dvid.Config) (string, error) {
	roiname, found, err := config.GetString("roi")
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return roiname, nil
}

// Return transmit type
func getTransmit(config dvid.Config) (string, error) {
	roiname, found, err := config.GetString("roi")
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return roiname, nil
}

// Return all data instances or just those selected in configuration.
func (r *repoT) getDataInstances(config dvid.Config) ([]DataService, error) {
	// Since we can have names separated by commas, split them
	namesString, found, err := config.GetString("data")
	if err != nil {
		return nil, err
	}
	datanames := strings.Split(namesString, ",")

	r.RLock()
	defer r.RUnlock()

	var datalist []DataService
	if !found || len(datanames) == 0 {
		// use all data instances
		for _, dataservice := range r.data {
			datalist = append(datalist, dataservice)
		}
		return datalist, nil
	}
	// use only those data instances given
	for _, name := range datanames {
		dataservice, found := r.data[dvid.InstanceName(name)]
		if !found {
			return nil, fmt.Errorf("Data instance %q was not found", name)
		}
		datalist = append(datalist, dataservice)
	}
	return datalist, nil
}
