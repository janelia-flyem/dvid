/*
	This file contains code supporting distributed operations such as push/pull with optional
	delimiting using an ROI.
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
	state        stateFn
	repo         *repoT                // TODO: assumes this is local not cluster.
	instanceMap  dvid.InstanceMap      // map from pushed to local instance ids
	versionMap   dvid.VersionMap       // map from pushed to local version ids
	procQueue    message.PostProcQueue // queue of post-processing commands
	smallBatcher storage.KeyValueBatcher
	bigBatcher   storage.KeyValueBatcher

	// tracking variables when reading in the key-value pairs
	storeType  storage.DataStoreType
	instanceID dvid.InstanceID
	versionID  dvid.VersionID
	batchSize  int
	batch      storage.Batch

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
	if Manager == nil {
		return nil, fmt.Errorf("Can't process pushes when datastore manager not running!")
	}

	p := &pusher{
		// messages:  make(chan *message.Message),
		procQueue: message.NewPostProcQueue(),
	}

	// Get two tiers of storage since we don't know which one will be used for incoming key/value.
	var ok bool
	smallStore, err := storage.SmallDataStore()
	if err != nil {
		return nil, err
	}
	p.smallBatcher, ok = smallStore.(storage.KeyValueBatcher)
	if !ok {
		return nil, fmt.Errorf("Aborting dvid push: Small datastore doesn't support Batch ops")
	}
	bigStore, err := storage.BigDataStore()
	if err != nil {
		return nil, err
	}
	p.bigBatcher, ok = bigStore.(storage.KeyValueBatcher)
	if !ok {
		return nil, fmt.Errorf("Aborting dvid push: Big datastore doesn't support Batch ops")
	}

	// Next step: get the repo
	p.state = readRepo

	return p, nil
}

func readRepo(p *pusher, m *message.Message) error {
	dvid.Debugf("Push Get Repo\n")
	if m.Type != message.BinaryType {
		return fmt.Errorf("Expected binary message for repo in dvid push.  Got %s message instead", m.Type)
	}

	// Get the repo metadata
	// TODO -- get additional information indicating origin and push configuration
	p.repo = new(repoT)
	if err := p.repo.GobDecode(m.Data); err != nil {
		return err
	}
	repoID, err := Manager.NewRepoID()
	if err != nil {
		return err
	}
	p.repo.repoID = repoID

	p.instanceMap, p.versionMap, err = p.repo.remapLocalIDs()
	if err != nil {
		return err
	}

	// Make sure pushed UUID doesn't already exist here.
	// TODO -- allow version-level pushes, not just repos
	duplicateRepo, err := RepoFromUUID(p.repo.rootID)
	if err != nil {
		return err
	}
	if duplicateRepo != nil {
		return fmt.Errorf("Repo %s already exists", duplicateRepo.RootUUID())
	}

	// Next step: read key-value pairs
	p.state = readKeyValue

	return nil
}

func readKeyValue(p *pusher, m *message.Message) error {
	if m.Type != message.KeyValueType {
		return fmt.Errorf("Expected key-value messages in dvid push.  Got %s message instead", m.Type)
	}

	var flush bool
	if m.SType != p.storeType {
		flush = true
		p.storeType = m.SType
	}
	if m.KV == nil || m.KV.K == nil || m.KV.V == nil {
		dvid.Debugf("Received bad keyvalue from socket: %v\n", m)
	}
	oldInstance, oldVersion, _, err := storage.KeyToLocalIDs(m.KV.K)
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

	// Check if we changed instance or version
	if newInstanceID != p.instanceID {
		flush = true
		p.instanceID = newInstanceID
	}
	if newVersionID != p.versionID {
		flush = true
		p.versionID = newVersionID
	}

	// Possibly flush batch
	if flush || p.batchSize >= MaxBatchSize {
		if p.batchSize > 0 && p.batch != nil {
			if err := p.batch.Commit(); err != nil {
				return err
			}
			p.batchSize = 0
		}
		// Use a nil storage.Context so we deal with raw keys and don't bother with
		// ConstructKey() transformations using data and version.   We operate at a low
		// level since we need to modify keys to reflect the receiving DVID server's
		// different local ids.
		switch p.storeType {
		case storage.SmallData:
			p.batch = p.smallBatcher.NewBatch(nil)
		case storage.BigData:
			p.batch = p.bigBatcher.NewBatch(nil)
		}
	}

	// Store the updated key-value
	if err = storage.UpdateDataContextKey(m.KV.K, p.instanceID, p.versionID, 0); err != nil {
		return fmt.Errorf("Unable to update DataContext key %v", m.KV.K)
	}
	p.batch.Put(m.KV.K, m.KV.V)
	p.batchSize++

	// No need to alter state since we stay in this state until we receive STOP signal
	return nil
}

func finishPush(p *pusher) error {
	// Make sure any partial batch is saved.
	if p.batchSize > 0 {
		if err := p.batch.Commit(); err != nil {
			return err
		}
	}

	// Add this repo to current DVID server
	if err := Manager.AddRepo(p.repo); err != nil {
		return err
	}

	// Run any post-processing requests asynchronously since they may take a long time.
	go p.procQueue.Run()

	return nil
}

// Push pushes a Repo to a remote DVID server at the target address.  An ROI delimiter
// can be specified in the Config.
func Push(repo Repo, target string, config dvid.Config) error {
	if target == "" {
		target = message.DefaultAddress
		dvid.Infof("No target specified for push, defaulting to %q\n", message.DefaultAddress)
	}

	// Get the push configuration
	roiname, err := getROI(config)
	if err != nil {
		return err
	}
	data, err := getDataInstances(repo, config)
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

	// Send the repo metadata
	// TODO -- add additional information indicating origin and push configuration
	dvid.Infof("Sending repo %s data to %q\n", repo.RootUUID(), target)
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
		if err := instance.Send(s, roiname, repo.RootUUID()); err != nil {
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

func Pull(repo Repo, target string, config dvid.Config) error {
	// To Pull() we initiate a push from target.
	// It's up to target whether it will push or not.
	return nil
}

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

// Return all data instances or just those selected in configuration.
func getDataInstances(repo Repo, config dvid.Config) ([]DataService, error) {
	// Since we can have names separated by commas, split them
	namesString, found, err := config.GetString("data")
	if err != nil {
		return nil, err
	}
	datanames := strings.Split(namesString, ",")

	var datalist []DataService
	if !found || len(datanames) == 0 {
		// use all data instances
		data, err := repo.GetAllData()
		if err != nil {
			return nil, err
		}
		for _, dataservice := range data {
			datalist = append(datalist, dataservice)
		}
		return datalist, nil
	}
	// use only those data instances given
	for _, name := range datanames {
		dataservice, err := repo.GetDataByName(dvid.InstanceName(name))
		if err != nil {
			return nil, err
		}
		datalist = append(datalist, dataservice)
	}
	return datalist, nil
}
