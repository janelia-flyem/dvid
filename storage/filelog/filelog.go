package filelog

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/blang/semver"
	"github.com/twinj/uuid"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in filelog: %v\n", err)
	}
	e := Engine{"filelog", "File-based log", ver}
	storage.RegisterEngine(e)
}

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) IsDistributed() bool {
	return false
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

// NewStore returns file-based log. The passed Config must contain "path" setting.
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newLogs(config)
}

func parseConfig(config dvid.StoreConfig) (path string, testing bool, err error) {
	c := config.GetAll()

	v, found := c["path"]
	if !found {
		err = fmt.Errorf("%q must be specified for log configuration", "path")
		return
	}
	var ok bool
	path, ok = v.(string)
	if !ok {
		err = fmt.Errorf("%q setting must be a string (%v)", "path", v)
		return
	}
	v, found = c["testing"]
	if found {
		testing, ok = v.(bool)
		if !ok {
			err = fmt.Errorf("%q setting must be a bool (%v)", "testing", v)
			return
		}
	}
	if testing {
		path = filepath.Join(os.TempDir(), path)
	}
	return
}

// newLogs returns a file-based append-only log backend, creating a log
// at the path if it doesn't already exist.
func (e Engine) newLogs(config dvid.StoreConfig) (*fileLogs, bool, error) {
	path, _, err := parseConfig(config)
	if err != nil {
		return nil, false, err
	}

	var created bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		dvid.Infof("Log not already at path (%s). Creating ...\n", path)
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, false, err
		}
		created = true
	} else {
		dvid.Infof("Found log at %s (err = %v)\n", path, err)
	}

	// opt, err := getOptions(config.Config)
	// if err != nil {
	// 	return nil, false, err
	// }

	log := &fileLogs{
		path:   path,
		config: config,
		files:  make(map[string]*fileLog),
	}
	return log, created, nil
}

type fileLog struct {
	*os.File
	sync.RWMutex
}

func (f *fileLog) writeHeader(msg storage.LogMessage) error {
	buf := make([]byte, 6)
	binary.LittleEndian.PutUint16(buf[:2], msg.EntryType)
	size := uint32(len(msg.Data))
	binary.LittleEndian.PutUint32(buf[2:], size)
	if _, err := f.Write(buf); err != nil {
		return err
	}
	return f.Sync()
}

type fileLogs struct {
	path   string
	config dvid.StoreConfig
	files  map[string]*fileLog // key = data + version UUID
	sync.RWMutex
}

// ReadBinary reads all the data from a given log
func (flogs *fileLogs) ReadBinary(dataID, version dvid.UUID) ([]byte, error) {
	k := string(dataID + "-" + version)
	filename := filepath.Join(flogs.path, k)
	data, err := ioutil.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

/***  Cleaner code for passing variety of log message processors might be too slow due to constant function calling ***

func (flogs *fileLogs) readEntireVersion(dataID, version dvid.UUID, processor func(entryType uint16, data []byte) (cancel bool)) error {
	k := string(dataID + "-" + version)
	filename := filepath.Join(flogs.path, k)

	flogs.RLock()
	fl, found := flogs.files[k]
	flogs.RUnlock()
	if found {
		// close then reopen later.
		fl.Lock()
		fl.Close()
		flogs.Lock()
		delete(flogs.files, k)
		flogs.Unlock()
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	f.Close()

	if len(data) > 0 {
		var pos uint32
		for {
			if len(data) < int(pos+6) {
				dvid.Criticalf("malformed filelog %q at position %d\n", filename, pos)
				break
			}
			entryType := binary.LittleEndian.Uint16(data[pos : pos+2])
			size := binary.LittleEndian.Uint32(data[pos+2 : pos+6])
			pos += 6
			databuf := data[pos : pos+size]
			pos += size
			if cancel := processor(entryType, databuf); cancel {
				break
			}
		}
	}

	if found {
		f2, err2 := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0755)
		if err2 != nil {
			dvid.Errorf("unable to reopen write log %s: %v\n", k, err)
		} else {
			flogs.Lock()
			flogs.files[k] = &fileLog{File: f2}
			flogs.Unlock()
		}
		fl.Unlock()
	}
	return nil
}

****/

// ReadAll reads all messages from a version's filelog.
func (flogs *fileLogs) ReadAll(dataID, version dvid.UUID) ([]storage.LogMessage, error) {
	k := string(dataID + "-" + version)
	filename := filepath.Join(flogs.path, k)

	flogs.RLock()
	fl, found := flogs.files[k]
	flogs.RUnlock()
	if found {
		// close then reopen later.
		fl.Lock()
		fl.Close()
		flogs.Lock()
		delete(flogs.files, k)
		flogs.Unlock()
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	f.Close()

	msgs := []storage.LogMessage{}
	if len(data) > 0 {
		var pos uint32
		for {
			if len(data) < int(pos+6) {
				dvid.Criticalf("malformed filelog %q at position %d\n", filename, pos)
				break
			}
			entryType := binary.LittleEndian.Uint16(data[pos : pos+2])
			size := binary.LittleEndian.Uint32(data[pos+2 : pos+6])
			pos += 6
			databuf := data[pos : pos+size]
			pos += size
			msg := storage.LogMessage{EntryType: entryType, Data: databuf}
			msgs = append(msgs, msg)
			if len(data) == int(pos) {
				break
			}
		}
	}

	if found {
		f2, err2 := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0755)
		if err2 != nil {
			dvid.Errorf("unable to reopen write log %s: %v\n", k, err)
		} else {
			flogs.Lock()
			flogs.files[k] = &fileLog{File: f2}
			flogs.Unlock()
		}
		fl.Unlock()
	}
	return msgs, nil
}

// StreamAll sends log messages down channel, adding one for each message to wait group if provided.
// Closes given channel when streaming is done or an error occurs.
func (flogs *fileLogs) StreamAll(dataID, version dvid.UUID, ch chan storage.LogMessage) error {
	defer close(ch)

	k := string(dataID + "-" + version)
	filename := filepath.Join(flogs.path, k)

	flogs.RLock()
	fl, found := flogs.files[k]
	flogs.RUnlock()
	if found {
		// close then reopen later.
		fl.Lock()
		fl.Close()
		flogs.Lock()
		delete(flogs.files, k)
		flogs.Unlock()
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	f.Close()

	if len(data) > 0 {
		var pos uint32
		for {
			if len(data) < int(pos+6) {
				dvid.Criticalf("malformed filelog %q at position %d\n", filename, pos)
				break
			}
			entryType := binary.LittleEndian.Uint16(data[pos : pos+2])
			size := binary.LittleEndian.Uint32(data[pos+2 : pos+6])
			pos += 6
			databuf := data[pos : pos+size]
			pos += size
			ch <- storage.LogMessage{EntryType: entryType, Data: databuf}
			if len(data) == int(pos) {
				break
			}
		}
	}

	if found {
		f2, err2 := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0755)
		if err2 != nil {
			dvid.Errorf("unable to reopen write log %s: %v\n", k, err)
		} else {
			flogs.Lock()
			flogs.files[k] = &fileLog{File: f2}
			flogs.Unlock()
		}
		fl.Unlock()
	}
	return nil
}

func (flogs *fileLogs) getWriteLog(topic string) (fl *fileLog, err error) {
	var found bool
	flogs.RLock()
	fl, found = flogs.files[topic]
	flogs.RUnlock()
	if !found {
		filename := filepath.Join(flogs.path, topic)
		var f *os.File
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0755)
		if err != nil {
			return
		}
		fl = &fileLog{File: f}
		flogs.Lock()
		flogs.files[topic] = fl
		flogs.Unlock()
	}
	return
}

func (flogs *fileLogs) closeWriteLog(topic string) error {
	flogs.Lock()
	fl, found := flogs.files[topic]
	flogs.Unlock()
	if found {
		fl.Lock()
		err := fl.Close()
		flogs.Lock()
		delete(flogs.files, topic)
		flogs.Unlock()
		fl.Unlock()
		return err
	}
	return nil
}

func (flogs *fileLogs) Append(dataID, version dvid.UUID, msg storage.LogMessage) error {
	topic := string(dataID + "-" + version)
	fl, err := flogs.getWriteLog(topic)
	if err != nil {
		return fmt.Errorf("append log %q: %v", flogs, err)
	}
	fl.Lock()
	if err = fl.writeHeader(msg); err != nil {
		fl.Unlock()
		return fmt.Errorf("bad write of log header to data %s, uuid %s: %v", dataID, version, err)
	}
	if _, err = fl.Write(msg.Data); err != nil {
		fl.Unlock()
		return fmt.Errorf("append log %q: %v", flogs, err)
	}
	err = fl.Sync()
	fl.Unlock()
	if err != nil {
		return fmt.Errorf("err on sync of append log %q: %v", flogs, err)
	}
	return nil
}

func (flogs *fileLogs) CloseLog(dataID, version dvid.UUID) error {
	topic := string(dataID + "-" + version)
	return flogs.closeWriteLog(topic)
}

func (flogs *fileLogs) TopicAppend(topic string, msg storage.LogMessage) error {
	fl, err := flogs.getWriteLog(topic)
	if err != nil {
		return fmt.Errorf("append log %q: %v", flogs, err)
	}
	fl.Lock()
	if err = fl.writeHeader(msg); err != nil {
		fl.Unlock()
		return fmt.Errorf("bad write of log header to topic %q: %v", topic, err)
	}
	if _, err = fl.Write(msg.Data); err != nil {
		fl.Unlock()
		return fmt.Errorf("bad write to append log %q, topic %q: %v", flogs, topic, err)
	}
	err = fl.Sync()
	fl.Unlock()
	if err != nil {
		return fmt.Errorf("err on sync of append log %q, topic %q: %v", flogs, topic, err)
	}
	return nil
}

func (flogs *fileLogs) TopicClose(topic string) error {
	return flogs.closeWriteLog(topic)
}

// --- dvid.Store interface implementation ----

func (flogs *fileLogs) Close() {
	flogs.Lock()
	for _, flogs := range flogs.files {
		err := flogs.Close()
		if err != nil {
			dvid.Errorf("closing log file %q: %v\n", flogs.Name(), err)
		}
	}
	flogs.Unlock()
}

func (flogs *fileLogs) String() string {
	return fmt.Sprintf("write logs @ %s", flogs.path)
}

// Equal returns true if the write log path matches the given store configuration.
func (flogs *fileLogs) Equal(config dvid.StoreConfig) bool {
	path, _, err := parseConfig(config)
	if err != nil {
		return false
	}
	return path == flogs.path
}

func (flogs *fileLogs) GetStoreConfig() dvid.StoreConfig {
	return flogs.config
}

// ---- TestableEngine interface implementation -------

// AddTestConfig sets the filelog as the default append-only log if it already hasn't been set.
func (e Engine) AddTestConfig(backend *storage.Backend) (storage.Alias, error) {
	alias := storage.Alias("filelog")
	if backend.DefaultLog == "" {
		backend.DefaultLog = alias
	}
	if backend.Stores == nil {
		backend.Stores = make(map[storage.Alias]dvid.StoreConfig)
	}
	tc := map[string]interface{}{
		"path":    fmt.Sprintf("dvid-test-filelog-%x", uuid.NewV4().Bytes()),
		"testing": true,
	}
	var c dvid.Config
	c.SetAll(tc)
	backend.Stores[alias] = dvid.StoreConfig{Config: c, Engine: "filelog"}
	return alias, nil
}

// Delete implements the TestableEngine interface by providing a way to dispose
// of the testable filelog.
func (e Engine) Delete(config dvid.StoreConfig) error {
	path, _, err := parseConfig(config)
	if err != nil {
		return err
	}

	// Delete the directory if it exists
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("Can't delete old append-only log directory %q: %v", path, err)
		}
	}
	return nil
}
