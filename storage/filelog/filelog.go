package filelog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/semver"
	"github.com/janelia-flyem/go/uuid"
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
	_, err := f.Write(buf)
	return err
}

func (f *fileLog) readAll() ([]storage.LogMessage, error) {
	msgs := []storage.LogMessage{}
	for {
		hdrbuf := make([]byte, 6)
		_, err := io.ReadFull(f, hdrbuf)
		if err == io.EOF {
			return msgs, nil
		}
		if err != nil {
			return nil, err
		}
		entryType := binary.LittleEndian.Uint16(hdrbuf[0:2])
		size := binary.LittleEndian.Uint32(hdrbuf[2:])
		databuf := make([]byte, size)
		_, err = io.ReadFull(f, databuf)
		if err != nil {
			return nil, err
		}
		msg := storage.LogMessage{EntryType: entryType, Data: databuf}
		msgs = append(msgs, msg)
	}
}

type fileLogs struct {
	path   string
	config dvid.StoreConfig
	files  map[string]*fileLog // key = data + version UUID
	sync.RWMutex
}

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
	fl2 := &fileLog{File: f}
	msgs, err := fl2.readAll()
	fl2.Close()

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
	return msgs, err
}

// StreamAll sends log messages down channel, adding one for each message to wait group if provided.
func (flogs *fileLogs) StreamAll(dataID, version dvid.UUID, ch chan storage.LogMessage, wg *sync.WaitGroup) error {
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
			err = nil
		}
		goto restart
	}
	for {
		hdrbuf := make([]byte, 6)
		_, err = io.ReadFull(f, hdrbuf)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}
		entryType := binary.LittleEndian.Uint16(hdrbuf[0:2])
		size := binary.LittleEndian.Uint32(hdrbuf[2:])
		databuf := make([]byte, size)
		_, err = io.ReadFull(f, databuf)
		if err != nil {
			break
		}
		ch <- storage.LogMessage{EntryType: entryType, Data: databuf}
		if wg != nil {
			wg.Add(1)
		}
	}
	close(ch)
	f.Close()

restart:
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
	return err
}

func (flogs *fileLogs) getWriteLog(dataID, version dvid.UUID) (fl *fileLog, err error) {
	k := string(dataID + "-" + version)
	var found bool
	flogs.RLock()
	fl, found = flogs.files[k]
	flogs.RUnlock()
	if !found {
		filename := filepath.Join(flogs.path, k)
		var f *os.File
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0755)
		if err != nil {
			return
		}
		fl = &fileLog{File: f}
		flogs.Lock()
		flogs.files[k] = fl
		flogs.Unlock()
	}
	return
}

func (flogs *fileLogs) Append(dataID, version dvid.UUID, msg storage.LogMessage) error {
	fl, err := flogs.getWriteLog(dataID, version)
	if err != nil {
		return fmt.Errorf("append log %q: %v", flogs, err)
	}
	fl.Lock()
	if err = fl.writeHeader(msg); err != nil {
		fl.Unlock()
		return fmt.Errorf("bad write of log header to data %s, uuid %s: %v\n", dataID, version, err)
	}
	_, err = fl.Write(msg.Data)
	fl.Unlock()
	if err != nil {
		err = fmt.Errorf("append log %q: %v", flogs, err)
	}
	return err
}

func (flogs *fileLogs) CloseLog(dataID, version dvid.UUID) error {
	k := string(dataID + "-" + version)
	flogs.Lock()
	fl, found := flogs.files[k]
	flogs.Unlock()
	if found {
		fl.Lock()
		err := fl.Close()
		flogs.Lock()
		delete(flogs.files, k)
		flogs.Unlock()
		fl.Unlock()
		return err
	}
	return nil
}

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

// ---- TestableEngine interface implementation -------

// AddTestConfig sets the filelog as the default append-only log.  If another engine is already
// set for the append-only log, it returns an error since only one append-only log backend should
// be tested via tags.
func (e Engine) AddTestConfig(backend *storage.Backend) error {
	if backend.DefaultLog != "" {
		return fmt.Errorf("filelog can't be testable log.  DefaultLog already set to %s", backend.DefaultLog)
	}
	alias := storage.Alias("filelog")
	backend.DefaultLog = alias
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
	return nil
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
