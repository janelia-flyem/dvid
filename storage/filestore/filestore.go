// +build filestore

/*
	Package filestore implements a simple file-based store that fulfills
	the KeyValueDB interface.  Keys are strings that must be valid file names.
*/
package filestore

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/semver"
	"github.com/janelia-flyem/go/uuid"
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in filestore: %v\n", err)
	}
	e := Engine{"filestore", "File-based key value store", ver}
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
	return e.newStore(config)
}

// ---- TestableEngine interface implementation -------

// AddTestConfig adds the filestore engine as a possible store.
func (e Engine) AddTestConfig(backend *storage.Backend) (storage.Alias, error) {
	alias := storage.Alias("filestore")
	if backend.Stores == nil {
		backend.Stores = make(map[storage.Alias]dvid.StoreConfig)
	}
	tc := map[string]interface{}{
		"path":    fmt.Sprintf("dvid-test-filestore-%x", uuid.NewV4().Bytes()),
		"testing": true,
	}
	var c dvid.Config
	c.SetAll(tc)
	backend.Stores[alias] = dvid.StoreConfig{Config: c, Engine: "filestore"}
	return alias, nil
}

// Delete implements the TestableEngine interface by providing a way to dispose
// of testing databases.
func (e Engine) Delete(config dvid.StoreConfig) error {
	path, _, err := parseConfig(config)
	if err != nil {
		return err
	}

	// Delete the directory if it exists
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("Can't delete old datastore %q: %v", path, err)
		}
	}
	return nil
}

func parseConfig(config dvid.StoreConfig) (path string, testing bool, err error) {
	c := config.GetAll()

	v, found := c["path"]
	if !found {
		err = fmt.Errorf("%q must be specified for filestore configuration", "path")
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

type fileStore struct {
	path   string
	config dvid.StoreConfig
}

// newStore returns a file-based key-value store, insuring a directory at the path.
func (e Engine) newStore(config dvid.StoreConfig) (*fileStore, bool, error) {
	path, _, err := parseConfig(config)
	if err != nil {
		return nil, false, err
	}

	var created bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		dvid.Infof("File store not already at path (%s). Creating ...\n", path)
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, false, err
		}
		created = true
	} else {
		dvid.Infof("Found file store at %s (err = %v)\n", path, err)
	}

	store := &fileStore{
		path:   path,
		config: config,
	}
	return store, created, nil
}

// ---- Store interface ------

func (fs *fileStore) String() string {
	return fmt.Sprintf("file store @ %s", fs.path)
}

func (fs *fileStore) Close() {}

func (fs *fileStore) Equal(config dvid.StoreConfig) bool {
	path, _, err := parseConfig(config)
	if err != nil {
		return false
	}
	return path == fs.path
}

// ---- KeyValueGetter interface ------

type instanceGetter interface {
	InstanceID() dvid.InstanceID
}

func (fs *fileStore) filepathFromTKey(ctx storage.Context, tk storage.TKey) (dirpath, filename string, err error) {
	var class storage.TKeyClass
	if class, err = tk.Class(); err != nil {
		return
	}
	var namebytes []byte
	if namebytes, err = tk.ClassBytes(class); err != nil {
		return
	}
	h := fnv.New32()
	if _, err = h.Write(namebytes); err != nil {
		return
	}
	hexHash := hex.EncodeToString(h.Sum(nil))
	if len(hexHash) < 5 {
		hexHash += strings.Repeat("0", 5-len(hexHash))
	}
	dirpath = filepath.Join(fs.path, hexHash[0:2], hexHash[2:4], hexHash[4:5])

	v := ctx.VersionID()
	filename = fmt.Sprintf("v%d", v)

	igetter, ok := ctx.(instanceGetter)
	if ok {
		filename += fmt.Sprintf("-i%d", igetter.InstanceID())
	}
	filename += "-" + string(namebytes)
	return
}

// Get returns a value given a key.
func (fs *fileStore) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	if fs == nil {
		return nil, fmt.Errorf("bad fileStore specified for Get on %s", ctx)
	}
	dirpath, filename, err := fs.filepathFromTKey(ctx, tk)
	if err != nil {
		return nil, err
	}
	fpath := filepath.Join(dirpath, filename)
	data, err := ioutil.ReadFile(fpath)
	if err != nil && os.IsNotExist(err) {
		return nil, nil // just return nil data
	}
	return data, err
}

// ---- KeyValueTimestampGetter interface -----

// GetWithTimestamp returns a value and its mod time given a key.
func (fs *fileStore) GetWithTimestamp(ctx storage.Context, tk storage.TKey) (data []byte, modTime time.Time, err error) {
	if fs == nil {
		err = fmt.Errorf("bad fileStore specified for GetWithTimestamp on %s", ctx)
		return
	}
	var dirpath, filename string
	if dirpath, filename, err = fs.filepathFromTKey(ctx, tk); err != nil {
		return
	}
	fpath := filepath.Join(dirpath, filename)
	var f *os.File
	if f, err = os.Open(fpath); err != nil {
		if os.IsNotExist(err) {
			err = nil // just return nil data
		}
		return
	}
	defer f.Close()
	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		return
	}
	modTime = info.ModTime()
	data, err = ioutil.ReadAll(f)
	f.Close()
	return
}

// ---- KeyValueChecker interface -----

// Exists returns true if the key exists.
func (fs *fileStore) Exists(ctx storage.Context, tk storage.TKey) (found bool, err error) {
	if fs == nil {
		err = fmt.Errorf("bad fileStore specified for Exists on %s", ctx)
		return
	}
	var dirpath, filename string
	if dirpath, filename, err = fs.filepathFromTKey(ctx, tk); err != nil {
		return
	}
	fpath := filepath.Join(dirpath, filename)
	_, err = os.Stat(fpath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ---- KeyValueSetter interface ------

// Put writes a value with given key.
func (fs *fileStore) Put(ctx storage.Context, tk storage.TKey, v []byte) error {
	if fs == nil {
		return fmt.Errorf("bad fileStore specified for Put on %s", ctx)
	}
	dirpath, filename, err := fs.filepathFromTKey(ctx, tk)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dirpath, 0777); err != nil {
		return err
	}
	fpath := filepath.Join(dirpath, filename)
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	if _, err := f.Write(v); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return nil
}

// Delete removes a value with given key.
func (fs *fileStore) Delete(ctx storage.Context, tk storage.TKey) error {
	if fs == nil {
		return fmt.Errorf("bad fileStore specified for Delete on %s", ctx)
	}
	dirpath, filename, err := fs.filepathFromTKey(ctx, tk)
	if err != nil {
		return err
	}
	fpath := filepath.Join(dirpath, filename)
	return os.Remove(fpath)
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// TODO: Unimplemented
func (fs *fileStore) RawPut(k storage.Key, v []byte) error {
	return fmt.Errorf("file store %q does not support RawPut() function at this time", fs)
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.
// TODO: Unimplemented
func (fs *fileStore) RawDelete(k storage.Key) error {
	return fmt.Errorf("file store %q does not support RawDelete() function at this time", fs)
}
