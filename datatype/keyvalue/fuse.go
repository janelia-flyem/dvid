// +build fuse

/*
	This file supports a FUSE file system implementation for keyvalue data.
*/

package keyvalue

import (
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// Mount creates (if not already present) a FUSE file system for this data.
func (d *Data) mount(request datastore.Request, reply *datastore.Response) error {
	var uuidStr, dataName, cmdStr string
	dirnames := request.CommandArgs(1, &uuidStr, &dataName, &cmdStr)
	if len(dirnames) > 1 || len(dirnames) == 0 {
		return fmt.Errorf("Please specify a single directory to be mounted.\n")
	}

	// Make sure we have a directory for this particular UUID.
	uuid, repoID, versionID, err := server.DatastoreService().NodeIDFromString(uuidStr)
	if err != nil {
		return err
	}

	// Make sure we have a FUSE file system at mount directory.
	versionInfo := storage.NewVersionInfo(uuid, repoID, versionID)
	err = storage.OpenFUSE(dirnames[0], d, versionInfo)
	if err != nil {
		return err
	}

	reply.Output = []byte(fmt.Sprintf("Using FUSE file system for UUID %s, data %s @ mount %s\n",
		uuid, d.DataName(), dirnames[0]))
	return nil
}

// ---- storage.Mountable interface implementation

func (d *Data) FUSEDir(vinfo storage.VersionInfo) storage.FUSEDirer {
	return Dir{d, vinfo}
}

// Dir implements both Node and Handle for this data directory
// Each directory is specific to one data in a repo.
type Dir struct {
	*Data
	storage.VersionInfo
}

func (d Dir) Attr() fuse.Attr {
	return fuse.Attr{
		Inode: storage.GenerateInode(d.Data.LocalID(), d.GetVersionID(), nil),
		Mode:  os.ModeDir | 0555,
	}
}

func (d Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	return File{&d, name}, nil
}

// ReadDir returns a list of keys available for this data and its version.
func (d Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	minDataKey := &datastore.DataKey{d.Data.DsetID, d.Data.ID, d.GetVersionID(), dvid.IndexString("")}
	maxDataKey := &datastore.DataKey{d.Data.DsetID, d.Data.ID, d.GetVersionID() + 1, dvid.IndexString("")}
	db, err := server.OrderedKeyValueGetter()
	if err != nil {
		return nil, fuse.EIO
	}
	keys, err := db.KeysInRange(minDataKey, maxDataKey)
	if err != nil {
		return nil, fuse.EIO
	}
	ddirs := []fuse.Dirent{}
	for _, key := range keys {
		dataKey, ok := key.(*datastore.DataKey)
		if ok {
			entry := fuse.Dirent{
				Inode: storage.GenerateInode(d.Data.LocalID(), d.GetVersionID(), dataKey.Index),
				Name:  dataKey.Index.String(),
				Type:  fuse.DT_File,
			}
			ddirs = append(ddirs, entry)
		}
	}
	return ddirs, nil
}

func (d Dir) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	fmt.Printf("Unsupported Dir Write: %s\n", req.String())
	return fuse.EIO
}

// File implements both Node and Handle for a keyvalue entry.
type File struct {
	*Dir
	keyStr string
}

func (f File) Attr() fuse.Attr {
	return fuse.Attr{Mode: 0444}
}

func (f File) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	fmt.Printf("Reading key %s at offset %d\n", f.keyStr, req.Offset)
	data, found, err := f.Dir.Data.GetData(f.Dir.GetUUID(), f.keyStr)
	if err != nil {
		return fuse.EIO
	}
	if !found {
		return fuse.ENOENT
	}
	resp.Data = data
	return nil
}

func (f File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	fmt.Printf("Reading all for key %s\n", f.keyStr)
	data, found, err := f.Dir.Data.GetData(f.Dir.GetUUID(), f.keyStr)
	if err != nil {
		return nil, fuse.EIO
	}
	if !found {
		return nil, fuse.ENOENT
	}
	return data, nil
}

// Append data
func (f File) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	fmt.Printf("Write of %d bytes at offset %d\n", len(req.Data), req.Offset)
	data, found, err := f.Dir.Data.GetData(f.Dir.GetUUID(), f.keyStr)
	if err != nil {
		return fuse.EIO
	}
	if !found {
		return fuse.ENOENT
	}
	if data == nil {
		data = []byte{}
	}
	data = append(data, req.Data...)
	err = f.Dir.Data.PutData(f.Dir.GetUUID(), f.keyStr, data)
	resp.Size = len(req.Data)
	return nil
}

// Replace data
func (f File) WriteAll(b []byte, intr fs.Intr) fuse.Error {
	err := f.Dir.Data.PutData(f.Dir.GetUUID(), f.keyStr, b)
	if err != nil {
		return fuse.EIO
	}
	return nil
}
