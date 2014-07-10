// +build fuse

/*
	This file implements FUSE file systems that can be used to access DVID data.
*/

package storage

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var fuseServer FUSEServer

func init() {
	fuseServer.mounts = make(map[dvid.UUID]Mount, 10)
}

// FUSEServer manages mounted UUIDs for specified data.
type FUSEServer struct {
	dir    string
	mutex  sync.Mutex
	mounts map[dvid.UUID]Mount
}

func (s FUSEServer) Root() (fs.Node, fuse.Error) {
	return MountDir{}, nil
}

type FUSEHandler interface {
	FUSESystem() FUSERooter
}

type FUSERooter interface {
	Root() (fs.Node, fuse.Error)
}

// FUSEDirer can return a type that implements FUSE directory functions.
type FUSEDirer interface {
	Attr() fuse.Attr
	Lookup(string, fs.Intr) (fs.Node, fuse.Error)
	ReadDir(fs.Intr) ([]fuse.Dirent, fuse.Error)
}

// FUSEFiler can return a type that implements Node and Handle.
type FUSEFiler interface {
	Attr() fuse.Attr
	ReadAll(fs.Intr) ([]byte, fuse.Error)
}

// Mountable is an interface for data that can be mounted as FUSE Files.
type Mountable interface {
	// DataName returns the name of the data (e.g., grayscale data that is grayscale8 data type).
	DataName() dvid.DataString

	// LocalDataID returns the server-specific ID (fewer bytes) for this particular data.
	LocalID() dvid.DataLocalID

	// FUSEDir returns a data-specific implementation of a FUSE Directory
	FUSEDir(VersionInfo) FUSEDirer
}

// Data is a slice of mountable data services for a particular version.
type Data []Mountable

// VersionInfo holds both local and global IDs for a node.
type VersionInfo struct {
	uuid      dvid.UUID
	repoID    dvid.RepoID
	versionID dvid.VersionID
}

func NewVersionInfo(uuid dvid.UUID, dID dvid.RepoID, vID dvid.VersionID) VersionInfo {
	return VersionInfo{uuid, dID, vID}
}

func (v VersionInfo) GetUUID() dvid.UUID {
	return v.uuid
}

func (v VersionInfo) GetRepoID() dvid.RepoID {
	return v.repoID
}

func (v VersionInfo) GetVersionID() dvid.VersionLocalID {
	return v.versionID
}

type Mount struct {
	data  Data
	vinfo VersionInfo
}

func (m *Mount) AddData(d Mountable, vinfo VersionInfo) error {
	if m.data == nil {
		m.data = Data{}
		m.vinfo = vinfo
	}
	m.data = append(m.data, d)
	return nil
}

// Inode numbers are uint64 which are composed from most to least significant bits:
// 1) uint16 describing local data ID
// 2) uint16 describing local version ID
// 3) uint32 fnv hash of the filename/key.
func GenerateInode(dID dvid.DataLocalID, vID dvid.VersionLocalID, index dvid.Index) uint64 {
	inode := uint64(dID) << 48
	version := uint64(vID) << 32
	inode |= version

	if index != nil {
		hashable := index.String()
		if hashable != "" {
			buf := bytes.NewBufferString(hashable)
			h := fnv.New32()
			io.Copy(h, buf)
			inode |= uint64(h.Sum32())
		}
	}
	return inode
}

// MountDir implements both Node and Handle for the root mount directory.
type MountDir struct{}

func (MountDir) Attr() fuse.Attr {
	return fuse.Attr{Inode: 1, Mode: os.ModeDir | 0555}
}

// Lookup returns a mounted UUID Node.
func (MountDir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	uuid := dvid.UUID(name) // We expect fully formed UUID, not partial.
	mount, found := fuseServer.mounts[uuid]
	if found {
		return &VersionDir{mount}, nil
	}
	return nil, fuse.ENOENT
}

// ReadDir returns a list of version directories.
func (MountDir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	fmt.Println("MountDir.ReadDir()")
	vdirs := []fuse.Dirent{}
	inode := uint64(2)
	for _, mount := range fuseServer.mounts {
		entry := fuse.Dirent{
			Inode: GenerateInode(0, mount.vinfo.versionID, nil),
			Name:  string(mount.vinfo.uuid),
			Type:  fuse.DT_Dir,
		}
		vdirs = append(vdirs, entry)
		inode++
	}
	return vdirs, nil
}

// VersionDir implements both Node and Handle for the version directories.
// Each directory is specific to one data in a repo.
type VersionDir struct {
	Mount
}

func (v VersionDir) Attr() fuse.Attr {
	return fuse.Attr{
		Inode: GenerateInode(0, v.vinfo.versionID, nil),
		Mode:  os.ModeDir | 0555,
	}
}

func (v VersionDir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	for _, data := range v.data {
		if name == string(data.DataName()) {
			return data.FUSEDir(v.vinfo), nil
		}
	}
	return nil, fuse.ENOENT
}

// ReadDir returns a list of mounted data for this version.
func (v VersionDir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	fmt.Println("VersionDir.ReadDir()")
	ddirs := []fuse.Dirent{}
	for _, data := range v.data {
		entry := fuse.Dirent{
			Inode: GenerateInode(data.LocalID(), v.vinfo.versionID, nil),
			Name:  string(data.DataName()),
			Type:  fuse.DT_Dir,
		}
		ddirs = append(ddirs, entry)
	}
	return ddirs, nil
}

// OpenFUSE mounts the given directory as a FUSE file system.
// The FUSE system is a singleton with only one FUSE server operable.
func OpenFUSE(dir string, data Mountable, vinfo VersionInfo) error {
	fuseServer.mutex.Lock()
	defer fuseServer.mutex.Unlock()

	// Make sure we haven't switched mount directory.
	if len(fuseServer.dir) > 0 {
		if fuseServer.dir != dir {
			return fmt.Errorf("Cannot open more than one FUSE directory.  Currently open: %s\n",
				fuseServer.dir)
		}
	}

	// Make sure our mount directory is present and a directory.
	finfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dir, 0744); err != nil {
				return fmt.Errorf("Cannot create mount directory: %s (%s)\n",
					dir, err.Error())
			}
		} else {
			return fmt.Errorf("Cannot access given mount directory: %s\n", dir)
		}
	} else if !finfo.IsDir() {
		return fmt.Errorf("Given mount point (%s) is not a directory\n", dir)
	}

	// Check if data is already mounted at this version.
	mount, found := fuseServer.mounts[vinfo.uuid]
	if found {
		mount.AddData(data, vinfo)
		return nil
	}

	fuseServer.mounts[vinfo.uuid] = Mount{Data{data}, vinfo}

	// Mount and serve if not already served.
	if fuseServer.dir == "" {
		fuseServer.dir = dir
		conn, err := fuse.Mount(dir)
		if err != nil {
			return err
		}

		// Run FUSE system in gothread.
		go func() {
			dvid.StartCgo()
			fs.Serve(conn, fuseServer)
			dvid.StopCgo()
		}()
	}
	return nil
}

// ShutdownFUSE shuts down any FUSE server that's running.
func ShutdownFUSE() {
	fuseServer.mutex.Lock()
	if len(fuseServer.dir) > 0 {
		err := exec.Command("umount", fuseServer.dir).Run()
		if err != nil && runtime.GOOS == "linux" {
			exec.Command("/bin/fusermount", "-u", fuseServer.dir).Run()
		}
	}
	fuseServer.mutex.Unlock()
}
