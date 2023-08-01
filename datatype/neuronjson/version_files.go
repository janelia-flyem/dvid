package neuronjson

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
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

type versionFiles struct {
	fmap map[dvid.UUID]*os.File
	path string
}

func initVersionFiles(path string) (vf *versionFiles, err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		dvid.Infof("creating path for version files: %s\n", path)
		if err = os.MkdirAll(path, 0744); err != nil {
			err = fmt.Errorf("can't make directory at %s: %v", path, err)
			return
		}
	} else if err != nil {
		err = fmt.Errorf("error initializing version files directory: %v", err)
		return
	}
	vf = &versionFiles{
		fmap: make(map[dvid.UUID]*os.File),
		path: path,
	}
	return
}

func (vf *versionFiles) write(uuid dvid.UUID, data string) (err error) {
	f, found := vf.fmap[uuid]
	if !found {
		path := filepath.Join(vf.path, string(uuid)+".json")
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
		if err != nil {
			return
		}
		vf.fmap[uuid] = f
		data = "[" + data
	} else {
		data = "," + data
	}
	_, err = f.Write([]byte(data))
	return
}

func (vf *versionFiles) close() {
	for uuid, f := range vf.fmap {
		if _, err := f.Write([]byte("]")); err != nil {
			dvid.Errorf("unable to close list for uuid %s version file: %v\n", uuid, err)
		}
		f.Close()
	}
}

// writeVersions creates a file per version with all changes, including tombstones, for that version.
// Because the data is streamed to appropriate files during full database scan, very little has to
// be kept in memory.
func (d *Data) writeVersions(filePath string) error {
	timedLog := dvid.NewTimeLog()
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}

	vf, err := initVersionFiles(filePath)
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	ch := make(chan *storage.KeyValue, 100)

	var numAnnotations, numTombstones uint64
	go func(wg *sync.WaitGroup, ch chan *storage.KeyValue) {
		for {
			kv := <-ch
			if kv == nil {
				wg.Done()
				break
			}

			_, versionID, _, err := storage.DataKeyToLocalIDs(kv.K)
			if err != nil {
				dvid.Errorf("GetAllVersions error trying to parse data key %x: %v\n", kv.K, err)
				continue
			}
			uuid, err := datastore.UUIDFromVersion(versionID)
			if err != nil {
				dvid.Errorf("GetAllVersions error trying to get UUID from version %d: %v", versionID, err)
				continue
			}

			// append to the appropriate uuid in map of annotations by version
			if kv.K.IsTombstone() {
				numTombstones++
				tk, err := storage.TKeyFromKey(kv.K)
				if err != nil {
					dvid.Errorf("GetAllVersions error trying to parse tombstone key %x: %v\n", kv.K, err)
					continue
				}
				bodyid, err := DecodeTKey(tk)
				if err != nil {
					dvid.Errorf("GetAllVersions error trying to decode tombstone key %x: %v\n", kv.K, err)
					continue
				}
				vf.write(uuid, fmt.Sprintf(`{"bodyid":%s, "tombstone":true}`, bodyid))
			} else {
				numAnnotations++
				vf.write(uuid, string(kv.V))
			}

			if (numTombstones+numAnnotations)%10000 == 0 {
				timedLog.Infof("Getting all neuronjson versions, instance %q, %d annotations, %d tombstones across %d versions",
					d.DataName(), numAnnotations, numTombstones, len(vf.fmap))
			}
		}
		vf.close()
	}(wg, ch)

	ctx := storage.NewDataContext(d, 0)
	begKey := ctx.ConstructKeyVersion(MinAnnotationTKey, 0)
	endKey := ctx.ConstructKeyVersion(MaxAnnotationTKey, 0) // version doesn't matter due to max prefix
	if err := db.RawRangeQuery(begKey, endKey, false, ch, nil); err != nil {
		return err
	}
	wg.Wait()

	timedLog.Infof("Finished GetAllVersions for neuronjson %q, %d annotations, %d tombstones across %d versions",
		d.DataName(), numAnnotations, numTombstones, len(vf.fmap))
	return nil
}
