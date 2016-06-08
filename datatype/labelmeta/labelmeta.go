/*
	Package labelmeta. store meta data pertaining to a labelid. supports syncing of label id if change. This version stores exemplar point
*/
package labelmeta
//removed imports "encoding/binary"
import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/datatype/labelvol"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/labelmeta"
	TypeName = "labelmeta"
)

const HelpMessage = `
API for labelmeta data type (github.com/janelia-flyem/dvid/datatype/labelmeta)
=======================================================================================

Command-line:

$ dvid repo <UUID> new labelmeta <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new labelmeta bodyannotations

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "bodyannotations"
    settings       Configuration settings in "key=value" format separated by spaces.
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/bodyannotations/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of labelmeta data.


POST <api URL>/node/<UUID>/<data name>/sync    NOTE: Still in development. Not ready for use.

    Establishes data instances with which the labelmetas are synced.  Expects JSON to be POSTed
    with the following format:

    { "sync": "labels,bodies" }

    The "sync" property should be followed by a comma-delimited list of data instances that MUST
    already exist.  Currently, syncs should be created before any labelmeta are pushed to
    the server.  If labelmeta already exist, these are currently not synced.

    The labelmeta data type only accepts syncs to labelblk and labelvol data instances.


Note: For the following URL endpoints that return and accept POSTed JSON values, see the JSON format
at end of this documentation.

GET <api URL>/node/<UUID>/<data name>/label/<label>

	Returns labelmeta for a given label.

GET <api URL>/node/<UUID>/<data name>/tag/<tag>

	Returns all point labelmeta with the given tag as an JSON array of labelmetas.

DELETE <api URL>/node/<UUID>/<data name>/label/<label>

	Deletes labelmeta for a given label(id).

GET <api URL>/node/<UUID>/<data name>/labelsall

        Returns all labelmeta data stored in data instance <data name> as a JSON array of labelmetas.

GET <api URL>/node/<UUID>/<data name>/labelids

        Retuns all labelmeta labels stored in data instance <data name> as a JSON array of labels(ids).

POST <api URL>/node/<UUID>/<data name>/labelmeta

	Adds or modifies labelmetas.  The POSTed content is an JSON array of labelmetas.

GET <api URL>/node/<UUID>/<data name>/labelmeta

        Returns an array of labelmetas.  The POSTed is an JSON array of labels(ids). Example: [23485, 281931, 49582, ...]

------

Example JSON Format of labelmetas with ... Note that Pos is option. If not specified it will load in as [0,0,0], still debating whether this is need or not, could be useful if we need to remap annotation to completely different segmentation that has entirely differnt labels:

[
        {
	        "Pos":[33,30,31],
                "Label":14372907,
                "Status":"NotExamined",                
                "Tags":["Review","PAM"],
		"Prop": {
                        "Name":"PAM-sc1",
                        "Another Var1": "A More Complex Value"
                }
        },
        {
		"Pos":[15,27,35],
                "Label":15138572,
                "Status":"Traced",
                "Tags":["Review","PAM"],
		"Prop": {
                        "Name":"PAM-12",
                        "Another Var2": "A More Complex Value"
                }
        }
]

The "Status" property can be one of "Unknown", "NotExamined", "PartiallyTraced", "HardToTrace","Orphan","Traced","Finalized"
The "Tags" property will be indexed and so can be costly if used for very large numbers of labelmetas.
The "Prop" property is an arbitrary object with string values.  The "Prop" object's key are not indexed.
`

var (
	dtype *Type

	DefaultBlockSize int32   = labelblk.DefaultBlockSize
	DefaultRes       float32 = labelblk.DefaultRes
	DefaultUnits             = labelblk.DefaultUnits
)

func init() {
	dtype = new(Type)
	dtype.Type = datastore.Type{
		Name:    TypeName,
		URL:     RepoURL,
		Version: Version,
		Requirements: &storage.Requirements{
			Batcher: true,
		},
	}

	// See doc for package on why channels are segregated instead of interleaved.
	// Data types must be registered with the datastore to be used.
	datastore.Register(dtype)

	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Type{})
	gob.Register(&Data{})
}

const (
	UnknownLmeta LabelmetaStatus = iota
	NotExamined                 // labelmeta has status NotExamined
        PartiallyTraced             // labelmeta has status PartiallyTraced
        HardToTrace                 // labelmeta has status Hard to Trace
        Orphan                      // labelmeta has status Orphan
        Traced                      // labelmeta has status Traced
	Finalized                   // labelmeta has status Finalized
)

// LabelmetaStatus gives the type of a labelmeta.
type LabelmetaStatus uint8

func (e LabelmetaStatus) MarshalJSON() ([]byte, error) {
	switch e {
	case UnknownLmeta:
		return []byte(`"Unknown"`), nil
        case NotExamined:
                return []byte(`"NotExamined"`), nil
        case PartiallyTraced:
                return []byte(`"PartiallyTraced"`), nil
        case HardToTrace:
                return []byte(`"HardToTrace"`), nil
        case Orphan:
                return []byte(`"Orphan"`), nil
        case Traced:
                return []byte(`"Traced"`), nil
        case Finalized:
	        return []byte(`"Finalized"`), nil
	default:
		return nil, fmt.Errorf("Unknown labelmeta type: %e", e)
	}
}

func (e *LabelmetaStatus) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"Unknown"`:
		*e = UnknownLmeta
        case `"NotExamined"`:
                *e = NotExamined
        case `"PartiallyTraced"`:
                *e = PartiallyTraced
        case `"HardToTrace"`:
                *e = HardToTrace
        case `"Orphan"`:
                *e = Orphan
	case `"Traced"`:
		*e = Traced
	case `"Finalized"`:
		*e = Finalized
	default:
		return fmt.Errorf("Unknown labelmeta type in JSON: %s", string(b))
	}
	return nil
}

// Tag is a string description of a labelmeta grouping, e.g., "Review, PAM".
type Tag string

// Labelmeta describes a labelmeta's properties.
type Labelmeta struct {
	Pos   dvid.Point3d
	Status  LabelmetaStatus
	Label uint64            // Indexed
	Tags  Tags              // Indexed
	Prop  map[string]string // Non-Indexed
}

func (e Labelmeta) Copy() *Labelmeta {
	c := new(Labelmeta)
	c.Pos = e.Pos
	c.Status = e.Status
	c.Label = e.Label
	c.Tags = make(Tags, len(e.Tags))
	copy(c.Tags, e.Tags)
	c.Prop = make(map[string]string, len(e.Prop))
	for k, v := range e.Prop {
		c.Prop[k] = v
	}
	return c
}

type Tags []Tag

func (t Tags) Len() int {
	return len(t)
}

func (t Tags) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t Tags) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type Labelmetas []Labelmeta

// helper function that just returns slice of positions suitable for intersect calcs in dvid package.
func (lmetas Labelmetas) positions() []dvid.Point3d {
	pts := make([]dvid.Point3d, len(lmetas))
	for i, lmeta := range lmetas {
		pts[i] = lmeta.Pos
	}
	return pts
}

// Returns Labelmetas that can be used for DeepEqual because all positions, relationships, and tags are sorted.
func (lmetas Labelmetas) Normalize() Labelmetas {
	// For every labelmeta, create a duplicate that has sorted relationships and sorted tags.
	out := make(Labelmetas, len(lmetas), len(lmetas))
	for i, lmeta := range lmetas {
		out[i].Pos = lmeta.Pos
		out[i].Status = lmeta.Status
		out[i].Label = lmeta.Label		
		out[i].Tags = make(Tags, len(lmeta.Tags))
		copy(out[i].Tags, lmeta.Tags)
		out[i].Prop = make(map[string]string, len(lmeta.Prop))
		for k, v := range lmeta.Prop {
			out[i].Prop[k] = v
		}
		sort.Sort(out[i].Tags)
	}

	// Sort all labelmetas based on their position.
	sort.Sort(out)
	return out
}


// Adds labelmetas but if labelmeta for a label already exists, it replaces the properties of that labelmeta.
func (lmetas *Labelmetas) add(toAdd Labelmetas) {
      emap := make(map[uint64]int)
      for i, lmeta := range *lmetas {
           emap[lmeta.Label] = i
      }
      for _, lmeta := range toAdd {
	    i, found := emap[lmeta.Label]
              if !found {
	              // this will probably never happen
                      //*lmetas = append(*lmetas, lmeta)
              } else {
                      (*lmetas)[i] = lmeta
              }
      }
}


// Deletes labelmeta position as well as relationships. // modify to delete using label id
func (lmetas *Labelmetas) deleteL(label uint64) (deleted *Labelmeta, changed bool) {
     // Delete any labelmetas with a specific label
     var cut = -1
     for i, lmeta := range *lmetas {
             if label == lmeta.Label {	               
	               cut = i		       
		       break
	     }
     }
     if cut >= 0 {
     	     deleted = (*lmetas)[cut].Copy()
             changed = true
             (*lmetas)[cut] = (*lmetas)[len(*lmetas)-1] // Delete without preserving order
             *lmetas = (*lmetas)[:len(*lmetas)-1]
     }
     return
}

func (lmetas *Labelmetas) delete(pt dvid.Point3d) (deleted *Labelmeta, changed bool) {
	// Delete any labelmetas at point.
	var cut = -1
	for i, lmeta := range *lmetas {
		if pt.Equals(lmeta.Pos) {
			cut = i
			break
		}
	}
	if cut >= 0 {
		deleted = (*lmetas)[cut].Copy()
		changed = true
		(*lmetas)[cut] = (*lmetas)[len(*lmetas)-1] // Delete without preserving order.
		*lmetas = (*lmetas)[:len(*lmetas)-1]
	}

	return
}

// Moves labelmeta position as well as relationships.

func (lmetas *Labelmetas) move(from, to dvid.Point3d, deleteLabelmeta bool) (moved *Labelmeta, changed bool) {
	for i, lmeta := range *lmetas {
		if from.Equals(lmeta.Pos) {
			changed = true
			(*lmetas)[i].Pos = to
			moved = (*lmetas)[i].Copy()
			if deleteLabelmeta {
				(*lmetas)[i] = (*lmetas)[len(*lmetas)-1] // Delete without preserving order.
				*lmetas = (*lmetas)[:len(*lmetas)-1]
				break
			}
		}
	}

	return
}

// --- Sort interface

func (lmetas Labelmetas) Len() int {
	return len(lmetas)
}

// Less returns true if labelmeta[i] < labelmeta[j] where ordering is determined by
// Pos and Status in that order.
func (lmetas Labelmetas) Less(i, j int) bool {
	if lmetas[i].Pos.Less(lmetas[j].Pos) {
		return true
	}
	if lmetas[i].Pos.Equals(lmetas[j].Pos) {
		return lmetas[i].Status < lmetas[j].Status
	}
	return false
}

func (lmetas Labelmetas) Swap(i, j int) {
	lmetas[i], lmetas[j] = lmetas[j], lmetas[i]
}

// LabelID is a unique id for label

type blockLabelmetas map[dvid.IZYXString]Labelmetas
type labelLabelmetas map[uint64]Labelmetas
type tagLabelmetas map[Tag]Labelmetas

// NewData returns a pointer to labelmeta data.
func NewData(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (*Data, error) {
	// Initialize the Data for this data type
	basedata, err := datastore.NewDataService(dtype, uuid, id, name, c)
	if err != nil {
		return nil, err
	}
	data := &Data{
		Data:       basedata,
		Properties: Properties{},
	}
	return data, nil
}

// --- Labelvol Datatype -----

type Type struct {
	datastore.Type
}

// --- TypeService interface ---

func (dtype *Type) NewDataService(uuid dvid.UUID, id dvid.InstanceID, name dvid.InstanceName, c dvid.Config) (datastore.DataService, error) {
	return NewData(uuid, id, name, c)
}

func (dtype *Type) Help() string {
	return HelpMessage
}

type formatType uint8

const (
	FormatFlatBuffers formatType = iota
	FormatProtobuf3
	FormatJSON
)

func getLabelmetas(ctx *datastore.VersionedCtx, tk storage.TKey) (Labelmetas, error) {
	store, err := ctx.GetOrderedKeyValueDB()
	if err != nil {
		return nil, err
	}
	val, err := store.Get(ctx, tk)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var lmetas Labelmetas
	if err := json.Unmarshal(val, &lmetas); err != nil {
		return nil, err
	}
	return lmetas, nil
}

func putLabelmetas(ctx *datastore.VersionedCtx, tk storage.TKey, lmetas Labelmetas) error {
	val, err := json.Marshal(lmetas)
	if err != nil {
		return err
	}
	store, err := ctx.GetOrderedKeyValueDB()
	if err != nil {
		return err
	}
	if err := store.Put(ctx, tk, val); err != nil {
		return err
	}
	return nil
}

// Returns Labelmetas within the sparse volume represented by the blocks of RLEs. Maybe one day we could use this for pulling down all the labels with coordates?
//func getLabelmetasInRLE(ctx *datastore.VersionedCtx, brles dvid.BlockRLEs) (Labelmetas, error) {
//	rleLmetas := Labelmetas{}
//	for izyx, rles := range brles {
//		// Get labelmetas for this block
//		blockCoord, err := izyx.ToChunkPoint3d()
//		tk := NewBlockTKey(blockCoord)
//		lmetas, err := getLabelmetas(ctx, tk)
//		if err != nil {
//			return nil, err
//		}
//
//		// Append the labelmetas in current block RLE
//		in := rles.Within(lmetas.positions())
//		for _, idx := range in {
//			rleLmetas = append(rleLmetas, lmetas[idx])
//		}
//	}
//	return rleLmetas, nil
//}

// Properties are additional properties for data beyond those in standard datastore.Data.
type Properties struct {
	// Currently unused since block sizes are either default or taken from synced labelblk.
}

// Data instance of labelvol, label sparse volumes.
type Data struct {
	*datastore.Data
	Properties

	// Keep track of sync operations that could be updating the data.
	datastore.Updater

	// Cached in-memory so we only have to lookup block size once.
	cachedBlockSize *dvid.Point3d

	sync.RWMutex // For CAS ops.  TODO: Make more specific (e.g., point locks) for efficiency.
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return reflect.DeepEqual(d.Properties, d2.Properties)
}

// blockSize is either defined by any synced labelblk or by the default block size.
// Also checks to make sure that synced data is consistent.
func (d *Data) blockSize(v dvid.VersionID) dvid.Point3d {
	if d.cachedBlockSize != nil {
		return *d.cachedBlockSize
	}
	var bsize dvid.Point3d
	d.cachedBlockSize = &bsize
	if lb := d.GetSyncedLabelblk(v); lb != nil {
		bsize = lb.BlockSize().(dvid.Point3d)
		return bsize
	}
	if lv := d.GetSyncedLabelvol(v); lv != nil {
		if len(bsize) != 0 && !bsize.Equals(lv.BlockSize) {
			dvid.Errorf("labelmeta %q is synced to labelblk and labelvol with different block sizes!\n", d.DataName())
		} else {
			bsize = lv.BlockSize
			return bsize
		}
	}
	if len(bsize) != 0 {
		bsize = dvid.Point3d{DefaultBlockSize, DefaultBlockSize, DefaultBlockSize}
	}
	return bsize
}

func (d *Data) GetSyncedLabelblk(v dvid.VersionID) *labelblk.Data {
	// Go through all synced names, and checking if there's a valid source.
	for _, name := range d.SyncedNames() {
		source, err := labelblk.GetByVersion(v, name)
		if err == nil {
			return source
		}
	}
	return nil
}

func (d *Data) GetSyncedLabelvol(v dvid.VersionID) *labelvol.Data {
	// Go through all synced names, and checking if there's a valid source.
	for _, name := range d.SyncedNames() {
		source, err := labelvol.GetByVersion(v, name)
		if err == nil {
			return source
		}
	}
	return nil
}

// delete all reference to given labelmeta point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) deleteLabelmetaInTags(ctx *datastore.VersionedCtx, label uint64, tags []Tag) error {
     for _, tag := range tags {
             tk := NewTagTKey(tag)
	     lmetas, err := getLabelmetas(ctx, tk)
	     if err != nil {
                     return err
             }
	     // Delete the label
	     if _, changed := lmetas.deleteL(label); !changed {
                     dvid.Errorf("Unable to find deleted labelmeta %d in tag %q", label, tag)
                     continue
             }
             // Save the tag.
	     if err := putLabelmetas(ctx, tk, lmetas); err != nil {
                     return err
             }
     }
     return nil
}

// Replace the labelmetas at the given TKey with the passed labelmetas.
// TODO: this needs to be transactional for this version + TKey but for now
// we just make sure it has mutex for all requests, so less efficient if
// there's many parallel changes.
func (d *Data) ModifyLabelmetas(ctx *datastore.VersionedCtx, tk storage.TKey, toAdd Labelmetas) error {
	d.Lock()
	defer d.Unlock()

	return d.modifyLabelmetas(ctx, tk, toAdd)
}

func (d *Data) modifyLabelmetas(ctx *datastore.VersionedCtx, tk storage.TKey, toAdd Labelmetas) error {
	storeM, err := getLabelmetas(ctx, tk)
	if err != nil {
		return err
	}
	if storeM != nil {
		storeM.add(toAdd)
	} else {
		storeM = toAdd
	}
	return putLabelmetas(ctx, tk, storeM)
}

// stores labelmetas arranged by block, replacing any
// labelmetas at same position.
func (d *Data) storeBlockLabelmetas(ctx *datastore.VersionedCtx, bm blockLabelmetas) error {
	for izyxStr, lmetas := range bm {
		blockCoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		// Modify the block labelmeta
		tk := NewBlockTKey(blockCoord)
		if err := d.modifyLabelmetas(ctx, tk, lmetas); err != nil {
			return err
		}
	}
	return nil
}

// stores labelmetas arranged by label, replacing any
// labelmetas at same position.
func (d *Data) storeLabelLabelmetas(ctx *datastore.VersionedCtx, lm labelLabelmetas) error {
        for label, lmetas := range lm {
	       tk := NewLabelTKey(label)
	       if err := d.modifyLabelmetas(ctx, tk, lmetas); err != nil {
                        return err
               }	       
	}
        return nil
}

// Old version keeping for reference
// storing labels using 3D point to determine label and storing the index. Might need to grab the syncing later on
// func (d *Data) storeLabelLabelmetas(ctx *datastore.VersionedCtx, be blockLabelmetas) error {
//	labelData := d.GetSyncedLabelblk(ctx.VersionID())
//	if labelData == nil {
//		return nil // no synced labels
//	}
//	store, err := d.BackendStore()
//	if err != nil {
//		return err
//	}
//	batcher, ok := store.(storage.KeyValueBatcher)
//	if !ok {
//		return fmt.Errorf("Data type labelmeta requires batch-enabled store, which %q is not\n", store)
//	}
//
//	// Compute the strides (in bytes)
//	blockSize := d.blockSize(ctx.VersionID())
//	bX := blockSize[0] * 8
//	bY := blockSize[1] * bX
//	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * 8)
//
//	toAdd := LabelLabelmetas{}
//	for izyxStr, lmetas := range be {
//		blockCoord, err := izyxStr.ToChunkPoint3d()
//		if err != nil {
//			return err
//		}
//
//		// Get the labels for this block
//		labels, err := labelData.GetLabelBlock(ctx.VersionID(), blockCoord)
//		if err != nil {
//			return err
//		}
//		if len(labels) == 0 {
//			continue
//		}
//		if len(labels) != blockBytes {
//			return fmt.Errorf("Expected %d bytes in %q label block, got %d instead.  Aborting.", blockBytes, d.DataName(), len(labels))
//		}
//
//		// Group labelmeta by label
//		for _, lmeta := range lmetas {
//			pt := lmeta.Pos.Point3dInChunk(blockSize)
//			i := pt[2]*bY + pt[1]*bX + pt[0]*8
//			label := binary.LittleEndian.Uint64(labels[i : i+8])
//			if label != 0 {
//				toAdd.add(label, lmeta)
//			}
//		}
//	}
//
//	// Store all the added labelmeta to the appropriate labels.
//	batch := batcher.NewBatch(ctx)
//	for label, additions := range toAdd {
//		tk := NewLabelTKey(label)
//		lmetas, err := getLabelmetas(ctx, tk)
//		if err != nil {
//			return fmt.Errorf("err getting labelmetas for label %d: %v\n", label, err)
//		}
//		lmetas.add(additions)
//		val, err := json.Marshal(lmetas)
//		if err != nil {
//			return fmt.Errorf("couldn't serialize label %d labelmeta in instance %q: %v\n", label, d.DataName(), err)
//		}
//		batch.Put(tk, val)
//	}
//	if err := batch.Commit(); err != nil {
//		dvid.Criticalf("bad commit in storing labelmeta %q: %v\n", d.DataName(), err)
//	}
//
//	return nil
//}




// stores labelmetas arranged by tag, replacing any
// labelmetas at same position.
func (d *Data) storeTagLabelmetas(ctx *datastore.VersionedCtx, tm tagLabelmetas) error {
	for tag, lmetas := range tm {
		tk := NewTagTKey(tag)
		if err := d.modifyLabelmetas(ctx, tk, lmetas); err != nil {
			return err
		}
	}
	return nil
}


// GetLabelMeta returns labelmetas for a given label. // old function for getting label that is sync
func (d *Data) GetLabelMeta(ctx *datastore.VersionedCtx, label uint64) (Labelmetas, error) {
	d.RLock()
	defer d.RUnlock()

	tk := NewLabelTKey(label)
	return getLabelmetas(ctx, tk)
}

// GetTagLabelMeta returns labelmetas for a given tag.
func (d *Data) GetTagLabelMeta(ctx *datastore.VersionedCtx, tag Tag) (Labelmetas, error) {
	d.RLock()
	defer d.RUnlock()

	tk := NewTagTKey(tag)
	return getLabelmetas(ctx, tk)
}

func (d *Data) GetMultiLabelMeta(ctx *datastore.VersionedCtx, r io.Reader) (Labelmetas, error) {        
        d.RLock()
        defer d.RUnlock()
	data, err := ioutil.ReadAll(r)
        if err != nil {                
                return nil, err
        }
        var labelids []uint64
        if err := json.Unmarshal(data, &labelids); err != nil {                
                return nil, err
        }
        var lmetas Labelmetas
        for _, label := range labelids {
                labelmetas, err := d.GetLabelMeta(ctx, label)
                if err != nil {
                        return nil, err
                }
                for _, lmeta := range labelmetas {
                        lmetas = append(lmetas, lmeta)
	        }                       
        }

	return lmetas, nil
}
func (d *Data) GetAllLabelMeta(ctx *datastore.VersionedCtx) (Labelmetas, error) {
     store, err := d.GetOrderedKeyValueDB()
     if err != nil {
             return nil, err
     }

     d.RLock()
     defer d.RUnlock()

     // Compute first and last key for range
     first := storage.MinTKey(keyLabel)
     last := storage.MaxTKey(keyLabel)

     keys, err := store.KeysInRange(ctx, first, last)
     if err != nil {
          return nil, err
     }
     var lmetas Labelmetas
     for _, key := range keys {
          keyVal, err := DecodeLabelTKey(key)
	  labelmetas, err := d.GetLabelMeta(ctx, keyVal)
          if err != nil {
		  return nil, err
          }
	  for _, lmeta := range labelmetas {
                  lmetas = append(lmetas, lmeta)
          }	  
     }
     return lmetas, nil
}

func (d *Data) GetAllLabelIDs(ctx *datastore.VersionedCtx) ([]uint64, error) {
     store, err := d.GetOrderedKeyValueDB()
     if err != nil {
             return nil, err
     }
     
     d.RLock()
     defer d.RUnlock()
     
     // Compute first and last key for range
     first := storage.MinTKey(keyLabel)
     last := storage.MaxTKey(keyLabel)

     keys, err := store.KeysInRange(ctx, first, last)
     if err != nil {
	  return nil, err
     }
     keyList := []uint64{}
     for _, key := range keys {
     	 keyVal, err := DecodeLabelTKey(key)
         if err != nil {
		    return nil, err
         }
         keyList = append(keyList, keyVal)
     }
     return keyList, nil
}

// GetRegionLabelMeta returns labelmetas for a given subvolume of image space.
func (d *Data) GetRegionLabelMeta(ctx *datastore.VersionedCtx, ext *dvid.Extents3d) (Labelmetas, error) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return nil, err
	}

	// Setup block bounds for labelmeta query in supplied Z range.
	blockSize := d.blockSize(ctx.VersionID())
	begBlockCoord, endBlockCoord := ext.BlockRange(blockSize)

	begTKey := NewBlockTKey(begBlockCoord)
	endTKey := NewBlockTKey(endBlockCoord)

	d.RLock()
	defer d.RUnlock()

	// Iterate through all labelmetas block k/v, making sure the labelmetas are also within the given subvolume.
	var labelmetas Labelmetas
	err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(chunk *storage.Chunk) error {
		blockCoord, err := DecodeBlockTKey(chunk.K)
		if err != nil {
			return err
		}
		if !ext.BlockWithin(blockSize, blockCoord) {
			return nil
		}
		// Deserialize the JSON value into a slice of labelmetas
		var blockLmetas Labelmetas
		if err := json.Unmarshal(chunk.V, &blockLmetas); err != nil {
			return err
		}
		// Iterate through labelmetas, screening on extents before adding to region labelmetas.
		for _, lmeta := range blockLmetas {
			if ext.VoxelWithin(lmeta.Pos) {
				labelmetas = append(labelmetas, lmeta)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return labelmetas, nil
}


// store label meta data, first read in json, generate maps to store indexes. 
func (d *Data) StoreLabelMeta(ctx *datastore.VersionedCtx, r io.Reader) error {
	jsonBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	var labelmetas Labelmetas
	if err := json.Unmarshal(jsonBytes, &labelmetas); err != nil {
		return err
	}

	d.Lock()
	defer d.Unlock()

	dvid.Infof("%d labelmetas received via POST", len(labelmetas))

	blockSize := d.blockSize(ctx.VersionID())
	blockM := make(blockLabelmetas)
	labelM := make(labelLabelmetas)
	tagM := make(tagLabelmetas)

	// Iterate through labelmetas, organizing them into blocks and tags.
	// Note: we do not check for redundancy and guarantee uniqueness at this stage.
	for _, lmeta := range labelmetas {

	        // Get Label for this labelmeta.
		label := lmeta.Label
                // Overwriting labels
		lm := labelM[label]
	        lm = append(lm, lmeta)
                labelM[label] = lm
				
		// Get block coord for this labelmeta. This will need to optional
		izyxStr := lmeta.Pos.ToIZYXString(blockSize)
		// Append to block
		if len(izyxStr) > 0 {
		   	bm := blockM[izyxStr]
			bm = append(bm, lmeta)
			blockM[izyxStr] = bm
		}

		// Append to tags if present
		if len(lmeta.Tags) > 0 {
			for _, tag := range lmeta.Tags {
				tm := tagM[tag]
				tm = append(tm, lmeta)
				tagM[tag] = tm
			}
		}
	}

	// Store the new block labelmetas. Index with POS if exists in metadata
	if err := d.storeBlockLabelmetas(ctx, blockM); err != nil {
		return err
	}

	// Store new labelmetas among label denormalizations... 
	if err := d.storeLabelLabelmetas(ctx, labelM); err != nil {
              return err
        }

	// Store the new tag labelmetas
	if err := d.storeTagLabelmetas(ctx, tagM); err != nil {
		return err
	}

	return nil
}
func (d *Data) DeleteLabelMeta(ctx *datastore.VersionedCtx, label uint64) error {
        tk := NewLabelTKey(label)
	d.Lock()
        defer d.Unlock()
	lmetas, err := getLabelmetas(ctx, tk)
	if err != nil {
                return err
	}

        // Delete the given labelmeta
        deleted, _ := lmetas.deleteL(label)
	if deleted == nil {
	        return fmt.Errorf("Did not find label %d in datastore", label)
        }

        // Put block key version without given labelmeta
        if err := putLabelmetas(ctx, tk, lmetas); err != nil {
	        return err
        }

        // Delete labelmeta in any tags
	if err := d.deleteLabelmetaInTags(ctx, label, deleted.Tags); err != nil {
                return err
        }

	return nil
}

// GetByUUID returns a pointer to labelmeta data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not an labelmeta datatype!", name)
	}
	return data, nil
}
// Here Yo
// --- datastore.DataService interface ---------

func (d *Data) Help() string {
	return HelpMessage
}

func (d *Data) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Base     *datastore.Data
		Extended Properties
	}{
		d.Data,
		d.Properties,
	})
}

func (d *Data) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&(d.Data)); err != nil {
		return err
	}
	if err := dec.Decode(&(d.Properties)); err != nil {
		return err
	}
	return nil
}

func (d *Data) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(d.Data); err != nil {
		return nil, err
	}
	if err := enc.Encode(d.Properties); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
	return nil
}

type Bounds struct {
	VoxelBounds *dvid.Bounds
	BlockBounds *dvid.Bounds
	Exact       bool // All RLEs must respect the voxel bounds.  If false, just screen on blocks.
}

// ServeHTTP handles all incoming HTTP requests for this data.
func (d *Data) ServeHTTP(uuid dvid.UUID, ctx *datastore.VersionedCtx, w http.ResponseWriter, r *http.Request) {
	timedLog := dvid.NewTimeLog()
	// versionID := ctx.VersionID()

	// Get the action (GET, POST)
	action := strings.ToLower(r.Method)

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}

	// Handle POST on data -> setting of configuration
	if len(parts) == 3 && action == "put" {
		config, err := server.DecodeJSON(r)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.ModifyConfig(config); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := datastore.SaveDataByUUID(uuid, d); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		fmt.Fprintf(w, "Changed '%s' based on received configuration:\n%s\n", d.DataName(), config)
		return
	}

	if len(parts) < 4 {
		server.BadRequest(w, r, "Incomplete API request")
		return
	}

	// Process help and info.
	switch parts[3] {
	case "help":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, dtype.Help())

	case "info":
		jsonBytes, err := d.MarshalJSON()
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(jsonBytes))

	case "sync":
		if action != "post" {
			server.BadRequest(w, r, "Only POST allowed to sync endpoint")
			return
		}
		if err := d.SetSync(uuid, r.Body); err != nil {
			server.BadRequest(w, r, err)
			return
		}

	case "label":
		switch action {
		case "get":
		     // GET <api URL>/node/<UUID>/<data name>/labelmeta/<label id uint64>
		     if len(parts) < 5 {
		          server.BadRequest(w, r, "Must include label after 'label' endpoint.")
			  return
		     }
		     label, err := strconv.ParseUint(parts[4], 10, 64)
		     if err != nil {
			  server.BadRequest(w, r, err)
			  return
		     }
		     if label == 0 {
		          server.BadRequest(w, r, "Label 0 is protected background value and cannot be used for query.")
			  return
		     }
		     labelmetas, err := d.GetLabelMeta(ctx, label)
		     if err != nil {
		    	  server.BadRequest(w, r, err)
			  return
		     }
		     w.Header().Set("Content-type", "application/json")
		     jsonBytes, err := json.Marshal(labelmetas)
		     if err != nil {
		 	  server.BadRequest(w, r, err)
			  return
		     }
		     if _, err := w.Write(jsonBytes); err != nil {
			  server.BadRequest(w, r, err)
			  return
		     }		
		     timedLog.Infof("HTTP %s: get labelmeta for label %d (%s)", r.Method, label, r.URL)

		case "delete":
		     // DELETE <api URL>/node/<UUID>/<data name>/labelmeta/<label id uint64>
                     if len(parts) < 5 {
                          server.BadRequest(w, r, "Must include label after 'label' endpoint.")
                          return
                     }
                     label, err := strconv.ParseUint(parts[4], 10, 64)
                     if err != nil {
                          server.BadRequest(w, r, err)
                          return
                     }
                     if label == 0 {
                          server.BadRequest(w, r, "Label 0 is protected background value and cannot be used for query.")
                          return
                     }
		     fmt.Fprintf(w, "%d", label)
		     if err := d.DeleteLabelMeta(ctx, label); err != nil {
                          server.BadRequest(w, r, err)
                          return
                     }
		     timedLog.Infof("HTTP %s: delete labelmeta for label %d (%s)", r.Method, label, r.URL)		     
		default:
		     server.BadRequest(w, r, "Only GET or DELETE action is available on 'label' endpoint.")
                     return
		}

	//case labelsall that returns all the labelmeta data
	case "labelsall":
	        if action != "get" {
                        server.BadRequest(w, r, "Only GET action is available on 'tag' endpoint.")
                        return
                }
		labelmetas, err := d.GetAllLabelMeta(ctx)
		if err != nil {
                        server.BadRequest(w, r, err)
                        return
                }
		w.Header().Set("Content-type", "application/json")
                jsonBytes, err := json.Marshal(labelmetas)
                if err != nil {
                        server.BadRequest(w, r, err)
                        return
                }
                if _, err := w.Write(jsonBytes); err != nil {
                        server.BadRequest(w, r, err)
                        return
                }
                timedLog.Infof("HTTP %s: get all labelmeta data (%s)", r.Method, r.URL)

        //case labelids that returns all label ids loaded	
        case "labelids":
                if action != "get" {
                        server.BadRequest(w, r, "Only GET action is available on 'tag' endpoint.")
                        return
                }
		labelids, err := d.GetAllLabelIDs(ctx)
                if err != nil {
                        server.BadRequest(w, r, err)
                        return
                }


//              for _, labelid := range labelids {
//                       fmt.Fprintf(w, "%d", labelid)
//                       fmt.Fprintf(w, ",")
//		}

                w.Header().Set("Content-type", "application/json")
                jsonBytes, err := json.Marshal(labelids)
	        if err != nil {
                        server.BadRequest(w, r, err)
                        return
                }
                if _, err := w.Write(jsonBytes); err != nil {
                        server.BadRequest(w, r, err)
                        return
                }
                timedLog.Infof("HTTP %s: get all labelmeta label ids (%s)", r.Method, r.URL)

	case "tag":
		if action != "get" {
			server.BadRequest(w, r, "Only GET action is available on 'tag' endpoint.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "Must include tag string after 'tag' endpoint.")
			return
		}
		tag := Tag(parts[4])
		labelmetas, err := d.GetTagLabelMeta(ctx, tag)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		jsonBytes, err := json.Marshal(labelmetas)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if _, err := w.Write(jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: get labelmetas for tag %s (%s)", r.Method, tag, r.URL)

	case "labelmeta":	        
		switch action {
		case "get":
                        // GET <api URL>/node/<UUID>/<data name>/labelmeta <json array of label ids>
			labelmetas, err := d.GetMultiLabelMeta(ctx, r.Body)
                	if err != nil {
		       	        server.BadRequest(w, r, err)
                                return
                	}

                	w.Header().Set("Content-type", "application/json")
                	jsonBytes, err := json.Marshal(labelmetas)
			if err != nil {
                                server.BadRequest(w, r, err)
                        	return
                	}
                	if _, err := w.Write(jsonBytes); err != nil {
                                server.BadRequest(w, r, err)
                        	return
                	}
                	timedLog.Infof("HTTP %s: get labelmeta for multiple labels (%s)", r.Method, r.URL)
		case "post":
			if err := d.StoreLabelMeta(ctx, r.Body); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		default:
			server.BadRequest(w, r, "Only GET or POST action is available on 'labelmetas' endpoint.")
			return
		}

	default:
		server.BadRequest(w, r, "Unrecognized API call %q for labelvol data %q.  See API help.",
			parts[3], d.DataName())
	}
}
