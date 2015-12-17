/*
	Package annotation supports point annotation management and queries.
*/
package annotation

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/message"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/annotation"
	TypeName = "annotation"
)

const HelpMessage = `
API for synapse data type (github.com/janelia-flyem/dvid/datatype/annotation)
=======================================================================================

Command-line:

$ dvid repo <UUID> new synapse <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new annotation synapses

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "synapses"
    settings       Configuration settings in "key=value" format separated by spaces.

    Configuration Settings (case-insensitive keys)
	
    Sync           Name of labelblk data to which this annotation data should be synced.  Changes
    			   in the synced labelblk data will change results of "label" endpoint requests (see REST API).
	
    ------------------

HTTP API (Level 2 REST):

GET  <api URL>/node/<UUID>/<data name>/help

	Returns data-specific help message.


GET  <api URL>/node/<UUID>/<data name>/info
POST <api URL>/node/<UUID>/<data name>/info

    Retrieves or puts DVID-specific data properties for these voxels.

    Example: 

    GET <api URL>/node/3f8c/synapses/info

    Returns JSON with configuration settings.

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    data name     Name of annotation data.


Note: For the following URL endpoints that return and accept POSTed JSON values, see the JSON format
at end of this documentation.

GET <api URL>/node/<UUID>/<data name>/label/<label>

	Returns all point annotations within the given label as an array of elements.

GET <api URL>/node/<UUID>/<data name>/tag/<tag>

	Returns all point annotations with the given tag as an array of elements.

DELETE <api URL>/node/<UUID>/<data name>/element/<coord>

	Deletes a point annotation given its location.

GET <api URL>/node/<UUID>/<data name>/elements/<size>/<offset>

	Returns all point annotations within subvolume of given size with upper left corner
	at given offset.  The size and offset should be voxels separated by underscore, e.g.,
	"400_300_200" can describe a 400 x 300 x 200 volume or an offset of (400,300,200).

	The returned point annotations will be an array of elements.

POST <api URL>/node/<UUID>/<data name>/elements

	Adds or modifies point annotations.  The POSTed content is an array of elements.

POST <api URL>/node/<UUID>/<data name>/move/<from_coord>/<to_coord>

	Moves the point annotation from <from_coord> to <to_coord> where
	<from_coord> and <to_coord> are of the form X_Y_Z.

------

Example JSON Format of point annotation elements with ... marking omitted elements:

[
	{
		"Pos":[33,30,31],
		"Kind":"PostSyn",
		"Rels":[ 
			{"Rel":"PostSynTo", "To":[15,27,35]} 
		],
		"Tags":["Synapse1"],
		"Prop": {
			"SomeVar": "SomeValue",
			"Another Var": "A More Complex Value"
		}
	},
	{
		"Pos":[15,27,35],
		"Kind":"PreSyn",
		"Rels":[
			{"Rel":"PreSynTo", "To":[20,30,40]},
			{"Rel":"PreSynTo", "To":[14,25,37]},
			{"Rel":"PreSynTo", "To":[33,30,31]}
		],
		"Tags":["Synapse1"]
	},
	{
		"Pos":[20,30,40],
		"Kind":"PostSyn",
		"Rels":[
			{"Rel":"PostSynTo","To":[15,27,35]}
		],
		"Tags":["Synapse1"],
		"Prop": {
			"SomeVar": "SomeValue",
			"Another Var 2": "A More Complex Value 2"
		}
	},
	...
]

The "Kind" property can be one of "Unknown", "PostSyn", "PreSyn", "Gap", or "Note".

The "Rel" property can be one of "UnknownRelationship", "PostSynTo", "PreSynTo", "ConvergentTo", or "GroupedWith".

The "Tags" property will be indexed and so can be costly if used for very large numbers of synapse elements.

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
	UnknownElem ElementType = iota
	PostSyn                 // Post-synaptic element
	PreSyn                  // Pre-synaptic element
	Gap                     // Gap junction
	Note                    // A note or bookmark with some description
)

// ElementType gives the type of a synaptic element.
type ElementType uint8

func (e ElementType) MarshalJSON() ([]byte, error) {
	switch e {
	case UnknownElem:
		return []byte(`"Unknown"`), nil
	case PostSyn:
		return []byte(`"PostSyn"`), nil
	case PreSyn:
		return []byte(`"PreSyn"`), nil
	case Gap:
		return []byte(`"Gap"`), nil
	case Note:
		return []byte(`"Note"`), nil
	default:
		return nil, fmt.Errorf("Unknown element type: %e", e)
	}
}

func (e *ElementType) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"Unknown"`:
		*e = UnknownElem
	case `"PostSyn"`:
		*e = PostSyn
	case `"PreSyn"`:
		*e = PreSyn
	case `"Gap"`:
		*e = Gap
	case `"Note"`:
		*e = Note
	default:
		return fmt.Errorf("Unknown element type in JSON: %s", string(b))
	}
	return nil
}

type RelationType uint8

const (
	UnknownRel RelationType = iota
	PostSynTo
	PreSynTo
	ConvergentTo
	GroupedWith
)

func (r RelationType) MarshalJSON() ([]byte, error) {
	switch r {
	case UnknownRel:
		return []byte(`"UnknownRelationship"`), nil
	case PostSynTo:
		return []byte(`"PostSynTo"`), nil
	case PreSynTo:
		return []byte(`"PreSynTo"`), nil
	case ConvergentTo:
		return []byte(`"ConvergentTo"`), nil
	case GroupedWith:
		return []byte(`"GroupedWith"`), nil
	default:
		return nil, fmt.Errorf("Unknown relation type: %d", r)
	}
}

func (r *RelationType) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"UnknownRelationship"`:
		*r = UnknownRel
	case `"PostSynTo"`:
		*r = PostSynTo
	case `"PreSynTo"`:
		*r = PreSynTo
	case `"ConvergentTo"`:
		*r = ConvergentTo
	case `"GroupedWith"`:
		*r = GroupedWith
	default:
		return fmt.Errorf("Unknown relationship type in JSON: %s", string(b))
	}
	return nil
}

// Tag is a string description of a synaptic element grouping, e.g., "convergent".
type Tag string

// Relationship is a link between two synaptic elements.
type Relationship struct {
	Rel RelationType
	To  dvid.Point3d
}

// Element describes a synaptic element's properties.
type Element struct {
	Pos  dvid.Point3d
	Kind ElementType
	Rels Relationships
	Tags Tags              // Indexed
	Prop map[string]string // Non-Indexed
}

func (e Element) Copy() *Element {
	c := new(Element)
	c.Pos = e.Pos
	c.Kind = e.Kind
	c.Rels = make(Relationships, len(e.Rels))
	copy(c.Rels, e.Rels)
	c.Tags = make(Tags, len(e.Tags))
	copy(c.Tags, e.Tags)
	c.Prop = make(map[string]string, len(e.Prop))
	for k, v := range e.Prop {
		c.Prop[k] = v
	}
	return c
}

type Relationships []Relationship

func (r Relationships) Len() int {
	return len(r)
}

func (r Relationships) Less(i, j int) bool {
	if r[i].To.Less(r[j].To) {
		return true
	}
	if r[i].To.Equals(r[j].To) {
		return r[i].Rel < r[j].Rel
	}
	return false
}

func (r Relationships) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// given a list of element indices to be deleted, it returns slice of remaining Relationships
func (r Relationships) delete(todel []int) Relationships {
	out := make(Relationships, len(r)-len(todel))
	j, k := 0, 0
	for i, rel := range r {
		if k >= len(todel) || i != todel[k] {
			out[j] = rel
			j++
		} else {
			k++
		}
	}
	return out
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

type Elements []Element

// Returns Elements that can be used for DeepEqual because all positions, relationships, and tags are sorted.
func (elems Elements) Normalize() Elements {
	// For every element, create a duplicate that has sorted relationships and sorted tags.
	out := make(Elements, len(elems), len(elems))
	for i, elem := range elems {
		out[i].Pos = elem.Pos
		out[i].Kind = elem.Kind
		out[i].Rels = make(Relationships, len(elem.Rels))
		copy(out[i].Rels, elem.Rels)
		out[i].Tags = make(Tags, len(elem.Tags))
		copy(out[i].Tags, elem.Tags)

		out[i].Prop = make(map[string]string, len(elem.Prop))
		for k, v := range elem.Prop {
			out[i].Prop[k] = v
		}

		sort.Sort(out[i].Rels)
		sort.Sort(out[i].Tags)
	}

	// Sort all elements based on their position.
	sort.Sort(out)
	return out
}

// Adds element but if element at position already exists, it replaces the properties of that element.
func (elems *Elements) add(toAdd Elements) {
	emap := make(map[string]int)
	for i, elem := range *elems {
		emap[elem.Pos.MapKey()] = i
	}
	for _, elem := range toAdd {
		i, found := emap[elem.Pos.MapKey()]
		if !found {
			*elems = append(*elems, elem)
		} else {
			(*elems)[i] = elem
		}
	}
}

// Deletes element position as well as relationships.
func (elems *Elements) delete(pt dvid.Point3d) (deleted *Element, changed bool) {
	// Delete any elements at point.
	var cut = -1
	for i, elem := range *elems {
		if pt.Equals(elem.Pos) {
			cut = i
			break
		}
	}
	if cut >= 0 {
		deleted = (*elems)[cut].Copy()
		changed = true
		(*elems)[cut] = (*elems)[len(*elems)-1] // Delete without preserving order.
		*elems = (*elems)[:len(*elems)-1]
	}

	// Delete any relationships with the point.
	if elems.deleteRel(pt) {
		changed = true
	}
	return
}

func (elems *Elements) deleteRel(pt dvid.Point3d) (changed bool) {
	for i, elem := range *elems {
		// Remove any relationship with given pt.
		var todel []int
		for j, r := range elem.Rels {
			if pt.Equals(r.To) {
				todel = append(todel, j)
			}
		}
		if len(todel) > 0 {
			(*elems)[i].Rels = elem.Rels.delete(todel)
			changed = true
		}
	}
	return
}

// Moves element position as well as relationships.
func (elems *Elements) move(from, to dvid.Point3d, deleteElement bool) (moved *Element, changed bool) {
	for i, elem := range *elems {
		if from.Equals(elem.Pos) {
			changed = true
			(*elems)[i].Pos = to
			moved = (*elems)[i].Copy()
			if deleteElement {
				(*elems)[i] = (*elems)[len(*elems)-1] // Delete without preserving order.
				*elems = (*elems)[:len(*elems)-1]
				break
			}
		}
	}

	// Check relationships for any moved points.
	for i, elem := range *elems {
		// Move any relationship with given pt.
		for j, r := range elem.Rels {
			if from.Equals(r.To) {
				r.To = to
				(*elems)[i].Rels[j] = r
				changed = true
			}
		}
	}
	return
}

// --- Sort interface

func (elems Elements) Len() int {
	return len(elems)
}

// Less returns true if element[i] < element[j] where ordering is determined by
// Pos and Kind in that order.  Relationships and Tags are not considered in ordering.
func (elems Elements) Less(i, j int) bool {
	if elems[i].Pos.Less(elems[j].Pos) {
		return true
	}
	if elems[i].Pos.Equals(elems[j].Pos) {
		return elems[i].Kind < elems[j].Kind
	}
	return false
}

func (elems Elements) Swap(i, j int) {
	elems[i], elems[j] = elems[j], elems[i]
}

type blockElements map[dvid.IZYXString]Elements
type tagElements map[Tag]Elements

// NewData returns a pointer to annotation data.
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

func getElements(ctx *datastore.VersionedCtx, tk storage.TKey) (Elements, error) {
	store, err := storage.MutableStore()
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
	var elems Elements
	if err := json.Unmarshal(val, &elems); err != nil {
		return nil, err
	}
	return elems, nil
}

func putElements(ctx *datastore.VersionedCtx, tk storage.TKey, elems Elements) error {
	val, err := json.Marshal(elems)
	if err != nil {
		return err
	}
	store, err := storage.MutableStore()
	if err != nil {
		return err
	}
	if err := store.Put(ctx, tk, val); err != nil {
		return err
	}
	return nil
}

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

	sync.RWMutex // For CAS ops.  TODO: Make more specific for efficiency.
}

func (d *Data) Equals(d2 *Data) bool {
	if !d.Data.Equals(d2.Data) {
		return false
	}
	return true
}

// blockSize is either defined by any synced labelblk or by the default block size.
func (d *Data) blockSize(v dvid.VersionID) dvid.Point3d {
	labelData, err := d.GetSyncedLabelblk(v)
	if err != nil {
		return dvid.Point3d{labelblk.DefaultBlockSize, labelblk.DefaultBlockSize, labelblk.DefaultBlockSize}
	}
	return labelData.BlockSize().(dvid.Point3d)
}

func (d *Data) GetSyncedLabelblk(v dvid.VersionID) (*labelblk.Data, error) {
	// Go through all synced names, and checking if there's a valid source.
	for _, name := range d.SyncedNames() {
		source, err := labelblk.GetByVersion(v, name)
		if err == nil {
			return source, nil
		}
	}
	return nil, fmt.Errorf("no labelblk data is syncing with %d", d.DataName())
}

// delete all reference to given element point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) deleteElementInTags(ctx *datastore.VersionedCtx, pt dvid.Point3d, tags []Tag) error {
	for _, tag := range tags {
		// Get the elements in tag.
		tk := NewTagTKey(tag)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Delete the point
		if _, changed := elems.delete(pt); !changed {
			dvid.Errorf("Unable to find deleted element %s in tag %q", pt, tag)
			continue
		}

		// Save the tag.
		if err := putElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// delete all reference to given element point in the related points.
// This is private method and assumes outer locking.
func (d *Data) deleteElementInRelationships(ctx *datastore.VersionedCtx, pt dvid.Point3d, rels []Relationship) error {
	blockSize := d.blockSize(ctx.VersionID())
	for _, rel := range rels {
		// Get the block elements containing the related element.
		blockCoord := rel.To.Chunk(blockSize).(dvid.ChunkPoint3d)
		tk := NewBlockTKey(blockCoord)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Delete the point in relationships
		if !elems.deleteRel(pt) {
			continue
		}

		// Save the block elements.
		if err := putElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// move all reference to given element point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) moveElementInTags(ctx *datastore.VersionedCtx, from, to dvid.Point3d, tags []Tag) error {
	for _, tag := range tags {
		// Get the elements in tag.
		tk := NewTagTKey(tag)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Move element in tag.
		if moved, _ := elems.move(from, to, false); moved == nil {
			dvid.Errorf("Unable to find moved element %s in tag %q", from, tag)
			continue
		}

		// Save the tag.
		if err := putElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// move all reference to given element point in the related points.
// This is private method and assumes outer locking.
func (d *Data) moveElementInRelationships(ctx *datastore.VersionedCtx, from, to dvid.Point3d, rels []Relationship) error {
	blockSize := d.blockSize(ctx.VersionID())
	for _, rel := range rels {
		// Get the block elements containing the related element.
		blockCoord := rel.To.Chunk(blockSize).(dvid.ChunkPoint3d)
		tk := NewBlockTKey(blockCoord)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Move element in related element.
		if _, changed := elems.move(from, to, false); !changed {
			dvid.Errorf("Unable to find moved element %s in related element %s:\n%v\n", from, rel.To, elems)
			continue
		}

		// Save the block elements.
		if err := putElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// Replace the synaptic elements at the given TKey with the passed elements.
// TODO: this needs to be transactional for this version + TKey but for now
// we just make sure it has mutex for all requests, so less efficient if
// there's many parallel changes.
func (d *Data) ModifyElements(ctx *datastore.VersionedCtx, tk storage.TKey, toAdd Elements) error {
	d.Lock()
	defer d.Unlock()

	storeE, err := getElements(ctx, tk)
	if err != nil {
		return err
	}
	if storeE != nil {
		storeE.add(toAdd)
	} else {
		storeE = toAdd
	}
	return putElements(ctx, tk, storeE)
}

// StoreBlockElements stores synaptic elements arranged by block, replacing any
// elements at same position.
func (d *Data) StoreBlockElements(ctx *datastore.VersionedCtx, be blockElements) error {
	for izyxStr, elems := range be {
		blockCoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(blockCoord)
		if err := d.ModifyElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// StoreTagElements stores synaptic elements arranged by tag, replacing any
// elements at same position.
func (d *Data) StoreTagElements(ctx *datastore.VersionedCtx, te tagElements) error {
	for tag, elems := range te {
		tk := NewTagTKey(tag)
		if err := d.ModifyElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// GetLabelSynapses returns synapse elements for a given label.
func GetLabelSynapses(ctx *datastore.VersionedCtx, label uint64) (Elements, error) {
	tk := NewLabelTKey(label)
	return getElements(ctx, tk)
}

// GetTagSynapses returns synapse elements for a given tag.
func GetTagSynapses(ctx *datastore.VersionedCtx, tag Tag) (Elements, error) {
	tk := NewTagTKey(tag)
	return getElements(ctx, tk)
}

// GetRegionSynapses returns synapse elements for a given subvolume of image space.
func (d *Data) GetRegionSynapses(ctx *datastore.VersionedCtx, ext *dvid.Extents3d) (Elements, error) {
	store, err := storage.MutableStore()
	if err != nil {
		return nil, fmt.Errorf("Data type synapse had error initializing store: %v\n", err)
	}

	// Setup block bounds for synapse element query in supplied Z range.
	blockSize := d.blockSize(ctx.VersionID())
	begBlockCoord, endBlockCoord := ext.BlockRange(blockSize)

	begTKey := NewBlockTKey(begBlockCoord)
	endTKey := NewBlockTKey(endBlockCoord)

	// Iterate through all synapse elements block k/v, making sure the elements are also within the given subvolume.
	var elements Elements
	err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(chunk *storage.Chunk) error {
		blockCoord, err := DecodeBlockTKey(chunk.K)
		if err != nil {
			return err
		}
		if !ext.BlockWithin(blockSize, blockCoord) {
			return nil
		}
		// Deserialize the JSON value into a slice of elements
		var blockElems Elements
		if err := json.Unmarshal(chunk.V, &blockElems); err != nil {
			return err
		}
		// Iterate through elements, screening on extents before adding to region elements.
		for _, elem := range blockElems {
			if ext.VoxelWithin(elem.Pos) {
				elements = append(elements, elem)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return elements, nil
}

func (d *Data) StoreSynapses(ctx *datastore.VersionedCtx, r io.Reader) error {
	jsonBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	var elements Elements
	if err := json.Unmarshal(jsonBytes, &elements); err != nil {
		return err
	}

	dvid.Infof("%d synaptic elements received via POST", len(elements))

	blockSize := d.blockSize(ctx.VersionID())
	blockE := make(blockElements)
	tagE := make(tagElements)

	// Iterate through elements, organizing them into blocks and tags.
	// Note: we do not check for redundancy and guarantee uniqueness at this stage.
	for _, elem := range elements {
		// Get block coord for this element.
		blockCoord := elem.Pos.Chunk(blockSize).(dvid.ChunkPoint3d)
		izyxStr := blockCoord.ToIZYXString()

		// Append to block
		be := blockE[izyxStr]
		be = append(be, elem)
		blockE[izyxStr] = be

		// Append to tags if present
		if len(elem.Tags) > 0 {
			for _, tag := range elem.Tags {
				te := tagE[tag]
				te = append(te, elem)
				tagE[tag] = te
			}
		}

		// TODO: Write to background label key or handle
	}

	// Store the new block elements
	fmt.Printf("Storing block elements\n")
	if err := d.StoreBlockElements(ctx, blockE); err != nil {
		return err
	}

	// Store the new tag elements
	fmt.Printf("Storing tag elements\n")
	if err := d.StoreTagElements(ctx, tagE); err != nil {
		return err
	}

	return nil
}

func (d *Data) DeleteElement(ctx *datastore.VersionedCtx, pt dvid.Point3d) error {
	// Get from block key
	blockSize := d.blockSize(ctx.VersionID())
	blockCoord := pt.Chunk(blockSize).(dvid.ChunkPoint3d)
	tk := NewBlockTKey(blockCoord)

	d.Lock()
	defer d.Unlock()

	elems, err := getElements(ctx, tk)
	if err != nil {
		return err
	}

	// Delete the given element
	deleted, _ := elems.delete(pt)
	if deleted == nil {
		return fmt.Errorf("Did not find element %s in datastore", pt)
	}

	// Put block key version without given element
	if err := putElements(ctx, tk, elems); err != nil {
		return err
	}

	// TODO: Delete in label key

	// Delete element in any tags
	if err := d.deleteElementInTags(ctx, deleted.Pos, deleted.Tags); err != nil {
		return err
	}

	// Modify any reference in relationships
	if err := d.deleteElementInRelationships(ctx, deleted.Pos, deleted.Rels); err != nil {
		return err
	}

	return nil
}

func (d *Data) MoveElement(ctx *datastore.VersionedCtx, from, to dvid.Point3d) error {
	// Calc block keys
	blockSize := d.blockSize(ctx.VersionID())
	fromCoord := from.Chunk(blockSize).(dvid.ChunkPoint3d)
	fromTk := NewBlockTKey(fromCoord)

	toCoord := to.Chunk(blockSize).(dvid.ChunkPoint3d)
	toTk := NewBlockTKey(toCoord)

	d.Lock()
	defer d.Unlock()

	// Handle from block
	fromElems, err := getElements(ctx, fromTk)
	if err != nil {
		return err
	}

	deleteElement := (bytes.Compare(fromTk, toTk) != 0)
	moved, _ := fromElems.move(from, to, deleteElement)
	if moved == nil {
		return fmt.Errorf("Did not find moved element %s in datastore", from)
	}

	if err := putElements(ctx, fromTk, fromElems); err != nil {
		return err
	}

	// If we've moved blocks, add the element in new place.
	if deleteElement {
		toElems, err := getElements(ctx, toTk)
		if err != nil {
			return err
		}
		toElems.add(Elements{*moved})

		if err := putElements(ctx, toTk, toElems); err != nil {
			return err
		}
	}

	// TODO: Move in label key

	// Move element in any tags
	if err := d.moveElementInTags(ctx, from, to, moved.Tags); err != nil {
		return err
	}

	// Move any reference in relationships
	if err := d.moveElementInRelationships(ctx, from, to, moved.Rels); err != nil {
		return err
	}

	return nil
}

// GetByUUID returns a pointer to labelvol data given a version (UUID) and data name.
func GetByUUID(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUID(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not a labelvol datatype!", name)
	}
	return data, nil
}

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

// Send transfers all key-value pairs pertinent to this data type as well as
// the storage.DataStoreType for them.
func (d *Data) Send(s message.Socket, roiname string, uuid dvid.UUID) error {
	dvid.Errorf("labelvol.Send() is not implemented yet, so push/pull will not work for this data type.\n")
	return nil
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

	case "label":
		if action != "get" {
			server.BadRequest(w, r, "Only GET action is available on 'label' endpoint.")
			return
		}
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
		elements, err := GetLabelSynapses(ctx, label)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		jsonBytes, err := json.Marshal(elements)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if _, err := w.Write(jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: get synaptic elements for label %d (%s)", r.Method, label, r.URL)

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
		elements, err := GetTagSynapses(ctx, tag)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
		jsonBytes, err := json.Marshal(elements)
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if _, err := w.Write(jsonBytes); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: get synaptic elements for tag %s (%s)", r.Method, tag, r.URL)

	case "elements":
		switch action {
		case "get":
			// GET <api URL>/node/<UUID>/<data name>/elements/<size>/<offset>
			if len(parts) < 6 {
				server.BadRequest(w, r, "Expect size and offset to follow 'elements' in URL")
				return
			}
			sizeStr, offsetStr := parts[4], parts[5]
			ext3d, err := dvid.NewExtents3dFromStrings(offsetStr, sizeStr, "_")
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			elements, err := d.GetRegionSynapses(ctx, ext3d)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			w.Header().Set("Content-type", "application/json")
			jsonBytes, err := json.Marshal(elements)
			if err != nil {
				server.BadRequest(w, r, err)
				return
			}
			if _, err := w.Write(jsonBytes); err != nil {
				server.BadRequest(w, r, err)
				return
			}
			timedLog.Infof("HTTP %s: synapse elements in subvolume (size %s, offset %s) (%s)", r.Method, sizeStr, offsetStr, r.URL)

		case "post":
			if err := d.StoreSynapses(ctx, r.Body); err != nil {
				server.BadRequest(w, r, err)
				return
			}
		default:
			server.BadRequest(w, r, "Only GET or POST action is available on 'elements' endpoint.")
			return
		}

	case "element":
		// DELETE <api URL>/node/<UUID>/<data name>/element/<coord>
		if action != "delete" {
			server.BadRequest(w, r, "Only DELETE action is available on 'element' endpoint.")
			return
		}
		if len(parts) < 5 {
			server.BadRequest(w, r, "Must include coordinate after DELETE on 'element' endpoint.")
			return
		}
		pt, err := dvid.StringToPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.DeleteElement(ctx, pt); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: delete synaptic element at %s (%s)", r.Method, pt, r.URL)

	case "move":
		// POST <api URL>/node/<UUID>/<data name>/move/<from_coord>/<to_coord>
		if action != "post" {
			server.BadRequest(w, r, "Only POST action is available on 'move' endpoint.")
			return
		}
		if len(parts) < 6 {
			server.BadRequest(w, r, "Must include 'from' and 'to' coordinate after 'move' endpoint.")
			return
		}
		fromPt, err := dvid.StringToPoint3d(parts[4], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		toPt, err := dvid.StringToPoint3d(parts[5], "_")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		if err := d.MoveElement(ctx, fromPt, toPt); err != nil {
			server.BadRequest(w, r, err)
			return
		}
		timedLog.Infof("HTTP %s: move synaptic element from %s to %s (%s)", r.Method, fromPt, toPt, r.URL)

	default:
		server.BadRequest(w, r, "Unrecognized API call %q for labelvol data %q.  See API help.",
			parts[3], d.DataName())
	}
}
