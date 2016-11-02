/*
	Package annotation supports point annotation management and queries.
*/
package annotation

import (
	"bytes"
	"encoding/binary"
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
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/annotation"
	TypeName = "annotation"
)

const HelpMessage = `
API for synapse data type (github.com/janelia-flyem/dvid/datatype/annotation)
=======================================================================================

Command-line:

$ dvid repo <UUID> new annotation <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new annotation synapses

    Arguments:

    UUID           Hexidecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "synapses"
    settings       Configuration settings in "key=value" format separated by spaces.
	
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


POST <api URL>/node/<UUID>/<data name>/sync?<options>

    Appends to list of data instances with which the annotations are synced.  Expects JSON to be POSTed
    with the following format:

    { "sync": "labels,bodies" }

	To delete syncs, pass an empty string of names with query string "replace=true":

	{ "sync": "" }

    The "sync" property should be followed by a comma-delimited list of data instances that MUST
    already exist.  Currently, syncs should be created before any annotations are pushed to
    the server.  If annotations already exist, these are currently not synced.

    The annotations data type only accepts syncs to labelblk and labelvol data instances.

    GET Query-string Options:

    replace    Set to "true" if you want passed syncs to replace and not be appended to current syncs.
			   Default operation is false.


Note: For the following URL endpoints that return and accept POSTed JSON values, see the JSON format
at end of this documentation.

GET <api URL>/node/<UUID>/<data name>/label/<label>[?<options>]

	Returns all point annotations within the given label as an array of elements.
	This endpoint is only available if the annotation data instance is synced with
	a labelblk data instance.
	
	GET Query-string Option:

	relationships   Set to true to return all relationships for each annotation.

	Example:

	GET http://foo.com/api/node/83af/myannotations/label/23?relationships=true


GET <api URL>/node/<UUID>/<data name>/tag/<tag>[?<options>]

	Returns all point annotations with the given tag as an array of elements.
	By default, the Relationships of an annotation to other annotations is not
	returned.  If you want the Relationships, use the query string below.

	GET Query-string Option:

	relationships   Set to true to return all relationships for each annotation.

	Example:

	GET http://foo.com/api/node/83af/myannotations/tag/goodstuff?relationships=true
	

DELETE <api URL>/node/<UUID>/<data name>/element/<coord>

	Deletes a point annotation given its location.

GET <api URL>/node/<UUID>/<data name>/elements/<size>/<offset>

	Returns all point annotations within subvolume of given size with upper left corner
	at given offset.  The size and offset should be voxels separated by underscore, e.g.,
	"400_300_200" can describe a 400 x 300 x 200 volume or an offset of (400,300,200).

	The returned point annotations will be an array of elements.

POST <api URL>/node/<UUID>/<data name>/elements

	Adds or modifies point annotations.  The POSTed content is an array of elements.
	Note that deletes are handled via a separate API (see above).

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

// IsSynaptic returns true if the ElementType is some synaptic component.
func (e ElementType) IsSynaptic() bool {
	switch e {
	case PostSyn, PreSyn, Gap:
		return true
	default:
		return false
	}
}

// StringToElementType converts a string
func StringToElementType(s string) ElementType {
	switch s {
	case "PostSyn":
		return PostSyn
	case "PreSyn":
		return PreSyn
	case "Gap":
		return Gap
	case "Note":
		return Note
	default:
		return UnknownElem
	}
}

func (e ElementType) String() string {
	switch e {
	case PostSyn:
		return "PostSyn"
	case PreSyn:
		return "PreSyn"
	case Gap:
		return "Gap"
	case Note:
		return "Note"
	default:
		return fmt.Sprintf("Unknown element type: %d", e)
	}
}

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
		return nil, fmt.Errorf("Unknown element type: %s", e)
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

// ElementNR describes a synaptic element's properties with No Relationships (NR),
// used purely for label and tag annotations.
type ElementNR struct {
	Pos   dvid.Point3d
	Kind  ElementType
	Label uint64
	Tags  Tags              // Indexed
	Prop  map[string]string // Non-Indexed
}

// Element describes a synaptic element's properties, including Relationships.
type Element struct {
	ElementNR
	Rels Relationships
}

func (e Element) Copy() *Element {
	c := new(Element)
	c.Pos = e.Pos
	c.Kind = e.Kind
	c.Label = e.Label
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

type ElementsNR []ElementNR

// Normalize returns ElementsNR that can be used for DeepEqual because all positions and tags are sorted.
func (elems ElementsNR) Normalize() ElementsNR {
	// For every element, create a duplicate that has sorted relationships and sorted tags.
	out := make(ElementsNR, len(elems), len(elems))
	for i, elem := range elems {
		out[i].Pos = elem.Pos
		out[i].Kind = elem.Kind
		out[i].Label = elem.Label
		out[i].Tags = make(Tags, len(elem.Tags))
		copy(out[i].Tags, elem.Tags)

		out[i].Prop = make(map[string]string, len(elem.Prop))
		for k, v := range elem.Prop {
			out[i].Prop[k] = v
		}

		sort.Sort(out[i].Tags)
	}

	// Sort all elements based on their position.
	sort.Sort(out)
	return out
}

// --- Sort interface

func (elems ElementsNR) Len() int {
	return len(elems)
}

// Less returns true if element[i] < element[j] where ordering is determined by
// Pos and Kind in that order.  Tags are not considered in ordering.
func (elems ElementsNR) Less(i, j int) bool {
	if elems[i].Pos.Less(elems[j].Pos) {
		return true
	}
	if elems[i].Pos.Equals(elems[j].Pos) {
		return elems[i].Kind < elems[j].Kind
	}
	return false
}

func (elems ElementsNR) Swap(i, j int) {
	elems[i], elems[j] = elems[j], elems[i]
}

type Elements []Element

// helper function that just returns slice of positions suitable for intersect calcs in dvid package.
func (elems Elements) positions() []dvid.Point3d {
	pts := make([]dvid.Point3d, len(elems))
	for i, elem := range elems {
		pts[i] = elem.Pos
	}
	return pts
}

// Returns Elements that can be used for DeepEqual because all positions, relationships, and tags are sorted.
func (elems Elements) Normalize() Elements {
	// For every element, create a duplicate that has sorted relationships and sorted tags.
	out := make(Elements, len(elems), len(elems))
	for i, elem := range elems {
		out[i].Pos = elem.Pos
		out[i].Kind = elem.Kind
		out[i].Label = elem.Label
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

// Adds elements but if element at position already exists, it replaces the properties of that element.
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

// Deletes element position as well as relationships that reference that element.
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

// --- Annotation Datatype -----

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
	var elems Elements
	if err := json.Unmarshal(val, &elems); err != nil {
		return nil, err
	}
	return elems, nil
}

// makes sure that no relationships are returned since they could be out of date.
func getElementsNR(ctx *datastore.VersionedCtx, tk storage.TKey) (ElementsNR, error) {
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
	var elems ElementsNR
	if err := json.Unmarshal(val, &elems); err != nil {
		return nil, err
	}
	return elems, nil
}

func putBatchElements(batch storage.Batch, tk storage.TKey, elems Elements) error {
	val, err := json.Marshal(elems)
	if err != nil {
		return err
	}
	batch.Put(tk, val)
	return nil
}

func putElements(ctx *datastore.VersionedCtx, tk storage.TKey, elems Elements) error {
	val, err := json.Marshal(elems)
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

// Returns elements within the sparse volume represented by the blocks of RLEs.
func getElementsInRLE(ctx *datastore.VersionedCtx, brles dvid.BlockRLEs) (Elements, error) {
	rleElems := Elements{}
	for izyx, rles := range brles {
		// Get elements for this block
		blockCoord, err := izyx.ToChunkPoint3d()
		tk := NewBlockTKey(blockCoord)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return nil, err
		}

		// Append the elements in current block RLE
		in := rles.Within(elems.positions())
		for _, idx := range in {
			rleElems = append(rleElems, elems[idx])
		}
	}
	return rleElems, nil
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

	// sync channels for receiving subscribed events like merge, split, and block changes.
	syncCh   chan datastore.SyncMessage
	syncDone chan *sync.WaitGroup

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
func (d *Data) blockSize() dvid.Point3d {
	if d.cachedBlockSize != nil {
		return *d.cachedBlockSize
	}
	var bsize dvid.Point3d
	d.cachedBlockSize = &bsize
	if lb := d.GetSyncedLabelblk(); lb != nil {
		bsize = lb.BlockSize().(dvid.Point3d)
		return bsize
	}
	if lv := d.GetSyncedLabelvol(); lv != nil {
		if len(bsize) != 0 && !bsize.Equals(lv.BlockSize) {
			dvid.Errorf("annotations %q is synced to labelblk and labelvol with different block sizes!\n", d.DataName())
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

func (d *Data) GetSyncedLabelblk() *labelblk.Data {
	for dataUUID := range d.SyncedData() {
		source, err := labelblk.GetByDataUUID(dataUUID)
		if err == nil {
			return source
		}
	}
	return nil
}

func (d *Data) GetSyncedLabelvol() *labelvol.Data {
	for dataUUID := range d.SyncedData() {
		source, err := labelvol.GetByDataUUID(dataUUID)
		if err == nil {
			return source
		}
	}
	return nil
}

// returns Elements with Relationships added by querying the block-indexed elements.
func (d *Data) getExpandedElements(ctx *datastore.VersionedCtx, tk storage.TKey) (Elements, error) {
	elems, err := getElements(ctx, tk)
	if err != nil {
		return elems, err
	}

	// Batch each element into blocks.
	blockSize := d.blockSize()
	blockE := make(blockElements)
	for _, elem := range elems {
		// Get block coord for this element.
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)

		// Append to block
		be := blockE[izyxStr]
		be = append(be, elem)
		blockE[izyxStr] = be
	}

	expanded := make(Elements, 0, len(elems))
	for izyx, be := range blockE {
		// Read the block-indexed elements
		chunkPt, err := izyx.ToChunkPoint3d()
		if err != nil {
			return nil, err
		}
		btk := NewBlockTKey(chunkPt)
		relElems, err := getElements(ctx, btk)
		if err != nil {
			return nil, err
		}

		// Construct a point map for quick lookup to element data
		emap := make(map[string]int)
		for i, relem := range relElems {
			emap[relem.Pos.MapKey()] = i
		}

		// Expand.
		for _, elem := range be {
			i, found := emap[elem.Pos.MapKey()]
			if found {
				expanded = append(expanded, relElems[i])
			} else {
				dvid.Errorf("Can't expand relationships for data %q, element @ %s, didn't find it in block %s!\n", d.DataName(), elem.Pos, izyx)
			}
		}
	}
	return expanded, nil
}

// delete all reference to given element point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) deleteElementInTags(ctx *datastore.VersionedCtx, pt dvid.Point3d, tags []Tag) error {
	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	for _, tag := range tags {
		// Get the elements in tag.
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Note all elements to be deleted.
		var toDel []int
		for i, elem := range elems {
			if pt.Equals(elem.Pos) {
				toDel = append(toDel, i)
			}
		}
		if len(toDel) == 0 {
			return nil
		}

		// Delete them from high index to low index due while reusing slice.
		for i := len(toDel) - 1; i >= 0; i-- {
			d := toDel[i]
			elems[d] = elems[len(elems)-1]
			elems[len(elems)-1] = Element{}
			elems = elems[:len(elems)-1]
		}

		// Save the tag.
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return batch.Commit()
}

func (d *Data) deleteElementInLabel(ctx *datastore.VersionedCtx, pt dvid.Point3d) error {
	labelData := d.GetSyncedLabelblk()
	if labelData == nil {
		return nil // no synced labels
	}
	label, err := labelData.GetLabelAtPoint(ctx.VersionID(), pt)
	if err != nil {
		return err
	}
	tk := NewLabelTKey(label)
	elems, err := getElements(ctx, tk)
	if err != nil {
		return fmt.Errorf("err getting elements for label %d: %v\n", label, err)
	}

	// Note all elements to be deleted.
	var delta DeltaModifyElements
	var toDel []int
	for i, elem := range elems {
		if pt.Equals(elem.Pos) {
			delta.Del = append(delta.Del, ElementPos{Label: label, Kind: elem.Kind, Pos: elem.Pos})
			toDel = append(toDel, i)
		}
	}
	if len(toDel) == 0 {
		return nil
	}

	// Delete them from high index to low index due while reusing slice.
	for i := len(toDel) - 1; i >= 0; i-- {
		d := toDel[i]
		elems[d] = elems[len(elems)-1]
		elems[len(elems)-1] = Element{}
		elems = elems[:len(elems)-1]
	}

	// Put the modified list of elements
	if err := putElements(ctx, tk, elems); err != nil {
		return err
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}
	return nil
}

// delete all reference to given element point in the related points.
// This is private method and assumes outer locking.
func (d *Data) deleteElementInRelationships(ctx *datastore.VersionedCtx, pt dvid.Point3d, rels []Relationship) error {
	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	blockSize := d.blockSize()
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
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return batch.Commit()
}

// move all reference to given element point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) moveElementInTags(ctx *datastore.VersionedCtx, from, to dvid.Point3d, tags []Tag) error {
	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	for _, tag := range tags {
		// Get the elements in tag.
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
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
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return batch.Commit()
}

func (d *Data) moveElementInLabels(ctx *datastore.VersionedCtx, from, to dvid.Point3d, moved *Element) error {
	labelData := d.GetSyncedLabelblk()
	if labelData == nil {
		return nil // no label denormalization possible
	}
	oldLabel, err := labelData.GetLabelAtPoint(ctx.VersionID(), from)
	if err != nil {
		return err
	}
	newLabel, err := labelData.GetLabelAtPoint(ctx.VersionID(), to)
	if err != nil {
		return err
	}
	if oldLabel == newLabel {
		return nil
	}
	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	var delta DeltaModifyElements
	if oldLabel != 0 {
		tk := NewLabelTKey(oldLabel)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for label %d: %v", oldLabel, err)
		}
		if _, changed := elems.delete(from); changed {
			if err := putBatchElements(batch, tk, elems); err != nil {
				return fmt.Errorf("err putting deleted label %d element: %v", oldLabel, err)
			}
			delta.Del = append(delta.Del, ElementPos{Label: oldLabel, Kind: moved.Kind, Pos: from})
		}
	}
	if newLabel != 0 {
		tk := NewLabelTKey(newLabel)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for label %d: %v", newLabel, err)
		}
		elems.add(Elements{*moved})
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
		delta.Add = append(delta.Add, ElementPos{Label: newLabel, Kind: moved.Kind, Pos: to})
	}

	// Notify any subscribers of label annotation changes.
	if len(delta.Del) != 0 || len(delta.Add) != 0 {
		evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
		msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			return err
		}
	}

	return batch.Commit()
}

// move all reference to given element point in the related points in different blocks.
// This is private method and assumes outer locking as well as current "from" block already being modified,
// including relationships.
func (d *Data) moveElementInRelationships(ctx *datastore.VersionedCtx, from, to dvid.Point3d, rels []Relationship) error {
	blockSize := d.blockSize()
	fromBlockCoord := from.Chunk(blockSize).(dvid.ChunkPoint3d)

	// Get list of blocks with related points.
	relBlocks := make(map[dvid.IZYXString]struct{})
	for _, rel := range rels {
		blockCoord := rel.To.Chunk(blockSize).(dvid.ChunkPoint3d)
		if blockCoord.Equals(fromBlockCoord) {
			continue // relationships are almoved in from block
		}
		relBlocks[blockCoord.ToIZYXString()] = struct{}{}
	}

	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	// Alter the moved points in those related blocks.
	for izyxstr := range relBlocks {
		blockCoord, err := izyxstr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(blockCoord)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Move element in related element.
		if _, changed := elems.move(from, to, false); !changed {
			dvid.Errorf("Unable to find moved element %s in related element @ block %s:\n%v\n", from, blockCoord, elems)
			continue
		}

		// Save the block elements.
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return batch.Commit()
}

func (d *Data) modifyElements(ctx *datastore.VersionedCtx, tk storage.TKey, toAdd Elements) error {
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

// stores synaptic elements arranged by block, replacing any
// elements at same position.
func (d *Data) storeBlockElements(ctx *datastore.VersionedCtx, be blockElements) error {
	for izyxStr, elems := range be {
		blockCoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		// Modify the block annotations
		tk := NewBlockTKey(blockCoord)
		if err := d.modifyElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// stores synaptic elements arranged by label, replacing any
// elements at same position.
func (d *Data) storeLabelElements(ctx *datastore.VersionedCtx, be blockElements) error {
	labelData := d.GetSyncedLabelblk()
	if labelData == nil {
		dvid.Infof("No synced labels for annotation %q, skipping label-aware denormalization.\n", d.DataName())
		return nil // no synced labels
	}
	store, err := d.BackendStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}

	// Compute the strides (in bytes)
	blockSize := d.blockSize()
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * 8)

	toAdd := LabelElements{}
	for izyxStr, elems := range be {
		blockCoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}

		// Get the labels for this block
		labels, err := labelData.GetLabelBlock(ctx.VersionID(), blockCoord)
		if err != nil {
			return err
		}
		if len(labels) == 0 {
			continue
		}
		if len(labels) != blockBytes {
			return fmt.Errorf("Expected %d bytes in %q label block, got %d instead.  Aborting.", blockBytes, d.DataName(), len(labels))
		}

		// Group annotations by label
		for _, elem := range elems {
			pt := elem.Pos.Point3dInChunk(blockSize)
			i := pt[2]*bY + pt[1]*bX + pt[0]*8
			label := binary.LittleEndian.Uint64(labels[i : i+8])
			if label != 0 {
				toAdd.add(label, elem)
			}
		}
	}

	// Store all the added annotations to the appropriate labels.
	var delta DeltaModifyElements
	batch := batcher.NewBatch(ctx)
	for label, additions := range toAdd {
		tk := NewLabelTKey(label)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for label %d: %v\n", label, err)
		}

		// Check if these annotations already exist.
		emap := make(map[string]int)
		for i, elem := range elems {
			emap[elem.Pos.MapKey()] = i
		}
		for _, elem := range additions {
			i, found := emap[elem.Pos.MapKey()]
			if !found {
				elems = append(elems, elem)
				delta.Add = append(delta.Add, ElementPos{Label: label, Kind: elem.Kind, Pos: elem.Pos})
			} else {
				elems[i] = elem // replace properties if same position
			}
		}
		if err := putBatchElements(batch, tk, elems); err != nil {
			return fmt.Errorf("couldn't serialize label %d annotations in instance %q: %v\n", label, d.DataName(), err)
		}
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	return batch.Commit()
}

// stores synaptic elements arranged by tag, replacing any
// elements at same position.
func (d *Data) storeTagElements(ctx *datastore.VersionedCtx, te tagElements) error {
	for tag, elems := range te {
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		if err := d.modifyElements(ctx, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// GetLabelJSON returns JSON for synapse elements in a given label.
func (d *Data) GetLabelJSON(ctx *datastore.VersionedCtx, label uint64, addRels bool) ([]byte, error) {
	d.RLock()
	defer d.RUnlock()

	tk := NewLabelTKey(label)
	if addRels {
		elems, err := d.getExpandedElements(ctx, tk)
		if err != nil {
			return nil, err
		}
		return json.Marshal(elems)
	}
	elems, err := getElementsNR(ctx, tk)
	if err != nil {
		return nil, err
	}
	return json.Marshal(elems)
}

// GetLabelSynapses returns synapse elements for a given label.
func (d *Data) GetLabelSynapses(ctx *datastore.VersionedCtx, label uint64) (Elements, error) {
	d.RLock()
	defer d.RUnlock()

	tk := NewLabelTKey(label)
	return d.getExpandedElements(ctx, tk)
}

// GetTagJSON returns JSON for synapse elements in a given tag.
func (d *Data) GetTagJSON(ctx *datastore.VersionedCtx, tag Tag, addRels bool) (jsonBytes []byte, err error) {
	d.RLock()
	defer d.RUnlock()

	var tk storage.TKey
	var elems interface{}
	tk, err = NewTagTKey(tag)
	if err != nil {
		return
	}
	if addRels {
		elems, err = d.getExpandedElements(ctx, tk)
	} else {
		elems, err = getElementsNR(ctx, tk)
	}
	if err == nil {
		jsonBytes, err = json.Marshal(elems)
	}
	return
}

// GetTagSynapses returns synapse elements for a given tag.
func (d *Data) GetTagSynapses(ctx *datastore.VersionedCtx, tag Tag) (Elements, error) {
	d.RLock()
	defer d.RUnlock()

	tk, err := NewTagTKey(tag)
	if err != nil {
		return nil, err
	}
	return d.getExpandedElements(ctx, tk)
}

// GetRegionSynapses returns synapse elements for a given subvolume of image space.
func (d *Data) GetRegionSynapses(ctx *datastore.VersionedCtx, ext *dvid.Extents3d) (Elements, error) {
	store, err := d.GetOrderedKeyValueDB()
	if err != nil {
		return nil, err
	}

	// Setup block bounds for synapse element query in supplied Z range.
	blockSize := d.blockSize()
	begBlockCoord, endBlockCoord := ext.BlockRange(blockSize)

	begTKey := NewBlockTKey(begBlockCoord)
	endTKey := NewBlockTKey(endBlockCoord)

	d.RLock()
	defer d.RUnlock()

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

// StoreSynapses performs a synchronous store of synapses in JSON format, not
// returning until the data and its denormalizations are complete.
func (d *Data) StoreSynapses(ctx *datastore.VersionedCtx, r io.Reader) error {
	jsonBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	var elems Elements
	if err := json.Unmarshal(jsonBytes, &elems); err != nil {
		return err
	}

	d.Lock()
	defer d.Unlock()

	dvid.Infof("%d synaptic elements received via POST", len(elems))

	blockSize := d.blockSize()
	blockE := make(blockElements)
	tagE := make(tagElements)

	// Iterate through elements, organizing them into blocks and tags.
	// Note: we do not check for redundancy and guarantee uniqueness at this stage.
	for _, elem := range elems {
		// Get block coord for this element.
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)

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
	}

	// TODO -- If we group these in batch transaction might be too many k/v mutations.
	// Really should be done via mutate log + mutation id for long-lived denormalizations.

	// Store the new block elements
	if err := d.storeBlockElements(ctx, blockE); err != nil {
		return err
	}

	// Store new elements among label denormalizations
	if err := d.storeLabelElements(ctx, blockE); err != nil {
		return err
	}

	// Store the new tag elements
	if err := d.storeTagElements(ctx, tagE); err != nil {
		return err
	}

	return nil
}

func (d *Data) DeleteElement(ctx *datastore.VersionedCtx, pt dvid.Point3d) error {
	// Get from block key
	blockSize := d.blockSize()
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

	// Delete in label key
	if err := d.deleteElementInLabel(ctx, deleted.Pos); err != nil {
		return err
	}

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
	blockSize := d.blockSize()
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

	// Move in label key
	if err := d.moveElementInLabels(ctx, from, to, moved); err != nil {
		return err
	}

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

// GetByDataUUID returns a pointer to annotation data given a data UUID.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not an annotation datatype!", source.DataName())
	}
	return data, nil
}

// GetByUUIDName returns a pointer to annotation data given a version (UUID) and data name.
func GetByUUIDName(uuid dvid.UUID, name dvid.InstanceName) (*Data, error) {
	source, err := datastore.GetDataByUUIDName(uuid, name)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("Instance '%s' is not an annotation datatype!", name)
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

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	switch request.TypeCommand() {
	default:
		return fmt.Errorf("Unknown command.  Data type '%s' [%s] does not support '%s' command.",
			d.DataName(), d.TypeName(), request.TypeCommand())
	}
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
		replace := r.URL.Query().Get("replace") == "true"
		if err := datastore.SetSyncByJSON(d, uuid, replace, r.Body); err != nil {
			server.BadRequest(w, r, err)
			return
		}

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
		queryStrings := r.URL.Query()
		jsonBytes, err := d.GetLabelJSON(ctx, label, queryStrings.Get("relationships") == "true")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
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
		queryStrings := r.URL.Query()
		jsonBytes, err := d.GetTagJSON(ctx, tag, queryStrings.Get("relationships") == "true")
		if err != nil {
			server.BadRequest(w, r, err)
			return
		}
		w.Header().Set("Content-type", "application/json")
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
				server.BadRequest(w, r, "Expect size and offset to follow 'elements' in GET request")
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
		server.BadAPIRequest(w, r, d)
	}
}
