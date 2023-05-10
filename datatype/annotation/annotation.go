// Package annotation supports point annotation management and queries.
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
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/labelarray"
	"github.com/janelia-flyem/dvid/datatype/labelblk"
	"github.com/janelia-flyem/dvid/datatype/labelmap"
	"github.com/janelia-flyem/dvid/datatype/labelvol"
	"github.com/janelia-flyem/dvid/datatype/roi"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version  = "0.1"
	RepoURL  = "github.com/janelia-flyem/dvid/datatype/annotation"
	TypeName = "annotation"
)

const helpMessage = `
API for synapse data type (github.com/janelia-flyem/dvid/datatype/annotation)
=======================================================================================

Note: UUIDs referenced below are strings that may either be a unique prefix of a
hexadecimal UUID string (e.g., 3FA22) or a branch leaf specification that adds
a colon (":") followed by the case-dependent branch name.  In the case of a
branch leaf specification, the unique UUID prefix just identifies the repo of
the branch, and the UUID referenced is really the leaf of the branch name.
For example, if we have a DAG with root A -> B -> C where C is the current
HEAD or leaf of the "master" (default) branch, then asking for "B:master" is
the same as asking for "C".  If we add another version so A -> B -> C -> D, then
references to "B:master" now return the data from "D".

Command-line:

$ dvid repo <UUID> new annotation <data name> <settings...>

	Adds newly named data of the 'type name' to repo with specified UUID.

	Example:

	$ dvid repo 3f8c new annotation synapses

    Arguments:

    UUID           Hexadecimal string with enough characters to uniquely identify a version node.
    data name      Name of data to create, e.g., "synapses"
    settings       Configuration settings in "key=value" format separated by spaces.
	
$ dvid node <UUID> <data name> reload <settings...>

	Forces asynchornous denormalization of all annotations for labels and tags.  Because
	this is a special request for mass mutations that require static "normalized" data
	(only verifies and changes the label and tag denormalizations), any POST requests
	while this is running results in an error.

    Configuration Settings (case-insensitive keys)

	check 		"true": (default "false") check denormalizations, writing to log when issues
					are detected, and only replacing denormalization when it is incorrect.
	inmemory 	"false": (default "true") use in-memory reload, which assumes the server
					has enough memory to hold all annotations in memory.

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

    UUID          Hexadecimal string with enough characters to uniquely identify a version node.
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

	The annotations data type only accepts syncs to label-oriented datatypes: labelblk, labelvol,
	labelarray, and labelmap.

    POST Query-string Options:

    replace    Set to "true" if you want passed syncs to replace and not be appended to current syncs.
			   Default operation is false.


GET <api URL>/node/<UUID>/<data name>/tags
POST <api URL>/node/<UUID>/<data name>/tags?<options>

	GET retrieves JSON of tags for this instance.
	POST appends or replaces tags provided in POST body.  Expects JSON to be POSTed
	with the following format:

	{ "tag1": "anything you want", "tag2": "something else" }

	To delete tags, pass an empty object with query string "replace=true".

	POST Query-string Options:

	replace   Set to "true" if you want passed tags to replace and not be appended to current tags.
			  Default operation is false (append).
		   
		   
Note: For the following URL endpoints that return and accept POSTed JSON values, see the JSON format
at end of this documentation.

GET <api URL>/node/<UUID>/<data name>/label/<label>[?<options>]

	Returns all point annotations within the given label as an array of elements.
	This endpoint is only available if the annotation data instance is synced with
	voxel label data instances (labelblk, labelarray, labelmap).
	
	GET Query-string Option:

	relationships   Set to true to return all relationships for each annotation.

	Example:

	GET http://foo.com/api/node/83af/myannotations/label/23?relationships=true


POST <api URL>/node/<UUID>/<data name>/labels

	Ingest point annotations for labels. The POSTed JSON should be an object
	with label string keys (allowing uint64) and a string that contains the
	JSON array of annotations (without relationships) for that label.  For example:

	{ "23": "[{...},{...}]", "45": "[{...},{...}]" }


GET <api URL>/node/<UUID>/<data name>/tag/<tag>[?<options>]

	Returns all point annotations with the given tag as an array of elements.
	By default, the Relationships of an annotation to other annotations is not
	returned.  If you want the Relationships, use the query string below.

	GET Query-string Option:

	relationships   Set to true to return all relationships for each annotation.

	Example:

	GET http://foo.com/api/node/83af/myannotations/tag/goodstuff?relationships=true
	

DELETE <api URL>/node/<UUID>/<data name>/element/<coord>[?<options>]

	Deletes a point annotation given its location.

	Kafka JSON message generated by this request where "User" is optional:
		{ 
			"Action": "element-delete",
			"Point": <3d point>,
			"UUID": <UUID on which delete was done>,
			"User": <user name>
		}

	POST Query-string Options:

	kafkalog    Set to "off" if you don't want this mutation logged to kafka.


GET <api URL>/node/<UUID>/<data name>/roi/<ROI specification>

	Returns all point annotations within the ROI.  The ROI specification must be specified
	using a string of format "roiname,uuid".  If just "roiname" is specified without
	a full UUID string, the current UUID of the request will be used.  Currently, this 
	request will only work for ROIs that have same block size as the annotation data instance.

	The returned point annotations will be an array of elements.

GET <api URL>/node/<UUID>/<data name>/elements/<size>/<offset>

	Returns all point annotations within subvolume of given size with upper left corner
	at given offset.  The size and offset should be voxels separated by underscore, e.g.,
	"400_300_200" can describe a 400 x 300 x 200 volume or an offset of (400,300,200).

	The returned point annotations will be an array of elements with relationships.

POST <api URL>/node/<UUID>/<data name>/elements[?<options>]

	Adds or modifies point annotations.  The POSTed content is an array of elements.
	Note that deletes are handled via a separate API (see above).

	Kafka JSON message generated by this request where "User" is optional:
		{ 
			"Action": "element-post",
			"DataRef": <string for reference to posted binary data>,
			"UUID": <UUID on which POST was done>,
			"User": <user name>
		}
	
	The data reference above can be used to download the binary data by calling
	this data instance's BlobStore API.  See the node-level HTTP API documentation.

		GET /api/node/{uuid}/{data name}/blobstore/{reference}

	POST Query-string Options:

	kafkalog    Set to "off" if you don't want this mutation logged to kafka.

GET <api URL>/node/<UUID>/<data name>/scan[?<options>]

	Scans the annotations stored in blocks and returns simple stats on usage
	in JSON format.

	GET Query-string Options:

	byCoord    If "true" (not set by default), the scan bounds will be by min/max 
			    block coord instead of internal constants.
	keysOnly   If "true" (not set by default), scans using keys only range query
	            and will not check if value is empty.


GET <api URL>/node/<UUID>/<data name>/all-elements

	Returns all point annotations in the entire data instance, which could exceed data
	response sizes (set by server) if too many elements are present.  This should be
	equivalent to the /blocks endpoint but without the need to determine extents.

	The returned stream of data is the same as /blocks endpoint below.


GET <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>

	Returns all point annotations within all blocks intersecting the subvolume of given size 
	with upper left corner at given offset.  The size and offset should be voxels separated by 
	underscore, e.g., "400_300_200" can describe a 400 x 300 x 200 volume or an offset of (400,300,200).

	Unlike the /elements endpoint, the /blocks endpoint is the fastest way to retrieve
	all point annotations within a bounding box.  It does not screen points based on the specified 
	subvolume but simply streams all elements (including relationships) in the intersecting blocks.
	The fastest way to get all point annotations in entire volume (no bounding box) is via /all-elements.

	The returned stream of data is an object with block coordinate as keys and an array of point
	annotation elements within that block, meeting the JSON described below.

	If the data instance has Tag "ScanAllForBlocks" is set to "true", it's assumed there are
	relatively few annotations across blocks so a single range query is used rather than many
	range queries to span the given X range of the bounding box.

	Returned JSON:

	{
		"10,381,28": [ array of point annotation elements ],
		"11,381,28": [ array of point annotation elements ],
		...
	}

POST <api URL>/node/<UUID>/<data name>/blocks[?<options>]

	Unlike the POST /elements endpoint, the /blocks endpoint is the fastest way to store
	all point annotations and assumes the caller has (1) properly partitioned the elements
	int the appropriate block for the block size (default 64) and (2) will do a POST /reload
	to create the denormalized Label and Tag versions of the annotations after all
	ingestion is completed.

	This low-level ingestion also does not transmit subscriber events to associated
	synced data (e.g., labelsz).

	The POSTed JSON should be similar to the GET version with the block coordinate as 
	the key:

	{
		"10,381,28": [ array of point annotation elements ],
		"11,381,28": [ array of point annotation elements ],
		...
	}

	POST Query-string Options:

	kafkalog    Set to "off" if you don't want this mutation logged to kafka.


POST <api URL>/node/<UUID>/<data name>/move/<from_coord>/<to_coord>[?<options>]

	Moves the point annotation from <from_coord> to <to_coord> where
	<from_coord> and <to_coord> are of the form X_Y_Z.

	Kafka JSON message generated by this request where "User" is optional:
		{ 
			"Action": "element-move",
			"From": <3d point>,
			"To": <3d point>,
			"UUID": <UUID on which move was done>,
			User: <user name>
		}

	POST Query-string Options:

	kafkalog    Set to "off" if you don't want this mutation logged to kafka.

		

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

--------

POST <api URL>/node/<UUID>/<data name>/reload[?<options>]

	Forces asynchronous recreation of its tag and label indexed denormalizations.  Can be 
	used to initialize a newly added instance.  Note that this instance will return errors
	for any POST request while denormalization is ongoing.

	POST Query-string Options:

	check 		"true": (default "false") check denormalizations, writing to log when issues
					are detected, and only replacing denormalization when it is incorrect.
	inmemory 	"false": (default "true") use in-memory reload, which assumes the server
					has enough memory to hold all annotations in memory.
`

var (
	dtype *Type

	DefaultBlockSize int32   = 64 // labelblk.DefaultBlockSize
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

// Changes returns tags removed or added from the receiver.
func (t Tags) Changes(t2 Tags) (removed, added Tags) {
	if len(t) == 0 {
		added = make(Tags, len(t2))
		copy(added, t2)
		return
	}
	if len(t2) == 0 {
		removed = make(Tags, len(t))
		copy(removed, t)
		return
	}
	curTags := make(map[Tag]struct{}, len(t))
	newTags := make(map[Tag]struct{}, len(t2))
	for _, tag := range t {
		curTags[tag] = struct{}{}
	}
	for _, tag := range t2 {
		newTags[tag] = struct{}{}
	}
	for _, tag := range t2 {
		if _, found := curTags[tag]; !found {
			added = append(added, tag)
		}
	}
	for _, tag := range t {
		if _, found := newTags[tag]; !found {
			removed = append(removed, tag)
		}
	}
	return
}

// Removed returns tags removed from the receiver.
func (t Tags) Removed(t2 Tags) Tags {
	if len(t) == 0 {
		return Tags{}
	}
	if len(t2) == 0 {
		removed := make(Tags, len(t))
		copy(removed, t)
		return removed
	}
	newTags := make(map[Tag]struct{}, len(t2))
	for _, tag := range t2 {
		newTags[tag] = struct{}{}
	}
	var removed Tags
	for _, tag := range t {
		if _, found := newTags[tag]; !found {
			removed = append(removed, tag)
		}
	}
	return removed
}

// ElementNR describes a synaptic element's properties with No Relationships (NR),
// used for label and tag annotations while block-indexed annotations include the
// relationships.
type ElementNR struct {
	Pos  dvid.Point3d
	Kind ElementType
	Tags Tags              // Indexed
	Prop map[string]string // Non-Indexed
}

func (e ElementNR) String() string {
	s := fmt.Sprintf("Pos %s; Kind: %s; ", e.Pos, e.Kind)
	s += fmt.Sprintf("Tags: %v; Prop: %v", e.Tags, e.Prop)
	return s
}

func (e ElementNR) Copy() *ElementNR {
	c := new(ElementNR)
	c.Pos = e.Pos
	c.Kind = e.Kind
	c.Tags = make(Tags, len(e.Tags))
	copy(c.Tags, e.Tags)
	c.Prop = make(map[string]string, len(e.Prop))
	for k, v := range e.Prop {
		c.Prop[k] = v
	}
	return c
}

// Element describes a synaptic element's properties, including Relationships.
type Element struct {
	ElementNR
	Rels Relationships
}

func (e Element) Copy() *Element {
	c := new(Element)
	c.ElementNR = *(e.ElementNR.Copy())
	c.Rels = make(Relationships, len(e.Rels))
	copy(c.Rels, e.Rels)
	return c
}

// ElementsNR is a slice of elements without relationships.
type ElementsNR []ElementNR

// Normalize returns ElementsNR that can be used for DeepEqual because all positions and tags are sorted.
func (elems ElementsNR) Normalize() ElementsNR {
	// For every element, create a duplicate that has sorted relationships and sorted tags.
	out := make(ElementsNR, len(elems), len(elems))
	for i, elem := range elems {
		out[i].Pos = elem.Pos
		out[i].Kind = elem.Kind
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

// Adds elements but if element at position already exists, it replaces the properties of that element.
func (elems *ElementsNR) add(toAdd ElementsNR) {
	emap := make(map[string]int)
	for i, elem := range *elems {
		emap[elem.Pos.MapKey()] = i
	}
	for _, elem := range toAdd {
		i, found := emap[elem.Pos.MapKey()]
		if !found {
			*elems = append(*elems, *elem.Copy())
		} else {
			(*elems)[i] = elem
		}
	}
}

// Deletes element position as well as relationships that reference that element.
func (elems *ElementsNR) delete(pt dvid.Point3d) (deleted *ElementNR, changed bool) {
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
	return
}

// Moves element position as well as relationships.
func (elems *ElementsNR) move(from, to dvid.Point3d, deleteElement bool) (moved *ElementNR, changed bool) {
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
	return
}

// Elements is a slice of Element, which includes relationships.
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
			*elems = append(*elems, *elem.Copy())
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
	return helpMessage
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
		return Elements{}, nil
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
		return ElementsNR{}, nil
	}
	var elems ElementsNR
	if err := json.Unmarshal(val, &elems); err != nil {
		return nil, err
	}
	return elems, nil
}

func getBlockElementsNR(ctx *datastore.VersionedCtx, tk storage.TKey, blockSize dvid.Point3d) (map[dvid.IZYXString]ElementsNR, error) {
	elems, err := getElementsNR(ctx, tk)
	if err != nil {
		return nil, err
	}
	blockE := make(map[dvid.IZYXString]ElementsNR)
	for _, elem := range elems {
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)
		be := blockE[izyxStr]
		be = append(be, elem)
		blockE[izyxStr] = be
	}
	return blockE, nil
}

func putBatchElements(batch storage.Batch, tk storage.TKey, elems interface{}) error {
	val, err := json.Marshal(elems)
	if err != nil {
		return err
	}
	batch.Put(tk, val)
	return nil
}

func putElements(ctx *datastore.VersionedCtx, tk storage.TKey, elems interface{}) error {
	val, err := json.Marshal(elems)
	if err != nil {
		return err
	}
	store, err := ctx.GetOrderedKeyValueDB()
	if err != nil {
		return err
	}
	return store.Put(ctx, tk, val)
}

// Returns elements within the sparse volume represented by the blocks of RLEs.
func getElementsInRLE(ctx *datastore.VersionedCtx, brles dvid.BlockRLEs) (Elements, error) {
	rleElems := Elements{}
	for izyx, rles := range brles {
		// Get elements for this block
		bcoord, err := izyx.ToChunkPoint3d()
		tk := NewBlockTKey(bcoord)
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

type tagDeltaT struct {
	add   ElementsNR          // elements to add or modify
	erase map[string]struct{} // points to erase
}

func addTagDelta(newBlockE, curBlockE Elements, tagDelta map[Tag]tagDeltaT) {
	if len(newBlockE) == 0 {
		return
	}
	elemsByPoint := make(map[string]ElementNR, len(newBlockE))
	for _, newElem := range newBlockE {
		zyx := string(newElem.Pos.ToZYXBytes())
		elemsByPoint[zyx] = newElem.ElementNR
		// add every new point -- could check to see if exactly same but costs computation
		for _, tag := range newElem.Tags {
			td, found := tagDelta[tag]
			if found {
				td.add = append(td.add, newElem.ElementNR)
			} else {
				td.add = ElementsNR{newElem.ElementNR}
			}
			tagDelta[tag] = td
		}
	}
	for _, curElem := range curBlockE {
		zyx := string(curElem.Pos.ToZYXBytes())
		newElem, found := elemsByPoint[zyx]
		if !found {
			continue
		}
		removed := curElem.Tags.Removed(newElem.Tags)
		for _, tag := range removed {
			td, found := tagDelta[tag]
			if found {
				td.erase[zyx] = struct{}{}
			} else {
				td.erase = map[string]struct{}{
					zyx: struct{}{},
				}
			}
			tagDelta[tag] = td
		}
		delete(elemsByPoint, zyx)
	}
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

	denormOngoing bool // true if we are doing denormalizations so avoid ops on them.

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
	if lb := d.getSyncedLabels(); lb != nil {
		bsize = lb.BlockSize().(dvid.Point3d)
		return bsize
	}
	if lv := d.GetSyncedLabelvol(); lv != nil {
		bsize = lv.BlockSize
		return bsize
	}
	bsize = dvid.Point3d{DefaultBlockSize, DefaultBlockSize, DefaultBlockSize}
	return bsize
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

type labelType interface {
	GetLabelAtPoint(dvid.VersionID, dvid.Point) (uint64, error)
	GetLabelBytes(dvid.VersionID, dvid.ChunkPoint3d) ([]byte, error)
	BlockSize() dvid.Point
	DataName() dvid.InstanceName
}

type labelPointType interface {
	GetLabelPoints(v dvid.VersionID, pts []dvid.Point3d, scale uint8, useSupervoxels bool) ([]uint64, error)
}

type supervoxelType interface {
	GetPointsInSupervoxels(v dvid.VersionID, pts []dvid.Point3d, supervoxels []uint64) ([]bool, error)
	BlockSize() dvid.Point
	DataName() dvid.InstanceName
}

func (d *Data) getSyncedLabels() labelType {
	for dataUUID := range d.SyncedData() {
		source0, err := labelmap.GetByDataUUID(dataUUID)
		if err == nil {
			return source0
		}
		source1, err := labelarray.GetByDataUUID(dataUUID)
		if err == nil {
			return source1
		}
		source2, err := labelblk.GetByDataUUID(dataUUID)
		if err == nil {
			return source2
		}
	}
	return nil
}

func (d *Data) getSyncedSupervoxels() supervoxelType {
	for dataUUID := range d.SyncedData() {
		source0, err := labelmap.GetByDataUUID(dataUUID)
		if err == nil {
			return source0
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
	blockE := make(map[dvid.IZYXString]Elements)
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
func (d *Data) deleteElementInTags(ctx *datastore.VersionedCtx, batch storage.Batch, pt dvid.Point3d, tags []Tag) error {
	for _, tag := range tags {
		// Get the elements in tag.
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		elems, err := getElementsNR(ctx, tk)
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
			continue
		}

		// Delete them from high index to low index due while reusing slice.
		for i := len(toDel) - 1; i >= 0; i-- {
			d := toDel[i]
			elems[d] = elems[len(elems)-1]
			elems[len(elems)-1] = ElementNR{}
			elems = elems[:len(elems)-1]
		}

		// Save the tag.
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) deleteElementInLabel(ctx *datastore.VersionedCtx, batch storage.Batch, pt dvid.Point3d) error {
	labelData := d.getSyncedLabels()
	if labelData == nil {
		return nil // no synced labels
	}
	label, err := labelData.GetLabelAtPoint(ctx.VersionID(), pt)
	if err != nil {
		return err
	}
	tk := NewLabelTKey(label)
	elems, err := getElementsNR(ctx, tk)
	if err != nil {
		return fmt.Errorf("err getting elements for label %d: %v", label, err)
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
		dvid.Errorf("Deleted point %s had label %d (from synced instance %q) but was not found in annotation %q for that label\n", pt, label, labelData.DataName(), d.DataName())
		return nil
	}

	// Delete them from high index to low index due while reusing slice.
	for i := len(toDel) - 1; i >= 0; i-- {
		d := toDel[i]
		elems[d] = elems[len(elems)-1]
		elems[len(elems)-1] = ElementNR{}
		elems = elems[:len(elems)-1]
	}

	// Put the modified list of elements
	if err := putBatchElements(batch, tk, elems); err != nil {
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
func (d *Data) deleteElementInRelationships(ctx *datastore.VersionedCtx, batch storage.Batch, pt dvid.Point3d, rels []Relationship) error {
	blockSize := d.blockSize()
	for _, rel := range rels {
		// Get the block elements containing the related element.
		bcoord := rel.To.Chunk(blockSize).(dvid.ChunkPoint3d)
		tk := NewBlockTKey(bcoord)
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
	return nil
}

// move all reference to given element point in the slice of tags.
// This is private method and assumes outer locking.
func (d *Data) moveElementInTags(ctx *datastore.VersionedCtx, batch storage.Batch, from, to dvid.Point3d, tags []Tag) error {
	for _, tag := range tags {
		// Get the elements in tag.
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		elems, err := getElementsNR(ctx, tk)
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
	return nil
}

func (d *Data) moveElementInLabels(ctx *datastore.VersionedCtx, batch storage.Batch, from, to dvid.Point3d, moved ElementNR) error {
	labelData := d.getSyncedLabels()
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

	var delta DeltaModifyElements
	if oldLabel != 0 {
		tk := NewLabelTKey(oldLabel)
		elems, err := getElementsNR(ctx, tk)
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
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for label %d: %v", newLabel, err)
		}
		elems.add(ElementsNR{moved})
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

	return nil
}

// move all reference to given element point in the related points in different blocks.
// This is private method and assumes outer locking as well as current "from" block already being modified,
// including relationships.
func (d *Data) moveElementInRelationships(ctx *datastore.VersionedCtx, batch storage.Batch, from, to dvid.Point3d, rels []Relationship) error {
	blockSize := d.blockSize()
	fromBlockCoord := from.Chunk(blockSize).(dvid.ChunkPoint3d)

	// Get list of blocks with related points.
	relBlocks := make(map[dvid.IZYXString]struct{})
	for _, rel := range rels {
		bcoord := rel.To.Chunk(blockSize).(dvid.ChunkPoint3d)
		if bcoord.Equals(fromBlockCoord) {
			continue // relationships are almoved in from block
		}
		relBlocks[bcoord.ToIZYXString()] = struct{}{}
	}

	// Alter the moved points in those related blocks.
	for izyxstr := range relBlocks {
		bcoord, err := izyxstr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(bcoord)
		elems, err := getElements(ctx, tk)
		if err != nil {
			return err
		}

		// Move element in related element.
		if _, changed := elems.move(from, to, false); !changed {
			dvid.Errorf("Unable to find moved element %s in related element @ block %s:\n%v\n", from, bcoord, elems)
			continue
		}

		// Save the block elements.
		if err := putBatchElements(batch, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) modifyElements(ctx *datastore.VersionedCtx, batch storage.Batch, tk storage.TKey, toAdd Elements) error {
	storeE, err := getElements(ctx, tk)
	if err != nil {
		return err
	}
	if storeE != nil {
		storeE.add(toAdd)
	} else {
		storeE = toAdd
	}
	return putBatchElements(batch, tk, storeE)
}

// stores synaptic elements arranged by block, replacing any
// elements at same position.
func (d *Data) storeBlockElements(ctx *datastore.VersionedCtx, batch storage.Batch, be map[dvid.IZYXString]Elements) error {
	for izyxStr, elems := range be {
		bcoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		// Modify the block annotations
		tk := NewBlockTKey(bcoord)
		if err := d.modifyElements(ctx, batch, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

// returns label elements with relationships for block elements, using
// specialized point requests if available (e.g., labelmap sync)
func (d *Data) getLabelElements(v dvid.VersionID, elems Elements) (labelElems LabelElements, err error) {
	labelData := d.getSyncedLabels()
	if labelData == nil {
		dvid.Errorf("No synced labels for annotation %q, skipping label-aware denormalization\n", d.DataName())
		return
	}
	labelPointData, ok := labelData.(labelPointType)
	labelElems = LabelElements{}
	if ok {
		pts := make([]dvid.Point3d, len(elems))
		for i, elem := range elems {
			pts[i] = elem.Pos
		}
		var labels []uint64
		labels, err = labelPointData.GetLabelPoints(v, pts, 0, false)
		if err != nil {
			return
		}
		for i, elem := range elems {
			if labels[i] != 0 {
				labelElems.add(labels[i], elem.ElementNR)
			}
		}
	} else {
		blockSize := d.blockSize()
		bX := blockSize[0] * 8
		bY := blockSize[1] * bX
		blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * 8)

		blockElems := make(map[dvid.IZYXString]Elements)
		for _, elem := range elems {
			izyxStr := elem.Pos.ToBlockIZYXString(blockSize)
			be := blockElems[izyxStr]
			be = append(be, elem)
			blockElems[izyxStr] = be
		}
		for izyxStr, elems := range blockElems {
			var bcoord dvid.ChunkPoint3d
			bcoord, err = izyxStr.ToChunkPoint3d()
			if err != nil {
				return
			}
			var labels []byte
			labels, err = labelData.GetLabelBytes(v, bcoord)
			if err != nil {
				return
			}
			if len(labels) == 0 {
				continue
			}
			if len(labels) != blockBytes {
				err = fmt.Errorf("expected %d bytes in %q label block, got %d instead.  aborting", blockBytes, d.DataName(), len(labels))
				return
			}

			// Group annotations by label
			for _, elem := range elems {
				pt := elem.Pos.Point3dInChunk(blockSize)
				i := pt[2]*bY + pt[1]*bX + pt[0]*8
				label := binary.LittleEndian.Uint64(labels[i : i+8])
				if label != 0 {
					labelElems.add(label, elem.ElementNR)
				}
			}
		}
	}
	return
}

// returns label elements without relationships, using specialized point
// requests if available or falling back to reading label blocks.
func (d *Data) getLabelElementsNR(labelData labelType, v dvid.VersionID, elems ElementsNR) (labelElems LabelElements, err error) {
	labelPointData, pointOK := labelData.(labelPointType)
	labelElems = LabelElements{}
	if pointOK {
		pts := make([]dvid.Point3d, len(elems))
		for i, elem := range elems {
			pts[i] = elem.Pos
		}
		var labels []uint64
		labels, err = labelPointData.GetLabelPoints(v, pts, 0, false)
		if err != nil {
			return
		}
		for i, elem := range elems {
			if labels[i] != 0 {
				labelElems.add(labels[i], elem)
			}
		}
		return
	}

	blockSize := d.blockSize()
	bX := blockSize[0] * 8
	bY := blockSize[1] * bX
	blockBytes := int(blockSize[0] * blockSize[1] * blockSize[2] * 8)

	blockElems := make(map[dvid.IZYXString]ElementsNR)
	for _, elem := range elems {
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)
		be := blockElems[izyxStr]
		be = append(be, elem)
		blockElems[izyxStr] = be
	}
	for izyxStr, elems := range blockElems {
		var bcoord dvid.ChunkPoint3d
		bcoord, err = izyxStr.ToChunkPoint3d()
		if err != nil {
			return
		}
		var labels []byte
		labels, err = labelData.GetLabelBytes(v, bcoord)
		if err != nil {
			return
		}
		if len(labels) == 0 {
			continue
		}
		if len(labels) != blockBytes {
			err = fmt.Errorf("expected %d bytes in %q label block, got %d instead.  aborting", blockBytes, d.DataName(), len(labels))
			return
		}

		// Group annotations by label
		for _, elem := range elems {
			pt := elem.Pos.Point3dInChunk(blockSize)
			i := pt[2]*bY + pt[1]*bX + pt[0]*8
			label := binary.LittleEndian.Uint64(labels[i : i+8])
			if label != 0 {
				labelElems.add(label, elem)
			}
		}
	}
	return
}

// lookup labels for given elements and add them to label element map
func (d *Data) addLabelElements(v dvid.VersionID, labelE LabelElements, bcoord dvid.ChunkPoint3d, elems Elements) (int, error) {
	le, err := d.getLabelElements(v, elems)
	if err != nil {
		return 0, err
	}

	// Add annotations by label
	var nonzeroElems int
	for label, elems := range le {
		for _, elem := range elems {
			if label != 0 {
				labelE.add(label, elem)
				nonzeroElems++
			} else {
				dvid.Infof("Annotation %s was at voxel with label 0\n", elem.Pos)
			}
		}
	}
	return nonzeroElems, nil
}

// stores synaptic elements arranged by label, replacing any
// elements at same position.
func (d *Data) storeLabelElements(ctx *datastore.VersionedCtx, batch storage.Batch, elems Elements) error {
	toAdd, err := d.getLabelElements(ctx.VersionID(), elems)
	if err != nil {
		return err
	}
	if len(toAdd) == 0 {
		return nil
	}

	// Store all the added annotations to the appropriate labels.
	var delta DeltaModifyElements
	for label, additions := range toAdd {
		tk := NewLabelTKey(label)
		elems, err := getElementsNR(ctx, tk)
		if err != nil {
			return fmt.Errorf("err getting elements for label %d: %v", label, err)
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
			return fmt.Errorf("couldn't serialize label %d annotations in instance %q: %v", label, d.DataName(), err)
		}
	}

	// Notify any subscribers of label annotation changes.
	evt := datastore.SyncEvent{Data: d.DataUUID(), Event: ModifyElementsEvent}
	msg := datastore.SyncMessage{Event: ModifyElementsEvent, Version: ctx.VersionID(), Delta: delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return err
	}

	return nil
}

// stores synaptic elements arranged by tag, replacing any
// elements at same position.
func (d *Data) storeTagElements(ctx *datastore.VersionedCtx, batch storage.Batch, te map[Tag]Elements) error {
	for tag, elems := range te {
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		if err := d.modifyElements(ctx, batch, tk, elems); err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) modifyTagElements(ctx *datastore.VersionedCtx, batch storage.Batch, tagDelta map[Tag]tagDeltaT) error {
	for tag, td := range tagDelta {
		tk, err := NewTagTKey(tag)
		if err != nil {
			return err
		}
		tagElems, err := getElementsNR(ctx, tk)
		if err != nil {
			return err
		}
		if len(td.add) != 0 {
			if tagElems != nil {
				tagElems.add(td.add)
			} else {
				tagElems = make(ElementsNR, len(td.add))
				copy(tagElems, td.add)
			}
		}
		// Note all elements to be deleted.
		var toDel []int
		for i, elem := range tagElems {
			zyx := string(elem.Pos.ToZYXBytes())
			if _, found := td.erase[zyx]; found {
				toDel = append(toDel, i)
			}
		}
		if len(toDel) != 0 {
			// Delete them from high index to low index due while reusing slice.
			for i := len(toDel) - 1; i >= 0; i-- {
				d := toDel[i]
				tagElems[d] = tagElems[len(tagElems)-1]
				tagElems[len(tagElems)-1] = ElementNR{}
				tagElems = tagElems[:len(tagElems)-1]
			}
		}

		// Save the tag.
		if err := putBatchElements(batch, tk, tagElems); err != nil {
			return err
		}
	}
	return nil
}

// ProcessLabelAnnotations will pass all annotations, label by label, to the given function.
func (d *Data) ProcessLabelAnnotations(v dvid.VersionID, f func(label uint64, elems ElementsNR)) error {
	minTKey := storage.MinTKey(keyLabel)
	maxTKey := storage.MaxTKey(keyLabel)

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("annotation %q had error initializing store: %v", d.DataName(), err)
	}
	ctx := datastore.NewVersionedCtx(d, v)
	err = store.ProcessRange(ctx, minTKey, maxTKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil {
			return fmt.Errorf("received nil chunk in reload for data %q", d.DataName())
		}
		if c.V == nil {
			return nil
		}
		label, err := DecodeLabelTKey(c.K)
		if err != nil {
			return fmt.Errorf("couldn't decode label key %v for data %q", c.K, d.DataName())
		}

		var elems ElementsNR
		if err := json.Unmarshal(c.V, &elems); err != nil {
			return fmt.Errorf("couldn't unmarshal elements for label %d of data %q", label, d.DataName())
		}
		if len(elems) == 0 {
			return nil
		}
		f(label, elems)
		return nil
	})
	if err != nil {
		return fmt.Errorf("Unable to get label-based annotations for data %q: %v\n", d.DataName(), err)
	}
	return nil
}

// GetLabelJSON returns JSON for synapse elements in a given label.
func (d *Data) GetLabelJSON(ctx *datastore.VersionedCtx, label uint64, addRels bool) ([]byte, error) {
	// d.RLock()
	// defer d.RUnlock()

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

// GetTagJSON returns JSON for synapse elements in a given tag.
func (d *Data) GetTagJSON(ctx *datastore.VersionedCtx, tag Tag, addRels bool) (jsonBytes []byte, err error) {
	// d.RLock()
	// defer d.RUnlock()

	var tk storage.TKey
	tk, err = NewTagTKey(tag)
	if err != nil {
		return
	}
	var elems interface{}
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

// StreamAll returns all elements for this data instance.
func (d *Data) StreamAll(ctx *datastore.VersionedCtx, w http.ResponseWriter) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	minTKey, maxTKey := BlockTKeyRange()

	// d.RLock()
	// defer d.RUnlock()

	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}
	numBlocks := 0
	err = store.ProcessRange(ctx, minTKey, maxTKey, nil, func(chunk *storage.Chunk) error {
		bcoord, err := DecodeBlockTKey(chunk.K)
		if err != nil {
			return err
		}
		if len(chunk.V) == 0 {
			return nil
		}
		s := fmt.Sprintf(`"%d,%d,%d":`, bcoord[0], bcoord[1], bcoord[2])
		if numBlocks > 0 {
			s = "," + s
		}
		if _, err := w.Write([]byte(s)); err != nil {
			return err
		}
		if _, err := w.Write(chunk.V); err != nil {
			return err
		}
		numBlocks++
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte("}")); err != nil {
		return err
	}
	dvid.Infof("Returned %d blocks worth of elements in a /all-elements request\n", numBlocks)
	return nil
}

// StreamBlocks returns synapse elements for a given subvolume of image space.
func (d *Data) StreamBlocks(ctx *datastore.VersionedCtx, w http.ResponseWriter, ext *dvid.Extents3d) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}

	// d.RLock()
	// defer d.RUnlock()

	blockSize := d.blockSize()
	begBlockCoord, endBlockCoord := ext.BlockRange(blockSize)

	// Check if we should use single range query based on suggested BlockSize Tag if present
	var useAllScan bool
	tags := d.Tags()
	if scanStr, found := tags["ScanAllForBlocks"]; found {
		useAllScan = (strings.ToLower(scanStr) == "true")
	}
	/**
	} else {
		// if there would be too many range requests, just get all
		dz := endBlockCoord[2] - begBlockCoord[2]
		dy := endBlockCoord[1] - begBlockCoord[1]
		if dy*dz > 100 {
			useAllScan = true
		}
	}
	**/

	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}
	numBlocks := 0
	if useAllScan {
		minTKey, maxTKey := BlockTKeyZRange(begBlockCoord, endBlockCoord)
		err = store.ProcessRange(ctx, minTKey, maxTKey, nil, func(chunk *storage.Chunk) error {
			bcoord, err := DecodeBlockTKey(chunk.K)
			if err != nil {
				return err
			}
			if !bcoord.WithinChunkBoundingBox(begBlockCoord, endBlockCoord) || len(chunk.V) == 0 {
				return nil
			}
			s := fmt.Sprintf(`"%d,%d,%d":`, bcoord[0], bcoord[1], bcoord[2])
			if numBlocks > 0 {
				s = "," + s
			}
			if _, err := w.Write([]byte(s)); err != nil {
				return err
			}
			if _, err := w.Write(chunk.V); err != nil {
				return err
			}
			numBlocks++
			return nil
		})
		if err != nil {
			return err
		}
		dvid.Infof("Using single range scan\n")
	} else {
		for blockZ := begBlockCoord[2]; blockZ <= endBlockCoord[2]; blockZ++ {
			for blockY := begBlockCoord[1]; blockY <= endBlockCoord[1]; blockY++ {
				begTKey := NewBlockTKey(dvid.ChunkPoint3d{begBlockCoord[0], blockY, blockZ})
				endTKey := NewBlockTKey(dvid.ChunkPoint3d{endBlockCoord[0], blockY, blockZ})
				err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(chunk *storage.Chunk) error {
					bcoord, err := DecodeBlockTKey(chunk.K)
					if err != nil {
						return err
					}
					if len(chunk.V) == 0 {
						return nil
					}
					s := fmt.Sprintf(`"%d,%d,%d":`, bcoord[0], bcoord[1], bcoord[2])
					if numBlocks > 0 {
						s = "," + s
					}
					if _, err := w.Write([]byte(s)); err != nil {
						return err
					}
					if _, err := w.Write(chunk.V); err != nil {
						return err
					}
					numBlocks++
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
	}
	if _, err := w.Write([]byte("}")); err != nil {
		return err
	}
	dvid.Infof("Returned %d blocks of elements in a /blocks with bounds %s -> %s (used single range query: %t)\n",
		numBlocks, begBlockCoord, endBlockCoord, useAllScan)
	return nil
}

// GetRegionSynapses returns synapse elements for a given subvolume of image space.
func (d *Data) GetRegionSynapses(ctx *datastore.VersionedCtx, ext *dvid.Extents3d) (Elements, error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// Setup block bounds for synapse element query in supplied Z range.
	blockSize := d.blockSize()
	begBlockCoord, endBlockCoord := ext.BlockRange(blockSize)

	// d.RLock()
	// defer d.RUnlock()

	// Iterate through all synapse elements block k/v, making sure the elements are also
	// within the given subvolume.
	var elements Elements
	for blockZ := begBlockCoord[2]; blockZ <= endBlockCoord[2]; blockZ++ {
		for blockY := begBlockCoord[1]; blockY <= endBlockCoord[1]; blockY++ {
			begTKey := NewBlockTKey(dvid.ChunkPoint3d{begBlockCoord[0], blockY, blockZ})
			endTKey := NewBlockTKey(dvid.ChunkPoint3d{endBlockCoord[0], blockY, blockZ})
			err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(chunk *storage.Chunk) error {
				bcoord, err := DecodeBlockTKey(chunk.K)
				if err != nil {
					return err
				}
				if !ext.BlockWithin(blockSize, bcoord) {
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
		}
	}
	return elements, nil
}

// GetROISynapses returns synapse elements for a given ROI.
func (d *Data) GetROISynapses(ctx *datastore.VersionedCtx, roiSpec storage.FilterSpec) (Elements, error) {
	roidata, roiV, roiFound, err := roi.DataByFilter(roiSpec)
	if err != nil {
		return nil, fmt.Errorf("ROI specification was not parsable (%s): %v\n", roiSpec, err)
	}
	if !roiFound {
		return nil, fmt.Errorf("No ROI found that matches specification %q", roiSpec)
	}
	roiSpans, err := roidata.GetSpans(roiV)
	if err != nil {
		return nil, fmt.Errorf("Unable to get ROI spans for %q: %v\n", roiSpec, err)
	}
	if !d.blockSize().Equals(roidata.BlockSize) {
		return nil, fmt.Errorf("/roi endpoint currently requires ROI %q to have same block size as annotation %q", roidata.DataName(), d.DataName())
	}

	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return nil, err
	}

	// d.RLock()
	// defer d.RUnlock()

	var elements Elements
	for _, span := range roiSpans {
		begBlockCoord := dvid.ChunkPoint3d{span[2], span[1], span[0]}
		endBlockCoord := dvid.ChunkPoint3d{span[3], span[1], span[0]}
		begTKey := NewBlockTKey(begBlockCoord)
		endTKey := NewBlockTKey(endBlockCoord)
		err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(chunk *storage.Chunk) error {
			var blockElems Elements
			if err := json.Unmarshal(chunk.V, &blockElems); err != nil {
				return err
			}
			elements = append(elements, blockElems...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error retrieving annotations from %s -> %s: %v", begBlockCoord, endBlockCoord, err)
		}
	}
	return elements, nil
}

type blockList map[string]Elements

// StoreBlocks performs a synchronous store of synapses in JSON format, not
// returning until the data blocks are complete.
func (d *Data) StoreBlocks(ctx *datastore.VersionedCtx, r io.Reader, kafkaOff bool) (numBlocks int, err error) {
	jsonBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	var blocks blockList
	if err := json.Unmarshal(jsonBytes, &blocks); err != nil {
		return 0, err
	}

	// d.Lock()
	// defer d.Unlock()

	// Do modifications under a batch.
	store, err := d.KVStore()
	if err != nil {
		return 0, err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return 0, fmt.Errorf("data type annotation requires batch-enabled store, which %q is not", store)
	}
	batch := batcher.NewBatch(ctx)

	var blockX, blockY, blockZ int32
	for key, elems := range blocks {
		_, err := fmt.Sscanf(key, "%d,%d,%d", &blockX, &blockY, &blockZ)
		if err != nil {
			return 0, err
		}
		blockCoord := dvid.ChunkPoint3d{blockX, blockY, blockZ}
		tk := NewBlockTKey(blockCoord)
		if err := putBatchElements(batch, tk, elems); err != nil {
			return 0, err
		}
	}

	if !kafkaOff {
		// store synapse info into blob store for kakfa reference
		var postRef string
		if postRef, err = d.PutBlob(jsonBytes); err != nil {
			dvid.Errorf("storing block posted synapse data %q to kafka: %v", d.DataName(), err)
		}

		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":    "blocks-post",
			"DataRef":   postRef,
			"UUID":      string(versionuuid),
			"Timestamp": time.Now().String(),
		}
		if ctx.User != "" {
			msginfo["User"] = ctx.User
		}
		jsonmsg, err := json.Marshal(msginfo)
		if err != nil {
			dvid.Errorf("error marshaling JSON for annotations %q blocks post: %v\n", d.DataName(), err)
		} else if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending block post op to kafka: %v\n", err)
		}
	}

	return len(blocks), batch.Commit()
}

// StoreElements performs a synchronous store of synapses in JSON format, not
// returning until the data and its denormalizations are complete.
func (d *Data) StoreElements(ctx *datastore.VersionedCtx, r io.Reader, kafkaOff bool) error {
	jsonBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	var elems Elements
	if err := json.Unmarshal(jsonBytes, &elems); err != nil {
		return err
	}

	// d.Lock()
	// defer d.Unlock()

	dvid.Infof("%d annotation elements received via POST\n", len(elems))

	blockSize := d.blockSize()
	addToBlock := make(map[dvid.IZYXString]Elements)
	tagDelta := make(map[Tag]tagDeltaT)

	// Organize added elements into blocks
	for _, elem := range elems {
		// Get block coord for this element.
		izyxStr := elem.Pos.ToBlockIZYXString(blockSize)

		// Append to block
		be := addToBlock[izyxStr]
		be = append(be, elem)
		addToBlock[izyxStr] = be
	}

	// Find current elements under the blocks.
	for izyxStr, elems := range addToBlock {
		bcoord, err := izyxStr.ToChunkPoint3d()
		if err != nil {
			return err
		}
		tk := NewBlockTKey(bcoord)
		curBlockE, err := getElements(ctx, tk)
		if err != nil {
			return err
		}
		addTagDelta(elems, curBlockE, tagDelta)
	}

	// Do modifications under a batch.
	store, err := d.KVStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("data type annotation requires batch-enabled store, which %q is not", store)
	}
	batch := batcher.NewBatch(ctx)

	// Store the new block elements
	if err := d.storeBlockElements(ctx, batch, addToBlock); err != nil {
		return err
	}

	// Store new elements among label denormalizations
	if err := d.storeLabelElements(ctx, batch, elems); err != nil {
		return err
	}

	// Store the new tag elements
	if err := d.modifyTagElements(ctx, batch, tagDelta); err != nil {
		return err
	}

	if !kafkaOff {
		// store synapse info into blob store for kakfa reference
		var postRef string
		if postRef, err = d.PutBlob(jsonBytes); err != nil {
			dvid.Errorf("storing posted synapse data %q to kafka: %v", d.DataName(), err)
		}

		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":    "element-post",
			"DataRef":   postRef,
			"UUID":      string(versionuuid),
			"Timestamp": time.Now().String(),
		}
		if ctx.User != "" {
			msginfo["User"] = ctx.User
		}
		jsonmsg, err := json.Marshal(msginfo)
		if err != nil {
			dvid.Errorf("error marshaling JSON for annotations %q element post: %v\n", d.DataName(), err)
		} else if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending move element op to kafka: %v\n", err)
		}
	}

	return batch.Commit()
}

func (d *Data) DeleteElement(ctx *datastore.VersionedCtx, pt dvid.Point3d, kafkaOff bool) error {
	// Get from block key
	blockSize := d.blockSize()
	bcoord := pt.Chunk(blockSize).(dvid.ChunkPoint3d)
	tk := NewBlockTKey(bcoord)

	// d.Lock()
	// defer d.Unlock()

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

	// Alter all stored versions of this annotation using a batch.
	store, err := d.KVStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

	// Delete in label key
	if err := d.deleteElementInLabel(ctx, batch, deleted.Pos); err != nil {
		return err
	}

	// Delete element in any tags
	if err := d.deleteElementInTags(ctx, batch, deleted.Pos, deleted.Tags); err != nil {
		return err
	}

	// Modify any reference in relationships
	if err := d.deleteElementInRelationships(ctx, batch, deleted.Pos, deleted.Rels); err != nil {
		return err
	}

	if !kafkaOff {
		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":    "element-delete",
			"Point":     pt,
			"UUID":      string(versionuuid),
			"Timestamp": time.Now().String(),
		}
		if ctx.User != "" {
			msginfo["User"] = ctx.User
		}
		jsonmsg, err := json.Marshal(msginfo)
		if err != nil {
			dvid.Errorf("error marshaling JSON for annotations %q element delete: %v\n", d.DataName(), err)
		} else if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending delete element op to kafka: %v\n", err)
		}
	}

	return batch.Commit()
}

func (d *Data) MoveElement(ctx *datastore.VersionedCtx, from, to dvid.Point3d, kafkaOff bool) error {
	// Calc block keys
	blockSize := d.blockSize()
	fromCoord := from.Chunk(blockSize).(dvid.ChunkPoint3d)
	fromTk := NewBlockTKey(fromCoord)

	toCoord := to.Chunk(blockSize).(dvid.ChunkPoint3d)
	toTk := NewBlockTKey(toCoord)

	// d.Lock()
	// defer d.Unlock()

	// Alter all stored versions of this annotation using a batch.
	store, err := d.KVStore()
	if err != nil {
		return err
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type annotation requires batch-enabled store, which %q is not\n", store)
	}
	batch := batcher.NewBatch(ctx)

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
	dvid.Infof("moved element %v from %s -> %s\n", *moved, fromCoord, toCoord)

	if err := putBatchElements(batch, fromTk, fromElems); err != nil {
		return err
	}

	// If we've moved blocks, add the element in new place.
	if deleteElement {
		toElems, err := getElements(ctx, toTk)
		if err != nil {
			return err
		}
		toElems.add(Elements{*moved})

		if err := putBatchElements(batch, toTk, toElems); err != nil {
			return err
		}
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	if !kafkaOff {
		versionuuid, _ := datastore.UUIDFromVersion(ctx.VersionID())
		msginfo := map[string]interface{}{
			"Action":    "element-move",
			"From":      from,
			"To":        to,
			"UUID":      string(versionuuid),
			"Timestamp": time.Now().String(),
		}
		if ctx.User != "" {
			msginfo["User"] = ctx.User
		}
		jsonmsg, err := json.Marshal(msginfo)
		if err != nil {
			dvid.Errorf("error marshaling JSON for annotations %q element move: %v\n", d.DataName(), err)
		} else if err = d.PublishKafkaMsg(jsonmsg); err != nil {
			dvid.Errorf("error on sending move element op to kafka: %v\n", err)
		}
	}

	batch = batcher.NewBatch(ctx)

	// Move in label key
	if err := d.moveElementInLabels(ctx, batch, from, to, moved.ElementNR); err != nil {
		return err
	}

	// Move element in any tags
	if err := d.moveElementInTags(ctx, batch, from, to, moved.Tags); err != nil {
		return err
	}

	// Move any reference in relationships
	if err := d.moveElementInRelationships(ctx, batch, from, to, moved.Rels); err != nil {
		return err
	}

	return batch.Commit()
}

// GetByDataUUID returns a pointer to annotation data given a data UUID.
func GetByDataUUID(dataUUID dvid.UUID) (*Data, error) {
	source, err := datastore.GetDataByDataUUID(dataUUID)
	if err != nil {
		return nil, err
	}
	data, ok := source.(*Data)
	if !ok {
		return nil, fmt.Errorf("instance %q is not an annotation datatype", source.DataName())
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
		return nil, fmt.Errorf("instance %q is not an annotation datatype", name)
	}
	return data, nil
}

// --- datastore.DataService interface ---------

func (d *Data) Help() string {
	return helpMessage
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
