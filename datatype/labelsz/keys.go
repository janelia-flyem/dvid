/*
	This file supports keyspaces for the labelsz data type.
*/

package labelsz

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/annotation"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// keyUnknown should never be used and is a check for corrupt or incorrectly set keys
	keyUnknown storage.TKeyClass = iota

	// reserved type-specific key for metadata
	keyProperties = datastore.PropertyTKeyClass

	// key is index type + size + label
	keyTypeSizeLabel = 97

	// key is index type + label, with value equal to size, necessary to delete old indices.
	keyTypeLabel = 98
)

// DescribeTKeyClass returns a string explanation of what a particular TKeyClass
// is used for.  Implements the datastore.TKeyClassDescriber interface.
func (d *Data) DescribeTKeyClass(tkc storage.TKeyClass) string {
	switch tkc {
	case keyTypeLabel:
		return "labelsz index type + label key"
	case keyTypeSizeLabel:
		return "labelsz index type + size + label key"
	default:
	}
	return "unknown labelsz key"
}

const (
	UnknownIndex IndexType = iota
	PostSyn                // Post-synaptic element
	PreSyn                 // Pre-synaptic element
	Gap                    // Gap junction
	Note                   // A note or bookmark with some description
	AllSyn                 // PostSyn, PreSyn, or Gap
	Voxels                 // Number of voxels
)

// IndexType gives the type of index.
type IndexType uint8

func (i IndexType) String() string {
	switch i {
	case PostSyn:
		return "PostSyn"
	case PreSyn:
		return "PreSyn"
	case Gap:
		return "Gap"
	case Note:
		return "Note"
	case AllSyn:
		return "AllSyn"
	case Voxels:
		return "Voxels"
	default:
		return "Unknown IndexType"
	}
}

// StringToIndexType converts a string to an IndexType
func StringToIndexType(s string) IndexType {
	switch s {
	case "PostSyn":
		return PostSyn
	case "PreSyn":
		return PreSyn
	case "Gap":
		return Gap
	case "Note":
		return Note
	case "AllSyn":
		return AllSyn
	case "Voxels":
		return Voxels
	default:
		return UnknownIndex
	}
}

func elementToIndexType(e annotation.ElementType) (i IndexType) {
	switch e {
	case annotation.PostSyn:
		i = PostSyn
	case annotation.PreSyn:
		i = PreSyn
	case annotation.Gap:
		i = Gap
	case annotation.Note:
		i = Note
	default:
		i = UnknownIndex
	}
	return
}

// can be used as map index and holds serialized (IndexType, Label)
type indexedLabel string

func (il indexedLabel) String() string {
	i := IndexType(il[0])
	labelBytes := []byte(il[1:])
	label := binary.BigEndian.Uint64(labelBytes)
	return fmt.Sprintf("[label %d, index %s]", label, i.String())
}

func toIndexedLabel(e annotation.ElementPos) indexedLabel {
	return newIndexedLabel(elementToIndexType(e.Kind), e.Label)
}

func newIndexedLabel(i IndexType, label uint64) indexedLabel {
	buf := make([]byte, 9)
	buf[0] = byte(i)
	binary.BigEndian.PutUint64(buf[1:], label)
	return indexedLabel(buf)
}

func decodeIndexedLabel(il indexedLabel) (i IndexType, label uint64, err error) {
	if len(il) != 9 {
		err = fmt.Errorf("indexed label %v is supposed to be 9 bytes but is %d bytes", il, len(il))
		return
	}
	i = IndexType(il[0])
	labelBytes := []byte(il[1:])
	label = binary.BigEndian.Uint64(labelBytes)
	return
}

// NewTypeSizeLabelTKey returns a type-specific key for the (index type, size, label) tuple.
func NewTypeSizeLabelTKey(i IndexType, sz uint32, label uint64) storage.TKey {
	// Since we want biggest -> smallest sort and initially only have forward range queries,
	// modify size to make smallest <-> largest.
	rsz := math.MaxUint32 - sz

	buf := make([]byte, 8+1+4)
	buf[0] = byte(i)
	binary.BigEndian.PutUint32(buf[1:5], rsz)
	binary.BigEndian.PutUint64(buf[5:], label)
	return storage.NewTKey(keyTypeSizeLabel, buf)
}

// DecodeTypeSizeLabelTKey decodes a type-specific key into a (index type, size, label) tuple.
func DecodeTypeSizeLabelTKey(tk storage.TKey) (i IndexType, sz uint32, label uint64, err error) {
	var ibytes []byte
	ibytes, err = tk.ClassBytes(keyTypeSizeLabel)
	if err != nil {
		return
	}
	if len(ibytes) != 13 {
		err = fmt.Errorf("labelsz element size type-specific key is wrong size: expected 13, got %d bytes", len(ibytes))
		return
	}
	i = IndexType(ibytes[0])
	sz = math.MaxUint32 - binary.BigEndian.Uint32(ibytes[1:5])
	label = binary.BigEndian.Uint64(ibytes[5:])
	return
}

// NewTypeLabelTKey returns a type-specific key for the (index type, label) tuple.
func NewTypeLabelTKey(i IndexType, label uint64) storage.TKey {
	buf := make([]byte, 1+8)
	buf[0] = byte(i)
	binary.BigEndian.PutUint64(buf[1:], label)
	return storage.NewTKey(keyTypeLabel, buf)
}

// DecodeTypeLabelTKey decodes a type-specific key into a (index type, label) tuple.
func DecodeTypeLabelTKey(tk storage.TKey) (i IndexType, label uint64, err error) {
	var ibytes []byte
	ibytes, err = tk.ClassBytes(keyTypeLabel)
	if err != nil {
		return
	}
	if len(ibytes) != 9 {
		err = fmt.Errorf("labelsz label size type-specific key is wrong size: expected 9, got %d bytes", len(ibytes))
		return
	}
	i = IndexType(ibytes[0])
	label = binary.BigEndian.Uint64(ibytes[1:])
	return
}
