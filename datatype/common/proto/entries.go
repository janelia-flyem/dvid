// This file holds the constants for entry types, necessary during deserialization
// of streams of entries.

package proto

const (
	UnknownType = iota
	SplitOpType
	MergeOpType
	MutationCompleteType
	AffinityType
	MappingOpType
	SupervoxelSplitType
	CleaveOpType
	RenumberOpType
)
