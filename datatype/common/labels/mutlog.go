/* Handles writing to mutation log for ops on labels. */

package labels

import (
	"sync"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// LogSplit logs the split of a set of voxels from the underlying label.
func LogSplit(d dvid.Data, v dvid.VersionID, op SplitOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeSplit(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.SplitOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogSupervoxelSplit logs the split of a supervoxel into two separate supervoxels.
func LogSupervoxelSplit(d dvid.Data, v dvid.VersionID, op SplitSupervoxelOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	pop := &proto.SupervoxelSplitOp{
		Mutid:       op.MutID,
		Supervoxel:  op.Supervoxel,
		Splitlabel:  op.SplitSupervoxel,
		Remainlabel: op.RemainSupervoxel,
	}
	serialization, err := pb.Marshal(pop)

	if err != nil {
		return err
	}

	msg := storage.LogMessage{EntryType: proto.SupervoxelSplitType, Data: serialization}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMerge logs the merge of supervoxels to a label.
func LogMerge(d dvid.Data, v dvid.VersionID, op MergeOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeMerge(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.MergeOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogCleave logs the cleave of supervoxels to a label.
func LogCleave(d dvid.Data, v dvid.VersionID, op CleaveOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeCleave(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.CleaveOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMapping logs the mapping of supervoxels to a label.
func LogMapping(d dvid.Data, v dvid.VersionID, op MappingOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := op.Marshal()
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.MappingOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMappings logs a collection of mapping operations to a UUID.
func LogMappings(d dvid.Data, v dvid.VersionID, ops proto.MappingOps) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	for _, op := range ops.Mappings {
		data, err := pb.Marshal(op)
		if err != nil {
			return err
		}
		msg := storage.LogMessage{EntryType: proto.MappingOpType, Data: data}
		if err := log.Append(d.DataUUID(), uuid, msg); err != nil {
			return err
		}
	}
	return nil
}

func ReadMappingLog(d dvid.Data, v dvid.VersionID) ([]MappingOp, error) {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return nil, err
	}
	logreadable, ok := d.(storage.LogReadable)
	if !ok {
		return nil, nil
	}
	rl := logreadable.GetReadLog()
	if rl == nil {
		return nil, nil
	}
	msgs, err := rl.ReadAll(d.DataUUID(), uuid)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	mappingOps := make([]MappingOp, len(msgs))
	var numMappings int
	for i, msg := range msgs {
		if msg.EntryType != proto.MappingOpType {
			continue
		}

		var op proto.MappingOp
		if err := pb.Unmarshal(msg.Data, &op); err != nil {
			return nil, err
		}
		mappingOps[i].Mapped = op.GetMapped()
		original := op.GetOriginal()
		mappingOps[i].Original = make(Set, len(original))
		for _, label := range original {
			mappingOps[i].Original[label] = struct{}{}
		}
		numMappings++
	}
	mappingOps = mappingOps[:numMappings]
	return mappingOps, nil
}

func StreamLog(d dvid.Data, v dvid.VersionID, ch chan storage.LogMessage, wg *sync.WaitGroup) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logreadable, ok := d.(storage.LogReadable)
	if !ok {
		close(ch)
		return nil
	}
	rl := logreadable.GetReadLog()
	if rl == nil {
		close(ch)
		return nil
	}
	return rl.StreamAll(d.DataUUID(), uuid, ch, wg)
}

func LogAffinity(d dvid.Data, v dvid.VersionID, aff Affinity) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeAffinity(aff)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.AffinityType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

func serializeSplit(op SplitOp) (serialization []byte, err error) {
	rlesBytes, err := op.RLEs.MarshalBinary()
	if err != nil {
		return nil, err
	}
	svsplits := make(map[uint64]*proto.SVSplit, len(op.SplitMap))
	for supervoxel, split := range op.SplitMap {
		svsplit := new(proto.SVSplit)
		svsplit.Splitlabel = split.Split
		svsplit.Remainlabel = split.Remain
		svsplits[supervoxel] = svsplit
	}
	pop := &proto.SplitOp{
		Mutid:    op.MutID,
		Target:   op.Target,
		Newlabel: op.NewLabel,
		Coarse:   op.Coarse,
		Rles:     rlesBytes,
		Svsplits: svsplits,
	}
	return pb.Marshal(pop)
}

func serializeMerge(op MergeOp) (serialization []byte, err error) {
	merged := make([]uint64, len(op.Merged))
	var i int
	for label := range op.Merged {
		merged[i] = label
		i++
	}
	pop := &proto.MergeOp{
		Mutid:  op.MutID,
		Target: op.Target,
		Merged: merged,
	}
	return pb.Marshal(pop)
}

func serializeCleave(op CleaveOp) (serialization []byte, err error) {
	pop := &proto.CleaveOp{
		Mutid:        op.MutID,
		Target:       op.Target,
		Cleavedlabel: op.CleavedLabel,
		Cleaved:      op.CleavedSupervoxels,
	}
	return pb.Marshal(pop)
}

func serializeAffinity(aff Affinity) (serialization []byte, err error) {
	pop := &proto.Affinity{
		Label1: aff.Label1,
		Label2: aff.Label2,
		Value:  aff.Value,
	}
	return pb.Marshal(pop)
}
