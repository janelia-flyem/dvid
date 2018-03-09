/* Handles writing to mutation log for ops on labels. */

package labels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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
		if err := op.Unmarshal(msg.Data); err != nil {
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

func LogSplit(d dvid.Data, v dvid.VersionID, mutID uint64, op SplitOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.Logable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeSplit(mutID, op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.SplitOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMerge logs the merge of supervoxels to a label.
func LogMerge(d dvid.Data, v dvid.VersionID, mutID uint64, op MergeOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.Logable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeMerge(mutID, op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.MergeOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMapping logs the mapping of supervoxels to a label.
func LogMapping(d dvid.Data, v dvid.VersionID, op MappingOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.Logable)
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
func LogMappings(d dvid.Data, uuid dvid.UUID, ops proto.MappingOps) error {
	logable, ok := d.(storage.Logable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	for _, op := range ops.Mappings {
		data, err := op.Marshal()
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

func LogAffinity(d dvid.Data, v dvid.VersionID, aff Affinity) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.Logable)
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

func serializeSplit(mutID uint64, op SplitOp) (serialization []byte, err error) {
	rlesBytes, err := op.RLEs.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pop := proto.SplitOp{
		Mutid:    mutID,
		Target:   op.Target,
		Newlabel: op.NewLabel,
		Coarse:   op.Coarse,
		Rles:     rlesBytes,
	}
	return pop.Marshal()
}

func serializeMerge(mutID uint64, op MergeOp) (serialization []byte, err error) {
	merged := make([]uint64, len(op.Merged))
	var i int
	for label := range op.Merged {
		merged[i] = label
		i++
	}
	pop := &proto.MergeOp{
		Mutid:  mutID,
		Target: op.Target,
		Merged: merged,
	}
	return pop.Marshal()
}

func serializeAffinity(aff Affinity) (serialization []byte, err error) {
	pop := &proto.Affinity{
		Label1: aff.Label1,
		Label2: aff.Label2,
		Value:  aff.Value,
	}
	return pop.Marshal()
}
