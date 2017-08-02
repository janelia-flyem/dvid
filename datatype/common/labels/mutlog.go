/* Handles writing to mutation log for ops on labels. */

package labels

import (
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

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
	return log.Append(proto.SplitOpType, d.DataUUID(), uuid, data)
}

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
	return log.Append(proto.MergeOpType, d.DataUUID(), uuid, data)
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
	for lbl := range op.Merged {
		merged[i] = lbl
		i++
	}
	pop := &proto.MergeOp{
		Mutid:  mutID,
		Target: op.Target,
		Merged: merged,
	}
	return pop.Marshal()
}
