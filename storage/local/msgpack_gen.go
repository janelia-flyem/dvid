package local

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Binary) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var tmp []byte
		tmp, err = dc.ReadBytes([]byte((*z)))
		(*z) = Binary(tmp)
	}
	if err != nil {
		return
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Binary) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteBytes([]byte(z))
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Binary) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendBytes(o, []byte(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Binary) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var tmp []byte
		tmp, bts, err = msgp.ReadBytesBytes(bts, []byte((*z)))
		(*z) = Binary(tmp)
	}
	if err != nil {
		return
	}
	o = bts
	return
}

func (z Binary) Msgsize() (s int) {
	s = msgp.BytesPrefixSize + len([]byte(z))
	return
}

// DecodeMsg implements msgp.Decodable
func (z *KV) DecodeMsg(dc *msgp.Reader) (err error) {
	var asz uint32
	asz, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if asz != 2 {
		err = msgp.ArrayError{Wanted: 2, Got: asz}
		return
	}
	for xvk := range z {
		{
			var tmp []byte
			tmp, err = dc.ReadBytes([]byte(z[xvk]))
			z[xvk] = Binary(tmp)
		}
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *KV) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(2)
	if err != nil {
		return
	}
	for xvk := range z {
		err = en.WriteBytes([]byte(z[xvk]))
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *KV) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, 2)
	for xvk := range z {
		o = msgp.AppendBytes(o, []byte(z[xvk]))
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KV) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var asz uint32
	asz, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if asz != 2 {
		err = msgp.ArrayError{Wanted: 2, Got: asz}
		return
	}
	for xvk := range z {
		{
			var tmp []byte
			tmp, bts, err = msgp.ReadBytesBytes(bts, []byte(z[xvk]))
			z[xvk] = Binary(tmp)
		}
		if err != nil {
			return
		}
	}
	o = bts
	return
}

func (z *KV) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for xvk := range z {
		s += msgp.BytesPrefixSize + len([]byte(z[xvk]))
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *KVs) DecodeMsg(dc *msgp.Reader) (err error) {
	var xsz uint32
	xsz, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(KVs, xsz)
	}
	for cmr := range *z {
		var asz uint32
		asz, err = dc.ReadArrayHeader()
		if err != nil {
			return
		}
		if asz != 2 {
			err = msgp.ArrayError{Wanted: 2, Got: asz}
			return
		}
		for ajw := range (*z)[cmr] {
			{
				var tmp []byte
				tmp, err = dc.ReadBytes([]byte((*z)[cmr][ajw]))
				(*z)[cmr][ajw] = Binary(tmp)
			}
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z KVs) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for wht := range z {
		err = en.WriteArrayHeader(2)
		if err != nil {
			return
		}
		for hct := range z[wht] {
			err = en.WriteBytes([]byte(z[wht][hct]))
			if err != nil {
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z KVs) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for wht := range z {
		o = msgp.AppendArrayHeader(o, 2)
		for hct := range z[wht] {
			o = msgp.AppendBytes(o, []byte(z[wht][hct]))
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KVs) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var xsz uint32
	xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(KVs, xsz)
	}
	for cua := range *z {
		var asz uint32
		asz, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			return
		}
		if asz != 2 {
			err = msgp.ArrayError{Wanted: 2, Got: asz}
			return
		}
		for xhx := range (*z)[cua] {
			{
				var tmp []byte
				tmp, bts, err = msgp.ReadBytesBytes(bts, []byte((*z)[cua][xhx]))
				(*z)[cua][xhx] = Binary(tmp)
			}
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

func (z KVs) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for lqf := range z {
		s += msgp.ArrayHeaderSize
		for daf := range z[lqf] {
			s += msgp.BytesPrefixSize + len([]byte(z[lqf][daf]))
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Ks) DecodeMsg(dc *msgp.Reader) (err error) {
	var xsz uint32
	xsz, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(Ks, xsz)
	}
	for jfb := range *z {
		{
			var tmp []byte
			tmp, err = dc.ReadBytes([]byte((*z)[jfb]))
			(*z)[jfb] = Binary(tmp)
		}
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Ks) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for cxo := range z {
		err = en.WriteBytes([]byte(z[cxo]))
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Ks) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for cxo := range z {
		o = msgp.AppendBytes(o, []byte(z[cxo]))
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Ks) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var xsz uint32
	xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(Ks, xsz)
	}
	for eff := range *z {
		{
			var tmp []byte
			tmp, bts, err = msgp.ReadBytesBytes(bts, []byte((*z)[eff]))
			(*z)[eff] = Binary(tmp)
		}
		if err != nil {
			return
		}
	}
	o = bts
	return
}

func (z Ks) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for rsw := range z {
		s += msgp.BytesPrefixSize + len([]byte(z[rsw]))
	}
	return
}
