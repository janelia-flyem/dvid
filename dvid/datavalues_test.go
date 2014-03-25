package dvid

import (
	_ "testing"
	. "github.com/janelia-flyem/go/gocheck"
)

type DatavalueSuite struct {
	values, values2 DataValues
}

var _ = Suite(&DatavalueSuite{})

func (s *DatavalueSuite) SetUpSuite(c *C) {
	s.values = DataValues{
		{
			T:     T_uint8,
			Label: "some uint8",
		},
		{
			T:     T_int16,
			Label: "spectacular int16",
		},
		{
			T:     T_uint64,
			Label: "a uint64",
		},
		{
			T:     T_float32,
			Label: "one float32",
		},
		{
			T:     T_float64,
			Label: "a float64",
		},
	}
	s.values2 = DataValues{
		{
			T:     T_uint8,
			Label: "some uint8",
		},
		{
			T:     T_uint8,
			Label: "another uint8",
		},
		{
			T:     T_uint8,
			Label: "yet another uint8",
		},
	}
}

func (s *DatavalueSuite) TestFunctions(c *C) {
	c.Assert(s.values.BytesPerElement(), Equals, int32(1+2+8+4+8))

	// Expect error to be returned given DataValues with elements of varying size.
	_, err := s.values.BytesPerValue()
	c.Assert(err, NotNil)

	numBytes, err := s.values2.BytesPerValue()
	c.Assert(err, IsNil)
	c.Assert(numBytes, Equals, int32(1))
	c.Assert(s.values2.BytesPerElement(), Equals, int32(3))

	c.Assert(s.values.ValueBytes(4), Equals, int32(8))
}

func (s *DatavalueSuite) TestMarshaling(c *C) {
	// Check round-trip for first sample DataValues
	b, err := s.values.MarshalBinary()
	c.Assert(err, IsNil)

	var newValues DataValues
	err = (&newValues).UnmarshalBinary(b)
	c.Assert(err, IsNil)
	c.Assert(s.values, DeepEquals, newValues)

	newValues[1].T = T_int8
	c.Assert(s.values, Not(DeepEquals), newValues)

	// Check round-trip for second DataValues
	b, err = s.values2.MarshalBinary()
	c.Assert(err, IsNil)

	var newValues2 DataValues
	err = (&newValues2).UnmarshalBinary(b)
	c.Assert(err, IsNil)
	c.Assert(s.values2, DeepEquals, newValues2)
}
