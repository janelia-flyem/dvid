package labels64

import "testing"

func TestAddMerge(t *testing.T) {
	tuples1 := MergeTuples{
		{20, 3, 5, 7},
		{30, 1, 6, 13, 19},
		{40, 2, 18},
	}
	/*
		tuples2 := MergeTuples{
			{20, 12, 14},
			{40, 8, 33},
		}
	*/
	tuples1.addMerge(98, 20)
	if len(tuples1[0]) != 5 {
		t.Errorf("Expected MergeTuples.addMerge() to add: %v\n", tuples1)
	}
}
