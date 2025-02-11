package neuronjson

import (
	"testing"
)

func TestSelectFields(t *testing.T) {
	data := NeuronJSON{
		"bodyid":    123,
		"name":      "neuron1",
		"name_user": "user1",
		"name_time": "2023-01-01T00:00:00Z",
		"type":      "X1",
		"type_user": "user2",
		"type_time": "2023-01-02T00:00:00Z",
	}

	tests := []struct {
		name     string
		fieldMap map[string]struct{}
		showUser bool
		showTime bool
		expected NeuronJSON
	}{
		{
			name:     "Show all fields",
			fieldMap: map[string]struct{}{},
			showUser: true,
			showTime: true,
			expected: data,
		},
		{
			name:     "Show only bodyid",
			fieldMap: map[string]struct{}{"bodyid": {}},
			showUser: true,
			showTime: true,
			expected: NeuronJSON{"bodyid": 123},
		},
		{
			name:     "Show name without user and time",
			fieldMap: map[string]struct{}{"name": {}},
			showUser: false,
			showTime: false,
			expected: NeuronJSON{"bodyid": 123, "name": "neuron1"},
		},
		{
			name:     "Show type with user but not time",
			fieldMap: map[string]struct{}{"type": {}},
			showUser: true,
			showTime: false,
			expected: NeuronJSON{"bodyid": 123, "type": "X1", "type_user": "user2"},
		},
		{
			name:     "Show type_user but not type",
			fieldMap: map[string]struct{}{"type_user": {}},
			showUser: true,
			showTime: false,
			expected: NeuronJSON{"bodyid": 123, "type_user": "user2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selectFields(data, tt.fieldMap, tt.showUser, tt.showTime)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("expected %v for key %s, got %v", v, k, result[k])
				}
			}
		})
	}
}
