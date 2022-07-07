package ngprecomputed

import (
	"encoding/json"
	"testing"
)

var sampleInfo = `
{
	"@type": "neuroglancer_multiscale_volume",
	"data_type": "uint8",
	"num_channels": 1,
	"scales": [
	  {
		"chunk_sizes": [
		  [64, 64, 64]
		],
		"encoding": "jpeg",
		"jpeg_quality": 75,
		"key": "4_4_4",
		"resolution": [4.0, 4.0, 4.0],
		"sharding": {
		  "@type": "neuroglancer_uint64_sharded_v1",
		  "data_encoding": "gzip",
		  "hash": "identity",
		  "minishard_bits": 6,
		  "minishard_index_encoding": "gzip",
		  "preshift_bits": 9,
		  "shard_bits": 15
		},
		"size": [11005, 9286, 9504],
		"voxel_offset": [0, 0, 0]
	  },
	  {
		"chunk_sizes": [
		  [64, 64, 64]
		],
		"encoding": "jpeg",
		"jpeg_quality": 75,
		"key": "8_8_8",
		"resolution": [
		  8.0,
		  8.0,
		  8.0
		],
		"sharding": {
		  "@type": "neuroglancer_uint64_sharded_v1",
		  "data_encoding": "gzip",
		  "hash": "identity",
		  "minishard_bits": 6,
		  "minishard_index_encoding": "gzip",
		  "preshift_bits": 9,
		  "shard_bits": 15
		},
		"size": [5502, 4643, 4752],
		"voxel_offset": [0, 0, 0]
	  },
	  {
		"chunk_sizes": [
		[64, 64, 64]
		],
		"encoding": "jpeg",
		"jpeg_quality": 75,
		"key": "16_16_16",
		"resolution": [
		16.0,
		16.0,
		16.0
		],
		"sharding": {
		"@type": "neuroglancer_uint64_sharded_v1",
		"data_encoding": "gzip",
		"hash": "identity",
		"minishard_bits": 6,
		"minishard_index_encoding": "gzip",
		"preshift_bits": 9,
		"shard_bits": 15
		},
		"size": [2751, 2321, 2376],
		"voxel_offset": [0, 0, 0]
	},
	{
		"chunk_sizes": [
		  [64, 64, 64]
		],
		"encoding": "jpeg",
		"jpeg_quality": 75,
		"key": "32_32_32",
		"resolution": [
		  32.0,
		  32.0,
		  32.0
		],
		"sharding": {
		  "@type": "neuroglancer_uint64_sharded_v1",
		  "data_encoding": "gzip",
		  "hash": "identity",
		  "minishard_bits": 6,
		  "minishard_index_encoding": "gzip",
		  "preshift_bits": 9,
		  "shard_bits": 15
		},
		"size": [1375, 1160, 1188],
		"voxel_offset": [0, 0, 0]
	  },
	  {
		  "chunk_sizes": [
			[64, 64, 64]
		  ],
		  "encoding": "jpeg",
		  "jpeg_quality": 75,
		  "key": "64_64_64",
		  "resolution": [
			64.0,
			64.0,
			64.0
		  ],
		  "sharding": {
			"@type": "neuroglancer_uint64_sharded_v1",
			"data_encoding": "gzip",
			"hash": "identity",
			"minishard_bits": 6,
			"minishard_index_encoding": "gzip",
			"preshift_bits": 9,
			"shard_bits": 15
		  },
		  "size": [687, 580, 594],
		  "voxel_offset": [0, 0, 0]
		},
		{
		  "chunk_sizes": [
			[64, 64, 64]
		  ],
		  "encoding": "jpeg",
		  "jpeg_quality": 75,
		  "key": "128_128_128",
		  "resolution": [
			128.0,
			128.0,
			128.0
		  ],
		  "sharding": {
			"@type": "neuroglancer_uint64_sharded_v1",
			"data_encoding": "gzip",
			"hash": "identity",
			"minishard_bits": 6,
			"minishard_index_encoding": "gzip",
			"preshift_bits": 9,
			"shard_bits": 15
		  },
		  "size": [343, 290, 297],
		  "voxel_offset": [0, 0, 0]
		},
		{
		  "chunk_sizes": [
			[64, 64, 64]
		  ],
		  "encoding": "jpeg",
		  "jpeg_quality": 75,
		  "key": "256_256_256",
		  "resolution": [
			256.0,
			256.0,
			256.0
		  ],
		  "sharding": {
			"@type": "neuroglancer_uint64_sharded_v1",
			"data_encoding": "gzip",
			"hash": "identity",
			"minishard_bits": 6,
			"minishard_index_encoding": "gzip",
			"preshift_bits": 9,
			"shard_bits": 15
		  },
		  "size": [171, 145, 148],
		  "voxel_offset": [0, 0, 0]
		},
		{
		  "chunk_sizes": [
			[64, 64, 64]
		  ],
		  "encoding": "jpeg",
		  "jpeg_quality": 75,
		  "key": "512_512_512",
		  "resolution": [
			512.0,
			512.0,
			512.0
		  ],
		  "sharding": {
			"@type": "neuroglancer_uint64_sharded_v1",
			"data_encoding": "gzip",
			"hash": "identity",
			"minishard_bits": 6,
			"minishard_index_encoding": "gzip",
			"preshift_bits": 9,
			"shard_bits": 15
		  },
		  "size": [85, 72, 74],
		  "voxel_offset": [0, 0, 0]
		}
	],
	"type": "image"
  }`

func TestConfig(t *testing.T) {
	var ng ngStore
	if err := json.Unmarshal([]byte(sampleInfo), &(ng.vol)); err != nil {
		t.Fatalf("unable to parse sample ngprecomputed info file: %v\n", err)
	}
	if len(ng.vol.Scales) != 8 {
		t.Fatalf("expected 8 scales got %d instead!\n", len(ng.vol.Scales))
	}
	if ng.vol.Scales[2].Resolution != [3]float64{16.0, 16.0, 16.0} {
		t.Fatalf("expected [4.0, 4.0, 4.0] got %v\n", ng.vol.Scales[2].Resolution)
	}
	if ng.vol.Scales[4].Resolution != [3]float64{64.0, 64.0, 64.0} {
		t.Fatalf("expected [4.0, 4.0, 4.0] got %v\n", ng.vol.Scales[2].Resolution)
	}
}
