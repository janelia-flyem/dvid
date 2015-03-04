package tests_integration

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"

	"github.com/janelia-flyem/dvid/datatype/imageblk"
	"github.com/janelia-flyem/dvid/datatype/labelblk"

	// Declare the data types the DVID server should support
	_ "github.com/janelia-flyem/dvid/datatype/keyvalue"
	_ "github.com/janelia-flyem/dvid/datatype/labelgraph"
	_ "github.com/janelia-flyem/dvid/datatype/multichan16"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
)

type sliceTester struct {
	orient string
	width  int32
	height int32
	offset dvid.Point3d
}

func (s sliceTester) apiStr(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/%s/%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, name, s.orient, s.width, s.height, s.offset[0], s.offset[1], s.offset[2])
}

// Returns a value for the DVID voxel coordinate given a slice's orientation and size.
func (s sliceTester) getLabel(t *testing.T, img *dvid.Image, x, y, z int32) uint64 {
	switch s.orient {
	case "xy":
		if z != s.offset[2] || x < s.offset[0] || x >= s.offset[0]+s.width || y < s.offset[1] || y >= s.offset[1]+s.height {
			break
		}
		ix := x - s.offset[0]
		iy := y - s.offset[1]
		data, err := img.DataPtr(ix, iy)
		if err != nil {
			t.Fatalf("Could not get data at (%d,%d): %s\n", ix, iy, err.Error())
		}
		if len(data) != 8 {
			t.Fatalf("Returned labelblk data that is not 8 bytes for a voxel")
		}
		return binary.LittleEndian.Uint64(data)
	default:
		t.Fatalf("Unknown slice orientation %q\n", s.orient)
	}
	t.Fatalf("Attempted to get voxel (%d, %d, %d) not in %d x %d %s slice at offset %s\n",
		x, y, z, s.width, s.height, s.orient, s.offset)
	return 0
}

type tuple [4]int

var labelsROI = []tuple{
	tuple{3, 3, 2, 4}, tuple{3, 4, 2, 3}, tuple{3, 5, 3, 3},
	tuple{4, 3, 2, 5}, tuple{4, 4, 3, 4}, tuple{4, 5, 2, 4},
	//tuple{5, 2, 3, 4}, tuple{5, 3, 3, 3}, tuple{5, 4, 2, 3}, tuple{5, 5, 2, 2},
}

func labelsJSON() string {
	b, err := json.Marshal(labelsROI)
	if err != nil {
		return ""
	}
	return string(b)
}

func inroi(x, y, z int) bool {
	for _, span := range labelsROI {
		if span[0] != z {
			continue
		}
		if span[1] != y {
			continue
		}
		if span[2] > x || span[3] < x {
			continue
		}
		return true
	}
	return false
}

func postLabelVolume(t *testing.T, labelsName string, uuid dvid.UUID) {
	server.CreateTestInstance(t, uuid, "labelblk", labelsName)

	// Post a 3d volume of data that is 10 blocks on a side and straddles block boundaries.
	payload := new(bytes.Buffer)
	var label uint64
	nx := 5
	ny := 5
	nz := 5
	blocksz := 32
	p := make([]byte, 8*blocksz)
	for z := 0; z < nz*blocksz; z++ {
		for y := 0; y < ny*blocksz; y++ {
			for x := 0; x < nx; x++ {
				label++
				for i := 0; i < blocksz; i++ {
					binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], label)
				}
				n, err := payload.Write(p)
				if n != 8*blocksz {
					t.Fatalf("Could not write test data: %d bytes instead of %d\n", n, 8*blocksz)
				}
				if err != nil {
					t.Fatalf("Could not write test data: %s\n", err.Error())
				}
			}
		}
	}
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz)
	server.TestHTTP(t, "POST", apiStr, payload)
}

func TestLabelmap(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := dvid.UUID(server.NewTestRepo(t))
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create and post a test labelblk volume
	labelsName := "mylabels"
	postLabelVolume(t, labelsName, uuid)
}

func TestLabels64(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := dvid.UUID(server.NewTestRepo(t))
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create a labelblk instance
	labelsName := "mylabels"
	postLabelVolume(t, labelsName, uuid)

	// Verify XY slice reads returns what we expect.
	slice := sliceTester{"xy", 200, 200, dvid.Point3d{10, 40, 72}}
	apiStr := slice.apiStr(uuid, labelsName)
	xy := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err := dvid.ImageFromBytes(xy, labelblk.EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on XY labels GET: %s\n", err.Error())
	}
	if format != "png" {
		t.Errorf("Expected XY labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 200*200*8 {
		t.Errorf("Expected %d bytes from XY labelblk GET.  Got %d instead.", 200*200*8, img.NumBytes())
	}
	// -- For z = 72, expect labels to go from 1601 to 2400.
	// -- Make sure corner points have 0 and non-zero labels where appropriate.
	value := slice.getLabel(t, img, 25, 47, 72)
	if value != 0 {
		t.Errorf("Expected 0, got %d\n", value)
	}
	value = slice.getLabel(t, img, 25, 48, 72)
	if value != 1601 {
		t.Errorf("Expected 1601.  Got %d instead.", value)
	}
	value = slice.getLabel(t, img, 25, 208, 72)
	if value != 0 {
		t.Errorf("Expected 0.  Got %d instead.", value)
	}
	value = slice.getLabel(t, img, 176, 100, 72)
	if value != 0 {
		t.Errorf("Expected 0.  Got %d instead.", value)
	}

	// TODO - Verify XZ slice read returns what we expect.

	// TODO - Verify YZ slice read returns what we expect.

	// Verify 3d volume read returns original.
	var label uint64
	nx := 5
	ny := 5
	nz := 5
	blocksz := 32
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz)
	xyz := server.TestHTTP(t, "GET", apiStr, nil)
	if len(xyz) != 160*160*160*8 {
		t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
	}
	label = 0
	j := 0
	for z := 0; z < nz*blocksz; z++ {
		for y := 0; y < ny*blocksz; y++ {
			for x := 0; x < nx; x++ {
				label++
				for i := 0; i < blocksz; i++ {
					gotlabel := binary.LittleEndian.Uint64(xyz[j : j+8])
					if gotlabel != label {
						t.Fatalf("Bad label %d instead of expected %d at (%d,%d,%d)\n",
							gotlabel, label, x*blocksz+i, y, z)
					}
					j += 8
				}
			}
		}
	}

	// Create a new ROI instance.
	roiName := "myroi"
	server.CreateTestInstance(t, uuid, "roi", roiName)

	// Add ROI data
	apiStr = fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, roiName)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(labelsJSON()))

	// Post updated labels without ROI.
	p := make([]byte, 8*blocksz)
	payload := new(bytes.Buffer)
	label = 200000
	for z := 0; z < nz*blocksz; z++ {
		for y := 0; y < ny*blocksz; y++ {
			for x := 0; x < nx; x++ {
				label++
				for i := 0; i < blocksz; i++ {
					binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], label)
				}
				n, err := payload.Write(p)
				if n != 8*blocksz {
					t.Fatalf("Could not write test data: %d bytes instead of %d\n", n, 8*blocksz)
				}
				if err != nil {
					t.Fatalf("Could not write test data: %s\n", err.Error())
				}
			}
		}
	}
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Verify 3d volume read returns modified data.
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz)
	xyz = server.TestHTTP(t, "GET", apiStr, nil)
	if len(xyz) != 160*160*160*8 {
		t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
	}
	label = 200000
	j = 0
	for z := 0; z < nz*blocksz; z++ {
		for y := 0; y < ny*blocksz; y++ {
			for x := 0; x < nx; x++ {
				label++
				for i := 0; i < blocksz; i++ {
					gotlabel := binary.LittleEndian.Uint64(xyz[j : j+8])
					if gotlabel != label {
						t.Fatalf("Bad label %d instead of expected modified %d at (%d,%d,%d)\n",
							gotlabel, label, x*blocksz+i, y, z)
					}
					j += 8
				}
			}
		}
	}

	// TODO - Use the ROI to retrieve a 2d xy image.

	// TODO - Make sure we aren't getting labels back in non-ROI points.

	// Post again but now with ROI
	payload = new(bytes.Buffer)
	label = 400000
	for z := 0; z < nz*blocksz; z++ {
		for y := 0; y < ny*blocksz; y++ {
			for x := 0; x < nx; x++ {
				label++
				for i := 0; i < blocksz; i++ {
					binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], label)
				}
				n, err := payload.Write(p)
				if n != 8*blocksz {
					t.Fatalf("Could not write test data: %d bytes instead of %d\n", n, 8*blocksz)
				}
				if err != nil {
					t.Fatalf("Could not write test data: %s\n", err.Error())
				}
			}
		}
	}
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96?roi=%s", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz, roiName)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Verify ROI masking on GET.
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96?roi=%s", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz, roiName)
	xyz2 := server.TestHTTP(t, "GET", apiStr, nil)
	if len(xyz) != 160*160*160*8 {
		t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
	}
	var newlabel uint64 = 400000
	var oldlabel uint64 = 200000
	j = 0
	offsetx := 16
	offsety := 48
	offsetz := 70
	for z := 0; z < nz*blocksz; z++ {
		voxz := z + offsetz
		blockz := voxz / int(imageblk.DefaultBlockSize)
		for y := 0; y < ny*blocksz; y++ {
			voxy := y + offsety
			blocky := voxy / int(imageblk.DefaultBlockSize)
			for x := 0; x < nx; x++ {
				newlabel++
				oldlabel++
				voxx := x*blocksz + offsetx
				for i := 0; i < blocksz; i++ {
					blockx := voxx / int(imageblk.DefaultBlockSize)
					gotlabel := binary.LittleEndian.Uint64(xyz2[j : j+8])
					if inroi(blockx, blocky, blockz) {
						if gotlabel != newlabel {
							t.Fatalf("Got label %d instead of in-ROI label %d at (%d,%d,%d)\n",
								gotlabel, newlabel, voxx, voxy, voxz)
						}
					} else {
						if gotlabel != 0 {
							t.Fatalf("Got label %d instead of 0 at (%d,%d,%d) outside ROI\n",
								gotlabel, voxx, voxy, voxz)
						}
					}
					j += 8
					voxx++
				}
			}
		}
	}

	// Verify everything in mask is new and everything out of mask is old, and everything in mask
	// is new.
	apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
		uuid, labelsName, nx*blocksz, ny*blocksz, nz*blocksz)
	xyz2 = server.TestHTTP(t, "GET", apiStr, nil)
	if len(xyz) != 160*160*160*8 {
		t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
	}
	newlabel = 400000
	oldlabel = 200000
	j = 0
	for z := 0; z < nz*blocksz; z++ {
		voxz := z + offsetz
		blockz := voxz / int(imageblk.DefaultBlockSize)
		for y := 0; y < ny*blocksz; y++ {
			voxy := y + offsety
			blocky := voxy / int(imageblk.DefaultBlockSize)
			for x := 0; x < nx; x++ {
				newlabel++
				oldlabel++
				voxx := x*blocksz + offsetx
				for i := 0; i < blocksz; i++ {
					blockx := voxx / int(imageblk.DefaultBlockSize)
					gotlabel := binary.LittleEndian.Uint64(xyz2[j : j+8])
					if inroi(blockx, blocky, blockz) {
						if gotlabel != newlabel {
							t.Fatalf("Got label %d instead of in-ROI label %d at (%d,%d,%d)\n",
								gotlabel, newlabel, voxx, voxy, voxz)
						}
					} else {
						if gotlabel != oldlabel {
							t.Fatalf("Got label %d instead of label %d at (%d,%d,%d) outside ROI\n",
								gotlabel, oldlabel, voxx, voxy, voxz)
						}
					}
					j += 8
					voxx++
				}
			}
		}
	}
}
